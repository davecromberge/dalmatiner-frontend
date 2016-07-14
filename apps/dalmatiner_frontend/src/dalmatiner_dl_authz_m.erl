-module(dalmatiner_dl_authz_m).
-behaviour(cowboy_middleware).

-export([execute/2]).

%%
%% Authentycation middleware entry point
%% =====================================

-spec execute(cowboy_req:req(), [{atom(), any()}]) ->
                     {ok, cowboy_req:req(), [{atom(), any()}]} |
                     {halt, cowboy_req:req()}.
execute(Req, Env) ->
    try authorize(Req, Env) of
        {ok, Req1} ->
            {ok, Req1, Env};
        {halt, Req1} ->
            {halt, Req1}
    catch
        Exception:Reason ->
            Stack = erlang:get_stacktrace(),
            lager:error("Error in authorization hook (~p:~p): ~p",
                        [Exception, Reason, Stack]),
            {error, 500, Req}
    end.

%%
%% Private functions
%% =================

authorize(Req, Env) ->
    Acl = proplists:get_value(acl, Env),
    case eval_acl(Acl, Req) of
        {allow, Req1} ->
            {ok, Req1};
        {deny, Req1} ->
            {ok, Req2} = cowboy_req:reply(403, Req1),
            {halt, Req2};
        {error, Status, Error, Req1} ->
            {ok, Req2} = cowboy_req:reply(Status, [], Error, Req1),
            {halt, Req2}
    end.

eval_acl([], Req) ->
    {allow, Req};
eval_acl([{Pattern, Perm} | Rest], Req) ->
    case match(Pattern, Req) of
        {true, Req1} ->
            assert(Perm, Req1);
        {_, Req1} ->
            eval_acl(Rest, Req1)
    end.

match([], Req) ->
    {true, Req};
match([Cond, Rest], Req) ->
    case match(Cond, Req) of
        {true, Req1} ->
            match(Rest, Req1);
        R ->
            R
    end;
match({path, Pattern}, Req) ->
    {Path, Req1} = cowboy_req:path(Req),
    case match_binary(Path, Pattern) of
        true ->
            {true, Req1};
        _ ->
            {false, Req1}
    end;
match({param, Name}, Req) ->
    case cowboy_req:qs_val(Name, Req) of
        {undefined, Req1} ->
            {false, Req1};
        {_, Req1} ->
            {true, Req1}
    end.

match_binary(Subject, Pattern) when size(Pattern) =< size(Subject) ->
    PSize = size(Pattern),
    case <<Subject:PSize/binary>> of
        Pattern -> true;
        _ -> false
    end;
match_binary(_, _) ->
    false.

assert(require_authenticated, Req) ->
    case cowboy_req:meta(dl_auth_is_authenticated, Req) of
        {true, Req1} ->
            {allow, Req1};
        {_, Req1} ->
            {deny, Req1}
    end;
assert(require_collection_access, Req) ->
    {OrgId, Req1} = cowboy_req:binding(collection, Req),
    {UserId, Req2} = cowboy_req:meta(dl_auth_user, Req1),
    case cowboy_req:meta(dl_auth_is_authenticated, Req) of
        {true, Req2} ->
            {ok, Access} = dalmatiner_dl_data:user_org_access(UserId, OrgId),
            {Access, Req2};
        {_, Req1} ->
            {deny, Req2}
    end;
assert(require_query_collection_access, Req) ->
    {Q, Req1} =  cowboy_req:qs_val(<<"q">>, Req),
    case dql:prepare(Q) of
        {ok, {Parts, _Start, _Count, _Res, Aliases, _Buckets}} ->
            {UserId, Req2} = cowboy_req:meta(dl_auth_user, Req1),
            {ok, Orgs} = dalmatiner_dl_data:user_orgs(UserId),
            OrgOidMap = lists:foldl(fun (O, Acc) ->
                                            K = maps:get(<<"_id">>, O),
                                            Acc#{K => allow}
                                   end, #{}, Orgs),
            AList = gb_trees:values(Aliases),
            Access = check_query_access(Parts ++ AList, OrgOidMap),
            {Access, Req2};
        {error, E} ->
            Error = list_to_binary(dqe:error_string({error, E})),
            lager:warning("Error in query validation [~s]: ~p", [Q, E]),
            {error, 400, Error, Req1}
    end.


check_query_access([], _) ->
    allow;
check_query_access([Part | Rest], OrgOidMap) ->
    case check_query_part_access(Part, OrgOidMap) of
        allow -> check_query_access(Rest, OrgOidMap);
        Access -> Access
    end.

check_query_part_access({named, _N, Nested}, OrgOidMap) ->
    check_query_part_access(Nested, OrgOidMap);
check_query_part_access({calc, _Chain, Selector}, OrgOidMap) ->
    check_query_part_access(Selector, OrgOidMap);
check_query_part_access({combine, _Operation, Parts}, OrgOidMap) ->
    check_query_access(Parts, OrgOidMap);
% Always allow access to variables, because they will be checked in aliases part
check_query_part_access({var, _Name}, _OrgOidMap) ->
    allow;
% Right now access by bucket is groupped and finger is first segment of metric
check_query_part_access({get, {_Bucket, Metric}}, OrgOidMap) ->
    Finger = hd(Metric),
    OrgOids = maps:keys(OrgOidMap),
    {ok, Access} = dalmatiner_dl_data:agent_access(Finger, OrgOids),
    Access;
% We do the same with globs
check_query_part_access({sget, {Bucket, Glob}}, OrgOidMap) ->
    check_query_part_access({get, {Bucket, Glob}}, OrgOidMap);
% Mapping by colleciton already uses collection as org id
check_query_part_access({lookup, {in, Collection, _Met}}, OrgOidMap) ->
    Oid = {base16:decode(Collection)},
    maps:get(Oid, OrgOidMap, deny);
check_query_part_access({lookup, {in, Collection, _Met, _Where}}, OrgOidMap) ->
    Oid = {base16:decode(Collection)},
    maps:get(Oid, OrgOidMap, deny).
