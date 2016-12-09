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
        {unauth, Req1} ->
            {ok, Req2} = cowboy_req:reply(401, Req1),
            {halt, Req2};
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
            {unauth, Req1}
    end;
assert(require_collection_access, Req) ->
    {OrgId, Req1} = cowboy_req:binding(collection, Req),
    {UserId, Req2} = cowboy_req:meta(dl_auth_user, Req1),
    case cowboy_req:meta(dl_auth_is_authenticated, Req) of
        {allow, Req2} ->
            {ok, Access} = dalmatiner_dl_data:user_org_access(UserId, OrgId),
            {Access, Req2};
        {_, Req1} ->
            {unauth, Req2}
    end;
assert(require_query_collection_access, Req) ->
    case assert(require_authenticated, Req) of
        {allow, Req1} ->
            assert_query(Req1);
        R ->
            R
    end.

assert_query(Req) ->
    {Q, Req1} =  cowboy_req:qs_val(<<"q">>, Req),
    case prepare_query(Q) of
        {ok, Parts} ->
            {UserId, Req2} = cowboy_req:meta(dl_auth_user, Req1),
            {Orgs, Req3} = user_scope_orgs(UserId, Req2),
            OrgOidMap = lists:foldl(fun (O, Acc) ->
                                            Acc#{O => allow}
                                    end, #{}, Orgs),
            Access = check_query_all_parts_access(Parts, OrgOidMap),
            {Access, Req3};
        {error, {badmatch, _}} ->
            %% In case of errors in query parsing, we want to let request
            %% throug, so response rendering gets oportunity to explain
            %% errors in query syntax
            {allow, Req1};
        {error, E} ->
            S = erlang:get_stacktrace(),
            lager:error("Error in query validation [~s]: ~p~n~p", [Q, E, S]),
            {error, 500, "Internal Server Error", Req1}
    end.

prepare_query(Q) ->
    S = binary_to_list(Q),
    try
        {ok, L, _} = dql_lexer:string(S),
        {ok, {select, Qs, Aliases, _T, _Limit}} = dql_parser:parse(L),
        {ok, Qs1} = dql_alias:expand(Qs, Aliases),
        {ok, Qs1}
    catch
        error:Reason ->
            {error, Reason}
    end.

check_query_all_parts_access([], _) ->
    allow;
check_query_all_parts_access([Part | Rest], OrgOidMap) ->
    case check_query_part_access(Part, OrgOidMap) of
        allow -> check_query_all_parts_access(Rest, OrgOidMap);
        Access -> Access
    end.

%% Go though each named subject or timeshift
check_query_part_access(#{op := Op,
                          args := [_Name, Nested]},
                        OrgOidMap) when Op =:= named orelse Op =:= timeshift ->
    check_query_part_access(Nested, OrgOidMap);
%% Check all function call arguments
check_query_part_access(#{op := fcall,
                          args := #{inputs := Parts}},
                        OrgOidMap) ->
    check_query_all_parts_access(Parts, OrgOidMap);
%% ,but skip all constant arguments
check_query_part_access(#{op := time}, _) ->
    allow;
check_query_part_access(Const, _) when is_number(Const) ->
    allow;
%% If argument is a lookup operation, we can check access directly by comparing
%% requested collection
check_query_part_access(#{op := lookup,
                          args := [Collection, _Met | _Optional]},
                        OrgOidMap) ->
    Oid = {base16:decode(Collection)},
    maps:get(Oid, OrgOidMap, deny);

%% If argument is old plain service, we need to figgure out if the finger
%% within query scope is allowed. Right now bucked is used only for grouping
%% and finger is first segment of metric. Glob is just a special case of metric
%% selector. We don't allow for first level globbing anyway.
check_query_part_access(#{op := Op,
                          args := [_Bucket, Metric]},
                        OrgOidMap) when Op =:= get orelse Op =:= sget ->
    Finger = hd(Metric),
    OrgOids = maps:keys(OrgOidMap),
    {ok, Access} = dalmatiner_dl_data:agent_access(Finger, OrgOids),
    Access.

user_scope_orgs(<<"guest">>, Req) ->
    {Orgs, Req2} = cowboy_req:meta(dl_auth_orgs, Req),
    OrgIds = [{base16:decode(Hex)} || Hex <- Orgs],
    {OrgIds, Req2};
user_scope_orgs(UserId, Req) ->
    {ok, Objs} = dalmatiner_dl_data:user_orgs(UserId),
    OrgIds = [maps:get(<<"_id">>, O) || O <- Objs],
    {OrgIds, Req}.
