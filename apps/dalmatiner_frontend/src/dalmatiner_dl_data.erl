-module(dalmatiner_dl_data).

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API
-export([start_link/1]).
-export([user_orgs/1, token/1, user_org_access/2, agent_access/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(POOL, dalmatiner_dl_data_pool).
-define(TIMEOUT, 30000).
-record(state, {connection}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec user_orgs(binary()) -> {ok, [#{}]}.
user_orgs(UserId) ->
    call({user_orgs, UserId}).

-spec token(binary()) -> {ok, #{}}.
token(TokenId) ->
    call({token, TokenId}).

-spec user_org_access(binary(), binary()) -> {ok, allow | deny}.
user_org_access(UserId, OrgId) ->
    call({user_org_access, UserId, OrgId}).

-spec agent_access(binary(), [{<<_:96>>}]) -> {ok, allow | deny}.
agent_access(Finger, OrgOids) ->
    call({agent_access, Finger, OrgOids}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

call(Request) ->
    poolboy:transaction(?POOL,
                        fun(S) ->
                                gen_server:call(S, Request, ?TIMEOUT)
                        end, ?TIMEOUT).

init(ConnectionArgs) ->
    {ok, C} = mc_worker_api:connect(ConnectionArgs),
    {ok, #state{connection = C}, 0}.

handle_call({user_orgs, UserId}, _From, #state{connection = C} = State) ->
    T0 = erlang:system_time(),
    Orgs = find_user_orgs(C, UserId),
    lager:debug("[dalmatiner_dl_data:user_orgs] It took ~wms", [tdelta(T0)]),
    {reply, {ok, Orgs}, State};
handle_call({token, TokenId}, _From, #state{connection = C} = State) ->
    T0 = erlang:system_time(),
    Token = find_token(C, TokenId),
    lager:debug("[dalmatiner_dl_data:token] It took ~wms", [tdelta(T0)]),
    {reply, {ok, Token}, State};
handle_call({user_org_access, UserId, OrgId}, _From,
            #state{connection = C} = State) ->
    T0 = erlang:system_time(),
    Access = check_user_org_access(C, UserId, OrgId),
    lager:debug("[dalmatiner_dl_data:user_org_access] It took ~wms",
                [tdelta(T0)]),
    {reply, {ok, Access}, State};
handle_call({agent_access, Finger, OrgOids}, _From,
            #state{connection = C} = State) ->
    T0 = erlang:system_time(),
    Access = check_agent_access(C, Finger, OrgOids),
    lager:debug("[dalmatiner_dl_data:agent_access] It took ~wms", [tdelta(T0)]),
    {reply, {ok, Access}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State ) ->
    {noreply, State}.

terminate(_Reason, #state{connection = C}) ->
    _ = mc_worker_api:disconnect(C),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

tdelta(T0) ->
    (erlang:system_time() - T0)/1000/1000.

find_user_orgs(C, UserId) ->
    Groups = find_user_groups(C, UserId),
    Orgs = find_orgs_by_group_in(C, Groups),
    populate_each_org_tenant(C, Orgs).

find_user_groups(C, UserId) ->
    UserOid = {base16:decode(UserId)},
    find(C, <<"groups">>,
         #{<<"users.user">> => UserOid},
         #{projector =>
               {<<"_id">>, true,
                <<"name">>, true}}).

find_orgs_by_group_in(C, Groups) ->
    Fn = fun(#{<<"name">> := <<"org:", _/binary>>,
               <<"_id">> := Gid}, {OAcc, TAcc}) ->
                 {[Gid | OAcc], TAcc};
            (#{<<"name">> := <<"ten:", _/binary>>,
               <<"_id">> := Gid}, {OAcc, TAcc}) ->
                 {OAcc, [Gid | TAcc]}
         end,
    {OrgGids, TenantGids} = lists:foldl(Fn, {[], []}, Groups),
    OrgProjector = {<<"name">>, true, <<"tenant">>, true},
    find_org_by_group_direct(C, OrgGids, OrgProjector) ++
        find_org_by_group_tenancy(C, TenantGids, OrgProjector).

find_org_by_group_direct(_C, [], _) ->
    [];
find_org_by_group_direct(C, Gids, OrgProjector) ->
    find(C, <<"orgs">>,
         #{<<"group">> => {<<"$in">>, Gids}},
         #{projector => OrgProjector}).

find_org_by_group_tenancy(_C, [], _) ->
    [];
find_org_by_group_tenancy(C, Gids, OrgProjector) ->
    Tenants = find(C, <<"tenants">>,
                   #{<<"group">> => {<<"$in">>, Gids}},
                   #{projector => {<<"_id">>, true}}),
    Tids = pick(<<"_id">>, Tenants),
    find(C, <<"orgs">>,
         #{<<"tenant">> => {<<"$in">>, Tids}},
         #{projector => OrgProjector}).

populate_each_org_tenant(C, Orgs) ->
    Uids = pick(<<"tenant">>, Orgs),
    Cursor = mc_worker_api:find(C, <<"tenants">>,
                                #{<<"_id">> => {<<"$in">>, Uids}},
                                #{projector => {<<"name">>, true}}),
    TMap = mc_cursor:foldl(fun (#{<<"_id">> := Uid} = T, Acc) ->
                                   Acc#{Uid => T}
                           end, #{}, Cursor, infinity),
    mc_cursor:close(Cursor),
    Orgs2 = [O#{<<"tenant">> => maps:get(T, TMap)} ||
                #{<<"tenant">> := T} = O <- Orgs],
    Orgs2.

find_token(C, TokenId) ->
    TokenOid = {base16:decode(TokenId)},
    mc_worker_api:find_one(C, <<"usertokens">>, #{<<"_id">> => TokenOid}).

check_user_org_access(C, UserId, OrgId) ->
    Groups = find_user_groups(C, UserId),
    OrgOid = {base16:decode(OrgId)},
    Org = mc_worker_api:find_one(C, <<"orgs">>, #{<<"_id">> => OrgOid},
                                 #{projector => {
                                     <<"group">>, true,
                                     <<"tenant">>, true}}),
    #{<<"tenant">> := Tenant, <<"group">> := Group} = Org,
    case includes(<<"_id">>, Group, Groups) of
        true -> allow;
        _ -> check_user_tenant_access(C, Groups, Tenant)
    end.

check_user_tenant_access(C, UserGroups, TenantOid) ->
    Tenant = mc_worker_api:find_one(C, <<"tenants">>, #{<<"_id">> => TenantOid},
                                    #{projector => {<<"group">>, true}}),
    #{<<"group">> := Group} = Tenant,
    case includes(<<"_id">>, Group, UserGroups) of
        true -> allow;
        false -> deny
    end.

check_agent_access(C, Finger, OrgOids) ->
    Agent = mc_worker_api:find_one(C, <<"agents">>,
                                   #{<<"_id">> => Finger,
                                     <<"org">> => {<<"$in">>, OrgOids}},
                                   #{projector => {<<"_id">>, true}}),
    case Agent of
        #{<<"_id">> := _} ->
            allow;
        _ ->
            deny
    end.

find(C, Coll, Selector, Args) ->
    Cursor = mc_worker_api:find(C, Coll, Selector, Args),
    Docs = mc_cursor:foldl(fun (Doc, Acc) ->
                                   [Doc | Acc]
                           end, [], Cursor, infinity),
    mc_cursor:close(Cursor),
    Docs.

pick(Key, List) ->
    [V || #{Key := V} <- List].

includes(_K, _V, []) ->
    false;
includes(K, V, [M | Rest]) ->
    case M of
        #{K := V} -> true;
        _ -> includes(K, V, Rest)
    end.
