-module(dalmatiner_dl_authn_m).
-behaviour(cowboy_middleware).

-export([execute/2]).

%%
%% Authentycation middleware entry point
%% =====================================

-spec execute(cowboy_req:req(), [{atom(), any()}]) ->
                     {ok, cowboy_req:req(), [{atom(), any()}]} |
                     {halt, cowboy_req:req()}.
execute(Req, Env) ->
    %% Cowboy requires middleware to wrap it's own exceptions. Otherwise
    %% you get empty response with very little information about what happened.
    try authenticate(Req) of
        {ok, Req1} ->
            {ok, Req1, Env};
        {halt, Req1} ->
            {halt, Req1}
    catch
        Exception:Reason ->
            Stack = erlang:get_stacktrace(),
            lager:error("Error in authntication middleware (~p:~p): ~p",
                        [Exception, Reason, Stack]),
            {error, 500, Req}
    end.


%%
%% Private functions
%% =================

authenticate(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        {undefined, Req1} ->
            %% If there is no authorization header, then continoue anonymously
            {Param, Req2} = cowboy_req:qs_val(<<"token">>, Req1),
            authenticate_param(Param, Req2);
        {Auth, Req1} ->
            [RawType | Rest] = binary:split(Auth, <<" ">>),
            Type = string:to_lower(binary_to_list(RawType)),
            authenticate_header([Type | Rest], Req1)
    end.

authenticate_param(undefined, Req) ->
    {ok, Req};
authenticate_param(Token, Req) ->
    authenticate_token(Token, Req).

authenticate_header(["bearer", Token], Req) ->
    authenticate_token(Token, Req);
authenticate_header(_Parts, Req) ->
    {ok, Req}.

authenticate_token(Token, Req) ->
    {ok, Key} = application:get_env(dalmatiner_frontend, jwt_secret),
    case ejwt:decode(Token, Key) of
        error ->
            {ok, Req1} = cowboy_req:reply(403, Req),
            {halt, Req1};
        Payload ->
            Req1 = populate_req_meta(Payload, Req),
            {ok, Req1}
    end.

populate_req_meta([], Req) ->
    Req;
populate_req_meta([{<<"scopes">>, S} | Rest], Req) ->
    Req1 = cowboy_req:set_meta(dl_auth_scopes, S, Req),
    populate_req_meta(Rest, Req1);
populate_req_meta([{<<"orgs">>, S} | Rest], Req) ->
    Req1 = cowboy_req:set_meta(dl_auth_orgs, S, Req),
    populate_req_meta(Rest, Req1);
populate_req_meta([{<<"aud">>, U} | Rest], Req) ->
    Req1 = cowboy_req:set_meta(dl_auth_user, U, Req),
    populate_req_meta(Rest, Req1);
%% SSO generated tokens have expiration, that need to be checked before
%% request is authenticated
populate_req_meta([{<<"exp">>, E} | Rest], Req) ->
    IsAuth = erlang:system_time(seconds) =< E,
    Req1 = cowboy_req:set_meta(dl_auth_is_authenticated, IsAuth, Req),
    populate_req_meta(Rest, Req1);
%% Application generated tokens don't have expiration, but require check
%% in mongo, because they may be rovoked.
populate_req_meta([{<<"jti">>, TokenId} | Rest], Req) ->
    case dalmatiner_dl_data:token(TokenId) of
        {ok, #{} = T} when map_size(T) == 0 ->
            populate_req_meta(Rest, Req);
        {ok, #{<<"scopes">> := Scopes}} ->
            IsAuth = has_org_scope(Scopes),
            Req1 = cowboy_req:set_meta(dl_auth_is_authenticated, IsAuth, Req),
            populate_req_meta(Rest, Req1)
    end;
populate_req_meta([_ | Rest], Req) ->
    populate_req_meta(Rest, Req).

has_org_scope([]) ->
    false;
has_org_scope([<<"org:", _/binary>> | _]) ->
    true;
has_org_scope([_OtherScope | Rest]) ->
    has_org_scope(Rest).
