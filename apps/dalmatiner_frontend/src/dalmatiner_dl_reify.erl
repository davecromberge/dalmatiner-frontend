-module(dalmatiner_dl_reify).

-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).
-export([allowed_methods/2, content_types_accepted/2]).

-ifdef(TEST).
-export([reify/1]).
-endif.

-include("dalmatiner_dl_query_model.hrl").

-ignore_xref([init/3, handle/2, terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
      {<<"application/json">>, foo}
     ], Req, State}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    Req0 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>,
                                      <<"*">>, Req),
    case cowboy_req:has_body(Req0) of
        true ->
            {ok, Body, Req1} = cowboy_req:body(Req0),
            handle_json(Body, Req1, State);
        false ->
            {ok, Req1} = cowboy_req:reply(
                           400,
                           [{<<"content-type">>, <<"text/plain">>}],
                           "Missing body contents",
                           Req0),
            {ok, Req1, State}
    end.

terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

handle_json(Payload, Req, State) ->
    try
        Model = jsone:decode(Payload, [{keys, atom}]),
        Reified = reify(Model),
        {ok, Req1} = cowboy_req:reply(
                          200,
                          [{<<"content-type">>, <<"text/plain">>}],
                          Reified,
                          Req),
        {ok, Req1, State}
    catch
        ErrorType:Reason ->
            Stack = erlang:get_stacktrace(),
            lager:error("Error in dalmatiner_dl_reify: ~p~n",
                        [{ErrorType, Reason, Stack}]),
            {ok, ReqN} = cowboy_req:reply(
                          400,
                          [{<<"content-type">>, <<"text/plain">>}],
                          "Could not decode request content",
                          Req),
            {ok, ReqN, State}
    end.

-spec reify(query() | part() | selector() | fn() | any()) -> binary().
reify(D = #{ parts := Parts }) ->
    <<"SELECT ", (reify(Parts))/binary, " ", (reify_timeframe(D))/binary>>;

reify(Ls) when is_list(Ls) ->
    Qs = [reify(Q) || Q <- Ls],
    combine(Qs);

%% -spec reify(query()) -> binary().
reify(#{ alias := #{ label := Label, subject := Subject }}) ->
    <<(reify(Subject))/binary, " AS ", Label/binary>>;
reify(#{ fn := F }) when F =/= undefined ->
    reify(F);
reify(#{ selector := S }) ->
    reify(S);

%% -spec reify(fn()) -> binary().
reify(#{ name := Name, args := Args }) ->
    Qs = reify(Args),
    <<Name/binary, "(", Qs/binary, ")">>;

%% -spec reify(selector()) -> binary().
reify(#{ bucket := B, metric := M, condition := Cond }) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " BUCKET '", B/binary, "'", Where/binary>>;

reify(#{ bucket := B, metric := M }) ->
    <<(reify_metric(M))/binary, " BUCKET '", B/binary, "'">>;

reify(#{ collection := C, metric := M, condition := Cond }) ->
    Where = reify_where(Cond),
    <<(reify_metric(M))/binary, " FROM '", C/binary, "' WHERE ", Where/binary>>;

reify(#{ collection := C, metric := M }) ->
    <<(reify_metric(M))/binary, " FROM '", C/binary, "'">>;

reify(N) when is_integer(N) ->
    <<(integer_to_binary(N))/binary>>;
reify(N) when is_float(N) ->
    <<(float_to_binary(N))/binary>>;
reify(now) ->
    <<"NOW">>;
reify(N) ->
    N.

-spec reify_metric(['*' | binary()]) -> binary().
reify_metric([M = <<"ALL">>]) ->
    M;
reify_metric(Ms) when is_list(Ms) ->
    <<".", Result/binary>> = reify_metric(Ms, <<>>),
    Result.
-spec reify_metric(['*' | binary()], binary()) -> binary().
reify_metric(['*' | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".*">>);
reify_metric([Metric | R], Acc) ->
    reify_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
reify_metric([], Acc) ->
    Acc.

-spec reify_tag([binary(), ...]) -> <<_:16, _:_*8>>.
reify_tag([<<>>, K]) ->
    <<"'", K/binary, "'">>;
reify_tag([N, K]) ->
    <<"'", N/binary, "':'", K/binary, "'">>.

%% -spec reify_where(condition()) -> binary().
reify_where(#{ op := Op, args := Args }) when is_binary(Op) ->
    reify_where(#{ op => binary_to_atom(Op, utf8), args => Args });
reify_where(#{ op := 'eq', args := [T, V] }) ->
    <<(reify_tag(T))/binary, " = '", V/binary, "'">>;
reify_where(#{ op := 'neq', args := [T, V] }) ->
    <<(reify_tag(T))/binary, " != '", V/binary, "'">>;
reify_where(#{ op := 'or', args := [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " OR (", P2/binary, ")">>;
reify_where(#{ op := 'and', args := [Clause1, Clause2] }) ->
    P1 = reify_where(Clause1),
    P2 = reify_where(Clause2),
    <<P1/binary, " AND (", P2/binary, ")">>.

-spec reify_timeframe(query()) -> binary().
reify_timeframe(#{ m := rel, beginning := B, ending := E }) ->
    <<"BETWEEN ", (reify(B))/binary, " AGO AND ", (reify(E))/binary>>;
reify_timeframe(#{ beginning := B, ending := E }) ->
    <<"BETWEEN ", (reify(B))/binary, " AND ", (reify(E))/binary>>;
reify_timeframe(#{ m := rel, beginning := B, duration := D }) ->
    <<"BEFORE ", (reify(B))/binary, " AGO FOR ", (reify(D))/binary>>;
reify_timeframe(#{ beginning := B, duration := D }) ->
    <<"AFTER ", (reify(B))/binary, " FOR ", (reify(D))/binary>>;
reify_timeframe(#{ ending := E, duration := D }) ->
    <<"BEFORE ", (reify(E))/binary, " FOR ", (reify(D))/binary>>;
reify_timeframe(#{ duration := D }) ->
    <<"LAST ", (reify(D))/binary>>.

-spec combine(list(binary())) -> binary().
combine(L) ->
    combine(L, <<>>).

-spec combine(list(binary()), binary()) -> binary().
combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).
