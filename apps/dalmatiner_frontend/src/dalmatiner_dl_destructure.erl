-module(dalmatiner_dl_destructure).

-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

-ifdef(TEST).
-export([destructure/1]).
-endif.

-include("dalmatiner_dl_query_model.hrl").

-ignore_xref([init/3, handle/2, terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

-dialyzer({no_opaque, handle/2}).
handle(Req, State) ->
    Req0 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>,
                                      <<"*">>, Req),
    case cowboy_req:qs_val(<<"q">>, Req0) of
        {undefined, Req1} ->
            {ok, Req2} = cowboy_req:reply(
                           400,
                           [{<<"content-type">>, <<"text/plain">>}],
                           "Missing required q= parameter",
                           Req1),
            {ok, Req2, State};
        {Q, Req1} ->
            {ok, Query} = destructure(Q),
            {CType, Req2} = dalmatiner_idx_handler:content_type(Req1),
            dalmatiner_idx_handler:send(CType, Query, Req2, State)
    end.

terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

-spec destructure(binary()) -> {ok, query()} | badmatch | term().
destructure(Query) when is_binary(Query) ->
    case expand_query(Query) of
        {ok, ExpandedQ} ->
            lager:info("Expanded query: ~p~n", [ExpandedQ]),
            R = destructure_(ExpandedQ),
            {ok, R};
        {error, {badmatch, _}} ->
            badmatch;
        {error, E} ->
            E
    end.

-spec expand_query(binary()) -> {ok, map()} | {error, term()}.
expand_query(Q) ->
    S = binary_to_list(Q),
    try
        {ok, L, _} = dql_lexer:string(S),
        dql_parser:parse(L)
    catch
        error:Reason ->
            {error, Reason}
    end.

destructure_parts(Parts) when is_list(Parts) ->
    [begin
         D = destructure_(P),
         #{ selector => extract_selector(D),
            alias => extract_alias(D),
            fn => extract_fn(D) }
     end || P <- Parts].

%% -spec destructure_(map()) -> query().
destructure_({ select, Q, [], T}) ->
    Parts = destructure_parts(Q),
    Query = timeframe(T),
    Query#{ parts => Parts };

%% -spec destructure_(map()) -> selector().
destructure_(#{ op := get, args := [B, M] }) ->
    #{ bucket => B, metric => M };
destructure_(#{ op := sget, args := [B, M] }) ->
    #{ bucket => B, metric => M };
destructure_(#{ op := lookup, args := [B, undefined] }) ->
    #{ collection => B, metric => [<<"ALL">>] };
destructure_(#{ op := lookup, args := [B, undefined, Where] }) ->
    Condition = destructure_where(Where),
    #{ collection => B, metric => [<<"ALL">>], condition => Condition };
destructure_(#{ op := lookup, args := [B, M] }) ->
    #{ collection => B, metric => M };
destructure_(#{ op := lookup, args := [B, M, Where] }) ->
    Condition = destructure_where(Where),
    #{ collection => B, metric => M, condition => Condition };

%% -spec destructure_(map()) -> fn().
destructure_(#{ op := fcall,
               args := #{name := Name, inputs := Args} }) ->
    #{ name => Name, args => destructure_(Args) };

%% -spec destructure_(map()) -> alias().
destructure_(#{op := named, args := [L, Q]}) when is_list(L) ->
    N = destructure_named(L),
    Qs = destructure_(Q),
    #{ label => N, subject => Qs };

destructure_(L) when is_list(L) ->
    [ destructure_(Q) || Q <- L];

destructure_(N) when is_integer(N) ->
    N;
destructure_(N) when is_float(N) ->
    N;
destructure_({time, T, U}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
destructure_(#{op := time, args := [T, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(T))/binary, " ", Us/binary>>;
destructure_(now) ->
    now.

-spec timeframe(map()) -> timeframe().
timeframe(#{ op := 'last', args := [Q] }) ->
    #{ m => abs, duration => destructure_(Q) };
timeframe(#{ op := 'between', args := [#{ op := 'ago', args := [T] }, B] }) ->
    #{ m => rel, beginning => destructure_(T), ending => destructure_(B) };
timeframe(#{ op := 'between', args := [A, B] }) ->
    #{ m => abs, beginning => destructure_(A), ending => destructure_(B) };
timeframe(#{ op := 'after', args := [A, B] }) ->
    #{ m => abs, beginning => destructure_(A), duration => destructure_(B) };
timeframe(#{ op := 'before', args := [#{ op := 'ago', args := [T] }, B] }) ->
    #{ m => rel, beginning => destructure_(T), duration => destructure_(B) };
timeframe(#{ op := 'before', args := [A, B] }) ->
    #{ m => abs, ending => destructure_(A), duration => destructure_(B) };
timeframe(#{ op := 'ago', args := [T] }) ->
    #{ m => rel, beginning => destructure_(T) }.

-spec deconstruct_tag({tag, binary(), binary()}) -> list(binary()).
deconstruct_tag({tag, <<>>, K}) ->
    [<<>>, K];
deconstruct_tag({tag, N, K}) ->
    [N, K].

destructure_where({'=', T, V}) ->
    #{ op => 'eq', args => [deconstruct_tag(T), V] };
destructure_where({'!=', T, V}) ->
    #{ op => 'neq', args => [deconstruct_tag(T), V] };
destructure_where({'or', Clause1, Clause2}) ->
    P1 = destructure_where(Clause1),
    P2 = destructure_where(Clause2),
    #{ op => 'or', args => [ P1, P2 ] };
destructure_where({'and', Clause1, Clause2}) ->
    P1 = destructure_where(Clause1),
    P2 = destructure_where(Clause2),
    #{ op => 'and', args => [ P1, P2 ] }.

destructure_named(Ms) ->
    Ms1 = [destructure_name(E) || E <- Ms],
    <<".", Result/binary>> = destructure_named(Ms1, <<>>),
    Result.
destructure_named([Named | R], Acc) ->
    destructure_named(R, <<Acc/binary, ".", Named/binary, "">>);
destructure_named([], Acc) ->
    Acc.

destructure_name(B) when is_binary(B) ->
    <<"'", B/binary, "'">>;
destructure_name({pvar, I}) ->
    <<"$", (integer_to_binary(I))/binary>>;
destructure_name({dvar, {<<>>, K}}) ->
    <<"$'", K/binary, "'">>;
destructure_name({dvar, {Ns, K}}) ->
    <<"$'", Ns/binary, "':'", K/binary, "'">>.

-spec extract_selector(alias() | fn() | selector()) -> selector().
extract_selector(S = #{ metric := _Metric }) ->
    S;
extract_selector(#{ name := _Name, args := [H | _T] }) ->
    extract_selector(H);
extract_selector(#{ subject := S }) ->
    extract_selector(S).

-spec extract_fn(alias() | fn() | selector()) -> selector() | undefined.
extract_fn(F = #{ name := _Name, args := _Args }) ->
    F;
extract_fn(#{ subject := S }) ->
    extract_fn(S);
extract_fn(#{ metric := _Metric }) ->
    undefined.

-spec extract_alias(alias | term()) -> alias() | undefined.
extract_alias(A = #{subject := _Subject}) ->
    A;
extract_alias(_M) ->
    undefined.
