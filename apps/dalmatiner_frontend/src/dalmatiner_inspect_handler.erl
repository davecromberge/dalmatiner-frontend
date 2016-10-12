-module(dalmatiner_inspect_handler).
-behaviour(cowboy_http_handler).

-export([init/3, handle/2, terminate/3]).

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
            %% TODO: dqe:prapre is rather expensive, because it involves
            %% meta-data lookups and glob expansion. Probably we should get raw
            %% parsed tree by calling dql:prepare and infere ownership from that
            %%
            %% Alternatively we may not need to inspec it we use only meta-data
            %% queries with tenency as one of dimensions
            case dqe:prepare(Q) of
                {error, E} ->
                    Error = list_to_binary(dqe:error_string({error, E})),
                    {ok, Req2} =
                        cowboy_req:reply(400, [], Error, Req1),
                    {ok, Req2, State};
                {ok, {Total, Unique, Parts}, Start} ->
                    #{buckets := BucketSet,
                      roots := RootSet} = inspect_parts(Parts),
                    D = [{<<"b">>, sets:to_list(BucketSet)},
                         {<<"r">>, sets:to_list(RootSet)},
                         {<<"s">>, Start},
                         {<<"t">>, Total},
                         {<<"u">>, Unique}],
                    {CType, Req2} = dalmatiner_idx_handler:content_type(Req1),
                    dalmatiner_idx_handler:send(CType, D, Req2, State)
            end
    end.

terminate(_Reason, _Req, _State) ->
    ok.

inspect_parts(Parts) ->
    inspect_parts(Parts,
                  #{buckets => sets:new(),
                    roots => sets:new()}).

inspect_parts([], Acc) ->
    Acc;
inspect_parts([Part | Rest], Acc) ->
    Acc1 = inspect_part(Part, Acc),
    inspect_parts(Rest, Acc1).

inspect_part({dqe_get, [_Start, _Count, _Res, Bucket, Metric]},
             Acc = #{buckets := BucketSet1,
                     roots := RootSet1}) ->
    Root = metric_root(Metric),
    BucketSet2 = sets:add_element(Bucket, BucketSet1),
    RootSet2 = sets:add_element(Root, RootSet1),
    Acc#{buckets := BucketSet2, roots := RootSet2};
inspect_part({dqe_sum, [Nested]}, Acc) ->
    inspect_part({dqe_sum_nested, Nested}, Acc);
inspect_part({dqe_collect, [_Q, _Res, Nested]}, Acc) ->
    inspect_part(Nested, Acc);
inspect_part({_Operand, []}, Acc) ->
    Acc;
inspect_part({_Operand, [Nested | Rest]}, Acc) ->
    Acc1 = case is_tuple(Nested) of
               true -> inspect_part(Nested, Acc);
               false -> Acc
           end,
    inspect_part({null, Rest}, Acc1);
inspect_part({_Operand, Nested}, Acc) when is_tuple(Nested) ->
    inspect_part(Nested, Acc).

metric_root(Metric) ->
    hd(Metric).
