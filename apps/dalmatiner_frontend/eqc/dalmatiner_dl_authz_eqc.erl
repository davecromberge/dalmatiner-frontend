-module(dalmatiner_dl_authz_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-define(M, dalmatiner_dl_authz_m).

%%%===================================================================
%%% Properties
%%%===================================================================

prop_check_parts_access() ->
    ?SETUP(fun mock/0,
           ?FORALL(T, dqe_eqc:select_stmt(),
                   begin
                       Unparsed = dql_unparse:unparse(T),
                       {ok, Parts} = ?M:prepare_query(Unparsed),

                       case check_all_parts_access(Parts) of
                           {error, E} ->
                               io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                         [T, Unparsed, E]),
                               false;
                           AccessList ->
                               ?WHENFAIL(
                                  io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                            [T, Unparsed, AccessList]),
                                AccessList =:= [])
                       end
                   end)).

%%%===================================================================
%%% Helper functions
%%%===================================================================


check_all_parts_access(Parts) ->
    Access = [begin
                  case Part of
                      #{op := lookup,
                        args := [Col, _Met | _Optional]} ->
                          ?M:check_query_part_access(Part, #{Col => allow});
                      _ ->
                          ?M:check_query_part_access(Part, #{})
                  end
              end || Part <- Parts],

    lists:filter(fun(A) -> case A of
                               allow -> false;
                               deny -> false;
                               _ -> true
                           end end, 
                 Access).

mock() ->
    meck:new(base16, [passthrough]),
    meck:expect(base16, decode,
                fun (Binary) ->
                    Binary
                end),
    meck:new(dalmatiner_dl_data, [passthrough]),
    meck:expect(dalmatiner_dl_data, agent_access,
                fun (_, _) ->
                        {ok, deny} 
                end),
    fun unmock/0.

unmock() ->
    meck:unload(base16),
    meck:unload(dalmatiner_dl_data),
    ok.
