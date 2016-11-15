-module(dalmatiner_dl_destructure_eqc).

-include_lib("eqc/include/eqc.hrl").

-import(dqe_helper, [select_stmt/0]).

-compile(export_all).

-define(P, dalmatiner_dl_destructure).
-define(R, dalmatiner_dl_reify).

prop_query_destructure_reify() ->
    ?FORALL(T, select_stmt(),
            begin
                Query = dql_unparse:unparse(T),
                {ok, Destructured} = ?P:destructure(Query),
                Reified = ?R:reify(Destructured),
                ?WHENFAIL(
                   io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                             [Query, Destructured, Reified]),
                   Query == Reified)
            end).
