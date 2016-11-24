-module(dalmatiner_frontend_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Listeners} = application:get_env(dalmatiner_frontend, http_listeners),
    {ok, Port} =  application:get_env(dalmatiner_frontend, http_port),

    Dispatch = cowboy_router:compile(
                 [
                  %% {URIHost, list({URIPath, Handler, Opts})}
                  {'_', [{"/", dalmatiner_idx_handler, []},
                         %% Old style API
                         {"/buckets/", dalmatiner_bucket_h, []},
                         {"/buckets/[...]", dalmatiner_key_h, []},


                         {"/functions", dalmatiner_function_h, []},

                         {"/events", dalmatiner_event_h, []},

                         %% New style API
                         %% List all collections
                         {"/collections", dalmatiner_collection_h, []},
                         %% List all tag namespaces in collection
                         {"/collections/:collection/namespaces",
                          dalmatiner_namespace_h, []},
                         %% List all tag in a namespace
                         {"/collections/:collection/namespaces/"
                          ":namespace/tags",
                          dalmatiner_tag_h, []},
                         %% List all values in a teg
                         {"/collections/:collection/namespaces/"
                          ":namespace/tags/:tag/values",
                          dalmatiner_value_h, []},
                         %% List all metrics in a collection
                         {"/collections/:collection/metrics/",
                          dalmatiner_metric_h, []},
                         %% List metrics in a collection by prefix
                         {"/collections/:collection/metrics/:prefix",
                          dalmatiner_metric_h, []},
                         %% List all namespaces per metric
                         {"/collections/:collection/metrics/"
                          ":metric/namespaces/",
                          dalmatiner_namespace_h, []},
                         %% List all tags in a namespace per metric
                         {"/collections/:collection/metrics/"
                          ":metric/namespaces/:namespace/tags/",
                          dalmatiner_tag_h, []},
                         %% List all values in a tag per metric
                         {"/collections/:collection/metrics/"
                          ":metric/namespaces/:namespace/tags/"
                          ":tag/values",
                          dalmatiner_value_h, []},

                         %% Dataloop API extension
                         {"/status", dalmatiner_status_handler, []},
                         {"/inspect", dalmatiner_inspect_handler, []},
                         {"/dl", dalmatiner_idx_handler, []},
                         %% List all functions
                         {"/dl/functions", dalmatiner_function_h, []},
                         %% List all collections
                         {"/dl/collections", dalmatiner_dl_collection_h, []},
                         %% List all tag namespaces in collection
                         {"/dl/collections/:collection/namespaces",
                          dalmatiner_namespace_h, []},
                         %% List all tag in a namespace
                         {"/dl/collections/:collection/namespaces/"
                          ":namespace/tags",
                          dalmatiner_tag_h, []},
                         %% List all values in a teg
                         {"/dl/collections/:collection/namespaces/"
                          ":namespace/tags/:tag/values",
                          dalmatiner_value_h, []},
                         %% List all metrics in a collection
                         {"/dl/collections/:collection/metrics/",
                          dalmatiner_metric_h, []},
                         %% List all namespaces per metric
                         {"/dl/collections/:collection/metrics/"
                          ":metric/namespaces/",
                          dalmatiner_namespace_h, []},
                         %% List all tags in a namespace per metric
                         {"/dl/collections/:collection/metrics/"
                          ":metric/namespaces/:namespace/tags/",
                          dalmatiner_tag_h, []},
                         %% List all values in a tag per metric
                         {"/dl/collections/:collection/metrics/"
                          ":metric/namespaces/:namespace/tags/"
                          ":tag/values",
                          dalmatiner_value_h, []},

                         %% STatic content.
                         {"/js/[...]", cowboy_static,
                          {priv_dir, dalmatiner_frontend, "static/js",
                           [{mimetypes, cow_mimetypes, web}]}},
                         {"/fonts/[...]", cowboy_static,
                          {priv_dir, dalmatiner_frontend, "static/fonts",
                           [{mimetypes, cow_mimetypes, web}]}},
                         {"/css/[...]", cowboy_static,
                          {priv_dir, dalmatiner_frontend, "static/css",
                           [{mimetypes, cow_mimetypes, web}]}},
                         {"/img/[...]", cowboy_static,
                          {priv_dir, dalmatiner_frontend, "static/img",
                           [{mimetypes, cow_mimetypes, web}]}},
                         {"/static/[...]", cowboy_static,
                          {priv_dir, dalmatiner_frontend, "static/",
                           [{mimetypes, cow_mimetypes, web}]}}]}
                 ]),

    %% Access permissions in a format: [{MatchPattern}, Requirement]
    Acl = [{{path, <<"/dl/collections/">>}, require_collection_access},
           {{path, <<"/dl/collections">>}, require_authenticated},
           {[{path, <<"/dl">>}, {param, <<"q">>}],
            require_query_collection_access}],

    %% Name, NbAcceptors, TransOpts, ProtoOpts
    {ok, _} = cowboy:start_http(dalmatiner_http_listener, Listeners,
                                [{port, Port}],
                                [{env,
                                  [{dispatch, Dispatch},
                                   {acl, Acl}]},
                                 {middlewares,
                                  [dalmatiner_dl_authn_m,
                                   cowboy_router,
                                   dalmatiner_cors_m,
                                   dalmatiner_dl_authz_m,
                                   cowboy_handler]},
                                 {max_keepalive, 5},
                                 %% Keep max url length in pair with nginx
                                 {max_request_line_length, 8192},
                                 {timeout, 50000}]),
    dalmatiner_frontend_sup:start_link().

stop(_State) ->
    ok.
