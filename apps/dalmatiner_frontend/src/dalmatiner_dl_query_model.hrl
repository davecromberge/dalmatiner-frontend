-type op() :: 'eq' | 'neq' | 'present' | 'and' | 'or'.

-type timetype() :: 'rel' | 'abs'.

-type condition() :: #{ op => op(),
                        args => [any()] }.

-type timeframe() :: #{ m => timetype(),
                        beginning => binary(),
                        ending => binary(),
                        duration  => binary() }.

-type selector() :: #{ bucket => binary(),
                       collection => binary(),
                       metric => ['*' | binary()],
                       condition => condition() }.

-type fn() :: #{ name => binary(),
                 args => [term()] }.

-type alias() :: #{ subject => term(),
                    prefix => binary(),
                    label => binary(),
                    tags => [binary()] }.

-type part() :: #{ selector => selector(),
                   timeshift => binary(),
                   fn => fn(),
                   alias => alias() }.

-type query() :: #{ m => timetype(),
                    parts => [part()],
                    beginning => binary(),
                    ending => binary(),
                    duration => binary() }.
