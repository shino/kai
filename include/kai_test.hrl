%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License.  You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations under
%% the License.

-define(assert(BoolExpr),
        ((fun() ->
            case (BoolExpr) of
                true -> ok;
                __V -> .erlang:error({assertion_failed,
                                      [{module, ?MODULE},
                                       {line, ?LINE},
                                       {expression, (??BoolExpr)},
                                       {expected, true},
                                       {value, case __V of
                                                   false -> __V;
                                                   _     -> {not_a_boolean,__V}
                                               end}]})
            end
          end)())).

-define(assertNot(BoolExpr), ?assert(not (BoolExpr))).

-define(assertEqual(Expect, Expr),
        ((fun(__X) ->
            case (Expr) of
                __X -> ok;
                __V -> .erlang:error({assertEqual_failed,
                                      [{module, ?MODULE},
                                       {line, ?LINE},
                                       {expression, (??Expr)},
                                       {expected, __X},
                                       {value, __V}]})
            end
          end)(Expect))).

-define(PORT1, 11011).
-define(PORT2, 11012).
-define(PORT3, 11013).
-define(NODE1, {{127,0,0,1}, ?PORT1}).
-define(NODE2, {{127,0,0,1}, ?PORT2}).
-define(NODE3, {{127,0,0,1}, ?PORT3}).
-define(INFO, [{virtual_nodes, 2}]).

init_node(Conf, I) ->
    init_node(Conf, I, []).
init_node(Conf, I, Options) ->
    Name = list_to_atom("kai" ++ integer_to_list(I)),
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    {ok, Node} =
        slave:start(Host,
                    Name,
                    " -pa ../../ebin"),
    ok = rpc:call(Node, application, load, [kai]),
    lists:foreach(fun({Par, Val}) ->
                          rpc:call(Node, application, set_env, [kai, Par, Val])
                  end, [{hostname, "127.0.0.1"},
                        {rpc_port, ?PORT1 + I - 1},
                        {memcache_port, ?PORT1 + I + 199},
                        {buckets, 4},
                        {virtual_nodes, 2}|Options]),
    ok = rpc:call(Node, application, start, [kai]),
    [{list_to_atom("node" ++ integer_to_list(I)), Node}|Conf].

end_node(Conf, I) ->
    Node = ?config(list_to_atom("node" ++ integer_to_list(I)), Conf),
    ok = slave:stop(Node).

wait() -> timer:sleep(100).

g(K, L) ->
    proplists:get_value(K, L).

re(S, R) ->
    case regexp:match(S, R) of
        {match, _, _} -> true;
        _             -> false
    end.
%% TODO: Can be replaced by re module in R13B
%    {ok, Re} = re:compile(R),
%    case re:run(S, Re) of
%        {match, _} -> true;
%        _          -> false
%    end.
