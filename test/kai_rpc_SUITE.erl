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

-module(kai_rpc_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{seq, [standalone, ok, node_info, node_list, crud]}].

all() -> [{sequence, seq}].

init_per_testcase(standalone, Conf) ->
    init_local(Conf);
init_per_testcase(_TestCase, Conf) ->
    Conf2 = init_node(Conf, 1),
    init_node(Conf2, 2).

init_local(Conf) ->
    kai_config:start_link([{hostname, "127.0.0.1"},
                           {rpc_port, ?PORT1},
                           {rpc_max_processes, 2},
                           {max_connections, 4},
                           {quorum, {3,2,2}},
                           {buckets, 4},
                           {virtual_nodes, 2}]),
    Conf.

end_per_testcase(standalone, Conf) ->
    end_local(Conf);
end_per_testcase(_TestCase, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2).

end_local(_Conf) ->
    kai_config:stop().

standalone(_Conf) ->
    ok = kai_rpc:ok(?NODE1, ?NODE1).

ok(Conf) ->
    Node1 = ?config(node1, Conf),
    ok = rpc:call(Node1, kai_rpc, ok, [?NODE2, ?NODE1]).

node_info(Conf) ->
    Node1 = ?config(node1, Conf),
    {ok, ?NODE2, ?INFO} = rpc:call(Node1, kai_rpc, node_info, [?NODE2, ?NODE1]).

node_list(Conf) ->
    Node1 = ?config(node1, Conf),
    {ok, [?NODE2]} = rpc:call(Node1, kai_rpc, node_list, [?NODE2, ?NODE1]).

crud(Conf) ->
    Node1 = ?config(node1, Conf),

    Data = #data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = vclock:fresh(),
      value         = <<"value1">>
     },
    ok = rpc:call(Node1, kai_rpc, put, [?NODE2, ?NODE1, Data]),

    Data = rpc:call(Node1, kai_rpc, get, [?NODE2, ?NODE1, #data{key="key1", bucket=3}]),

    {ok, [Key]} = rpc:call(Node1, kai_rpc, list, [?NODE2, ?NODE1, 3]),
    "key1" = Key#data.key,

    ok = rpc:call(Node1, kai_rpc, delete, [?NODE2, ?NODE1, #data{key="key1", bucket=3}]).

%% TOCO: kai_rpc:check_node, kai_rpc:route
