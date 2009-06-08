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

%% Status of the hash ring used in this test suite:
%%
%%             0  Bucket0   [3,2]
%%   401,940,458  Node3(2)
%%   909,203,333  key3
%%
%% 1,073,741,824  Bucket1   [2,3]
%% 1,471,129,163  Node2(2)
%% 1,548,361,913  Node3(1)
%% 2,029,528,490  key2
%%
%% 2,147,483,648  Bucket2   [2,1]
%% 2,213,434,410  Node2(1)
%% 2,311,136,591  Node1(2)
%%
%% 3,221,225,472  Bucket3   [1,3]
%% 3,266,172,564  key1
%% 3,495,790,055  Node1(1)

-module(kai_hash_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

-undef(NODE1).
-undef(NODE2).
-undef(NODE3).
-define(NODE1, {{127,0,0,1}, ?PORT1}).
-define(NODE2, {{127,0,0,2}, ?PORT1}).
-define(NODE3, {{127,0,0,3}, ?PORT1}).

sequences() ->
    [{seq, [standalone, add_nodes, remove_nodes, add_duplicated, remove_ghost,
            remove_myself, perf]}].

all() -> [{sequence, seq}].

init_per_testcase(perf, Conf) ->
    kai_config:start_link([{rpc_port, 1},
                           {quorum, {3,2,2}},
                           {buckets, 15000},
                           {virtual_nodes, 128}]),
    kai_hash:start_link(),
    Conf;
init_per_testcase(_TestCase, Conf) ->
    kai_config:start_link([{hostname, "127.0.0.1"},
                           {rpc_port, 11011},
                           {quorum, {2,2,2}},
                           {buckets, 4},
                           {virtual_nodes, 2}]),
    kai_hash:start_link(),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_hash:stop(),
    kai_config:stop().

standalone(_Conf) ->
    {ok, ?NODE1, Info} = kai_hash:node_info(),
    ?assertEqual(?INFO, Info),
    {ok, ?NODE1, Info} = kai_hash:node_info(?NODE1),
    ?assertEqual(?INFO, Info),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual([?NODE1], NodeList),

    {ok, VirtualNodeList} = kai_hash:virtual_node_list(),
    ?assertEqual(
       [{2311136591, ?NODE1}, {3495790055, ?NODE1}],
       VirtualNodeList
      ),

    {ok, BucketList} = kai_hash:bucket_list(),
    ?assertEqual(
       [{0, [?NODE1]}, {1, [?NODE1]}, {2, [?NODE1]}, {3, [?NODE1]}],
       BucketList
      ),

    {ok, Buckets} = kai_hash:buckets(),
    ?assertEqual([0,1,2,3], Buckets).

add_nodes(_Conf) ->
    {ok, ReplacedBuckets} =
        kai_hash:update_nodes([{?NODE2, ?INFO}, {?NODE3, ?INFO}], []),

    ?assertEqual(
       [{0,undefined,1},{1,undefined,1},{2,2,1}],
       ReplacedBuckets
      ),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual(3, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),
    ?assert(lists:member(?NODE3, NodeList)),

    {ok, VirtualNodeList} = kai_hash:virtual_node_list(),
    ?assertEqual(
       [{ 401940458, ?NODE3},
        {1471129163, ?NODE2},
        {1548361913, ?NODE3},
        {2213434410, ?NODE2},
        {2311136591, ?NODE1 },
        {3495790055, ?NODE1 }],
       VirtualNodeList
      ),

    {ok, BucketList} = kai_hash:bucket_list(),
    ?assertEqual(
       [{0, [?NODE3, ?NODE2]},
        {1, [?NODE2, ?NODE3]},
        {2, [?NODE2, ?NODE1]},
        {3, [?NODE1, ?NODE3]}],
       BucketList
      ),

    {ok, Buckets} = kai_hash:buckets(),
    ?assertEqual([2,3], Buckets),

    {ok, Bucket} = kai_hash:find_bucket("key1"),
    ?assertEqual(3, Bucket),

    {ok, Nodes} = kai_hash:find_nodes(Bucket),
    {ok, Nodes} = kai_hash:find_nodes("key1"),
    ?assertEqual([?NODE1, ?NODE3], Nodes),

    {ok, ReplicaIndex} = kai_hash:replica_index(Bucket),
    ?assertEqual(1, ReplicaIndex),

    {ok, Nodes2} = kai_hash:find_nodes("key2"),
    ?assertEqual([?NODE2, ?NODE3], Nodes2),

    lists:foreach(
      fun(_) ->
              {node, Node} = kai_hash:choose_node_randomly(),
              ?assertNot(Node == ?NODE1)
      end,
      lists:seq(1, 8)
     ),

    lists:foreach(
      fun(_) ->
              {ok, Bucket2} = kai_hash:choose_bucket_randomly(),
              ?assert((Bucket2 == 2) or (Bucket2 == 3))
      end,
      lists:seq(1, 8)
     ).

remove_nodes(_Conf) ->
    kai_hash:update_nodes([{?NODE2, ?INFO}, {?NODE3, ?INFO}], []),

    {ok, ReplacedBuckets} = kai_hash:update_nodes([], [?NODE2]),
    ?assertEqual(
       [{0,2,undefined},{1,2,undefined},{2,1,2}],
       ReplacedBuckets
      ),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual(2, length(NodeList)),
    ?assertNot(lists:member(?NODE2, NodeList)),

    {ok, VirtualNodeList} = kai_hash:virtual_node_list(),
    ?assertEqual(
       [{ 401940458, ?NODE3},
        {1548361913, ?NODE3},
        {2311136591, ?NODE1},
        {3495790055, ?NODE1}],
       VirtualNodeList
      ),

    {ok, BucketList} = kai_hash:bucket_list(),
    ?assertEqual(
       [{0, [?NODE3, ?NODE1]},
        {1, [?NODE3, ?NODE1]},
        {2, [?NODE1, ?NODE3]},
        {3, [?NODE1, ?NODE3]}],
       BucketList
      ),

    {ok, Buckets} = kai_hash:buckets(),
    ?assertEqual([0,1,2,3], Buckets),

    {ok, Nodes} = kai_hash:find_nodes("key1"),
    ?assertEqual([?NODE1, ?NODE3], Nodes),

    {ok, Nodes2} = kai_hash:find_nodes("key2"),
    ?assertEqual([?NODE3, ?NODE1], Nodes2).

add_duplicated(_Conf) ->
    {ok, _ReplacedBuckets} =
        kai_hash:update_nodes([{?NODE2, ?INFO}, {?NODE3, ?INFO}], []),

    {ok, _ReplacedBuckets2} =
        kai_hash:update_nodes([{?NODE2, ?INFO}, {?NODE3, ?INFO}], []),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual(3, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),
    ?assert(lists:member(?NODE3, NodeList)).

remove_ghost(_Conf) ->
    {ok, _ReplacedBuckets} = kai_hash:update_nodes([], [?NODE2]),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual(1, length(NodeList)),
    ?assertNot(lists:member(?NODE2, NodeList)).    

remove_myself(_Conf) ->
    {ok, _ReplacedBuckets} = kai_hash:update_nodes([], [?NODE1]),

    {ok, NodeList} = kai_hash:node_list(),
    ?assertEqual(1, length(NodeList)),
    ?assert(lists:member(?NODE1, NodeList)).

perf(_Conf) ->
    ct:log("Simulates a cluster of 64 nodes."),

    Info = [{virtual_nodes, 128}],
    Nodes = lists:map(
              fun(Port) -> {{{127,0,0,1}, Port}, Info} end,
              lists:seq(2, 63)
             ),
    kai_hash:update_nodes(Nodes, []),

    Args = [[{{{127,0,0,1}, 64}, Info}], []],
    {Usec, _} = timer:tc(kai_hash, update, Args),
    ct:log("Time to add a node: ~p us", [Usec]),

    {Usec2, _} = timer:tc(kai_hash, find, ["key1", 1]),
    ct:log("Time to find a node: ~p us", [Usec2]),

    {Usec3, _} = timer:tc(kai_hash, choose_node_randomly, []),
    ct:log("Time to choose a node randomly: ~p us", [Usec3]),

    {Usec4, _} = timer:tc(kai_hash, choose_bucket_randomly, []),
    ct:log("Time to choose a bucket randomly: ~p us", [Usec4]),

    {Usec5, _} = timer:tc(kai_hash, update, [[], [{{127,0,0,1}, 1}]]),
    ct:log("Time to remove a node: ~p us", [Usec5]).
