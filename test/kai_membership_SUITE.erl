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

-module(kai_membership_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() ->
    [{seq, [add_directly, add_indirectly,
            remove_directly, remove_indirectly,
            update_by_timeout, sync_and_move_out, sync_and_collect]}].

all() -> [{sequence, seq}].

init_per_testcase(add_directly, Conf) ->
    Conf2 = init_node(Conf, 1),
    init_node(Conf2, 2);
init_per_testcase(_TestCase, Conf) ->
    Conf2 = init_node(Conf,  1, [{quorum, {2,2,2}}]),
    Conf3 = init_node(Conf2, 2, [{quorum, {2,2,2}}]),
    init_node(Conf3, 3, [{quorum, {2,2,2}}]).

end_per_testcase(add_directly, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2);
end_per_testcase(_TestCase, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2),
    end_node(Conf, 3).

add_directly(Conf) ->
    Node1 = ?config(node1, Conf),

    {ok, [?NODE1]} = rpc:call(Node1, kai_hash, node_list, []),

    %% Node1 finds Node2, directly
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(2, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)).

add_indirectly(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    %% Node1 finds Node2
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(2, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),

    %% Node2 finds Node3
    rpc:call(Node2, kai_membership, check_node, [?NODE3]),

    wait(),

    %% Node1 finds Node3, indirectly (in Node2's membership)
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList2} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(3, length(NodeList2)),
    ?assert(lists:member(?NODE3, NodeList2)).

remove_directly(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    %% Node1 finds Node2
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(2, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),
    
    %% Node2 is down
    ok = slave:stop(Node2),

    %% Node1 knows Node2 is down, directly
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, [?NODE1]} = rpc:call(Node1, kai_hash, node_list, []).

remove_indirectly(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),
    Node3 = ?config(node3, Conf),

    %% Node2 finds Node3
    rpc:call(Node2, kai_membership, check_node, [?NODE3]),

    wait(),

    %% Node1 finds Node2 and Node3
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(3, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),
    ?assert(lists:member(?NODE3, NodeList)),

    %% Node3 is down
    ok = slave:stop(Node3),

    %% Node2 knows Node3 is down
    rpc:call(Node2, kai_membership, check_node, [?NODE3]),

    %% Node1 indirectly knows Node3 is down
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList2} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(2, length(NodeList2)),
    ?assert(lists:member(?NODE2, NodeList)).

update_by_timeout(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    %% Node1 finds Node2
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),

    wait(),

    {ok, NodeList} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(2, length(NodeList)),
    ?assert(lists:member(?NODE2, NodeList)),

    %% Node2 finds Node3
    rpc:call(Node2, kai_membership, check_node, [?NODE3]),

    %% Node1 synchronizes membership with Node2, which is triggered by timer
    Interval = rpc:call(Node1, kai_config, get, [membership_interval]),
    timer:sleep(Interval),
    wait(),

    {ok, NodeList2} = rpc:call(Node1, kai_hash, node_list, []),
    ?assertEqual(3, length(NodeList2)),
    ?assert(lists:member(?NODE3, NodeList2)).

sync_and_move_out(Conf) ->
    Node1 = ?config(node1, Conf),

    %% 16 data are put at Node1
    DataNum = 16,
    prep(Node1, DataNum),

    rpc:call(Node1, kai_membership, check_node, [?NODE2]),
    rpc:call(Node1, kai_membership, check_node, [?NODE3]),

    wait(),

    %% The number of data must be decreased at Node1, since some data are moved
    %% to new commers
    DataNum2 = count(Node1, 4, 0),
    ?assert((0 < DataNum2) and (DataNum2 < DataNum)).

sync_and_collect(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),
    Node3 = ?config(node3, Conf),

    %% 16 data are put at Node1 and Node2
    DataNum = 16,
    prep(Node1, DataNum),
    prep(Node2, DataNum),

    rpc:call(Node1, kai_membership, check_node, [?NODE2]),
    rpc:call(Node2, kai_membership, check_node, [?NODE1]),

    wait(),

    rpc:call(Node1, kai_membership, check_node, [?NODE3]),
    rpc:call(Node3, kai_membership, check_node, [?NODE1]),

    %% Some data are moved to Node3.
    wait(),

    %% Node3 is down, and Node 1 checks that.
    ok = slave:stop(Node3),
    rpc:call(Node1, kai_membership, check_node, [?NODE3]),

    wait(),

    %% All data must come back to Node1, since Node3 was down
    ?assertEqual(DataNum, count(Node1, 4, 0)).

prep(_Node, 0) ->
    ok;
prep(Node, I) ->
    Key = "key" ++ integer_to_list(I),
    {ok, Bucket} = rpc:call(Node, kai_hash, find_bucket, [Key]),
    ok = rpc:call(Node, kai_store, put, [#data{
      key           = Key,
      bucket        = Bucket,
      last_modified = now(),
      checksum      = erlang:md5(<<"value">>),
      flags         = "0",
      vector_clocks = vclock:increment(Node, vclock:fresh()),
      value         = <<"value">>
     }]),
    prep(Node, I-1).

count(_Node, 0, DataNum) ->
    DataNum;
count(Node, Bucket, DataNum) ->
    {ok, KeyList} = rpc:call(Node, kai_store, list, [Bucket-1]),
    ct:log("~p 's Bucket #~p:  ~p", [Node, Bucket-1, KeyList]),
    count(Node, Bucket-1, DataNum + length(KeyList)).

%% TODO: Check whether data is synchronized
