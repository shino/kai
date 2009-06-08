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

-module(kai_coordinator_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{seq, [standalone, cluster, get_concurrent_data,
                       put_concurrent_data, get_latest_data,
                       overwrite_by_stale]}].

all() -> [{sequence, seq}].

init_per_testcase(standalone, Conf) ->
    init_node(Conf, 1, [{quorum, {1,1,1}}]);
init_per_testcase(cluster, Conf) ->
    Conf2 = init_node(Conf,  1, [{quorum, {2,2,2}}]),
    Conf3 = init_node(Conf2, 2, [{quorum, {2,2,2}}]),
    Conf4 = init_node(Conf3, 3, [{quorum, {2,2,2}}]),
    [Node1, Node2, Node3] =
        [?config(node1, Conf4), ?config(node2, Conf4), ?config(node3, Conf4)],
    rpc:call(Node1, kai_membership, check_node, [?NODE2]),
    rpc:call(Node2, kai_membership, check_node, [?NODE3]),
    rpc:call(Node3, kai_membership, check_node, [?NODE1]),
    Interval = rpc:call(Node1, kai_config, get, [membership_interval]),
    timer:sleep(Interval),
    wait(),
    Conf4;
init_per_testcase(_TestCase, Conf) ->
    Conf2 = init_node(Conf,  1, [{quorum, {2,2,2}}]),
    Conf3 = init_node(Conf2, 2, [{quorum, {2,2,2}}]),
    rpc:call(?config(node1, Conf3), kai_membership, check_node, [?NODE2]),
    rpc:call(?config(node2, Conf3), kai_membership, check_node, [?NODE1]),
    wait(),
    Conf3.

end_per_testcase(standalone, Conf) ->
    end_node(Conf, 1);
end_per_testcase(cluster, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2),
    end_node(Conf, 3);
end_per_testcase(_TestCase, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2).

standalone(Conf) ->
    crud("key1", {1,1,1}, [?config(node1, Conf)]).

cluster(Conf) ->
    Nodes = [?config(node1, Conf), ?config(node2, Conf), ?config(node3, Conf)],
    lists:foreach(
      fun(I) ->
              crud("key" ++ integer_to_list(I), {2,2,2}, Nodes)
      end,
      lists:seq(1, 16)
     ).

crud(Key, {N,_R,_W} = Quorum, Nodes) ->
    [Node|_] = Nodes,

    Data = #data{
      key   = Key,
      flags = "0",
      value = <<"value">>
     },
    ok = rpc:call(Node, kai_coordinator, route, [?NODE1, {put, Data, Quorum}]),
    [_] = rpc:call(Node, kai_coordinator, route, [?NODE1, {get, #data{key=Key}, Quorum}]),
    N = replica_counts(Key, Nodes),

    ok = rpc:call(Node, kai_coordinator, route, [?NODE1, {delete, #data{key=Key}, Quorum}]),
    0 = replica_counts(Key, Nodes),

    %% TODO: Should return [], not undefined, when the key is not found
    undefined = rpc:call(Node, kai_coordinator, route, [?NODE1, {get, #data{key=Key}, Quorum}]),
    undefined = rpc:call(Node, kai_coordinator, route, [?NODE1, {delete, #data{key=Key}, Quorum}]).

replica_counts(Key, Nodes) ->
    replica_counts(Key, Nodes, 0).

replica_counts(_Key, [], Acc) ->
    Acc;
replica_counts(Key, [Node|Rest], Acc) ->
    {ok, Bucket} = rpc:call(Node, kai_hash, find_bucket, [Key]),
    case rpc:call(Node, kai_store, get, [#data{key=Key, bucket=Bucket}]) of
        Data when is_record(Data, data) ->
            replica_counts(Key, Rest, Acc + 1);
        undefined ->
            replica_counts(Key, Rest, Acc)
    end.

get_concurrent_data(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    Key = "key1",
    prep_concurrent_data(Key, [Node1, Node2]),

    [_,_] = rpc:call(Node1, kai_coordinator, route, [?NODE1, {get, #data{key=Key}, {2,2,2}}]),

    Stats = rpc:call(Node1, kai_stat, all, []),
    "1(1)" = proplists:get_value(kai_unreconciled_get, Stats).

put_concurrent_data(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    Key = "key1",
    prep_concurrent_data(Key, [Node1, Node2]),

    Data = #data{
      key   = Key,
      flags = "0",
      value = <<"value">>
     },
    {error, ebusy} = rpc:call(Node1, kai_coordinator, route, [?NODE1, {put, Data, {2,2,2}}]).

prep_concurrent_data(Key, Nodes) when is_list(Key) ->
    Data = #data{
      key           = Key,
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value">>),
      flags         = "0",
      value         = <<"value">>
     },
    prep_concurrent_data(Data, Nodes);
prep_concurrent_data(Data, [Node|Rest]) ->
    N = rpc:call(Node, kai_config, get, [node]),
    VC = vclock:increment(N, vclock:fresh()),
    ok = rpc:call(Node, kai_store, put, [Data#data{vector_clocks=VC}]),
    prep_concurrent_data(Data, Rest);
prep_concurrent_data(_Data, []) ->
    ok.

get_latest_data(Conf) ->
    Node1 = ?config(node1, Conf),

    Key = "key1",
    Data = #data{
      key   = Key,
      flags = "0",
      value = <<"value">>
     },
    ok = rpc:call(Node1, kai_coordinator, route, [?NODE1, {put, Data, {2,2,2}}]),

    set_clock_ahead(Key, Node1),

    [NewData] = rpc:call(Node1, kai_coordinator, route, [?NODE1, {get, #data{key=Key}, {2,2,2}}]),
    ?assertEqual(<<"updated">>, NewData#data.value).

overwrite_by_stale(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    Key = "key1",
    Data = #data{
      key   = Key,
      flags = "0",
      value = <<"value">>
     },
    ok = rpc:call(Node1, kai_coordinator, route, [?NODE1, {put, Data, {2,2,2}}]),

    set_clock_ahead(Key, Node1),

    %% FIXME: This operation must be succeeded
    {error, ebusy} = rpc:call(Node2, kai_coordinator, route, [?NODE2, {put, Data, {2,2,2}}]).

set_clock_ahead(Key, Node) ->
    SrcNode = rpc:call(Node, kai_config, get, [node]),
    [Data] = rpc:call(Node, kai_coordinator, route, [SrcNode, {get, #data{key=Key}, {2,2,2}}]),

    %% The VectorClocks is set ahead at only Node
    N = rpc:call(Node, kai_config, get, [node]),
    VC = vclock:increment(N, Data#data.vector_clocks),
    NewData = Data#data{
                checksum      = erlang:md5(<<"updated">>),
                last_modified = now(),
                vector_clocks = VC,
                value         = <<"updated">>
               },
    ok = rpc:call(Node, kai_store, put, [NewData]).
