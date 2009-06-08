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

-module(kai_sync_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{seq, [sync_explicitly, sync_by_timeout]}].

all() -> [{sequence, seq}].

init_per_testcase(_TestCase, Conf) ->
    Conf2 = init_node(Conf, 1),
    init_node(Conf2, 2).
    
end_per_testcase(_TestCase, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2).

sync_explicitly(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    ok = rpc:call(Node2, kai_store, put, [#data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = vclock:fresh(),
      value         = <<"value1">>
     }]),

    {ok, _ReplacedBuckets} =
        rpc:call(Node1, kai_hash, update_nodes, [[{?NODE2, ?INFO}], []]),

    {ok, []} = rpc:call(Node1, kai_store, list, [3]),

    %% Node1 tries to synchronize Bucket3 with Node2
    ok = rpc:call(Node1, kai_sync, update_bucket, [3]),

    wait(),

    {ok, [_]} = rpc:call(Node1, kai_store, list, [3]).

sync_by_timeout(Conf) ->
    Node1 = ?config(node1, Conf),
    Node2 = ?config(node2, Conf),

    ok = rpc:call(Node2, kai_store, put, [#data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = vclock:fresh(),
      value         = <<"value1">>
     }]),

    {ok, _ReplacedBuckets} =
        rpc:call(Node1, kai_hash, update_nodes, [[{?NODE2, ?INFO}], []]),

    {ok, []} = rpc:call(Node1, kai_store, list, [3]),

    %% Node1 tries to synchronize Bucket3 every TIMER sec.
    check(Node1, 16).

check(_Node, 0) ->
    ct:fail(never_synchronized);
check(Node, I) ->
    Interval = rpc:call(Node, kai_config, get, [sync_interval]),
    timer:sleep(Interval),
    case rpc:call(Node, kai_store, list, [3]) of
        {ok, [_]} -> ok;
        {ok, []}  -> check(Node, I-1)
    end.
