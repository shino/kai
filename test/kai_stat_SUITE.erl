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

-module(kai_stat_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{seq, [stat, incr]}].

all() -> [{sequence, seq}].

init_per_testcase(_TestCase, Conf) ->
    kai_config:start_link([{hostname, "127.0.0.1"},
                           {rpc_port, 11011},
                           {quorum, {3,2,2}},
                           {buckets, 4},
                           {virtual_nodes, 2},
                           {store, ets}]),
    kai_hash:start_link(),
    kai_store:start_link(),
    kai_stat:start_link(),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_stat:stop(),
    kai_store:stop(),
    kai_hash:stop(),
    kai_config:stop().

stat(_Conf) ->
    Stats = kai_stat:all(),

    ?assert(re(g(uptime, Stats), "[0-9]+")),
    ?assert(re(g(time, Stats), "[0-9]+")),
    ?assert(re(g(version, Stats), "[.0-9]+")),
    ?assert(re(g(bytes, Stats), "[0-9]+")),

    ?assertEqual("0", g(curr_items, Stats)),
    ?assertEqual("0", g(curr_connections, Stats)),
    ?assertEqual("0", g(cmd_get, Stats)),
    ?assertEqual("0", g(cmd_set, Stats)),
    ?assertEqual("0", g(bytes_read, Stats)),
    ?assertEqual("0", g(bytes_write, Stats)),

    ?assertEqual("127.0.0.1:11011", g(kai_node, Stats)),
    ?assertEqual("3,2,2", g(kai_quorum, Stats)),
    ?assertEqual("4", g(kai_buckets, Stats)),
    ?assertEqual("2", g(kai_virtual_nodes, Stats)),
    ?assertEqual("ets", g(kai_store, Stats)),
    ?assertEqual("127.0.0.1:11011", g(kai_curr_nodes, Stats)),
    ?assertEqual("0(0) 0(0)", g(kai_unreconciled_get, Stats)),
%    ?assertEqual("0 1 2 3", g(kai_curr_buckets, Stats)),
    
    ?assert(re(g(erlang_procs, Stats), "[0-9]+")),
    ?assert(re(g(erlang_version, Stats), "[.0-9]+")).

incr(_Conf) ->
    kai_stat:incr_cmd_get(),
    kai_stat:incr_cmd_set(),

    Data = #data{value = <<"value1">>},
    kai_stat:add_bytes_read([Data, Data]),
    kai_stat:add_bytes_write(Data),

    lists:map(
      fun(Arg) ->
              kai_stat:incr_unreconciled_get(Arg)
      end,
      [{2,true}, {2,true}, {2,false}, {3,true}, {5,false}]
     ),

    Stats = kai_stat:all(),

    ?assertEqual("1", g(cmd_get, Stats)),
    ?assertEqual("1", g(cmd_set, Stats)),
    ?assertEqual("12", g(bytes_read, Stats)),
    ?assertEqual("6", g(bytes_write, Stats)),

    ?assertEqual("1(3) 0(1) 0(0) 1(1)", g(kai_unreconciled_get, Stats)).
