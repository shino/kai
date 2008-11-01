% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License.  You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
% License for the specific language governing permissions and limitations under
% the License.

-module(kai_coordinator_SUITE).
-compile(export_all).

-include("kai.hrl").
-include("kai_test.hrl").

all() -> [test1].

test1() -> [].
test1(_Conf) ->
    kai_config:start_link([
        {hostname, "localhost"},
        {rpc_port, 11011},
        {rpc_max_processes, 2},
        {max_connections, 32},
        {n, 1}, {r, 1}, {w, 1},
        {number_of_buckets, 8},
        {number_of_virtual_nodes, 2},
        {store, ets}
    ]),
    kai_hash:start_link(),
    kai_store:start_link(),
    kai_version:start_link(),
    kai_connection:start_link(),
    kai_rpc:start_link(),

    ?assertEqual(
       ok,
       kai_coordinator:route({put, #data{
           key   = "item-1",
           flags = "0",
           value = (<<"value-1">>)}
       })
      ),

    ListOfData = kai_coordinator:route({get, #data{key="item-1"}}),
    ?assertEqual(1, length(ListOfData)),

    [Data|_] = ListOfData,
    ?assert(is_record(Data, data)),
    ?assertEqual("item-1", Data#data.key),
    ?assertEqual(3, Data#data.bucket),
    ?assertEqual(erlang:md5(<<"value-1">>), Data#data.checksum),
    ?assertEqual("0", Data#data.flags),
    ?assertEqual(<<"value-1">>, Data#data.value),

    ?assertEqual(
       ok,
       kai_coordinator:route({delete, #data{key="item-1"}})
      ),

    ?assertEqual(
       undefined,
       kai_coordinator:route({get, #data{key="item-1"}})
      ),

    ?assertEqual(
       undefined,
       kai_coordinator:route({delete, #data{key="item-1"}})
      ),

    kai_rpc:stop(),
    kai_connection:stop(),
    kai_version:stop(),
    kai_store:stop(),
    kai_hash:stop(),
    kai_config:stop().
