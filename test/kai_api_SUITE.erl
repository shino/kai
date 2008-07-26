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

-module(kai_api_SUITE).
-compile(export_all).

-include("kai.hrl").
-include("kai_test.hrl").

all() -> [test1].

test1() -> [].
test1(_Conf) ->
    kai_config:start_link([
        {hostname, "localhost"},
        {port, 11011},
        {max_connections, 2},
        {n, 3},
        {number_of_buckets, 8},
        {number_of_virtual_nodes, 2}]),
    kai_hash:start_link(),
    kai_store:start_link(),
    kai_api:start_link(),

    timer:sleep(100), % wait for starting kai_api

    {node_info, ?NODE1, ?INFO} = kai_api:node_info(?NODE1),

    {node_list, [?NODE1]} = kai_api:node_list(?NODE1),

    Data = #data{
        key           = "item-1",
        bucket        = 3,
        last_modified = now(),
        checksum      = erlang:md5(<<"value-1">>),
        flags         = "0",
        value         = (<<"value-1">>)
    },
    ok = kai_api:put(?NODE1, Data),
    ?assertEqual(Data, kai_store:get("item-1")),

    ListOfData = #data{
        key           = "item-1",
        bucket        = 3,
        last_modified = Data#data.last_modified,
        checksum      = erlang:md5(<<"value-1">>)
    },
    {list_of_data, [ListOfData]} = kai_api:list(?NODE1, 3),

    Data = kai_api:get(?NODE1, "item-1"),

    ok = kai_api:delete(?NODE1, "item-1"),

    undefined = kai_api:get(?NODE1, "item-1"),

    kai_api:stop(),
    kai_store:stop(),
    kai_hash:stop(),
    kai_config:stop().
