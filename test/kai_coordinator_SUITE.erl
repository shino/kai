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
    kai_config:start_link([{hostname, "localhost"}, {port, 11011}, {memcache_port, 11211},
			   {n, 1}, {r, 1}, {w, 1},
			   {number_of_buckets, 8},
			   {number_of_virtual_nodes, 2}]),
    kai_hash:start_link(),
    kai_store:start_link(),
    kai_api:start_link(),
    kai_coordinator:start_link(),

    timer:sleep(100), % wait for starting kai_memcache

    Data = #data{key="item-1", bucket=3, last_modified=now(), checksum=erlang:md5(<<"value-1">>), flags="0", value=(<<"value-1">>)},

    ?assertEqual(
       ok,
       kai_coordinator:put(Data)
      ),

    ?assertEqual(
       [Data],
       kai_coordinator:get("item-1")
      ),

    ?assertEqual(
       ok,
       kai_coordinator:delete("item-1")
      ),

    ?assertEqual(
       undefined,
       kai_coordinator:get("item-1")
      ),

    ?assertEqual(
       undefined,
       kai_coordinator:delete("item-1")
      ),

    kai_coordinator:stop(),
    kai_store:stop(),
    kai_hash:stop(),
    kai_config:stop().
