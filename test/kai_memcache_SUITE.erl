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

-module(kai_memcache_SUITE).
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
    kai_memcache:start_link(),

    timer:sleep(100), % wait for starting kai_memcache

    {ok, MemcacheSocket} =
	gen_tcp:connect({127,0,0,1}, 11211, [binary, {packet, line}, {active, false}]),

    Value = <<"value-1">>,
    Buf = io_lib:format("set item-1 0 0 ~w\r\n~s\r\n", [?byte_size(Value), Value]),
    gen_tcp:send(MemcacheSocket, Buf),

    ?assertEqual(
       {ok, <<"STORED\r\n">>},
       gen_tcp:recv(MemcacheSocket, 0)
      ),

    gen_tcp:send(MemcacheSocket, "get item-1\r\n"),

    ?assertEqual(
       {ok, <<"VALUE item-1 0 7\r\n">>},
       gen_tcp:recv(MemcacheSocket, 0)
      ),
    inet:setopts(MemcacheSocket, [{packet, raw}]),
    ?assertEqual(
       {ok, <<"value-1">>},
       gen_tcp:recv(MemcacheSocket, ?byte_size(<<"value-1">>))
      ),
    gen_tcp:recv(MemcacheSocket, ?byte_size(<<"\r\nEND\r\n">>)),
    inet:setopts(MemcacheSocket, [{packet, raw}]),

    gen_tcp:send(MemcacheSocket, "delete item-1\r\n"),

    ?assertEqual(
       {ok, <<"DELETED\r\n">>},
       gen_tcp:recv(MemcacheSocket, 0)
      ),

    gen_tcp:send(MemcacheSocket, "delete item-2\r\n"),

    ?assertEqual(
       {ok, <<"NOT_FOUND\r\n">>},
       gen_tcp:recv(MemcacheSocket, 0)
      ),

    gen_tcp:send(MemcacheSocket, "get item-1\r\n"),

    ?assertEqual(
       {ok, <<"END\r\n">>},
       gen_tcp:recv(MemcacheSocket, 0)
      ),

    gen_tcp:send(MemcacheSocket, "quit\r\n"),

    gen_tcp:close(MemcacheSocket),

    kai_coordinator:stop(),
    kai_store:stop(),
    kai_hash:stop(),
    kai_config:stop().
