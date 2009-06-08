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

-module(kai_memcache_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{seq, [standalone, cluster, stats, version, no_such_command]}].

all() -> [{sequence, seq}].

init_per_testcase(standalone, Conf) ->
    init_node(Conf, 1, [{quorum, {1,1,1}}]);
init_per_testcase(_TestCase, Conf) ->
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
    Conf4.

end_per_testcase(standalone, Conf) ->
    end_node(Conf, 1);
end_per_testcase(_TestCase, Conf) ->
    end_node(Conf, 1),
    end_node(Conf, 2),
    end_node(Conf, 3).

standalone(_Conf) ->
    crud().

cluster(_Conf) ->
    crud().

crud() ->
    Socket = connect(),

    Value = <<"value1">>,
    Buf = io_lib:format("set key1 0 0 ~w\r\n~s\r\n", [byte_size(Value), Value]),
    gen_tcp:send(Socket, Buf),

    {ok, <<"STORED\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "get key1\r\n"),

    {ok, <<"VALUE key1 0 6\r\n">>} = gen_tcp:recv(Socket, 0),
    inet:setopts(Socket, [{packet, raw}]),
    {ok, <<"value1\r\n">>} = gen_tcp:recv(Socket, 6+2),
    inet:setopts(Socket, [{packet, line}]),
    {ok, <<"END\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "delete key1\r\n"),
    {ok, <<"DELETED\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "delete key1 0\r\n"),
    {ok, <<"NOT_FOUND\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "get key1\r\n"),
    {ok, <<"END\r\n">>} = gen_tcp:recv(Socket, 0),

    close(Socket).

stats(_Conf) ->
    Socket = connect(),

    Value = <<"value1">>,
    Buf = io_lib:format("set key1 0 0 ~w\r\n~s\r\n", [byte_size(Value), Value]),
    gen_tcp:send(Socket, Buf),

    {ok, <<"STORED\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "get key1\r\n"),

    {ok, <<"VALUE key1 0 6\r\n">>} = gen_tcp:recv(Socket, 0),
    inet:setopts(Socket, [{packet, raw}]),
    {ok, <<"value1\r\n">>} = gen_tcp:recv(Socket, 6+2),
    inet:setopts(Socket, [{packet, line}]),
    {ok, <<"END\r\n">>} = gen_tcp:recv(Socket, 0),

    gen_tcp:send(Socket, "stats\r\n"),
    Stats = recv_stats(Socket, []),

    ?assert(re(g(uptime, Stats), "[0-9]+")),
    ?assert(re(g(time, Stats), "[0-9]+")),
    ?assert(re(g(version, Stats), "[.0-9]+")),
    ?assert(re(g(bytes, Stats), "[0-9]+")),

    ?assertEqual("1", g(curr_items, Stats)),
    ?assertEqual("1", g(curr_connections, Stats)),
    ?assertEqual("1", g(cmd_get, Stats)),
    ?assertEqual("1", g(cmd_set, Stats)),
    ?assertEqual("6", g(bytes_read, Stats)),
    ?assertEqual("6", g(bytes_write, Stats)),

    ?assertEqual("127.0.0.1:11011", g(kai_node, Stats)),
    ?assertEqual("2,2,2", g(kai_quorum, Stats)),
    ?assertEqual("4", g(kai_buckets, Stats)),
    ?assertEqual("2", g(kai_virtual_nodes, Stats)),
    ?assertEqual("ets", g(kai_store, Stats)),
    ?assertEqual("127.0.0.1:11011 127.0.0.1:11012 127.0.0.1:11013", g(kai_curr_nodes, Stats)),
    ?assertEqual("0(0)", g(kai_unreconciled_get, Stats)),
%    ?assertEqual("0 1 2 3", g(kai_curr_buckets, Stats)),
    
    ?assert(re(g(erlang_procs, Stats), "[0-9]+")),
    ?assert(re(g(erlang_version, Stats), "[.0-9]+")),

    close(Socket).

recv_stats(Socket, Stats) ->
    {ok, Data} = gen_tcp:recv(Socket, 0),
    case Data of
        <<"STAT", _/binary>> ->
            ["STAT", Name|Values] =
                string:tokens(binary_to_list(Data), " \r\n"),
            Stats2 = [{list_to_atom(Name), string:join(Values, " ")} | Stats],
            recv_stats(Socket, Stats2);
        <<"END", _/binary>> ->
            Stats
    end.

version(_Conf) ->
    Socket = connect(),

    gen_tcp:send(Socket, "version\r\n"),

    {ok, Data} = gen_tcp:recv(Socket, 0),
    ["VERSION", Version] = string:tokens(binary_to_list(Data), " \r\n"),
    ?assert(re(Version, "[.0-9]+")),

    close(Socket).

no_such_command(_Conf) ->
    Socket = connect(),

    gen_tcp:send(Socket, "no_such_command\r\n"),
    {ok, <<"ERROR\r\n">>} = gen_tcp:recv(Socket, 0).

connect() ->
    {ok, Socket} =
        gen_tcp:connect({127,0,0,1}, 11211, [binary, {packet, line}, {active, false}]),
    Socket.

close(Socket) ->
    gen_tcp:send(Socket, "quit\r\n"),
    gen_tcp:close(Socket).
