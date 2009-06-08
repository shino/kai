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

-module(kai_connection_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

-record(connection, {node, available, socket}). %% Defined in src/kai_connection.erl

-define(UNKNOWN, {{127,0,0,1}, 1}).
-define(MAX_CONNS, 4).

sequences() ->
    [{seq, [lease, return, close, unknown, multiple_processes, max_connections,
            lru]}].

all() -> [{sequence, seq}].

init_per_testcase(_TestCase, Conf) ->
    kai_config:start_link([{rpc_port, ?PORT1},
                           {quorum, {3,2,2}},
                           {max_connections, ?MAX_CONNS},
                           {buckets, 4},
                           {virtual_nodes, 2}]),
    kai_connection:start_link(),
    spawn_link(?MODULE, echo_start, [?PORT2]),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_connection:stop(),
    kai_config:stop().

lease(_Conf) ->
    {ok, []} = kai_connection:connections(),

    {ok, Socket} = kai_connection:lease(?NODE2, self()),
    ?assert(is_port(Socket)),

    gen_tcp:send(Socket, term_to_binary(ok)),
    ok = receive {tcp, _, Bin} -> binary_to_term(Bin) end,

    {ok, [_]} = kai_connection:connections(),

    {ok, Socket2} =
        kai_connection:lease(?NODE2, self(), [{active, true}, {packet, 4}]),
    ?assert(Socket =/= Socket2),

    {ok, [_,_]} = kai_connection:connections().

return(_Conf) ->
    {ok, Socket} = kai_connection:lease(?NODE2, self()),
    ?assert(is_port(Socket)),
    
    {ok, [_]} = kai_connection:connections(),

    ok = kai_connection:return(Socket),

    {ok, [_]} = kai_connection:connections(),

    {ok, Socket2} = kai_connection:lease(?NODE2, self()),
    ?assert(Socket =:= Socket2),

    {ok, [_]} = kai_connection:connections().

close(_Conf) ->
    {ok, Socket} = kai_connection:lease(?NODE2, self()),
    ?assert(is_port(Socket)),

    {ok, [_]} = kai_connection:connections(),

    ok = kai_connection:close(Socket),

    {ok, []} = kai_connection:connections().

unknown(_Conf) ->
    {error, econnrefused} = kai_connection:lease(?UNKNOWN, self()).

multiple_processes(_Conf) ->
    {ok, Socket} = kai_connection:lease(?NODE2, self()),
    ?assert(is_port(Socket)),

    lists:foreach(
      fun(_) ->
              spawn_link(?MODULE, lease_and_send, [self()]),
              ?assert(receive ok -> true; _ -> false end)
      end,
      lists:seq(1, 4)
     ).

max_connections(_Conf) ->
    Sockets =
        lists:map(
          fun(_) ->
                  {ok, Socket} = kai_connection:lease(?NODE2, self()),
                  Socket
          end,
          lists:seq(1, ?MAX_CONNS + 1)
         ),

    %% The number of connections can be greater than MAX_CONNS when all
    %% connections are in use.
    {ok, Conns} = kai_connection:connections(),
    ?assertEqual(?MAX_CONNS + 1, length(Conns)),

    %% The number of connections equals to MAX_CONNS, because a connection has
    %% been returned.
    [Socket|_] = Sockets,
    ok = kai_connection:return(Socket),
    {ok, Conns2} = kai_connection:connections(),
    ?assertEqual(?MAX_CONNS, length(Conns2)).

lru(_Conf) ->
    {ok, _Socket} = kai_connection:lease(?NODE2, self()),
    {ok, Socket2} = kai_connection:lease(?NODE2, self()),
    {ok, Socket3} = kai_connection:lease(?NODE2, self()),

    %% Here, the connections are ordered like 3, 2, and 1.
    {ok, [Conn|_]} = kai_connection:connections(),
    Socket3 = Conn#connection.socket,

    %% Socket2 is moved to the head.
    ok = kai_connection:return(Socket2),
    {ok, [Conn2|_]} = kai_connection:connections(),
    Socket2 = Conn2#connection.socket.

echo_start(Port) ->
    {ok, ListenSocket} =
        gen_tcp:listen(Port, [binary, {packet, 4}, {reuseaddr, true}]),
    echo_accpet(ListenSocket).

echo_accpet(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    Pid = spawn_link(?MODULE, echo_proc, [Socket]),
    gen_tcp:controlling_process(Socket, Pid),
    echo_accpet(ListenSocket).

echo_proc(Socket) ->
    receive {tcp, Socket, Bin} -> gen_tcp:send(Socket, Bin) end.

lease_and_send(Pid) ->
    {ok, Socket} = kai_connection:lease(?NODE2, self()),
    ok = gen_tcp:send(Socket, term_to_binary(ok)),
    Pid ! receive {tcp, Socket, Bin} -> binary_to_term(Bin) end.
