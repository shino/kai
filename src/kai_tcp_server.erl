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

-module(kai_tcp_server).
-behaviour(supervisor).

-export([behaviour_info/1]).
-export([start_link/1, start_link/2, start_link/3, start_link/4, stop/0]).
-export([init/1, acceptor_init/5]).
-export([acceptor_start_link/4]).

-include("kai.hrl").

% Behaviour Callbacks
behaviour_info(callbacks) -> [{init, 1}, {handle_call, 3}];
behaviour_info(_Other)    -> undefined.

% Supervisor - tcp_server
%% External APIs
start_link(Mod)       -> start_link(Mod, [], {}).
start_link(Mod, Args) -> start_link(Mod, Args, {}).
start_link(Mod, Args, Option) ->
    supervisor:start_link(?MODULE, [?MODULE, Mod, Args, Option]).
start_link({_Destination, Name} = SupName, Mod, Args, Option) ->
    supervisor:start_link(SupName, ?MODULE, [Name, Mod, Args, Option]).

stop() ->
    case whereis(?MODULE) of
        Pid when pid(Pid) ->
            exit(Pid, normal),
            ok;
        _ -> not_started
    end.

%% Callbacks
init([Name, Mod, Args, Option]) ->
    case Mod:init(Args) of 
        {ok, State}    -> listen(State, Name, Mod, Option);
        {stop, Reason} -> Reason;
        Other          -> Other % 'ignore' is contained.
    end.

%% Internal Functions
listen(State, Name, Mod, Option) ->
    case gen_tcp:listen(
        Option#tcp_server_option.port,
        Option#tcp_server_option.listen
    ) of
        {ok, ListenSocket} ->
            init_acceptors(ListenSocket, State, Name, Mod, Option);
        {error, Reason} ->
            {stop, Reason}
    end.

init_acceptors(ListenSocket, State, Name, Mod, Option) ->
    #tcp_server_option{
        max_connections = MaxConn,
        max_restarts    = MaxRestarts,
        time            = Time,
        shutdown        = Shutdown
    } = Option,
    {ok, {{one_for_one, MaxRestarts, Time}, lists:map(
        fun (N) -> {
            list_to_atom(
                   atom_to_list(Name)
                ++ "_acceptor_"
                ++ integer_to_list(N)
            ),
            {
                ?MODULE,
                acceptor_start_link,
                [ListenSocket, State, Mod, Option]
            },
            permanent,
            Shutdown,
            worker,
            []
        } end,
        lists:seq(1, MaxConn)
    )}}.

% ProcLib - tcp_acceptor_N
%% External APIs
acceptor_start_link(ListenSocket, State, Mod, Option) ->
    proc_lib:start_link(
        ?MODULE, acceptor_init, [self(), ListenSocket, State, Mod, Option]
    ).

%% Callbacks
acceptor_init(Parent, ListenSocket, State, Mod, Option) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    acceptor_accept(ListenSocket, State, Mod, Option).

acceptor_accept(ListenSocket, State, Mod, Option) ->
    {ok, Socket} = gen_tcp:accept(
        ListenSocket, Option#tcp_server_option.accept_timeout
    ),
    acceptor_loop(
        proplists:get_value(active, Option#tcp_server_option.listen),
        Socket, State, Mod, Option
    ),
    acceptor_accept(ListenSocket, State, Mod, Option).

acceptor_loop(false, Socket, State, Mod, Option) ->
    case gen_tcp:recv(
        Socket,
        Option#tcp_server_option.recv_length,
        Option#tcp_server_option.recv_timeout
    ) of
        {ok, Data} ->
            call_mod(false, Socket, Data, State, Mod, Option);
        {error, closed} ->
            tcp_closed;
        {error, Reason} ->
            exit({error, Reason})
    end;

acceptor_loop(true, _DummySocket, State, Mod, Option) ->
    receive
        {tcp, Socket, Data} ->
            call_mod(true, Socket, Data, State, Mod, Option);
        {tcp_closed, _Socket} ->
            tcp_closed;
        Error ->
            exit({error, Error})
    after Option#tcp_server_option.recv_timeout ->
        exit({error, tcp_timeout})
    end.
 
call_mod(Active, Socket, Data, State, Mod, Option) ->
    case Mod:handle_call(Socket, Data, State) of
        {reply, DataToSend, State} ->
            gen_tcp:send(Socket, DataToSend),
            acceptor_loop(Active, Socket, State, Mod, Option);
        {noreply, State} ->
            acceptor_loop(Active, Socket, State, Mod, Option);
        {close, DataToSend, State} ->
            gen_tcp:send(Socket, DataToSend),
            gen_tcp:close(Socket);
        _Error ->
            gen_tcp:close(Socket)
    end.

