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
-export([start_link/1, start_link/2, start_link/3, start_link/4]).
-export([stop/0, stop/1]).
-export([init/1, acceptor_init/5]).
-export([acceptor_start_link/5]).

-include("kai.hrl").

% Behaviour Callbacks
behaviour_info(callbacks) -> [{init, 1}, {handle_call, 3}];
behaviour_info(_Other)    -> undefined.

% Supervisor - tcp_server
%% External APIs
start_link(Mod)       -> start_link(Mod, []).
start_link(Mod, Args) -> start_link(Mod, Args, #tcp_server_option{}).
start_link(Mod, Args, Option) ->
    start_link({local, ?MODULE}, Mod, Args, Option).
start_link(Name, Mod, Args, Option) ->
    supervisor:start_link(Name, ?MODULE, [Name, Mod, Args, Option]).

stop() -> stop(?MODULE).
stop(Name) ->
    case whereis(Name) of
        Pid when is_pid(Pid) ->
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
            ?warning(io_lib:format("listen(~p) ~p", [Mod, {error, Reason}])),
            {stop, Reason}
    end.

init_acceptors(ListenSocket, State, {Dest, Name}, Mod, Option) ->
    #tcp_server_option{
        max_processes = MaxProcesses,
        max_restarts  = MaxRestarts,
        time          = Time,
        shutdown      = Shutdown
    } = Option,
    {ok, {{one_for_one, MaxRestarts, Time}, lists:map(
        fun (N) ->
            AcceptorName = list_to_atom(
                atom_to_list(Name) ++ "_acceptor_" ++ integer_to_list(N)
            ),
            {
                AcceptorName,
                {
                    ?MODULE,
                    acceptor_start_link,
                    [{Dest, AcceptorName}, ListenSocket, State, Mod, Option]
                },
                permanent,
                Shutdown,
                worker,
                []
            }
        end,
        lists:seq(1, MaxProcesses)
    )}}.

% ProcLib - tcp_acceptor_N
%% External APIs
acceptor_start_link({Dest, Name}, ListenSocket, State, Mod, Option) ->
    {ok, Pid} = proc_lib:start_link(
        ?MODULE, acceptor_init, [self(), ListenSocket, State, Mod, Option]
    ),
    case Dest of
        local   -> register(Name, Pid);
        _Global -> global:register_name(Name, Pid)
    end,
    {ok, Pid}.

%% Callbacks
acceptor_init(Parent, ListenSocket, State, Mod, Option) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    acceptor_accept(ListenSocket, State, Mod, Option).

acceptor_accept(ListenSocket, State, Mod, Option) ->
    case gen_tcp:accept(
        ListenSocket, Option#tcp_server_option.accept_timeout
    ) of 
        {ok, Socket} ->
            acceptor_loop(
                proplists:get_value(active, Option#tcp_server_option.listen),
                Socket, State, Mod, Option
            ),
            gen_tcp:close(Socket);
        {error, Reason} ->
            ?warning(io_lib:format("acceptor_accept(~p) ~p", [Mod, {error, Reason}])),
            timer:sleep(Option#tcp_server_option.accept_error_sleep_time)
    end,
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
            ?warning(io_lib:format("acceptor_loop(~p) ~p", [Mod, {error, Reason}])),
            error
    end;

acceptor_loop(true, _DummySocket, State, Mod, Option) ->
    receive
        {tcp, Socket, Data} ->
            call_mod(true, Socket, Data, State, Mod, Option);
        {tcp_closed, _Socket} ->
            tcp_closed;
        Error ->
            ?warning(io_lib:format("acceptor_loop(~p) ~p", [Mod, {error, Error}])),
            error
    after Option#tcp_server_option.recv_timeout ->
        tcp_timeout
    end.
 
call_mod(Active, Socket, Data, State, Mod, Option) ->
    case Mod:handle_call(Socket, Data, State) of
        {reply, DataToSend, State} ->
            gen_tcp:send(Socket, DataToSend),
            acceptor_loop(Active, Socket, State, Mod, Option);
        {noreply, State} ->
            acceptor_loop(Active, Socket, State, Mod, Option);
        {close, State} ->
            tcp_closed;
        {close, DataToSend, State} ->
            gen_tcp:send(Socket, DataToSend);
        Other ->
            ?warning(io_lib:format("call_mod(~p) ~p", [Mod, {unexpected_result, Other}]))
    end.

