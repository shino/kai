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

-module(kai_memcache).
-behaviour(kai_tcp_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3]).

-include("kai.hrl").

-define(TIMEOUT_CLIENT, 3000).

start_link() ->
    kai_tcp_server:start_link(
        {local, ?MODULE},
        ?MODULE,
        [],
        #tcp_server_option{
            port            = kai_config:get(memcache_port),
            max_connections = kai_config:get(memcache_max_connections)
        }
    ).

stop() -> kai_tcp_server:stop(?MODULE).

init(_Args) -> {ok, {}}.

handle_call(Socket, Data, State) ->
    dispatch(Socket, string:tokens(binary_to_list(Data), " \r\n"), State).

dispatch(_Socket, ["get", Key], State) ->
    case kai_coordinator:route({get, #data{key=Key}}) of
        Data when is_list(Data) ->
            Response = get_response(Data),
            {reply, [Response|"END\r\n"], State};
        undefined ->
            {reply, <<"END\r\n">>, State};
        _Other ->
            send_error_and_close("Failed to read.", State)
    end;

dispatch(Socket, ["set", _Key, _Flags, "0", _Bytes] = Data, State) ->
    inet:setopts(Socket, [{packet, raw}]),
    Result = recv_set_data(Socket, Data, State),
    inet:setopts(Socket, [{packet, line}]),
    Result;

dispatch(_Socket, ["set", _Key, _Flags, _Exptime, _Bytes], State) ->
    {reply, <<"CLIENT_ERROR Exptime must be zero.\r\n">>, State};

dispatch(_Socket, ["delete", Key], State) ->
    case kai_coordinator:route({delete, #data{key=Key}}) of
        ok        -> {reply, <<"DELETED\r\n">>, State};
        undefined -> {reply, <<"NOT_FOUND\r\n">>, State};
        _Other    ->
            send_error_and_close(State, "Failed to delete.")
    end;

dispatch(_Socket, ["quit"], _State) -> quit;

dispatch(_Socket, _Unknown, _State) -> {reply, <<"ERROR\r\n">>}.

get_response(Data) ->
    lists:map(fun(Elem) ->
        Key = Elem#data.key,
        Flags = Elem#data.flags,
        Value = Elem#data.value,
        [
            io_lib:format("VALUE ~s ~s ~w", [Key, Flags, byte_size(Value)]),
            "\r\n", Value, "\r\n"
        ]
    end, Data).
 
recv_set_data(Socket, ["set", Key, Flags, "0", Bytes], State) ->
    case gen_tcp:recv(Socket, list_to_integer(Bytes), ?TIMEOUT_CLIENT) of
        {ok, Value} ->
            gen_tcp:recv(Socket, 2, ?TIMEOUT_CLIENT),
            case kai_coordinator:route(
                {put, #data{key=Key, flags=Flags, value=Value}}
            ) of
                ok ->
                    gen_tcp:send(Socket, <<"STORED\r\n">>),
                    {noreply, State};
                _Other ->
                    send_error_and_close("Failed to write.", State)
            end;
        _Other ->
            {noreply, State}
    end.

send_error_and_close(Message, State) ->
    ?warning("send_error_and_close/2: ~p", [Message]),
    {close, ["SERVER_ERROR ", Message, "\r\n"], State}.

