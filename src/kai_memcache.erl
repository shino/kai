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

-export([start_link/0]).
-export([init/1, proc/1]).

-include("kai.hrl").

-define(TIMEOUT_CLIENT, 3000).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

init(Pid) ->
    proc_lib:init_ack(Pid, {ok, self()}),
    Port = kai_config:get(memcache_port),
    {ok, ListeningSocket} =
	gen_tcp:listen(Port, [binary, {packet, line}, {active, false}, {reuseaddr, true}]),
    accept(ListeningSocket).

accept(ListeningSocket) ->
    {ok, MemcacheSocket} = gen_tcp:accept(ListeningSocket),
    Pid = spawn(?MODULE, proc, [MemcacheSocket]), % Don't link
    gen_tcp:controlling_process(MemcacheSocket, Pid),
    accept(ListeningSocket).

proc(MemcacheSocket) ->
    case gen_tcp:recv(MemcacheSocket, 0) of % Don't timeout
        {ok, Line} ->
            Token = string:tokens(binary_to_list(Line), " \r\n"),
            case Token of
                ["get", Key] ->
                    get(MemcacheSocket, Key);
                ["set", Key, Flags, Exptime, Bytes] ->
		    case list_to_integer(Exptime) of
			0 ->
			    put(MemcacheSocket, Key, Flags, list_to_integer(Bytes));
			_Exptime ->
			    gen_tcp:send(MemcacheSocket, "CLIENT_ERROR Exptime must be zero.\r\n")
		    end;
                ["delete", Key] ->
		    delete(MemcacheSocket, Key);
                ["quit"] ->
		    gen_tcp:close(MemcacheSocket);
                _Unknown ->
		    gen_tcp:send(MemcacheSocket, "ERROR\r\n")
            end,
            proc(MemcacheSocket);
	Other ->
	    Other
    end.

send_error_and_close(MemcacheSocket, Message) ->
    gen_tcp:send(MemcacheSocket, ["SERVER_ERROR ", Message, "\r\n"]),
    gen_tcp:close(MemcacheSocket).
    
get(MemcacheSocket, Key) ->
    case kai_coordinator:route({get, Key}) of
	Data when is_list(Data) ->
	    Response =
		lists:map(
		  fun(D) ->
			  Key = D#data.key,
			  Flags = D#data.flags,
			  Value = D#data.value,
			  [io_lib:format("VALUE ~s ~s ~w", [Key, Flags, ?byte_size(Value)]),
			   "\r\n", Value, "\r\n"]
		  end,
		  Data
		 ),
	    gen_tcp:send(MemcacheSocket, [Response|"END\r\n"]);
	undefined ->
	    gen_tcp:send(MemcacheSocket, "END\r\n");
	_Other ->
	    send_error_and_close(MemcacheSocket, "Failed to read.")
    end.

put(MemcacheSocket, Key, Flags, Bytes) ->
    inet:setopts(MemcacheSocket, [{packet, raw}]),
    case gen_tcp:recv(MemcacheSocket, Bytes, ?TIMEOUT_CLIENT) of
        {ok, Value} ->
	    gen_tcp:recv(MemcacheSocket, 2, ?TIMEOUT_CLIENT),
	    Data = #data{key=Key, last_modified=now(),
			 checksum=erlang:md5(Value), flags=Flags, value=Value},
	    case kai_coordinator:route({put, Data}) of
		ok ->
		    gen_tcp:send(MemcacheSocket, "STORED\r\n");
		_Other ->
		    send_error_and_close(MemcacheSocket, "Failed to write.")
	    end;
	_Other ->
	    nop
    end,
    inet:setopts(MemcacheSocket, [{packet, line}]).

delete(MemcacheSocket, Key) ->
    case kai_coordinator:route({delete, Key}) of
	ok ->
	    gen_tcp:send(MemcacheSocket, "DELETED\r\n");
	undefined ->
	    gen_tcp:send(MemcacheSocket, "NOT_FOUND\r\n");
	_Other ->
	    send_error_and_close(MemcacheSocket, "Failed to delete.")
    end.
