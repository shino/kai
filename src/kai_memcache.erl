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
-export([init/1, proc/1, get_map/4, put_map/4, delete_map/4]).

-include("kai.hrl").

-define(TIMEOUT_CLIENT, 3000).
-define(TIMEOUT_GATHER, 200).

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
                ["stats"] ->
		    gen_tcp:send(MemcacheSocket, "SERVER_ERROR 'stats' not supported.\r\n");
                ["quit"] ->
		    gen_tcp:close(MemcacheSocket);
                _Unknown ->
		    gen_tcp:send(MemcacheSocket, "ERROR\r\n")
            end,
            proc(MemcacheSocket);
	Other ->
	    Other
    end.

get(MemcacheSocket, Key) ->
    {nodes, Nodes} = kai_hash:find_nodes(Key),
    Ref = make_ref(),
    lists:foreach(
      fun(Node) -> spawn(?MODULE, get_map, [Node, Key, Ref, self()]) end, % Don't link
      Nodes
     ),
    N = kai_config:get(n),
    R = kai_config:get(r),
    Response =
	case get_gather(Ref, N, R, []) of
	    [Data|RestData] ->
		lists:map(
		  fun(D) ->
			  Key = D#data.key,
			  Flags = D#data.flags,
			  Value = D#data.value,
			  [io_lib:format("VALUE ~s ~s ~w", [Key, Flags, ?byte_size(Value)]),
			   "\r\n", Value, "\r\n"]
		  end,
		  get_uniq(RestData, [Data])
		 );
	    _NoData -> []
	end,
    gen_tcp:send(MemcacheSocket, [Response|"END\r\n"]).

get_map(Node, Key, Ref, Pid) ->
    case kai_api:get(Node, Key) of
	{error, Reason} ->
	    kai_network:check_node(Node),
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

get_gather(_Ref, _N, 0, Results) ->
    Results;
get_gather(_Ref, 0, _R, _Results) ->
    {error, enodata};
get_gather(Ref, N, R, Results) ->
    receive
	{Ref, Data} when is_record(Data, data) ->
	    get_gather(Ref, N-1, R-1, [Data|Results]);
	{Ref, undefined} ->
	    get_gather(Ref, N-1, R-1, Results);
	{Ref, _Other} ->
	    get_gather(Ref, N-1, R, Results)
    after ?TIMEOUT_GATHER ->
	    Results
    end.

get_uniq([], UniqData) ->
    UniqData;
get_uniq([Data|RestData], UniqData) ->
    Checksum = Data#data.checksum,
    case length(lists:filter(fun(U) -> Checksum =:= U#data.checksum end, UniqData)) of
	0 -> get_uniq(RestData, [Data|UniqData]);
	_ -> get_uniq(RestData, UniqData)
    end.

put(MemcacheSocket, Key, Flags, Bytes) ->
    inet:setopts(MemcacheSocket, [{packet, raw}]),
    case gen_tcp:recv(MemcacheSocket, Bytes, ?TIMEOUT_CLIENT) of
        {ok, Value} ->
	    {bucket, Bucket} = kai_hash:find_bucket(Key),
	    Data = #data{key=Key, bucket=Bucket, last_modified=now(),
			 checksum=erlang:md5(Value), flags=Flags, value=Value},
	    {nodes, Nodes} = kai_hash:find_nodes(Bucket),
	    Ref = make_ref(),
	    lists:foreach(
	      fun(Node) -> spawn(?MODULE, put_map, [Node, Data, Ref, self()]) end,
	      Nodes
	     ),
	    N = kai_config:get(n),
	    W = kai_config:get(w),
	    case put_gather(Ref, N, W) of
		ok ->
		    gen_tcp:send(MemcacheSocket, "STORED\r\n");
		_Other ->
		    gen_tcp:send(MemcacheSocket, "SERVER_ERROR Failed to write.\r\n"),
		    gen_tcp:close(MemcacheSocket)
	    end,
	    gen_tcp:recv(MemcacheSocket, 2, ?TIMEOUT_CLIENT);
        Other ->
	    Other
    end,
    inet:setopts(MemcacheSocket, [{packet, line}]).

put_map(Node, Data, Ref, Pid) ->
    case kai_api:put(Node, Data) of
	{error, Reason} ->
	    kai_network:check_node(Node),
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

put_gather(_Ref, _N, 0) ->
    ok;
put_gather(_Ref, 0, _W) ->
    {error, ebusy};
put_gather(Ref, N, W) ->
    receive
	{Ref, ok} ->
	    put_gather(Ref, N-1, W-1);
	{Ref, _Other} ->
	    put_gather(Ref, N-1, W)
    after ?TIMEOUT_GATHER ->
	    {error, etimedout}
    end.

delete(MemcacheSocket, Key) ->
    {nodes, Nodes} = kai_hash:find_nodes(Key),
    Ref = make_ref(),
    lists:foreach(
      fun(Node) -> spawn(?MODULE, delete_map, [Node, Key, Ref, self()]) end,
      Nodes
     ),
    N = kai_config:get(n),
    W = kai_config:get(w),
    case delete_gather(Ref, N, W, []) of
	ok ->
	    gen_tcp:send(MemcacheSocket, "DELETED\r\n");
	undefined ->
	    gen_tcp:send(MemcacheSocket, "NOT_FOUND\r\n");
	_Other ->
	    gen_tcp:send(MemcacheSocket, "SERVER_ERROR Failed to delete.\r\n"),
	    gen_tcp:close(MemcacheSocket)
    end.

delete_map(Node, Key, Ref, Pid) ->
    case kai_api:delete(Node, Key) of
	{error, Reason} ->
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

delete_gather(_Ref, _N, 0, Results) ->
    case lists:member(ok, Results) of
	true -> ok;
	_ -> undefined
    end;
delete_gather(_Ref, 0, _W, _Results) ->
    {error, ebusy};
delete_gather(Ref, N, W, Results) ->
    receive
	{Ref, ok} ->
	    delete_gather(Ref, N-1, W-1, [ok|Results]);
	{Ref, undefined} ->
	    delete_gather(Ref, N-1, W-1, [undefined|Results]);
	{Ref, _Other} ->
	    delete_gather(Ref, N-1, W, Results)
    after ?TIMEOUT_GATHER ->
	    {error, etimedout}
    end.
