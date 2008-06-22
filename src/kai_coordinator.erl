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

-module(kai_coordinator).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	 code_change/3]).
-export([map_in_get/4, map_in_put/4, map_in_delete/4]).
-export([stop/0, route/1]).

-include("kai.hrl").

-define(SERVER, ?MODULE).
-define(TIMEOUT_CLIENT, 3000).
-define(TIMEOUT_GATHER, 200).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], _Opts = []).

init(_Args) ->
    {ok, []}.

terminate(_Reason, _State) ->
    ok.

route({Type, Message}, State) ->
    % XXX should be routed to appropriate coordinator node
    Result =
	case Type of
	    get -> do_get(Message);
	    put -> do_put(Message);
	    delete -> do_delete(Message);
	    _Other -> {error, ebadrpc}
	end,
    {reply, Result, State}.

do_get(Key) ->
    {nodes, Nodes} = kai_hash:find_nodes(Key),
    Ref = make_ref(),
    lists:foreach(
      fun(Node) -> spawn(?MODULE, map_in_get, [Node, Key, Ref, self()]) end, % Don't link
      Nodes
     ),
    N = kai_config:get(n),
    R = kai_config:get(r),
    case gather_in_get(Ref, N, R, []) of
	[Data|RestData] -> uniq_in_get(RestData, [Data]);
	_NoData -> undefined
    end.

map_in_get(Node, Key, Ref, Pid) ->
    case kai_api:get(Node, Key) of
	{error, Reason} ->
	    kai_membership:check_node(Node),
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

gather_in_get(_Ref, _N, 0, Results) ->
    Results;
gather_in_get(_Ref, 0, _R, _Results) ->
    {error, enodata};
gather_in_get(Ref, N, R, Results) ->
    receive
	{Ref, Data} when is_record(Data, data) ->
	    gather_in_get(Ref, N-1, R-1, [Data|Results]);
	{Ref, undefined} ->
	    gather_in_get(Ref, N-1, R-1, Results);
	{Ref, _Other} ->
	    gather_in_get(Ref, N-1, R, Results)
    after ?TIMEOUT_GATHER ->
	    Results
    end.

uniq_in_get([], UniqData) ->
    UniqData;
uniq_in_get([Data|RestData], UniqData) ->
    Checksum = Data#data.checksum,
    case length(lists:filter(fun(U) -> Checksum =:= U#data.checksum end, UniqData)) of
	0 -> uniq_in_get(RestData, [Data|UniqData]);
	_ -> uniq_in_get(RestData, UniqData)
    end.

do_put(Data1) ->
    Key = Data1#data.key,
    {bucket, Bucket} = kai_hash:find_bucket(Key),
    {nodes, Nodes} = kai_hash:find_nodes(Bucket),
    Data2 = Data1#data{bucket=Bucket},
    Ref = make_ref(),
    lists:foreach(
      fun(Node) -> spawn(?MODULE, map_in_put, [Node, Data2, Ref, self()]) end,
      Nodes
     ),
    N = kai_config:get(n),
    W = kai_config:get(w),
    gather_in_put(Ref, N, W).

map_in_put(Node, Data, Ref, Pid) ->
    case kai_api:put(Node, Data) of
	{error, Reason} ->
	    kai_membership:check_node(Node),
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

gather_in_put(_Ref, _N, 0) ->
    ok;
gather_in_put(_Ref, 0, _W) ->
    {error, ebusy};
gather_in_put(Ref, N, W) ->
    receive
	{Ref, ok} ->
	    gather_in_put(Ref, N-1, W-1);
	{Ref, _Other} ->
	    gather_in_put(Ref, N-1, W)
    after ?TIMEOUT_GATHER ->
	    {error, etimedout}
    end.

do_delete(Key) ->
    {nodes, Nodes} = kai_hash:find_nodes(Key),
    Ref = make_ref(),
    lists:foreach(
      fun(Node) -> spawn(?MODULE, map_in_delete, [Node, Key, Ref, self()]) end,
      Nodes
     ),
    N = kai_config:get(n),
    W = kai_config:get(w),
    gather_in_delete(Ref, N, W, []).

map_in_delete(Node, Key, Ref, Pid) ->
    case kai_api:delete(Node, Key) of
	{error, Reason} ->
	    Pid ! {Ref, {error, Reason}};
	Other ->
	    Pid ! {Ref, Other}
    end.

gather_in_delete(_Ref, _N, 0, Results) ->
    case lists:member(ok, Results) of
	true -> ok;
	_ -> undefined
    end;
gather_in_delete(_Ref, 0, _W, _Results) ->
    {error, ebusy};
gather_in_delete(Ref, N, W, Results) ->
    receive
	{Ref, ok} ->
	    gather_in_delete(Ref, N-1, W-1, [ok|Results]);
	{Ref, undefined} ->
	    gather_in_delete(Ref, N-1, W-1, [undefined|Results]);
	{Ref, _Other} ->
	    gather_in_delete(Ref, N-1, W, Results)
    after ?TIMEOUT_GATHER ->
	    {error, etimedout}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({route, Request}, _From, State) ->
    route(Request, State).
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stop() ->
    gen_server:call(?SERVER, stop).
route(Request) ->
    gen_server:call(?SERVER, {route, Request}).
