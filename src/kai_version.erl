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

-module(kai_version).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	 code_change/3]).
-export([stop/0, update/1, order/1]).

-include("kai.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], _Opts = []).

init(_Args) ->
    {ok, []}.

terminate(_Reason, _State) ->
    ok.

update(Data, State) ->
    % TODO: update #data.vector_clocks
    {reply, Data#data{last_modified=now()}, State}.

do_order([], UniqData) ->
    UniqData;
do_order([Data|RestData], UniqData) ->
    % TODO: resolve ordering of versions by using VectorClocks
    Checksum = Data#data.checksum,
    case length(lists:filter(fun(U) -> Checksum =:= U#data.checksum end, UniqData)) of
	0 -> do_order(RestData, [Data|UniqData]);
	_ -> do_order(RestData, UniqData)
    end.

order([Data|Rest], State) ->
    OrderedData = do_order(Rest, [Data]),
    {reply, OrderedData, State};
order(_Other, State) ->
    {reply, undefined, State}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({update, Data}, _From, State) ->
    update(Data, State);
handle_call({order, ListOfData}, _From, State) ->
    order(ListOfData, State).
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stop() ->
    gen_server:call(?SERVER, stop).
update(Data) ->
    gen_server:call(?SERVER, {update, Data}).
order(ListOfData) ->
    gen_server:call(?SERVER, {order, ListOfData}).
