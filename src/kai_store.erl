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

-module(kai_store).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	 code_change/3]).
-export([stop/0, list/1, get/1, put/1, delete/1]).

-include("kai.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], _Opts = []).

init(_Args) ->
    ets:new(data, [set, private, named_table, {keypos, 2}]),
    {ok, []}.

terminate(_Reason, _State) ->
    ets:delete(data),
    ok.

do_list(Bucket, State) ->
    Head = #data{key='$1', bucket=Bucket, last_modified='$2', vector_clocks='$3', 
		 checksum='$4', flags='_', value='_'},
    Cond = [],
    Body = [{#data{key='$1', bucket=Bucket, last_modified='$2', vector_clocks='$3', checksum='$4'}}],
    ListOfData = ets:select(data, [{Head, Cond, Body}]),
    {reply, {list_of_data, ListOfData}, State}.

do_get(Key, State) ->
    case ets:lookup(data, Key) of
	[Data] -> {reply, Data, State};
	_ -> {reply, undefined, State}
    end.

do_put(Data, State) when is_record(Data, data) ->
    ets:insert(data, Data),
    {reply, ok, State}.

do_delete(Key, State) ->
    case ets:lookup(data, Key) of
	[_Data] ->
	    ets:delete(data, Key),
	    {reply, ok, State};
	_ ->
	    {reply, undefined, State}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({list, Bucket}, _From, State) ->
    do_list(Bucket, State);
handle_call({get, Key}, _From, State) ->
    do_get(Key, State);
handle_call({put, Data}, _From, State) ->
    do_put(Data, State);
handle_call({delete, Key}, _From, State) ->
    do_delete(Key, State).
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stop() ->
    gen_server:call(?SERVER, stop).
list(Bucket) ->
    gen_server:call(?SERVER, {list, Bucket}).
get(Key) ->
    gen_server:call(?SERVER, {get, Key}).
put(Data) ->
    gen_server:call(?SERVER, {put, Data}).
delete(Key) ->
    gen_server:call(?SERVER, {delete, Key}).
