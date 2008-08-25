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

-module(kai_sync).
-behaviour(gen_fsm).

-export([start_link/0, stop/0]).
-export([update_bucket/1, delete_bucket/1]).
-export([
    init/1, ready/2, handle_event/3, handle_sync_event/4, handle_info/3,
    terminate/3, code_change/4
]).

-include("kai.hrl").

-define(SERVER, ?MODULE).
-define(TIMEOUT, 3000).
-define(TIMER, 1000).

start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], _Opts = []).
    
init(_Args) ->
    {ok, ready, [], ?TIMER}.

terminate(_Reason, _StateName, _StateData) ->
    ok.

retrieve_data(_Node, []) ->
    ok;
retrieve_data(Node, [Metadata|Rest]) ->
    Key = Metadata#data.key,
    case kai_store:get(Key) of
        Data when is_record(Data, data) ->
            retrieve_data(Node, Rest);
        undefined ->
            case kai_api:get(Node, Key) of
                Data when is_record(Data, data) ->
                    kai_store:put(Data),
                    retrieve_data(Node, Rest);
                undefined ->
                    retrieve_data(Node, Rest);
                {error, Reason} ->
                    ?warning("retrieve_data/2: ~p", [{error, Reason}]),
                    {error, Reason}
            end
    end.

do_update_bucket(_Bucket, []) ->
    {error, enodata};
do_update_bucket(Bucket, [Node|Rest]) ->
    case kai_api:list(Node, Bucket) of
        {list_of_data, ListOfData} ->
            retrieve_data(Node, ListOfData);
        {error, Reason} ->
            ?warning("do_update_bucket/2: ~p", [{error, Reason}]),
            do_update_bucket(Bucket, Rest)
    end.

do_update_bucket(Bucket) ->
    {nodes, Nodes} = kai_hash:find_nodes(Bucket),
    LocalNode = kai_config:get(node),
    do_update_bucket(Bucket, Nodes -- [LocalNode]).

do_delete_bucket([]) ->
    ok;
do_delete_bucket([Metadata|Rest]) ->
    kai_store:delete(Metadata#data.key),
    do_delete_bucket(Rest);
do_delete_bucket(Bucket) ->
    {list_of_data, ListOfData} = kai_store:list(Bucket),
    do_delete_bucket(ListOfData).

ready({update_bucket, Bucket}, State) ->
    do_update_bucket(Bucket),
    {next_state, ready, State, ?TIMER};
ready({delete_bucket, Bucket}, State) ->
    do_delete_bucket(Bucket),
    {next_state, ready, State, ?TIMER};
ready(timeout, State) ->
    case kai_hash:choose_bucket_randomly() of
        {bucket, Bucket} -> do_update_bucket(Bucket);
        _                -> nop
    end,
    {next_state, ready, State, ?TIMER}.

handle_event(stop, _StateName, StateData) ->
    {stop, normal, StateData}.
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {next_state, wait, StateData, 3000}.
handle_info(_Info, _StateName, StateData) ->
    {next_state, ready, StateData, ?TIMER}.
code_change(_OldVsn, _StateName, StateData, _Extra) ->
    {ok, ready, StateData}.

stop() ->
    gen_fsm:send_all_state_event(?SERVER, stop).
update_bucket(Bucket) ->
    gen_fsm:send_event(?SERVER, {update_bucket, Bucket}).
delete_bucket(Bucket) ->
    gen_fsm:send_event(?SERVER, {delete_bucket, Bucket}).
