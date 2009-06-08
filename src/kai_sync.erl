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

-module(kai_sync).
-behaviour(gen_fsm).

-export([start_link/0, stop/0]).
-export([update_bucket/1, delete_bucket/1]).
-export([
    init/1, ready/2, handle_event/3, handle_sync_event/4, handle_info/3,
    terminate/3, code_change/4
]).

-include("kai.hrl").

-record(state, {node, interval}).

-define(SERVER, ?MODULE).

start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], _Opts = []).
    
init(_Args) ->
    [LocalNode, Interval] = kai_config:get([node, sync_interval]),
    {ok, ready, #state{
           node     = LocalNode,
           interval = Interval
          }, Interval}.

terminate(_Reason, _StateName, _StateData) ->
    ok.

do_update_bucket(Bucket, State) when is_record(State, state) ->
    {ok, Nodes} = kai_hash:find_nodes(Bucket),
    do_update_bucket(Bucket, Nodes -- [State#state.node], State).

do_update_bucket(_Bucket, [], _State) ->
    {error, enodata};
do_update_bucket(Bucket, [Node|Rest], State) ->
    case kai_rpc:list(Node, State#state.node, Bucket) of
        {ok, KeyList} ->
            retrieve_data(Node, KeyList, State);
        {error, Reason} ->
            ?warning(io_lib:format("do_update_bucket/2: ~p", [{error, Reason}])),
            do_update_bucket(Bucket, Rest, State)
    end.

retrieve_data(_Node, [], _State) ->
    ok;
retrieve_data(Node, [Metadata|Rest], State) ->
    case kai_store:get(Metadata) of
        Data when is_record(Data, data) ->
            retrieve_data(Node, Rest, State);
        undefined ->
            case kai_rpc:get(Node, State#state.node, Metadata) of
                Data when is_record(Data, data) ->
                    kai_store:put(Data),
                    retrieve_data(Node, Rest, State);
                undefined ->
                    retrieve_data(Node, Rest, State);
                {error, Reason} ->
                    ?warning(io_lib:format("retrieve_data/2: ~p", [{error, Reason}])),
                    {error, Reason}
            end
    end.

do_delete_bucket([]) ->
    ok;
do_delete_bucket([Metadata|Rest]) ->
    kai_store:delete(Metadata),
    do_delete_bucket(Rest);
do_delete_bucket(Bucket) ->
    {ok, KeyList} = kai_store:list(Bucket),
    do_delete_bucket(KeyList).

ready({update_bucket, Bucket}, State) ->
    do_update_bucket(Bucket, State),
    {next_state, ready, State, State#state.interval};
ready({delete_bucket, Bucket}, State) ->
    do_delete_bucket(Bucket),
    {next_state, ready, State, State#state.interval};
ready(timeout, State) ->
    case kai_hash:choose_bucket_randomly() of
        {ok, Bucket} -> do_update_bucket(Bucket, State);
        _            -> nop
    end,
    {next_state, ready, State, State#state.interval}.

handle_event(stop, _StateName, StateData) ->
    {stop, normal, StateData}.
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {next_state, wait, StateData, _Interval = 1000}.
handle_info(_Info, _StateName, StateData) ->
    {next_state, ready, StateData, _Interval = 1000}.
code_change(_OldVsn, _StateName, StateData, _Extra) ->
    {ok, ready, StateData}.

stop() ->
    gen_fsm:send_all_state_event(?SERVER, stop).
update_bucket(Bucket) ->
    gen_fsm:send_event(?SERVER, {update_bucket, Bucket}).
delete_bucket(Bucket) ->
    gen_fsm:send_event(?SERVER, {delete_bucket, Bucket}).
