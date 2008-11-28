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

-module(kai_store_dets).
-behaviour(gen_server).

-export([start_link/1]).
-export([
    init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3
]).

-include("kai.hrl").

start_link(Server) ->
    gen_server:start_link({local, Server}, ?MODULE, [], _Opts = []).

init(_Args) ->
    Dir = kai_config:get(dets_dir),
    NumberOfTables = kai_config:get(number_of_tables),
    Tables =
        lists:map(
          fun(I) ->
                  Name = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(I)),
                  File = Dir ++ "/" ++ integer_to_list(I),
                  case dets:open_file(Name, [{type, set}, {keypos, 2}, {file, File}]) of
                      {ok, Table} -> {I, Table};
                      {error, Reason} -> ?info(Reason),
                                         exit(Reason)
                  end
          end,
          lists:seq(1, NumberOfTables)
         ),
    {ok, Tables}.

terminate(_Reason, Tables) ->
    lists:foreach(
      fun({_I, Table}) -> dets:close(Table) end,
      Tables
     ),
    ok.

bucket_to_table(Bucket, Tables) ->
    NumberOfTables = kai_config:get(number_of_tables),
    I = Bucket rem NumberOfTables + 1,
    proplists:get_value(I, Tables).

do_list(Bucket, Tables) ->
    Table = bucket_to_table(Bucket, Tables),
    Head = #data{
        key           = '$1',
        bucket        = Bucket,
        last_modified = '$2',
        vector_clocks = '$3', 
        checksum      = '$4',
        flags         = '_',
        value         = '_'
    },
    Cond = [],
    Body = [{#data{
        key           = '$1',
        bucket        = Bucket,
        last_modified = '$2',
        vector_clocks = '$3',
        checksum      = '$4'
    }}],
    ListOfData = dets:select(Table, [{Head, Cond, Body}]),
    {reply, {list_of_data, ListOfData}, Tables}.

do_get(#data{key=Key, bucket=Bucket} = _Data, Tables) ->
    Table = bucket_to_table(Bucket, Tables),
    case dets:lookup(Table, Key) of
        [Data] -> {reply, Data, Tables};
        _      -> {reply, undefined, Tables}
    end.

do_put(Data, Tables) when is_record(Data, data) ->
    Table = bucket_to_table(Data#data.bucket, Tables),
    case dets:lookup(Table, Data#data.key) of
        [StoredData] ->
            case vclock:descends(Data#data.vector_clocks, StoredData#data.vector_clocks) of
                true -> insert_and_reply(Data, Table, Tables);
                _ -> {reply, {error, "stale or concurrent state found in kai_store"}, Tables}
            end;
        _ -> insert_and_reply(Data, Table, Tables)
    end.

insert_and_reply(Data, Table, Tables) ->
    dets:insert(Table, Data),
    dets:sync(Table),
    {reply, ok, Tables}.

do_delete(#data{key=Key, bucket=Bucket} = _Data, Tables) ->
    Table = bucket_to_table(Bucket, Tables),
    case dets:lookup(Table, Key) of
        [_Data2] ->
            dets:delete(Table, Key),
            dets:sync(Table),
            {reply, ok, Tables};
        _ ->
            {reply, undefined, Tables}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({list, Bucket}, _From, State) ->
    do_list(Bucket, State);
handle_call({get, Data}, _From, State) ->
    do_get(Data, State);
handle_call({put, Data}, _From, State) ->
    do_put(Data, State);
handle_call({delete, Data}, _From, State) ->
    do_delete(Data, State).
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
