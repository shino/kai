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

-module(kai_store_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

-define(ETS_ARGS,  [{rpc_port, 11011},
                    {quorum, {3,2,2}},
                    {buckets, 4},
                    {virtual_nodes, 2},
                    {store, ets}]).
-define(DETS_ARGS, [{rpc_port, 11011},
                    {quorum, {3,2,2}},
                    {buckets, 4},
                    {virtual_nodes, 2},
                    {store, dets},
                    {dets_dir, "."},
                    {dets_tables, 2}]).

sequences() ->
    [{ ets, [ ets_crud,  ets_all_delete, ets_conflict,  ets_info,  ets_perf]},
     {dets, [dets_crud, dets_all_delete,dets_conflict, dets_info, dets_perf]}].

all() -> [{sequence, ets}, {sequence, dets}].

init_per_testcase(TestCase, Conf) ->
    case atom_to_list(TestCase) of
        "dets" ++ _ -> init(Conf, ?DETS_ARGS);
        _           -> init(Conf, ?ETS_ARGS)
    end.

init(Conf, Args) ->
    kai_config:start_link(Args),
    kai_store:start_link(),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_store:stop(),
    kai_config:stop(),
    file:delete("./1"), file:delete("./2"). %% Deletes dets files

ets_crud(_Conf) -> crud().
dets_crud(_Conf) -> crud().

crud() ->
    Data = #data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = vclock:fresh(),
      value         = <<"value1">>
     },
    ok = kai_store:put(Data),

    [Data] = kai_store:get(#data{key="key1", bucket=3}),
    undefined = kai_store:get(#data{key="key2", bucket=1}),

    {ok, [Key]} = kai_store:list(3),
    "key1" = Key#data.key,

    Data2 = #data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value2">>),
      flags         = "0",
      vector_clocks = vclock:increment(node1, Data#data.vector_clocks),
      value         = <<"value2">>
     },

    ok = kai_store:put(Data2),
    [Data2] = kai_store:get(#data{key="key1", bucket=3}),

    ok = kai_store:delete(#data{key="key1", bucket=3}),

    undefined = kai_store:get(#data{key="key1", bucket=3}),
    undefined = kai_store:delete(#data{key="key1", bucket=3}).

ets_conflict(_Conf) -> conflict().
dets_conflict(_Conf) -> conflict().

conflict() ->
    InitialVc = vclock:increment(node1, vclock:fresh()),
    Data = #data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = InitialVc,
      value         = <<"value1">>
     },
    ok = kai_store:put(Data),

    ConflictingVc = vclock:increment(node2, vclock:fresh()),
    ok = kai_store:put(Data#data{ vector_clocks = ConflictingVc }),
    ?assertEqual(2, length(kai_store:get(Data))),

    AscendingVc = vclock:increment(node2, ConflictingVc),
    ok = kai_store:put(Data#data{ vector_clocks = AscendingVc }),
    ?assertEqual(2, length(kai_store:get(Data))),

    MergedVc = vclock:merge([InitialVc, AscendingVc]),
    ok = kai_store:put(Data#data{ vector_clocks = MergedVc }),
    ?assertEqual(1, length(kai_store:get(Data))),
    ok.

ets_all_delete(_Conf) -> all_delete().
dets_all_delete(_Conf) -> all_delete().

all_delete() ->
    Data = #data{
      key           = "key1",
      bucket        = 3,
      last_modified = now(),
      checksum      = erlang:md5(<<"value1">>),
      flags         = "0",
      vector_clocks = vclock:increment(node1, vclock:fresh()),
      value         = <<"value1">>
     },
    ok = kai_store:put(Data),

    ok = kai_store:put(
           Data#data{vector_clocks = vclock:increment(node2, vclock:fresh())}),
    ?assertEqual(2, length(kai_store:get(Data))),

    ok = kai_store:delete(Data),
    undefined = kai_store:get(Data),
    ok.

ets_info(_Conf) -> info().
dets_info(_Conf) -> info().

info() ->
    ?assert(is_integer(kai_store:info(bytes))),
    ?assertEqual(0, kai_store:info(size)),

    ok = kai_store:put(#data{
        key           = "key1",
        bucket        = 3,
        last_modified = now(),
        checksum      = erlang:md5(<<"value1">>),
        flags         = "0",
        vector_clocks = vclock:fresh(),
        value         = <<"value1">>
    }),

    ?assert(is_integer(kai_store:info(bytes))),
    ?assertEqual(1, kai_store:info(size)),

    ok = kai_store:delete(#data{key="key1", bucket=3}),

    ?assert(is_integer(kai_store:info(bytes))),
    ?assertEqual(0, kai_store:info(size)).

ets_perf(_Conf) -> perf(1000).
dets_perf(_Conf) -> perf(10).

perf(T) ->
    {Usec, _} = timer:tc(?MODULE, perf_put, [T]),
    ct:log("Average time to put a key: ~p us", [Usec/T]),

    {Usec2, _} = timer:tc(?MODULE, perf_get, [T]),
    ct:log("Average time to get a key: ~p us", [Usec2/T]).

perf_put(0) -> ok;
perf_put(I) ->
    Key = "key" ++ integer_to_list(I),
    Value = <<0:1024>>, %% 1 KB
    ok = kai_store:put(#data{
      key           = Key,
      bucket        = 0,
      last_modified = now(),
      checksum      = erlang:md5(Value),
      flags         = "0",
      vector_clocks = vclock:fresh(),
      value         = Value
     }),
    perf_put(I-1).

perf_get(0) -> ok;
perf_get(I) ->
    Key = "key" ++ integer_to_list(I),
    Data = kai_store:get(#data{key=Key, bucket=0}),
    ?assertEqual(
       Key,
       Data#data.key
      ),
    perf_get(I-1).
