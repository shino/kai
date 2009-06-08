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

-module(kai_version_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() ->
    [{seq, [update, order, cas_unique1, cas_unique2, cas_unique7, cas_unique16]}].

all() -> [{sequence, seq}].

init_per_testcase(_TestCase, Conf) ->
    kai_config:start_link([{rpc_port, 11011},
                           {quorum, {3,2,2}},
                           {buckets, 4},
                           {virtual_nodes, 2}]),
    kai_version:start_link(),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_version:stop(),
    kai_config:stop().

update(_Conf) ->
    VClock1 = vclock:increment(kai_config:get(node), vclock:fresh()),
    Data1 = #data{
      vector_clocks = VClock1
     },
    {ok, Data2} = kai_version:update(Data1),
    ?assert(is_list(Data2#data.vector_clocks)),
    ?assert(vclock:descends(Data2#data.vector_clocks, Data1#data.vector_clocks)),
    ?assertNot(vclock:descends(Data1#data.vector_clocks, Data2#data.vector_clocks)).

order(_Conf) ->
    VClock1 = vclock:increment(node1, vclock:fresh()),
    Data1 = #data{
      vector_clocks = VClock1
     },

    %% Trivial case
    ?assertEqual(1, length(kai_version:order([Data1]))),

    %% Two concurrent data
    Data2 = Data1#data{
              vector_clocks = vclock:increment(otherNode, vclock:fresh())
             },
    [_,_] = kai_version:order([Data1, Data2]),

    %% One data is dropped
    Data3 = Data1#data{
              vector_clocks = vclock:increment(otherNode2, Data1#data.vector_clocks)
             },
    [_,_] = kai_version:order([Data1, Data2, Data3]).

cas_unique1(_Conf) ->
    Data1 = #data{
      checksum = list_to_binary(all_bit_on(16))
     },
    {ok, CasUnique} = kai_version:cas_unique([Data1]),
    Expected = list_to_binary([<<1:4, 2#1111:4>>, all_bit_on(7)]),
    ?assertEqual(Expected, CasUnique).

cas_unique2(_Conf) ->
    Data1 = #data{
      checksum = <<16#FFFFFFFFFFFFFFFF:64, 0:64>>
     },
    Data2 = #data{
      checksum = <<0:64, 16#FFFFFFFFFFFFFFFF:64>>
     },
    {ok, CasUnique} = kai_version:cas_unique([Data1, Data2]),
    Expected = <<2:4, 2#1111:4, 16#FFFFFFF:26, 0:30>>,
    ?assertEqual(Expected, CasUnique).

cas_unique7(_Conf) ->
    %% trunc(60/7) = 8
    DataList = lists:map(fun (I) ->
                                   #data{checksum = <<I:8, 0:120>>}
                           end,
                           lists:seq(1,7)),
    {ok, CasUnique} = kai_version:cas_unique(DataList),
    Expected = <<7:4, 1:8, 2:8, 3:8, 4:8, 5:8, 6:8, 7:8, 0:4>>,
    ?assertEqual(Expected, CasUnique).

cas_unique16(_Conf) ->
    %% 16 exceeds 4bit range (2#1111 = 15)
    DataList = lists:map(fun (I) ->
                                   #data{checksum = <<I:4, 0:60>>}
                           end,
                           lists:seq(1,16)),
    {error, Reason} = kai_version:cas_unique(DataList),
    ?assert(string:str(Reason, "16") > 0).

all_bit_on(Bytes) ->
    lists:duplicate(Bytes, 16#FF).

all_bit_off(Bytes) ->
    lists:duplicate(Bytes, 0).
