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

-module(kai_store_SUITE).
-compile(export_all).

-include("kai.hrl").
-include("kai_test.hrl").

all() -> [test1, test2].

test1() -> [].
test1(_Conf) ->
    kai_store:start_link(),

    Data1 = #data{key="item-1", bucket=3, last_modified=now(),
		  checksum=erlang:md5(<<"value-1">>), flags="0", value=(<<"value-1">>)},
    kai_store:put(Data1),
    ?assertEqual(
       Data1,
       kai_store:get("item-1")
      ),
    ?assertEqual(
       undefined,
       kai_store:get("item-2")
      ),

    Data2 = #data{key="item-2", bucket=1, last_modified=now(),
		  checksum=erlang:md5(<<"value-2">>), flags="0", value=(<<"value-2">>)},
    kai_store:put(Data2),
    ?assertEqual(
       Data2,
       kai_store:get("item-2")
      ),

    Data3 = #data{key="item-3", bucket=3, last_modified=now(),
		  checksum=erlang:md5(<<"value-3">>), flags="0", value=(<<"value-3">>)},
    kai_store:put(Data3),
    ?assertEqual(
       Data3,
       kai_store:get("item-3")
      ),

    {list_of_data, ListOfData1} = kai_store:list(1),
    ?assertEqual(1, length(ListOfData1)),
    ?assert(lists:keymember("item-2", 2, ListOfData1)),

    {list_of_data, ListOfData2} = kai_store:list(2),
    ?assertEqual(0, length(ListOfData2)),

    {list_of_data, ListOfData3} = kai_store:list(3),
    ?assertEqual(2, length(ListOfData3)),
    ?assert(lists:keymember("item-1", 2, ListOfData3)),
    ?assert(lists:keymember("item-3", 2, ListOfData3)),
    
    Data1b = #data{key="item-1", bucket=3, last_modified=now(),
		   checksum=erlang:md5(<<"value-1">>), flags="0", value=(<<"value-1b">>)},
    kai_store:put(Data1b),
    ?assertEqual(
       Data1b,
       kai_store:get("item-1")
      ),

    kai_store:delete("item-1"),
    ?assertEqual(
       undefined,
       kai_store:get("item-1")
      ),

    {list_of_data, ListOfData4} = kai_store:list(3),
    ?assertEqual(1, length(ListOfData4)),
    ?assert(lists:keymember("item-3", 2, ListOfData4)),

    kai_store:stop().

test2_put(T) ->
    lists:foreach(
      fun(I) -> 
	      Key = "item-" ++ integer_to_list(I),
	      Value = list_to_binary("value-" ++ integer_to_list(I)),
	      Data = #data{key=Key, bucket=3, last_modified=now(), checksum=erlang:md5(Value), flags="0", value=Value},
	      kai_store:put(Data)
      end,
      lists:seq(1, T)
     ).

test2_get(T) ->
    lists:foreach(
      fun(I) -> 
	      Key = "item-" ++ integer_to_list(I),
	      kai_store:get(Key)
      end,
      lists:seq(1, T)
     ).

test2() -> [].
test2(_Conf) ->
    kai_store:start_link(),

    T = 10000,

    {Usec, _} = timer:tc(?MODULE, test2_put, [T]),
    ?assert(Usec < 100*T),
    io:format("average time to put data: ~p [usec]", [Usec/T]),

    {Usec2, _} = timer:tc(?MODULE, test2_get, [T]),
    ?assert(Usec2 < 100*T),
    io:format("average time to get data: ~p [usec]", [Usec2/T]),

    kai_store:stop().
