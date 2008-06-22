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

-module(kai_version_SUITE).
-compile(export_all).

-include("kai.hrl").
-include("kai_test.hrl").

all() -> [test1].

test1() -> [].
test1(_Conf) ->
    kai_version:start_link(),

    Data1 = #data{key="item-1", bucket=3, checksum=erlang:md5(<<"value-1">>),
		  flags="0", value=(<<"value-1">>)},

    Data2 = kai_version:update(Data1),

    ?assert(is_tuple(Data2#data.last_modified)),

    ListOfData1 = kai_version:order([Data2, Data2]),

    ?assertEqual(1, length(ListOfData1)),

    Data3 = #data{key="item-1", bucket=3, checksum=erlang:md5(<<"value-1b">>),
		  flags="0", value=(<<"value-1b">>)},

    Data4 = kai_version:update(Data3),

    ListOfData2 = kai_version:order([Data2, Data4]),

    ?assertEqual(2, length(ListOfData2)),

    ?assertEqual(undefined, kai_version:order([])),

    kai_version:stop().
