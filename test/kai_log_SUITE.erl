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

-module(kai_log_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() ->
    [{seq, [debug, info, warning, error]}].

all() -> [{sequence, seq}].

init_per_testcase(_TestCase, Conf) ->
    kai_config:start_link([{rpc_port, 11011},
                           {quorum, {3,2,2}},
                           {buckets, 15000},
                           {virtual_nodes, 128}]),
    kai_log:start_link(),
    Conf.

end_per_testcase(_TestCase, _Conf) ->
    kai_log:stop(),
    kai_config:stop().

error(_Conf) ->
    ?error("This is an error log").

warning(_Conf) ->
    ?warning("This is a warning log").

info(_Conf) ->
    ?info("This is an info log").

debug(_Conf) ->
    ?debug("This is a debug log").
