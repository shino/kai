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

-module(kai_config_SUITE).
-compile(export_all).

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

-define(ARGS, [{rpc_port, ?PORT1},
               {quorum, {3,2,2}},
               {buckets, 15000}, %% 15000 is upgraded to 2^14 = 16384
               {virtual_nodes, 128}]).

sequences() ->
    [{seq, [node, node_by_ipaddr, node_without_hostname, buckets,
            no_such_parameter, quorum_error, parameter_list, node_info, 
            get_env, compat]}].

all() -> [{sequence, seq}].

init_per_testcase(node_by_ipaddr, Conf) ->
    init(Conf, [{hostname, "127.0.0.1"}|?ARGS]);
init_per_testcase(node_without_hostname, Conf) ->
    init(Conf, ?ARGS);
init_per_testcase(quorum_error, Conf) ->
    spawn_link(?MODULE, init, [Conf, [{quorum, {3,2,1}}|?ARGS]]),
    Conf;
init_per_testcase(compat, Conf) ->
    init(Conf, [{number_of_buckets, 1000}, %% 1000 is upgraded to 2^10 = 1024
                {number_of_virtual_nodes, 256},
                {n,2}, {r,2}, {w,2},
                {number_of_tables, 128}|?ARGS]);
init_per_testcase(_TestCase, Conf) ->
    init(Conf, [{hostname, "localhost"}|?ARGS]).

init(Conf, Args) ->
    kai_config:start_link(Args),
    Conf.

end_per_testcase(quorum_error, _Conf) ->
    ok;
end_per_testcase(_TestCase, _Conf) ->
    kai_config:stop().

node(_Conf) ->
    ?NODE1 = kai_config:get(node).

node_by_ipaddr(_Conf) ->
    ?NODE1 = kai_config:get(node).

node_without_hostname(_Conf) ->
    {_Addr, ?PORT1} = kai_config:get(node).

buckets(_Conf) ->
    16384 = kai_config:get(buckets).

no_such_parameter(_Conf) ->
    undefined = kai_config:get(no_such_parameter).

quorum_error(_Conf) ->
    process_flag(trap_exit, true),
    receive {'EXIT', _Pid, _Reason} -> ok end.

parameter_list(_Conf) ->
    [?NODE1, 16384, 128] = kai_config:get([node, buckets, virtual_nodes]).

node_info(_Conf) ->
    {ok, ?NODE1, [{virtual_nodes, 128}]} = kai_config:node_info().

get_env(_Conf) ->
    application:set_env(kai, quorum, {2,2,2}),
    Configs = kai_config:get_env(),
    {2,2,2} = proplists:get_value(quorum, Configs).

compat(_Conf) ->
    1024 = kai_config:get(buckets),
    256 = kai_config:get(virtual_nodes),
    {2,2,2} = kai_config:get(quorum),
    128 = kai_config:get(dets_tables).
