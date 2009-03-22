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

-module(kai_tcp_server_SUITE).
-compile(export_all).
-export([init/1, handle_call/3]). % for echo server

-include("ct.hrl").
-include("kai.hrl").
-include("kai_test.hrl").

sequences() -> [{sequences1, [testcase1, testcase2, testcase3]}].

all() -> [{sequence, sequences1}].

init_per_testcase(testcase1, Config) ->
    start_server(),
    Config;

init_per_testcase(testcase2, Config) ->
    start_server(),
    Config;

init_per_testcase(testcase3, Config) ->
    start_server(),
    Config;

init_per_testcase(_TestCase, Config) ->
    Config.

start_server() ->
    kai_tcp_server:start_link(
        ?MODULE, [], #tcp_server_option{max_processes=1}
    ).

end_per_testcase(testcase1, _Config) ->
    kai_tcp_server:stop(),
    ok;

end_per_testcase(testcase2, _Config) ->
    kai_tcp_server:stop(),
    ok;

end_per_testcase(testcase3, _Config) ->
    kai_tcp_server:stop(),
    ok;

end_per_testcase(_TestCase, _Config) ->
    ok.

testcase1() -> [].
testcase1(_Conf) ->
    normal_test(),
    ok.

normal_test() ->
    {ok, Socket} = connect_to_echo_server(),
    gen_tcp:send(Socket, <<"hello\r\n">>),
    case gen_tcp:recv(Socket, 0) of
       {ok, <<"hello\r\n">>} -> ok;
       _HelloError           -> ct:fail(bad_echo_value)
    end,
    gen_tcp:send(Socket, <<"bye\r\n">>),
    case gen_tcp:recv(Socket, 0) of
       {ok, <<"cya\r\n">>} -> ok;
       _ByeError           -> ct:fail(bad_return_value)
    end,
    gen_tcp:close(Socket),
    ok.

testcase2() -> [].
testcase2(_Conf) ->
    {ok, Socket} = connect_to_echo_server(),
    gen_tcp:send(Socket, <<"error\r\n">>),
    {error, closed} = gen_tcp:recv(Socket, 0),
    gen_tcp:close(Socket),

    normal_test(), % check the echo server rebooted.
    ok.

testcase3() -> [].
testcase3(_Conf) ->
    lists:foreach(fun (_N) ->
        {ok, Socket} = connect_to_echo_server(),
        gen_tcp:close(Socket)
    end, lists:seq(1, 10000)),
    ok.

connect_to_echo_server() ->
    gen_tcp:connect(
        {127,0,0,1}, 11211, [binary, {packet, line}, {active, false}]
    ).

% echo server
init(_Args) -> {ok, {}}.

handle_call(_Socket, <<"bye\r\n">>, State) ->
    {close, <<"cya\r\n">>, State};
handle_call(_Socket, <<"error\r\n">>, State) ->
    BadArith = 1/0,
    {close, <<"error\r\n">>, State};
handle_call(_Socket, Data, State) ->
    {reply, Data, State}.

