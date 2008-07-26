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

%sequences() -> [{sequences1, [testcase1, testcase2]}].
sequences() -> [{sequences1, [testcase1]}].

all() -> [{sequence, sequences1}].

init_per_testcase(testcase1, Config) ->
    kai_tcp_server:start_link(
        ?MODULE, [], #tcp_server_option{max_connections=1}
    ),
    Config;

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(testcase1, _Config) ->
    kai_tcp_server:stop(),
    ok;

end_per_testcase(_TestCase, _Config) -> ok.

testcase1() -> [].
testcase1(_Conf) ->
    {ok, Socket} = gen_tcp:connect(
        {127,0,0,1}, 11211, [binary, {packet, line}, {active, false}]
    ),
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

% echo server
init(_Args) -> {ok, {}}.

handle_call(_Socket, <<"bye\r\n">>, State) ->
    {close, <<"cya\r\n">>, State};
handle_call(_Socket, Data, State) ->
    {reply, Data, State}.

