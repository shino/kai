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

-module(kai_api).
-behaviour(kai_tcp_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3]).
-export([
    node_info/1, node_list/1,
    list/2, get/2, put/2, delete/2,
    check_node/2, route/2
]).

-include("kai.hrl").

-define(TIMEOUT, 3000).

start_link() ->
    kai_tcp_server:start_link(
        {local, ?MODULE},
        ?MODULE,
        [],
        #tcp_server_option{
            listen = [binary, {packet, 4}, {active, true}, {reuseaddr, true}],
            port            = kai_config:get(port),
            max_connections = kai_config:get(max_connections)
        }
    ).

stop() -> kai_tcp_server:stop(?MODULE).

init(_Args) -> {ok, {}}.

handle_call(Socket, Data, State) ->
    dispatch(Socket, binary_to_term(Data), State).

dispatch(_Socket, node_info, State) ->
    reply(kai_config:node_info(), State);

dispatch(_Socket, node_list, State) ->
    reply(kai_hash:node_list(), State);

dispatch(_Socket, {list, Bucket}, State) ->
    reply(kai_store:list(Bucket), State);

dispatch(_Socket, {get, Key}, State) ->
    reply(kai_store:get(Key), State);

dispatch(_Socket, {put, Data}, State) when is_record(Data, data)->
    reply(kai_store:put(Data), State);

dispatch(_Socket, {delete, Key}, State) ->
    reply(kai_store:delete(Key), State);

dispatch(_Socket, {check_node, Node}, State) ->
    reply(kai_membership:check_node(Node), State);

dispatch(_Socket, {route, Request}, State) ->
    reply(kai_coordinator:route(Request), State);

dispatch(_Socket, _Unknown, State) ->
    reply({error, enotsup}, State).

reply(Data, State) ->
    {reply, term_to_binary(Data), State}.

recv_response(ApiSocket) ->
    receive
        {tcp, ApiSocket, Bin} ->
            binary_to_term(Bin);
        {tcp_closed, ApiSocket} ->
            {error, econnreset};
        {error, Reason} ->
            {error, Reason}

        % Don't place Other alternative here.  This is to avoid to catch event
        % messages, '$gen_event' or something like that.  Remember that this
        % function can be called from gen_fsm/gen_event.

    after ?TIMEOUT ->
            ?warning("recv_response/1: timeout"),
            {error, etimedout}
    end.

send_request({Address, Port}, Message) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}, {reuseaddr, true}], ?TIMEOUT) of
        {ok, ApiSocket} ->
            gen_tcp:send(ApiSocket, term_to_binary(Message)),
            Response = recv_response(ApiSocket),
            gen_tcp:close(ApiSocket),
            Response;
        {error, Reason} ->
            ?warning(io_lib:format("send_request/2: ~p", [{error, Reason}])),
            {error, Reason}
    end.

node_info(Node) ->
    send_request(Node, node_info).
node_list(Node) ->
    send_request(Node, node_list).
list(Node, Bucket) ->
    send_request(Node, {list, Bucket}).
get(Node, Key) ->
    send_request(Node, {get, Key}).
put(Node, Data) ->
    send_request(Node, {put, Data}).
delete(Node, Key) ->
    send_request(Node, {delete, Key}).
check_node(Node1, Node2) ->
    send_request(Node1, {check_node, Node2}).
route(Node, Request) ->
    send_request(Node, {route, Request}).
