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

recv_response(ApiSocket, Node, Message) ->
    receive
        {tcp, ApiSocket, Bin} ->
            binary_to_term(Bin);
        {tcp_closed, ApiSocket} ->
            ?warning("recv_response(~p, ~p): ~p",
                     [Node, Message, {error, econnreset}]),
            {error, econnreset};
        {error, Reason} ->
            ?warning("recv_response(~p, ~p): ~p",
                     [Node, Message, {error, Reason}]),
            {error, Reason}

        % Don't place Other alternative here.  This is to avoid to catch event
        % messages, '$gen_event' or something like that.  Remember that this
        % function can be called from gen_fsm/gen_event.

    after ?TIMEOUT ->
            ?warning("recv_response(~p, ~p): ~p",
                     [Node, Message, {error, etimedout}]),
            {error, etimedout}
    end.

send_request({Address, Port} = Node, Message) ->
    case gen_tcp:connect(
        Address, Port, [binary, {packet, 4}, {reuseaddr, true}], ?TIMEOUT
    ) of
        {ok, ApiSocket} ->
            gen_tcp:send(ApiSocket, term_to_binary(Message)),
            Response = recv_response(ApiSocket, Node, Message),
            gen_tcp:close(ApiSocket),
            Response;
        {error, Reason} ->
            ?warning("send_request(~p, ~p): ~p",
                     [Node, Message, {error, Reason}]),
            {error, Reason}
    end.

is_local_node(Node) ->
    LocalNode = kai_config:get(node),
    Node =:= LocalNode.

node_info(Node) ->
    case is_local_node(Node) of
        true -> kai_config:node_info();
        _    -> send_request(Node, node_info)
    end.

node_list(Node) ->
    case is_local_node(Node) of
        true -> kai_hash:node_list();
        _    -> send_request(Node, node_list)
    end.

list(Node, Bucket) ->
    case is_local_node(Node) of
        true -> kai_store:list(Bucket);
        _    -> send_request(Node, {list, Bucket})
    end.

get(Node, Key) ->
    case is_local_node(Node) of
        true -> kai_store:get(Key);
        _    -> send_request(Node, {get, Key})
    end.

put(Node, Data) ->
    case is_local_node(Node) of
        true -> kai_store:put(Data);
        _    -> send_request(Node, {put, Data})
    end.

delete(Node, Key) ->
    case is_local_node(Node) of
        true -> kai_store:delete(Key);
        _    -> send_request(Node, {delete, Key})
    end.

check_node(Node, Node2) ->
    case is_local_node(Node) of
        true -> kai_membership:check_node(Node2);
        _    -> send_request(Node, {check_node, Node2})
    end.

route(Node, Request) ->
    case is_local_node(Node) of
        true -> {error, ewouldblock};
        _    -> send_request(Node, {route, Request})
    end.
