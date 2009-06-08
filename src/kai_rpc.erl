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

-module(kai_rpc).
-behaviour(kai_tcp_server).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3]).
-export([
    ok/2,
    node_info/2, node_list/2,
    list/3, get/3, put/3, delete/3,
    check_node/3, route/3
]).

-include("kai.hrl").

-record(state, {node_info}).

start_link() ->
    kai_tcp_server:start_link(
        {local, ?MODULE},
        ?MODULE,
        [],
        #tcp_server_option{
            listen = [binary, {packet, 4}, {active, true}, {reuseaddr, true}],
            port          = kai_config:get(rpc_port),
            max_processes = kai_config:get(rpc_max_processes)
        }
    ).

stop() -> kai_tcp_server:stop(?MODULE).

init(_Args) ->
    {ok, #state{node_info = kai_config:node_info()}}.

handle_call(Socket, Data, State) ->
    dispatch(Socket, binary_to_term(Data), State).

dispatch(_Socket, ok, State) ->
    reply(ok, State);

dispatch(_Socket, node_info, State) ->
    reply(State#state.node_info, State);

dispatch(_Socket, node_list, State) ->
    reply(kai_hash:node_list(), State);

dispatch(_Socket, {list, Bucket}, State) ->
    reply(kai_store:list(Bucket), State);

dispatch(_Socket, {get, Data}, State) ->
    reply(kai_store:get(Data), State);

dispatch(_Socket, {put, Data}, State) when is_record(Data, data)->
    reply(kai_store:put(Data), State);

dispatch(_Socket, {delete, Data}, State) ->
    reply(kai_store:delete(Data), State);

dispatch(_Socket, {check_node, Node}, State) ->
    reply(kai_membership:check_node(Node), State);

dispatch(_Socket, {route, SrcNode, Request}, State) ->
    reply(kai_coordinator:route(SrcNode, Request), State);

dispatch(_Socket, _Unknown, State) ->
    reply({error, enotsup}, State).

reply(Data, State) ->
    {reply, term_to_binary(Data), State}.

recv_response(Socket) ->
    receive
        {tcp, Socket, Bin} ->
            {ok, binary_to_term(Bin)};
        {tcp_closed, Socket} ->
            {error, econnreset};
        {error, Reason} ->
            {error, Reason}

        %% Don't place Other alternative here.  This is to avoid to catch event
        %% messages, '$gen_event' or something like that.  Remember that this
        %% function can be called from gen_fsm/gen_event.

    after ?TIMEOUT ->
            {error, timeout}
    end.

do_request(Node, Message) ->
    case kai_connection:lease(Node, self()) of
        {ok, Socket} ->
            case gen_tcp:send(Socket, term_to_binary(Message)) of
                ok ->
                    case recv_response(Socket) of
                        {ok, Result} ->
                            kai_connection:return(Socket),
                            {ok, Result};
                        {error, Reason} ->
                            kai_connection:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    kai_connection:close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

request(Node, Message) ->
    case do_request(Node, Message) of
        {ok, Result} ->
            Result;
        {error, Reason} ->
            ?warning(io_lib:format("request(~p, ~p): ~p",
                                   [Node, Message, {error, Reason}])),
%            kai_membership:check_node(Node),
            {error, Reason}
    end.

ok(DstNode, SrcNode) ->
    case DstNode =:= SrcNode of
        true -> ok;
        _    -> request(DstNode, ok)
    end.

node_info(DstNode, SrcNode) ->
    case DstNode =:= SrcNode of
        true -> kai_config:node_info();
        _    -> request(DstNode, node_info)
    end.

node_list(DstNode, SrcNode) ->
    case DstNode =:= SrcNode of
        true -> kai_hash:node_list();
        _    -> request(DstNode, node_list)
    end.

list(DstNode, SrcNode, Bucket) ->
    case DstNode =:= SrcNode of
        true -> kai_store:list(Bucket);
        _    -> request(DstNode, {list, Bucket})
    end.

get(DstNode, SrcNode, Data) ->
    case DstNode =:= SrcNode of
        true -> kai_store:get(Data);
        _    -> request(DstNode, {get, Data})
    end.

put(DstNode, SrcNode, Data) ->
    case DstNode =:= SrcNode of
        true -> kai_store:put(Data);
        _    -> request(DstNode, {put, Data})
    end.

delete(DstNode, SrcNode, Data) ->
    case DstNode =:= SrcNode of
        true -> kai_store:delete(Data);
        _    -> request(DstNode, {delete, Data})
    end.

check_node(DstNode, SrcNode, Node) ->
    case DstNode =:= SrcNode of
        true -> kai_membership:check_node(Node);
        _    -> request(DstNode, {check_node, Node})
    end.

route(DstNode, SrcNode, Request) ->
    case DstNode =:= SrcNode of
        true -> {error, ewouldblock};
        _    -> request(DstNode, {route, DstNode, Request})
    end.
