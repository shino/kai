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

-export([start_link/0]).
-export([init/1, proc/1]).
-export([node_info/1, node_list/1, list/2, get/2, put/2, delete/2, check_node/2]).

-include("kai.hrl").

-define(TIMEOUT, 3000).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).
    
init(Pid) ->
    proc_lib:init_ack(Pid, {ok, self()}),
    Port = kai_config:get(port),
    {ok, ListeningSocket} =
        gen_tcp:listen(Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}]),
    accept(ListeningSocket).

accept(ListeningSocket) ->
    {ok, ApiSocket} = gen_tcp:accept(ListeningSocket),
    Pid = spawn(?MODULE, proc, [ApiSocket]), % Don't link
    gen_tcp:controlling_process(ApiSocket, Pid),
    accept(ListeningSocket).

proc(ApiSocket) ->
    receive
	{tcp, ApiSocket, Bin} ->
	    case binary_to_term(Bin) of
		node_info ->
		    NodeInfo = kai_hash:node_info(),
		    gen_tcp:send(ApiSocket, term_to_binary(NodeInfo));
		node_list ->
		    NodeList = kai_hash:node_list(),
		    gen_tcp:send(ApiSocket, term_to_binary(NodeList));
		{list, Bucket} ->
		    Metadata = kai_store:list(Bucket),
		    gen_tcp:send(ApiSocket, term_to_binary(Metadata));
		{get, Key} ->
		    Data = kai_store:get(Key),
		    gen_tcp:send(ApiSocket, term_to_binary(Data));
		{put, Data} when is_record(Data, data) ->
		    Res = kai_store:put(Data),
		    gen_tcp:send(ApiSocket, term_to_binary(Res));
		{delete, Key} ->
		    Res = kai_store:delete(Key),
		    gen_tcp:send(ApiSocket, term_to_binary(Res));
		{check_node, Node} ->
		    Res = kai_membership:check_node(Node),
		    gen_tcp:send(ApiSocket, term_to_binary(Res));
		_Unknown ->
		    gen_tcp:send(ApiSocket, term_to_binary({error, enotsup}))
	    end,
	    proc(ApiSocket);
	Error ->
	    Error
    after ?TIMEOUT ->
	  {error, etimedout}
    end.

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
	    {error, etimedout}
    end.

send_request({Address, Port}, Message) ->
    case gen_tcp:connect(Address, Port, [binary, {packet, 4}], ?TIMEOUT) of
	{ok, ApiSocket} ->
	    gen_tcp:send(ApiSocket, term_to_binary(Message)),
	    Response = recv_response(ApiSocket),
	    gen_tcp:close(ApiSocket),
	    Response;
	{error, Reason} ->
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
