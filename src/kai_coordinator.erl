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

-module(kai_coordinator).

-export([route/2]).
-export([start_route/4, map_in_get/5, map_in_put/5, map_in_delete/5]).

-include("kai.hrl").

-define(SERVER, ?MODULE).

route(SrcNode, {_Type, Data, _Quorum} = Request) ->
    Ref = make_ref(),
    %% Application exits abnormally if it doesn't spawn the process, though 
    %% the reason is unknown.
    spawn(?MODULE, start_route, [SrcNode, Request, self(), Ref]),
    receive
        {Ref, Result} -> Result
    after ?TIMEOUT ->
            ?warning(io_lib:format("route(~p): timeout", [Data#data.key])),
            []
    end.

start_route(SrcNode, {_Type, Data, _Quorum} = Request, Pid, Ref) ->
    {ok, DstNodes} = kai_hash:find_nodes(Data#data.key),
    Results =
        case lists:member(SrcNode, DstNodes) of
            true -> dispatch(SrcNode, Request);
            _    -> do_route(DstNodes, SrcNode, Request)
        end,
    Pid ! {Ref, Results}.

dispatch(SrcNode, {Type, Data, Quorum} = _Request) ->
    case Type of
        get    -> coordinate_get(SrcNode, Data, Quorum);
        put    -> coordinate_put(SrcNode, Data, Quorum);
        delete -> coordinate_delete(SrcNode, Data, Quorum);
        _Other -> {error, ebadrpc}
    end.

do_route([], _SrcNode, _Request) ->
    {error, ebusy};
do_route([DstNode|RestNodes], SrcNode, {_Type, Data, _Quorum} = Request) ->
    %% TODO: introduce TTL, in order to avoid infinite loop
    case kai_rpc:route(DstNode, SrcNode, Request) of
        {error, Reason} ->
            ?warning(io_lib:format("do_route(~p, ~p): ~p",
                                   [DstNode, Data#data.key, {error, Reason}])),
            do_route(RestNodes, SrcNode, Request);
        Results ->
            Results
    end.

coordinate_get(SrcNode, Data, {N,R,_W}) ->
    {ok, Bucket} = kai_hash:find_bucket(Data#data.key),
    {ok, DstNodes} = kai_hash:find_nodes(Bucket),
    Data2 = Data#data{bucket=Bucket},
    Ref = make_ref(),
    lists:foreach(
      fun(DstNode) ->
              %% Don't link
              spawn(?MODULE, map_in_get, [DstNode, SrcNode, Data2, Ref, self()])
      end, 
      DstNodes
     ),
    case gather_in_get(Ref, N, R, []) of
        ListOfData when is_list(ListOfData) ->
            %% TODO: write back recent if multiple versions are found and they can be resolved
            InternalNum = sets:size(
                            sets:from_list(
                              lists:map(fun(E) -> E#data.vector_clocks end,
                                       ListOfData))),
            ReconciledList = kai_version:order(ListOfData),
            if
                InternalNum > 1 ->
                    kai_stat:incr_unreconciled_get(
                      {InternalNum, length(ReconciledList) =:= 1});
                true -> ok
            end,
            ReconciledList;
        _NoData ->
            undefined
    end.

map_in_get(DstNode, SrcNode, Data, Ref, Pid) ->
    case kai_rpc:get(DstNode, SrcNode, Data) of
        {error, Reason} ->
%            kai_membership:check_node(DstNode),
            Pid ! {Ref, {error, Reason}};
        Other ->
            Pid ! {Ref, Other}
    end.

gather_in_get(_Ref, _N, 0, Results) ->
    Results;
gather_in_get(_Ref, 0, _R, _Results) ->
    {error, enodata};
gather_in_get(Ref, N, R, Results) ->
    receive
        {Ref, Data} when is_record(Data, data) ->
            gather_in_get(Ref, N-1, R-1, [Data|Results]);
        {Ref, undefined} ->
            gather_in_get(Ref, N-1, R-1, Results);
        {Ref, _Other} ->
            gather_in_get(Ref, N-1, R, Results)
    after ?TIMEOUT ->
            ?warning("gather_in_get/4: timeout"),
            Results
    end.

coordinate_put(SrcNode, Data, {N,_R,W}) ->
    Key   = Data#data.key,
    Flags = Data#data.flags,
    Value = Data#data.value,
    {ok, Bucket} = kai_hash:find_bucket(Key),
    {ok, DstNodes} = kai_hash:find_nodes(Bucket),
    Ref = make_ref(),
    Data1 =
        case kai_store:get(Data#data{bucket=Bucket}) of
            PreviousData when is_record(PreviousData, data) ->
                PreviousData;
            undefined ->
                #data{key=Key, vector_clocks=vclock:fresh()}
        end,
    {ok, Data2} = kai_version:update(Data1),
    Data3 = Data2#data{
        bucket   = Bucket,
        checksum = erlang:md5(Value),
        flags    = Flags,
        value    = Value
    },
    lists:foreach(
      fun(DstNode) ->
              spawn(?MODULE, map_in_put, [DstNode, SrcNode, Data3, Ref, self()])
      end,
      DstNodes
     ),
    gather_in_put(Ref, N, W).

map_in_put(DstNode, SrcNode, Data, Ref, Pid) ->
    case kai_rpc:put(DstNode, SrcNode, Data) of
        {error, Reason} ->
%            kai_membership:check_node(DstNode),
            Pid ! {Ref, {error, Reason}};
        Other ->
            Pid ! {Ref, Other}
    end.

gather_in_put(_Ref, _N, 0) ->
    ok;
gather_in_put(_Ref, 0, _W) ->
    {error, ebusy};
gather_in_put(Ref, N, W) ->
    receive
        {Ref, ok}     -> gather_in_put(Ref, N-1, W-1);
        {Ref, _Other} -> gather_in_put(Ref, N-1, W)
    after ?TIMEOUT ->
            ?warning("gather_in_put/3: timeout"),
            {error, etimedout}
    end.

coordinate_delete(SrcNode, Data, {N,_R,W}) ->
    {ok, Bucket} = kai_hash:find_bucket(Data#data.key),
    {ok, DstNodes} = kai_hash:find_nodes(Bucket),
    Data2 = Data#data{bucket=Bucket},
    Ref = make_ref(),
    lists:foreach(
      fun(DstNode) ->
              spawn(?MODULE, map_in_delete, [DstNode, SrcNode, Data2, Ref, self()])
      end,
      DstNodes
     ),
    gather_in_delete(Ref, N, W, []).

map_in_delete(DstNode, SrcNode, Data, Ref, Pid) ->
    case kai_rpc:delete(DstNode, SrcNode, Data) of
        {error, Reason} ->
%            kai_membership:check_node(DstNode),
            Pid ! {Ref, {error, Reason}};
        Other ->
            Pid ! {Ref, Other}
    end.

gather_in_delete(_Ref, _N, 0, Results) ->
    case lists:member(ok, Results) of
        true -> ok;
        _    -> undefined
    end;
gather_in_delete(_Ref, 0, _W, _Results) ->
    {error, ebusy};
gather_in_delete(Ref, N, W, Results) ->
    receive
    {Ref, ok} ->
        gather_in_delete(Ref, N-1, W-1, [ok|Results]);
    {Ref, undefined} ->
        gather_in_delete(Ref, N-1, W-1, [undefined|Results]);
    {Ref, _Other} ->
        gather_in_delete(Ref, N-1, W, Results)
    after ?TIMEOUT ->
            ?warning("gather_in_delete/4: timeout"),
        {error, etimedout}
    end.
