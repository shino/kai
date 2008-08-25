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

-module(kai_sup).
-behaviour(supervisor).
    
-export([start_link/1]).
-export([init/1]).
    
-define(SERVER, ?MODULE).

start_link(Args) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Args).

init(Args) ->
    Config = {
        kai_config,
        {kai_config, start_link, [Args]},
        permanent, 1000, worker,
        [kai_config]
    },
    Hash = {
        kai_hash,
        {kai_hash, start_link, []},
        permanent, 1000, worker,
        [kai_hash]
    },
    Store = {
        kai_store,
        {kai_store, start_link, []},
        permanent, 1000, worker,
        [kai_store]
    },
    Version = {
        kai_version,
        {kai_version, start_link, []},
        permanent, 1000, worker,
        [kai_version]
    },
    Sync = {
        kai_sync,
        {kai_sync, start_link, []},
        permanent, 1000, worker,
        [kai_sync]
    },
    Membership = {
        kai_membership,
        {kai_membership, start_link, []},
        permanent, 1000, worker,
        [kai_membership]
    },
    Api = {
        kai_api,
        {kai_api, start_link, []},
        permanent, 1000, worker,
        [kai_api]
    },
    Memcache = {
        kai_memcache,
        {kai_memcache, start_link, []},
        permanent, 1000, worker,
        [kai_memcache]
    },
    {ok, {{one_for_one, 3, 10}, [
        Config, Hash, Store, Version, Sync, Membership, Api, Memcache
    ]}}.
