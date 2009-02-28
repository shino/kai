## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License.  You may obtain a copy
## of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
## License for the specific language governing permissions and limitations
## under the License.

INCLUDE = $(ROOT)/include
TEST_SERVER_INCLUDE = $(TEST_SERVER)/include
RUN_TEST_INCLUDE = $(RUN_TEST)/include
ERL_COMPILE_FLAGS += +warn_unused_vars +nowarn_shadow_vars +warn_unused_import

.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W $(ERL_COMPILE_FLAGS) \
		-I$(INCLUDE) -I$(RUN_TEST_INCLUDE) -I$(TEST_SERVER_INCLUDE) \
		$<

.yrl.erl:
	erlc -W $(ERL_COMPILE_FLAGS) -I$(INCLUDE) $<

MODS = kai_config_SUITE kai_log_SUITE kai_hash_SUITE kai_store_SUITE \
	kai_stat_SUITE kai_version_SUITE kai_connection_SUITE kai_sync_SUITE \
	kai_membership_SUITE kai_coordinator_SUITE kai_tcp_server_SUITE \
	kai_rpc_SUITE kai_memcache_SUITE vclock_SUITE

all: compile

compile: ${MODS:%=%.beam}

clean:	
	rm -rf *.beam erl_crash.dump *~
