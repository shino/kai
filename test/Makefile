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

.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W -I ../include $<

.yrl.erl:
	erlc -W -I ../include $<

MODS = kai_config_SUITE kai_log_SUITE kai_hash_SUITE kai_store_SUITE \
	kai_sync_SUITE kai_membership_SUITE kai_api_SUITE kai_memcache_SUITE

all: compile

compile: ${MODS:%=%.beam}

clean:	
	rm -rf *.beam erl_crash.dump *~
