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

MODS = kai_config kai_log kai_hash kai_store kai_coordinator kai_sync \
	kai_membership kai_api kai_memcache kai_sup kai

all: compile

compile: ${MODS:%=%.beam}

clean:	
	rm -rf *.beam erl_crash.dump *~
