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
EMULATOR=beam
EBIN = $(ROOT)/ebin
INCLUDE = $(ROOT)/include

APP_TARGET = $(EBIN)/kai.app

ERL_COMPILE_FLAGS += \
	+warn_unused_vars +nowarn_shadow_vars +warn_unused_import \
	+debug_info

SOURCES = \
	kai_config kai_log kai_hash kai_store kai_store_ets kai_store_dets \
	kai_stat kai_version kai_connection kai_sync kai_membership \
	kai_coordinator kai_tcp_server \
	kai_tcp_server_sup kai_tcp_server_acceptor kai_tcp_server_monitor \
	kai_rpc kai_memcache kai_sup kai vclock

MODS = ${SOURCES:%=$(EBIN)/%.$(EMULATOR)} $(APP_TARGET)

$(EBIN)/%.$(EMULATOR):%.erl
	erlc -pa $(EBIN) -W $(ERL_COMPILE_FLAGS) -I$(INCLUDE) -o$(EBIN) $<

all: $(MODS)

$(MODS): $(INCLUDE)/kai.hrl

dialyze:
	dialyzer --succ_typings -c ${SOURCES:%=%.erl}

clean:	
	rm -rf $(EBIN)/*.$(EMULATOR) $(APP_TARGET) $(EBIN)/erl_crash.dump *~

$(APP_TARGET): kai.app.src Makefile
	sed -e 's;%VSN%;$(KAI_VSN);' $< > $@
