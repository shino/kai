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

ifndef ROOT
	ROOT=$(shell pwd)
endif

ERLANG_LIB = /opt/local/lib/erlang/lib
TEST_SERVER = $(ERLANG_LIB)/test_server-3.2.2
RUN_TEST = $(ERLANG_LIB)/common_test-1.3.1
RUN_TEST_CMD = $(RUN_TEST)/priv/bin/run_test

all: subdirs

subdirs:
	cd src; ROOT=$(ROOT) make

test: test_do

test_compile: subdirs
	cd test; \
		ROOT=$(ROOT) TEST_SERVER=$(TEST_SERVER) RUN_TEST=$(RUN_TEST) make

test_do: test_compile
	mkdir -p test/log
	${RUN_TEST_CMD} -dir . \
		-logdir test/log -cover test/kai.coverspec \
		-I$(ROOT)/include -pa $(ROOT)/ebin

clean:	
	rm -rf *.beam erl_crash.dump *~
	rm -rf test/log
	cd src; ROOT=$(ROOT) make clean
	cd test; ROOT=$(ROOT) make clean
