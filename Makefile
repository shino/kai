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

RUN_TEST = /opt/local/lib/erlang/lib/common_test-1.3.1/priv/bin/run_test

all: subdirs

subdirs:
	cd src; ROOT=$(ROOT) make

test: test_do

test_compile: subdirs
	cd test; ROOT=$(ROOT) make

test_do: test_compile
	cp ebin/*.beam test/
	mkdir -p test/log
	${RUN_TEST} -dir . -logdir test/log

clean:	
	rm -rf *.beam erl_crash.dump *~
	rm -rf test/log
	cd src; ROOT=$(ROOT) make clean
	cd test; ROOT=$(ROOT) make clean
