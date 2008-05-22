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

-record(data, {key, bucket, last_modified, checksum, flags, value}).
-record(metadata, {key, bucket, last_modified, checksum}).

-define(error(Data), kai_log:log(error, ?FILE, ?LINE, Data)).
-define(warning(Data), kai_log:log(warning, ?FILE, ?LINE, Data)).
-define(info(Data), kai_log:log(info, ?FILE, ?LINE, Data)).

%-define(debug(Data), kai_log:log(debug, ?FILE, ?LINE, Data)).
-define(debug(_Data), ok).

-define(byte_size, byte_size).
%-define(byte_size, size).
