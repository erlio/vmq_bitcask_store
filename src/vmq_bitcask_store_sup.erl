%% Copyright 2014 Erlio GmbH Basel Switzerland (http://erl.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(vmq_bitcask_store_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         get_bucket_pid/1,
         get_bucket_pids/0]).

%% Supervisor callbacks
-export([init/1]).

-define(NR_OF_BUCKETS, 8).
-define(TABLE, vmq_bitcask_store_buckets).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    [begin
         {ok, CPid} = supervisor:start_child(Pid, child_spec(I)),
         ets:insert(?TABLE, {I, CPid})
     end || I <- lists:seq(1, ?NR_OF_BUCKETS)],
    {ok, Pid}.

get_bucket_pid(Key) when is_binary(Key) ->
    Id = (erlang:phash2(Key) rem ?NR_OF_BUCKETS) + 1,
    case ets:lookup(?TABLE, Id) of
        [] ->
            {error, no_bucket_found};
        [{Id, Pid}] ->
            {ok, Pid}
    end.

get_bucket_pids() ->
    [Pid || [{_, Pid}] <- ets:match(?TABLE, '$1')].

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, { {one_for_one, 5, 10}, []} }.

child_spec(I) ->
    {{vmq_bitcask_store_bucket, I},
     {vmq_bitcask_store, start_link, [I]},
     permanent, 5000, worker, [vmq_bitcask_store]}.
