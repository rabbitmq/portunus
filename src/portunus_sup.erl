%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_sup).
-moduledoc false.

-behaviour(supervisor).

-export([start_link/0, init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% portunus servers are Ra servers, supervised by Ra's own systems. This
    %% supervisor owns the node-global delayed-restart marker table for the
    %% application's lifetime, and hosts any future node-local helpers.
    ok = portunus_delayed_restart:ensure_table(),
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    BatchKeepalive = #{id => portunus_batch_keepalive,
                       start => {portunus_batch_keepalive, start_link, []},
                       shutdown => 5000},
    {ok, {SupFlags, [BatchKeepalive]}}.
