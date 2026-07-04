%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_failing_election).

%% A `portunus_election` callback whose start always crashes, to exercise
%% the election's recovery when the elected child cannot start.

-behaviour(portunus_election).

-export([elected/1, stepped_down/1]).

elected(_Ctx) ->
    error(deliberate_start_failure).

stepped_down(_State) ->
    ok.
