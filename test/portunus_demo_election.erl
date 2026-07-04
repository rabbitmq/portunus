%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_demo_election).

%% A trivial `portunus_election` callback used by the test suites: it
%% reports election and step-down to an observer pid passed as Args.

-behaviour(portunus_election).

-export([elected/1, stepped_down/1]).

elected(#{key := Key, token := Token, args := Observer}) ->
    Observer ! {elected, Key, Token, self()},
    {ok, {Observer, Key}}.

stepped_down({Observer, Key}) ->
    Observer ! {stepped_down, Key, self()},
    ok.
