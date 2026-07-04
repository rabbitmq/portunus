%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_metric).
-moduledoc """
A dynamic strategy: this node bids the value of a local metric. `Args` is
a `fun(() -> integer())` read on each node, for example spare capacity, so
the least-loaded node wins. The bid is a snapshot taken at contention
time, not a continuous signal.
""".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> dynamic.
kind() -> dynamic.

-spec score(term(), [node()], fun(() -> integer())) -> integer().
score(_Key, _Members, MetricFun) when is_function(MetricFun, 0) ->
    MetricFun().
