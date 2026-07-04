%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_fifo).
-moduledoc "Every node scores 0, so succession is plain FIFO in arrival (registration) order. For an even spread across nodes use `hash`.".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> deterministic.
kind() -> deterministic.

-spec score(term(), [node()], term()) -> 0.
score(_Key, _Members, _Args) -> 0.
