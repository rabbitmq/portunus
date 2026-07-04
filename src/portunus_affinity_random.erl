%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_random).
-moduledoc """
A dynamic strategy: each node bids a random weight, so a uniformly random
contender wins. The roll runs on the client, never in the machine, so it
does not affect determinism. For a *stable* even spread prefer
`portunus_affinity_hash`, whose owner does not move on every contention.
""".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> dynamic.
kind() -> dynamic.

%% A wide range keeps ties rare; a tie falls back to FIFO.
-spec score(term(), [node()], term()) -> pos_integer().
score(_Key, _Members, _Args) -> rand:uniform(1 bsl 30).
