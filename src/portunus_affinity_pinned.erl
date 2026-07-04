%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_pinned).
-moduledoc """
Run the child on `Args`, the pinned node. Every other node scores 0,
so if the pinned node is absent any of them can take over (a soft pin).
""".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> deterministic.
kind() -> deterministic.

-spec score(term(), [node()], node()) -> 0..1.
score(_Key, _Members, Node) when Node =:= node() -> 1;
score(_Key, _Members, _Node) -> 0.
