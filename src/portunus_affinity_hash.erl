%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_hash).
-moduledoc """
Rendezvous (highest-random-weight) hashing: each node scores its weight
for the key, the highest wins. Keys spread evenly across members, and a
membership change only moves the keys that hashed to the gained or lost
node. `Args` is ignored. `phash2/1` is portable, so every node agrees.
""".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> deterministic.
kind() -> deterministic.

-spec score(term(), [node()], term()) -> non_neg_integer().
score(Key, _Members, _Args) -> erlang:phash2({Key, node()}).
