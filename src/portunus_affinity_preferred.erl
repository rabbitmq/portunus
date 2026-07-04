%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity_preferred).
-moduledoc """
Prefer the nodes in `Args`, a node list, earliest first. A node in the
list always outscores one that is not, so the list is the order in which
ownership moves between nodes.
""".
-behaviour(portunus_affinity).

-export([kind/0, score/3]).

-spec kind() -> deterministic.
kind() -> deterministic.

-spec score(term(), [node()], [node()]) -> non_neg_integer().
score(_Key, _Members, Order) ->
    case index_of(node(), Order, 0) of
        not_found -> 0;
        I -> length(Order) - I
    end.

index_of(_X, [], _I) -> not_found;
index_of(X, [X | _], I) -> I;
index_of(X, [_ | T], I) -> index_of(X, T, I + 1).
