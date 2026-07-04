%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_affinity).
-moduledoc """
An affinity strategy gives each `portunus` node (cluster member) a score
for a key, and `portunus_machine` succession promotes the highest-scoring
contender. Ties are broken in favor of the node that joined the succession
queue first.

Two strategies are supported:

 * `deterministic`: a pure function of the key and the cluster members,
    so every node computes the same ranking. Built-in variants: `fifo`, `pinned`,
    `preferred`, `hash`
  * `dynamic`: a node scores itself from local dynamically calculated state
    such as metrics (load, resource consumption, etc). Built-in variants: `metric`, `random`

Affinity strategies are passed around as a `spec()` that carries a short
name (`fifo`, `pinned`, `random`, and so on). To use a custom module,
pass a `{Module, Args}` tuple where `Module` implements the `kind/0`
and `score/3` callbacks.
""".

-callback kind() -> deterministic | dynamic.
-callback score(Key :: term(), Members :: [node()], Args :: term()) ->
    integer().

-type strategy() :: fifo | pinned | preferred | hash | metric | random.
-type spec() :: default
              | {strategy() | module(), term()}
              | fun((term(), [node()]) -> integer()).
-export_type([strategy/0, spec/0]).

-export([score/3, kind/1]).

%% Map a built-in tag to its strategy module; let any other module name
%% (a custom behaviour) through unchanged.
-spec module(strategy() | module()) -> module().
module(fifo)      -> portunus_affinity_fifo;
module(pinned)    -> portunus_affinity_pinned;
module(preferred) -> portunus_affinity_preferred;
module(hash)      -> portunus_affinity_hash;
module(metric)    -> portunus_affinity_metric;
module(random)    -> portunus_affinity_random;
module(Mod)       -> Mod.

-doc "This node's score for `Key`, given the current cluster `Members`.".
-spec score(spec(), term(), [node()]) -> integer().
score(default, _Key, _Members) ->
    0;
score({Strategy, Args}, Key, Members) ->
    (module(Strategy)):score(Key, Members, Args);
score(Fun, Key, Members) when is_function(Fun, 2) ->
    Fun(Key, Members).

-spec kind(spec()) -> deterministic | dynamic.
kind(default) ->
    deterministic;
kind({Strategy, _Args}) ->
    (module(Strategy)):kind();
kind(Fun) when is_function(Fun, 2) ->
    dynamic.
