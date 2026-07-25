%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_transfer_batch_prop_SUITE).

%% `portunus_transfer_prop_SUITE` covers single-transfer scenarios.
%% This suite adds the `transfer_batch` command to the mix.

-include_lib("proper/include/proper.hrl").

-export([all/0, batch_chain_stays_safe/1]).

all() ->
    [batch_chain_stays_safe].

batch_chain_stays_safe(_Config) ->
    true = portunus_test_helpers:quickcheck(
             fun prop_batch_chain_stays_safe/0, 500).

%% Over a random chain of single transfers, batches and owner kills, every
%% observed key stays with a known participant and its token never decreases.
prop_batch_chain_stays_safe() ->
    ?FORALL({K, W}, {choose(2, 4), choose(2, 4)},
            ?FORALL(Ops, list(op(K, W)),
                    begin
                        {Keys, Owners, S0} = setup(K, W),
                        run(Keys, Owners, Ops, S0)
                    end)).

op(K, W) ->
    oneof([{single, choose(1, K), choose(0, W - 1), tok()},
           {batch, non_empty(list({choose(1, K), choose(0, W - 1), tok()}))},
           {kill, oneof([holder | [{waiter, I} || I <- lists:seq(0, W - 1)]])}]).

tok() -> oneof([current, stale]).

%% Holder o0 (lease l0) holds every key; contenders {w,0}..{w,W-1} each wait on
%% every key with a distinct score, so promotion order is deterministic.
setup(K, W) ->
    Keys = [{k, I} || I <- lists:seq(1, K)],
    Cmds = [{grant_lease, l0, 100000, o0, self()}] ++
           [{acquire, l0, Key, o0, undefined, nowait} || Key <- Keys] ++
           lists:append(
             [[{grant_lease, {l, I}, 100000, {w, I}, self()} |
               [{acquire, {l, I}, Key, {w, I}, undefined, wait, I} || Key <- Keys]]
              || I <- lists:seq(0, W - 1)]),
    {S, _} = lists:foldl(fun(Cmd, {St, Ix}) ->
                                 {_, St1, _} = step(Cmd, Ix, St),
                                 {St1, Ix + 1}
                         end,
                         {portunus_machine:init(#{cluster => test}), 1}, Cmds),
    Owners = [o0 | [{w, I} || I <- lists:seq(0, W - 1)]],
    {Keys, Owners, S}.

run(Keys, Owners, Ops, S0) ->
    {_, _, Ok, _} =
        lists:foldl(
          fun(_Op, {Ix, S, false, TM}) ->
                  {Ix, S, false, TM};
             (Op, {Ix, S, true, TM}) ->
                  {_, S1, _} = do_op(Op, Keys, Ix, S),
                  {Ok, TM1} = check(Keys, Owners, S1, TM),
                  {Ix + 1, S1, Ok, TM1}
          end, {1000, S0, true, #{}}, Ops),
    Ok.

do_op({single, KeyIx, TgtIx, TokChoice}, Keys, Ix, S) ->
    Key = lists:nth(KeyIx, Keys),
    step({transfer, Key, token_for(Key, TokChoice, S), {w, TgtIx}}, Ix, S);
do_op({batch, Items}, Keys, Ix, S) ->
    Batch = [begin
                 Key = lists:nth(KeyIx, Keys),
                 {Key, token_for(Key, TokChoice, S), {w, TgtIx}}
             end || {KeyIx, TgtIx, TokChoice} <- Items],
    step({transfer_batch, Batch}, Ix, S);
do_op({kill, Who}, _Keys, Ix, S) ->
    step({revoke_lease, lease_of(Who)}, Ix, S).

token_for(Key, current, S) ->
    case portunus_machine:query_owner(Key, S) of
        {ok, #{token := T}} -> T;
        _ -> 1
    end;
token_for(Key, stale, S) ->
    %% One above the current token no longer matches, so it fences exactly as
    %% a genuinely old token would.
    token_for(Key, current, S) + 1.

lease_of(holder) -> l0;
lease_of({waiter, I}) -> {l, I}.

check(Keys, Owners, S, TokenMap) ->
    lists:foldl(
      fun(Key, {Ok, TM}) ->
              case portunus_machine:query_owner(Key, S) of
                  {ok, #{owner := O, token := T}} ->
                      Last = maps:get(Key, TM, 0),
                      {Ok andalso lists:member(O, Owners) andalso T >= Last,
                       TM#{Key => max(T, Last)}};
                  {error, not_held} ->
                      {Ok, TM}
              end
      end, {true, TokenMap}, Keys).

step(Cmd, Ix, State) ->
    Meta = portunus_test_helpers:meta(Ix),
    case portunus_machine:apply(Meta, Cmd, State) of
        {S, Reply} -> {Reply, S, []};
        {S, Reply, Effects} -> {Reply, S, Effects}
    end.
