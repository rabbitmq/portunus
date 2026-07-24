%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_aux).
-moduledoc """
The pure decision core behind `portunus_machine`'s aux renewal and expiry
sweep. Aux state is per-server and in-memory, never replicated, so lease
renewal writes nothing to the Raft log; only expiry, an actual state
change, becomes a logged `{expire_leases, ...}` command.

The rules, the same ones the etcd lessor uses:

 * the operative deadline of every lease lives here, on the leader, in
   monotonic time; renewal moves it forward without touching the log
 * a lease known to the machine but absent from `deadlines` is seeded at
   its full TTL, so a new leader, a restarted server, and a fresh grant
   all err toward late expiry, never early
 * an expiry proposal carries the lease's `refreshed` index as a fence;
   while the machine still holds the lease with that exact index the
   proposal is live, renewals for the lease answer `lease_expired`, and
   the sweep does not re-propose it. Anything that changes `refreshed`
   (or removes the lease) voids the entry
 * a term change means another leader renewed these holders in between,
   so both maps are cleared before anything else

Every function takes the applied leases as a view
(`#{lease_id() => {ttl_ms, refreshed}}`), the current Raft term, and a
caller-supplied monotonic `now`, so the decisions are testable without a
Ra cluster; `portunus_machine:handle_aux/5` extracts the inputs and turns
the outputs into effects.
""".

-export([new/0,
         non_leader_tick/1,
         leader_tick/4,
         renew/5,
         refreshed/5]).

-record(aux, {term :: non_neg_integer() | undefined,
              %% operative deadlines, in the caller's monotonic milliseconds
              deadlines = #{} :: #{portunus:lease_id() => integer()},
              %% expiry proposals in flight: lease id to the `refreshed`
              %% index the proposal was fenced with
              pending = #{} :: #{portunus:lease_id() => ra:index()}}).

-opaque aux() :: #aux{}.
-type lease_view() :: #{portunus:lease_id() =>
                            {pos_integer(), ra:index()}}.
-type expire_pair() :: {portunus:lease_id(), ra:index()}.

-export_type([aux/0, lease_view/0, expire_pair/0]).

-spec new() -> aux().
new() ->
    #aux{}.

-doc "A non-leader holds no operative deadlines: clear both maps.".
-spec non_leader_tick(aux()) -> aux().
non_leader_tick(#aux{term = Term}) ->
    #aux{term = Term}.

-doc """
The leader sweep: reconcile the term, drop entries for leases the machine
no longer holds and void pending entries, seed leases not yet tracked at
their full TTL, then propose expiry for every deadline at or past `Now`
that has no live proposal. Returns the expire pairs to append as one
`{expire_leases, ...}` command; pairs are sorted so tests see a stable
order.
""".
-spec leader_tick(aux(), lease_view(), non_neg_integer(), integer()) ->
    {aux(), [expire_pair()]}.
leader_tick(Aux0, Leases, Term, Now) ->
    #aux{deadlines = Deadlines0, pending = Pending0} = reconcile(Aux0, Term),
    Deadlines1 = maps:with(maps:keys(Leases), Deadlines0),
    Pending = maps:filter(fun(Id, Fence) -> live(Id, Fence, Leases) end,
                          Pending0),
    Deadlines = maps:merge(
                  #{Id => Now + Ttl || Id := {Ttl, _} <- Leases,
                                       not is_map_key(Id, Deadlines1)},
                  Deadlines1),
    Pairs = lists:sort(
              [{Id, fence(Id, Leases)}
               || Id := Deadline <- Deadlines,
                  Deadline =< Now,
                  not is_map_key(Id, Pending)]),
    {#aux{term = Term,
          deadlines = Deadlines,
          pending = maps:merge(Pending, maps:from_list(Pairs))},
     Pairs}.

-doc """
Renew each lease the machine still holds and that has no live expiry
proposal. A lease with a live proposal answers `lease_expired` (the
standard possible-loss answer) even though the command has not applied
yet: the appended command may still expire it, so acknowledging the
renewal would be wrong.
""".
-spec renew(aux(), lease_view(), non_neg_integer(), integer(),
            [portunus:lease_id()]) ->
    {aux(), [{portunus:lease_id(), ok | {error, lease_expired}}]}.
renew(Aux0, Leases, Term, Now, LeaseIds) ->
    Aux1 = reconcile(Aux0, Term),
    lists:foldr(
      fun(Id, {#aux{deadlines = Ds, pending = Pending} = Aux, Acc}) ->
              case Leases of
                  #{Id := {Ttl, Fence}}
                    when not is_map_key(Id, Pending);
                         map_get(Id, Pending) =/= Fence ->
                      {Aux#aux{deadlines = Ds#{Id => Now + Ttl}},
                       [{Id, ok} | Acc]};
                  _ ->
                      {Aux, [{Id, {error, lease_expired}} | Acc]}
              end
      end, {Aux1, []}, LeaseIds).

-doc """
A grant committed (initial or an idempotent re-grant): extend the aux
deadlines to the full TTL, so a re-granted lease whose old deadline had
passed is not proposed for expiry right after a successful grant.
""".
-spec refreshed(aux(), lease_view(), non_neg_integer(), integer(),
                [portunus:lease_id()]) -> aux().
refreshed(Aux0, Leases, Term, Now, LeaseIds) ->
    #aux{deadlines = Ds0} = Aux = reconcile(Aux0, Term),
    Ds = lists:foldl(fun(Id, Acc) ->
                             case Leases of
                                 #{Id := {Ttl, _}} -> Acc#{Id => Now + Ttl};
                                 _ -> Acc
                             end
                     end, Ds0, LeaseIds),
    Aux#aux{deadlines = Ds}.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

%% Deadlines from a previous leadership of this server are stale: the
%% holders were renewing with the interim leader.
reconcile(#aux{term = Term} = Aux, Term) ->
    Aux;
reconcile(_Aux, Term) ->
    #aux{term = Term}.

live(Id, Fence, Leases) ->
    case Leases of
        #{Id := {_Ttl, Fence}} -> true;
        _ -> false
    end.

fence(Id, Leases) ->
    {_Ttl, Fence} = maps:get(Id, Leases),
    Fence.
