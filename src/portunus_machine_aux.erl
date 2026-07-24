%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_machine_aux).
-moduledoc """
Pure decision logic for `portunus_machine`'s aux renewal and expiry sweep.
Aux state is per-server, in-memory and never replicated. Renewal writes
nothing to the Raft log. Only expiry becomes a logged
`{expire_leases, ...}` command.

The rules match the etcd lessor:

 * every lease's operative deadline lives here, on the leader, in
   monotonic time. Renewal moves it forward without touching the log
 * a lease known to the machine but missing from `deadlines` is seeded
   at its full TTL. A new leader, a restarted server and a fresh grant
   all err toward late expiry, never early
 * an expiry proposal carries the lease's `refreshed` index as a fence.
   While the machine still holds the lease at that exact index, the
   proposal is live: renewals answer `lease_expired` and the sweep does
   not re-propose. Anything that changes `refreshed` or removes the
   lease voids the entry
 * a term change means another leader renewed these holders in between,
   so both maps are cleared first

Every function takes the applied leases as a `lease_view()` map
(`#{lease_id() => {ttl_ms, refreshed}}`), the current Raft term and a
caller-supplied monotonic `now`.

This means that the decisions can be tested
without a Ra cluster.

`portunus_machine:handle_aux/5` extracts the
inputs and turns the outputs into effects.
""".

-export([new/0,
         non_leader_tick/1,
         leader_tick/4,
         renew/5,
         refreshed/5]).

-record(aux, {term :: non_neg_integer() | undefined,
              %% deadlines in the caller's monotonic milliseconds
              deadlines = #{} :: #{portunus:lease_id() => integer()},
              %% in-flight expiry proposals, fenced by `refreshed` index
              pending = #{} :: #{portunus:lease_id() => ra:index()}}).

-opaque aux() :: #aux{}.
-type lease_view() :: #{portunus:lease_id() =>
                            {pos_integer(), ra:index()}}.
-type expire_pair() :: {portunus:lease_id(), ra:index()}.

-export_type([aux/0, lease_view/0, expire_pair/0]).

-spec new() -> aux().
new() ->
    #aux{}.

-doc "A non-leader holds no deadlines. Clears both maps.".
-spec non_leader_tick(aux()) -> aux().
non_leader_tick(#aux{term = Term}) ->
    #aux{term = Term}.

-doc """
As the name suggests, this function is called on every leader tick.

Reconciles the term, drops entries for leases
the machine no longer holds, seeds untracked leases at their full TTL, then
proposes expiry for every deadline at or past `Now` that doesn't already have
a live proposal.

Returns the pairs to append as one `{expire_leases, ...}`
command. Pairs are sorted so that tests observe a stable order.
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
Renews each lease the machine still holds that has no live expiry proposal.

A lease with a live proposal answers `lease_expired` even
though the command has not applied yet. The appended command may still
expire it, so acknowledging the renewal would be wrong.
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
Called once a `grant_lease` command has been committed, whether it created the
lease or was a repeat grant by the same owner.

Resets each granted lease's
aux deadline to a full TTL (`Now + Ttl`) so that a lease whose old deadline
had already passed is not removed as expired right after its grant succeeded.
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

%% Deadlines from a previous leadership of this server are stale.
%% The holders were renewing with the interim leader.
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
