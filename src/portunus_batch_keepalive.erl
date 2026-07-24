%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_batch_keepalive).
-moduledoc """
Renews multiple leases in a single renewal round, for nodes with a lot
of processes that use automatic lease renewal.

`portunus_keepalive` gives every holder its own renewer, so a node with N
leases sends N renewal calls every TTL/3, each a leader round trip with a
quorum heartbeat.

This module is one registered process per node
that holders attach leases to; leases sharing a `{ClusterName, TTL}` pair renew
together in a single `portunus:renew_leases/3` call, so the rate
is per node, not per lease.

Resource owner processes monitor it and
re-attach should this process fail and restart.
A lease survives such a failure scenario as long as the owner process
re-attaches within a TTL of the last renewal.
""".

-include("portunus.hrl").

-behaviour(gen_server).

-export([start_link/0, attach/3, detach/2, overview/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-type group_key() :: {portunus:cluster_name(), portunus:ttl()}.

%% `last_ok` starts at attach time: the give-up rule needs a baseline.
-record(entry, {holder :: pid(),
                mon :: reference(),
                last_ok :: integer()}).

-record(group, {leases = #{} :: #{portunus:lease_id() => #entry{}},
                %% Rounds never overlap: the next timer is armed only when
                %% the current round's result (or crash) arrives.
                round = idle :: idle | reference()}).

-record(state, {groups = #{} :: #{group_key() => #group{}},
                %% Lock owner monitor to its lease, for O(1) 'DOWN' handling.
                mons = #{} :: #{reference() => {group_key(), portunus:lease_id()}}}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc """
Attach `LeaseId` for batched renewal, with the caller as its lock owner.
Pass the TTL the lease was granted with. The owner process should monitor
this server (registered as `portunus_batch_keepalive`) and re-attach on
`'DOWN'`.
""".
-spec attach(portunus:cluster_name(), portunus:lease_id(), portunus:ttl()) -> ok.
attach(ClusterName, LeaseId, TtlMs)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS ->
    gen_server:call(?MODULE, {attach, ClusterName, LeaseId, TtlMs, self()}).

-doc "Stop renewing `LeaseId`. Idempotent.".
-spec detach(portunus:cluster_name(), portunus:lease_id()) -> ok.
detach(ClusterName, LeaseId) ->
    gen_server:call(?MODULE, {detach, ClusterName, LeaseId}).

-doc "The attached leases per `{ClusterName, TTL}` group. For introspection and tests.".
-spec overview() -> #{group_key() => [portunus:lease_id()]}.
overview() ->
    gen_server:call(?MODULE, overview).

init([]) ->
    {ok, #state{}}.

handle_call({attach, ClusterName, LeaseId, TtlMs, Holder}, _From,
            #state{groups = Groups, mons = Mons} = State) ->
    Key = {ClusterName, TtlMs},
    Group0 = maps:get(Key, Groups, #group{}),
    case map_size(Group0#group.leases) of
        0 -> schedule(Key, interval(TtlMs));
        _ -> ok
    end,
    {Group1, Mons1} =
        case maps:take(LeaseId, Group0#group.leases) of
            {#entry{mon = OldMon}, Rest} ->
                %% Drop the old monitor so a retried attach does not leak one.
                erlang:demonitor(OldMon, [flush]),
                {Group0#group{leases = Rest}, maps:remove(OldMon, Mons)};
            error ->
                {Group0, Mons}
        end,
    Mon = erlang:monitor(process, Holder),
    Entry = #entry{holder = Holder, mon = Mon, last_ok = now_ms()},
    Group = Group1#group{leases = maps:put(LeaseId, Entry, Group1#group.leases)},
    {reply, ok, State#state{groups = Groups#{Key => Group},
                            mons = maps:put(Mon, {Key, LeaseId}, Mons1)}};
handle_call({detach, ClusterName, LeaseId}, _From, State) ->
    {reply, ok, remove_lease_by_name(ClusterName, LeaseId, State)};
handle_call(overview, _From, #state{groups = Groups} = State) ->
    {reply, maps:map(fun(_K, #group{leases = Ls}) -> maps:keys(Ls) end, Groups),
     State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({renew, Key}, #state{groups = Groups} = State) ->
    case Groups of
        #{Key := #group{round = Ref}} when is_reference(Ref) ->
            %% Stale timer from a dropped and re-created group; the result
            %% handler arms the next one.
            {noreply, State};
        #{Key := #group{leases = Leases} = Group} when map_size(Leases) > 0 ->
            {ClusterName, TtlMs} = Key,
            Ids = maps:keys(Leases),
            Server = self(),
            {_Pid, Ref} =
                spawn_monitor(
                  fun() ->
                          Res = portunus:renew_leases(ClusterName, Ids,
                                                      renew_timeout(TtlMs)),
                          Server ! {renew_result, Key, Res}
                  end),
            {noreply,
             State#state{groups = Groups#{Key := Group#group{round = Ref}}}};
        _ ->
            {noreply, State#state{groups = maps:remove(Key, Groups)}}
    end;
handle_info({renew_result, Key, Results}, #state{groups = Groups} = State0) ->
    case Groups of
        #{Key := #group{round = Ref} = Group} when is_reference(Ref) ->
            erlang:demonitor(Ref, [flush]),
            State = State0#state{groups =
                                     Groups#{Key := Group#group{round = idle}}},
            {noreply, apply_results(Key, Results, State)};
        _ ->
            {noreply, State0}
    end;
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            #state{groups = Groups, mons = Mons} = State) ->
    case maps:take(Ref, Mons) of
        {{Key, LeaseId}, Mons1} ->
            %% A lock owner died; the machine's monitor releases its lease.
            {noreply, remove_lease(Key, LeaseId,
                                   State#state{mons = Mons1})};
        error ->
            %% A helper crashed mid-round: a transient failure for its group.
            case [K || {K, #group{round = R}} <- maps:to_list(Groups),
                       R =:= Ref] of
                [Key] ->
                    #{Key := Group} = Groups,
                    State1 = State#state{groups =
                                             Groups#{Key := Group#group{round = idle}}},
                    {noreply, apply_results(Key, transient, State1)};
                [] ->
                    {noreply, State}
            end
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

%% `transient` (a crashed helper or a non-list return) counts as no result
%% for every lease; a lease attached mid-round has no result either and
%% waits for the next round.
apply_results({_ClusterName, TtlMs} = Key, Results, State0) ->
    Now = now_ms(),
    ByLease = case Results of
                  L when is_list(L) -> maps:from_list(L);
                  _ -> #{}
              end,
    #state{groups = #{Key := Group0}} = State0,
    {Leases, State1, AnyTransient} =
        maps:fold(
          fun(LeaseId, #entry{last_ok = LastOk} = E, {Acc, St, Transient}) ->
                  case maps:get(LeaseId, ByLease, transient) of
                      ok ->
                          {Acc#{LeaseId => E#entry{last_ok = Now}}, St, Transient};
                      {error, lease_expired} ->
                          {Acc, lose(Key, LeaseId, E, St), Transient};
                      _ when Now - LastOk >= TtlMs ->
                          %% A full TTL without a confirmed renewal: the machine
                          %% has expired it (or will before we can renew).
                          {Acc, lose(Key, LeaseId, E, St), Transient};
                      _ ->
                          {Acc#{LeaseId => E}, St, true}
                  end
          end, {#{}, State0, false}, Group0#group.leases),
    Groups = State1#state.groups,
    case map_size(Leases) of
        0 ->
            State1#state{groups = maps:remove(Key, Groups)};
        _ ->
            #{Key := Group1} = Groups,
            Next = case AnyTransient of
                       true -> retry_interval(TtlMs);
                       false -> interval(TtlMs)
                   end,
            schedule(Key, Next),
            State1#state{groups =
                             Groups#{Key := Group1#group{leases = Leases}}}
    end.

lose(_Key, LeaseId, #entry{holder = Holder, mon = Mon},
     #state{mons = Mons} = State) ->
    erlang:demonitor(Mon, [flush]),
    Holder ! {portunus, lease_lost, LeaseId},
    State#state{mons = maps:remove(Mon, Mons)}.

%% Detach without knowing the TTL: scan this cluster's groups.
remove_lease_by_name(ClusterName, LeaseId, #state{groups = Groups} = State) ->
    case [K || {N, _} = K <- maps:keys(Groups), N =:= ClusterName,
               maps:is_key(LeaseId, (maps:get(K, Groups))#group.leases)] of
        [Key | _] -> remove_lease(Key, LeaseId, State);
        [] -> State
    end.

remove_lease(Key, LeaseId, #state{groups = Groups, mons = Mons} = State) ->
    case Groups of
        #{Key := #group{leases = Leases} = Group} ->
            case maps:take(LeaseId, Leases) of
                {#entry{mon = Mon}, Leases1} ->
                    erlang:demonitor(Mon, [flush]),
                    Groups1 = case {map_size(Leases1), Group#group.round} of
                                  {0, idle} -> maps:remove(Key, Groups);
                                  %% Keep an emptied group until its round
                                  %% settles, so the result handler finds it.
                                  _ -> Groups#{Key := Group#group{leases = Leases1}}
                              end,
                    State#state{groups = Groups1, mons = maps:remove(Mon, Mons)};
                error ->
                    State
            end;
        _ ->
            State
    end.

schedule(Key, Interval) ->
    _ = erlang:send_after(Interval, self(), {renew, Key}),
    ok.

interval(TtlMs) ->
    max(TtlMs div 3, 1000).

retry_interval(TtlMs) ->
    max(TtlMs div 10, 250).

%% An unreachable leader makes the command block for this long.
renew_timeout(TtlMs) ->
    max(TtlMs div 5, 500).

now_ms() ->
    erlang:monotonic_time(millisecond).
