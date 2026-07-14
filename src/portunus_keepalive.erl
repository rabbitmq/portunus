%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_keepalive).
-moduledoc """
This module implements automatic background lease renewal
for a holder that is an Erlang process.

It is linked to the caller (usually the resource holder) and
traps exits.

If the resource holder process dies, the renewer stops.
Owners that can be restarted should set up the renewer in
their init function.

This module can be used directly, but is usually set up through
`portunus:grant_lease/3` with the `auto_renew` option, or
`portunus:lock/3`.

`LeaseId` is renewed every `max(TTL/3, 1000)` milliseconds, and the
minimum supported TTL is 2000 ms, allowing for at least two renewal
attempts per TTL interval.

A lease renewal attempt can fail if the `portunus` Ra machine does not
have an online quorum.

The renewer retries until the lease expires.
""".

-include("portunus.hrl").

-behaviour(gen_server).

-export([start_link/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {name :: portunus:name(),
                lease_id :: portunus:lease_id(),
                holder :: pid(),
                ttl_ms :: pos_integer(),
                interval :: pos_integer(),
                last_ok :: integer()}).

-spec start_link(portunus:name(), portunus:lease_id(), pos_integer()) ->
    {ok, pid()}.
start_link(Name, LeaseId, TtlMs)
  when is_integer(TtlMs), TtlMs >= ?MIN_RENEWABLE_TTL_MS ->
    Holder = self(),
    gen_server:start_link(?MODULE, {Name, LeaseId, Holder, TtlMs}, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    %% An already-stopped renewer is this call's goal state, not an error:
    %% it stops itself after declaring the lease lost.
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end.

init({Name, LeaseId, Holder, TtlMs}) ->
    %% Trap exits so the holder's exit for any reason, including `normal`,
    %% stops the renewer.
    process_flag(trap_exit, true),
    proc_lib:set_label({portunus_keepalive, Name, LeaseId}),
    Interval = max(TtlMs div 3, 1000),
    schedule(Interval),
    {ok, #state{name = Name, lease_id = LeaseId, holder = Holder,
                ttl_ms = TtlMs, interval = Interval, last_ok = now_ms()}}.

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(renew, #state{name = Name, lease_id = LeaseId, ttl_ms = TtlMs,
                          interval = Interval} = State) ->
    case portunus:renew_leases(Name, [LeaseId], renew_timeout(TtlMs)) of
        [{LeaseId, ok}] ->
            schedule(Interval),
            {noreply, State#state{last_ok = now_ms()}};
        [{LeaseId, {error, lease_expired}}] ->
            lose(State);
        _ ->
            %% Transient (no quorum, timeout): the lease is still valid until
            %% its deadline, so keep trying until a whole TTL has elapsed
            %% without a confirmed renewal.
            case now_ms() - State#state.last_ok >= TtlMs of
                true -> lose(State);
                false -> schedule(retry_interval(TtlMs)),
                         {noreply, State}
            end
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

lose(#state{holder = Holder, lease_id = LeaseId} = State) ->
    Holder ! {portunus, lease_lost, LeaseId},
    {stop, normal, State}.

schedule(Interval) ->
    erlang:send_after(Interval, self(), renew).

retry_interval(TtlMs) ->
    max(TtlMs div 10, 250).

%% An unreachable leader makes the command block for this long.
renew_timeout(TtlMs) ->
    max(TtlMs div 5, 500).

now_ms() ->
    erlang:monotonic_time(millisecond).
