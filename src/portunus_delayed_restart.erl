%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_delayed_restart).
-moduledoc """
Rate-limited restarts for plain Erlang/OTP supervisors, accepting the extended
`supervisor2` restart type without a dependency on `supervisor2`.

`mirrored_supervisor` is built on rabbit's `supervisor2`, which accepts an
extended restart type `{permanent, Delay}` or `{transient, Delay}` (Delay
in seconds). A child spec ported from `mirrored_supervisor` can carry that
form; `portunus` runs elected children under a plain Erlang/OTP
`supervisor`, which rejects it.

`child_spec/1` rewrites such a spec into a standard one whose start is
rate-limited to one attempt per `Delay`: an isolated crash restarts at
once, and only a restart within `Delay` of the previous attempt waits out
the remainder. This is not exactly `supervisor2`, which restarts
immediately until MaxR/MaxT is exceeded and only then delays on a timer;
the practical property migrated specs rely on (a one-off crash recovers
immediately while a crash loop is dampened) is the same. The wait runs in
the local supervisor's own process (a supervisor runs a child's start
synchronously), so a crash-looping child holds up the others under that
supervisor for up to `Delay`; the running pid is always the real child, so
introspection and links are unchanged.

Last-attempt timestamps live in one node-global ETS set owned by
`portunus_sup`, keyed by `{LocalSup, ChildId}` so entries are namespaced
per local supervisor; `forget/2` clears one when the child is stopped, so
a later first start (for example after ownership moves back to this
node) is immediate.
""".

-export([child_spec/1, forget/2, forget_all/1, ensure_table/0]).
%% Invoked as the wrapped child's start function.
-export([start_link/3]).

-define(MARKERS, portunus_delayed_restart_markers).

-type mfargs() :: {module(), atom(), [term()]}.
-doc """
The extended `supervisor2` restart: a standard type plus a delay in seconds
before a restart.
""".
-type delayed_restart() :: {permanent | transient, number()}.
-doc """
Accepted input: a standard child spec (passed through untouched), or one
carrying the extended restart, as a map or a `supervisor2` tuple.
""".
-type child_spec_in() ::
        supervisor:child_spec()
      | #{id := term(), start := mfargs(), restart := delayed_restart(),
          shutdown => timeout() | brutal_kill,
          type => worker | supervisor,
          modules => [module()] | dynamic}
      | {term(), mfargs(), delayed_restart(),
         timeout() | brutal_kill, worker | supervisor,
         [module()] | dynamic}.
-export_type([child_spec_in/0]).

%% `supervisor2` accepts a delay of 0 (restart immediately) and float
%% delays, so both forms are rewritten too.
-spec child_spec(child_spec_in()) -> supervisor:child_spec().
child_spec(#{restart := {Type, 0}} = Spec)
  when Type =:= permanent orelse Type =:= transient ->
    Spec#{restart => Type};
child_spec(#{restart := {Type, Delay}, start := {M, F, A}, id := Id} = Spec)
  when (Type =:= permanent orelse Type =:= transient),
       is_number(Delay), Delay > 0 ->
    Spec#{restart => Type, start => {?MODULE, start_link, [Id, Delay, {M, F, A}]}};
child_spec({Id, MFA, {Type, 0}, Shutdown, ChildType, Modules})
  when Type =:= permanent orelse Type =:= transient ->
    {Id, MFA, Type, Shutdown, ChildType, Modules};
child_spec({Id, {M, F, A}, {Type, Delay}, Shutdown, ChildType, Modules})
  when (Type =:= permanent orelse Type =:= transient),
       is_number(Delay), Delay > 0 ->
    {Id, {?MODULE, start_link, [Id, Delay, {M, F, A}]},
     Type, Shutdown, ChildType, Modules};
child_spec(Spec) ->
    Spec.

%% Returns whatever the wrapped start MFA returns.
-spec start_link(term(), number(), {module(), atom(), [term()]}) -> dynamic().
start_link(Id, Delay, {M, F, A}) ->
    %% A supervisor runs this start function in its own process, so `self()` is
    %% the local supervisor and namespaces the marker without threading it in.
    Key = {self(), Id},
    DelayMs = round(Delay * 1000),
    Now = erlang:monotonic_time(millisecond),
    case ets:lookup(markers(), Key) of
        [{Key, Last}] when Now - Last < DelayMs ->
            timer:sleep(DelayMs - (Now - Last));
        _ ->
            ok
    end,
    %% Attempt time, recorded whether or not the start succeeds, so a
    %% failing start is paced too.
    true = ets:insert(markers(), {Key, erlang:monotonic_time(millisecond)}),
    apply(M, F, A).

-doc "Clear the last-attempt marker so the next start of `Id` is immediate.".
-spec forget(pid(), term()) -> ok.
forget(LocalSup, Id) ->
    case ets:whereis(?MARKERS) of
        undefined -> ok;
        _ -> _ = ets:delete(?MARKERS, {LocalSup, Id}), ok
    end.

-doc "Clear every marker of a local supervisor that is going away.".
-spec forget_all(pid()) -> ok.
forget_all(LocalSup) ->
    case ets:whereis(?MARKERS) of
        undefined -> ok;
        _ -> _ = ets:match_delete(?MARKERS, {{LocalSup, '_'}, '_'}), ok
    end.

%% Create the node-global marker table, owned by the caller. Called from
%% `portunus_sup:init/1` so the owner is the application's supervisor, not a
%% transient local supervisor whose death would drop every registry's markers.
-spec ensure_table() -> ok.
ensure_table() ->
    _ = markers(),
    ok.

markers() ->
    case ets:whereis(?MARKERS) of
        undefined ->
            try ets:new(?MARKERS, [named_table, public, set])
            catch error:badarg -> ?MARKERS
            end;
        _ ->
            ?MARKERS
    end.
