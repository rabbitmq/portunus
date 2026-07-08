%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_supervisor).
-moduledoc """
A declarative, static supervisor. "Static" isn't an Erlang/OTP supervisor
type here: it means the child set is fixed, returned once by `init/1`,
unlike `portunus_registry`, whose children are added and removed at
runtime.

Only one of each child spec returned by `init/1` can exist in the cluster at any
given time. The elected owner runs it under a local Erlang/OTP
supervisor, and `portunus` drives the cross-node leadership (ownership)
transfer.

It is a thin layer built on top of `portunus_registry`: `start_link/3,4` reads
the desired children from `init/1` once and registers each one, and returns
the underlying `portunus_registry` pid. The fixed child set fixes only what
runs, not where it runs, so `which_children/1` reports what this node runs and
`transfer/3` hands a child to a chosen node, the same deliberate rebalancing
that dynamic children get. Both take the returned handle, which is a
`portunus_registry`, so its other operations act on it too. The registry owns
the local supervisor, so the whole tree shares one lifetime and nothing is
leaked or orphaned. Children are namespaced by group (the
callback module by default, or `#{group => G}`), so several declarative
supervisors share one cluster without colliding. The callback mirrors
`supervisor`:

```erlang
-callback init(Args :: term()) ->
    {ok, {supervisor:sup_flags(),
          [portunus_delayed_restart:child_spec_in()]}} |
    ignore.
```

Child specs are validated at registration: an invalid one aborts
`start_link` with `{error, {invalid_child_spec, _}}`.
""".

-export([start_link/3, start_link/4, transfer/3, which_children/1]).

-callback init(term()) ->
    {ok, {supervisor:sup_flags(),
          [portunus_delayed_restart:child_spec_in()]}} |
    ignore.

-spec start_link(portunus:name(), module(), term()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Name, Mod, Args) ->
    start_link(Name, Mod, Args, #{}).

-spec start_link(portunus:name(), module(), term(), portunus_registry:registry_opts()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(Name, Mod, Args, Opts) ->
    case Mod:init(Args) of
        {ok, {SupFlags, Children}} ->
            RegOpts = Opts#{sup_flags => SupFlags,
                            group => maps:get(group, Opts, Mod)},
            case portunus_registry:start_link(Name, RegOpts) of
                {ok, Registry} ->
                    add_children(Registry, Children);
                {error, _} = Err ->
                    Err
            end;
        ignore ->
            ignore
    end.

-doc """
Hand the child registered under `Key` to `TargetNode`, if this node currently
owns it. Delegates to `portunus_registry:transfer/3`; see there for the reply.
""".
-spec transfer(pid(), term(), node()) ->
    portunus:ok_or_error({no_contender, node()} | not_owner | no_quorum).
transfer(Supervisor, Key, TargetNode) ->
    portunus_registry:transfer(Supervisor, Key, TargetNode).

-doc "The children this node currently runs. Delegates to `portunus_registry:which_children/1`.".
-spec which_children(pid()) ->
    [{term(), pid() | restarting | undefined, worker | supervisor,
      [module()] | dynamic}].
which_children(Supervisor) ->
    portunus_registry:which_children(Supervisor).

%% An invalid spec aborts the start, as `supervisor:start_link/3` does for a
%% child that fails to start.
add_children(Registry, Children) ->
    Bad = lists:filtermap(
            fun(Spec) ->
                    case portunus_registry:add(Registry, Spec) of
                        ok -> false;
                        {error, Reason} -> {true, {Spec, Reason}}
                    end
            end, Children),
    case Bad of
        [] ->
            {ok, Registry};
        [{_Spec, Reason} | _] ->
            ok = portunus_registry:stop(Registry),
            {error, Reason}
    end.
