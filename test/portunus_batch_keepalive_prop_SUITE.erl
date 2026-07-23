%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_batch_keepalive_prop_SUITE).

-include_lib("proper/include/proper.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, attach_detach_balance/1]).

%% Long enough that no renewal round fires during a run; the property then
%% needs no Ra cluster.
-define(TTL, 60000).
-define(NAME, batch_keepalive_prop).

all() ->
    [attach_detach_balance].

init_per_suite(Config) ->
    case portunus_batch_keepalive:start_link() of
        {ok, Hub} ->
            %% The setup process does not exit `normal` under common_test,
            %% and the renewer must outlive it.
            unlink(Hub),
            [{hub, Hub} | Config];
        {error, {already_started, _}} ->
            %% An earlier suite started the portunus application; its
            %% supervised renewer serves as well.
            Config
    end.

end_per_suite(Config) ->
    case proplists:get_value(hub, Config) of
        undefined -> ok;
        Hub -> exit(Hub, kill)
    end,
    Config.

attach_detach_balance(_Config) ->
    true = portunus_test_helpers:quickcheck(fun prop_attach_detach_balance/0, 200).

%% After any sequence of attach, detach, and owner process kill, the renewer
%% tracks exactly the attached leases whose owner processes are alive.
prop_attach_detach_balance() ->
    ?FORALL(Ops, list(op()),
            begin
                {Model, Holders} = run_ops(Ops, #{}, #{}),
                Ok = await_attached(lists:sort(maps:keys(Model))),
                %% Leave no leases or owner processes behind for the next run.
                [ok = portunus_batch_keepalive:detach(?NAME, L)
                 || L <- maps:keys(Model)],
                [exit(H, kill) || H <- maps:values(Holders)],
                Ok =:= ok andalso await_attached([]) =:= ok
            end).

op() ->
    {oneof([attach, detach, kill]), integer(1, 5)}.

%% `Model`: leases the hub should be tracking; `Holders`: the last owner
%% process spawned per lease id.
run_ops([], Model, Holders) ->
    {Model, Holders};
run_ops([{attach, Id} | Ops], Model, Holders) ->
    H = spawn_holder(Id),
    %% The renewer has already replaced the entry; the superseded owner is
    %% just a leftover process.
    case Holders of
        #{Id := Old} -> exit(Old, kill);
        _ -> ok
    end,
    run_ops(Ops, Model#{Id => H}, Holders#{Id => H});
run_ops([{detach, Id} | Ops], Model, Holders) ->
    ok = portunus_batch_keepalive:detach(?NAME, Id),
    run_ops(Ops, maps:remove(Id, Model), Holders);
run_ops([{kill, Id} | Ops], Model, Holders) ->
    case Holders of
        #{Id := H} ->
            exit(H, kill),
            run_ops(Ops, maps:remove(Id, Model), Holders);
        _ ->
            run_ops(Ops, Model, Holders)
    end.

%% `attach/3` takes the caller as the lock owner, so each attach needs a
%% new (separate) process.
spawn_holder(Id) ->
    Ctrl = self(),
    H = spawn(fun() ->
                      ok = portunus_batch_keepalive:attach(?NAME, Id, ?TTL),
                      Ctrl ! {attached, self()},
                      receive stop -> ok end
              end),
    receive {attached, H} -> H
    after 5000 -> error(holder_attach_timeout)
    end,
    H.

%% 'DOWN' processing races the check; poll briefly.
await_attached(Expected) ->
    await_attached(Expected, 100).

await_attached(Expected, Left) ->
    %% Only this suite's group: a shared renewer may track other suites' leases.
    Attached = lists:sort(
                 maps:get({?NAME, ?TTL},
                          portunus_batch_keepalive:overview(), [])),
    case {Attached, Left} of
        {Expected, _} -> ok;
        {_, 0} -> {unexpected, Attached, Expected};
        _ -> timer:sleep(10), await_attached(Expected, Left - 1)
    end.
