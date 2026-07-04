%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_local_sup).
-moduledoc false.

%% A generic, initially-empty Erlang/OTP supervisor parameterised by sup
%% flags. Started by `portunus_registry` as the local supervisor an elected
%% owner boots its children into. Accepts sup flags in either the map
%% or the legacy tuple form, like `supervisor` itself, so a callback ported
%% from `mirrored_supervisor` can return its `init/1` flags unchanged.

-behaviour(supervisor).

-export([init/1]).

-spec init({term(), supervisor:sup_flags()}) ->
    {ok, {supervisor:sup_flags(), []}}.
init({Label, SupFlags}) when is_map(SupFlags); is_tuple(SupFlags) ->
    proc_lib:set_label({?MODULE, Label}),
    {ok, {SupFlags, []}}.
