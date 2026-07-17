%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

-module(portunus_rejoin_unit_SUITE).

%% The `join_cluster1/3` decision over (locally known, listed as member). Four
%% inputs, three actions; the domain is exhaustively enumerated, so no property
%% suite accompanies this table.

-include_lib("eunit/include/eunit.hrl").

-export([all/0]).
-export([rejoin_action_table/1]).

all() ->
    [rejoin_action_table].

rejoin_action_table(_Config) ->
    %% Known and listed: at most stopped, restart it.
    ?assertEqual(restart, portunus:rejoin_action(true, true)),
    %% Listed but unknown locally: the remembered identity is gone; a fresh
    %% server under it could double-vote, so evict and rejoin as new.
    ?assertEqual(evict_then_join, portunus:rejoin_action(false, true)),
    %% Not listed: the ordinary join, whether or not a local server exists.
    ?assertEqual(join, portunus:rejoin_action(true, false)),
    ?assertEqual(join, portunus:rejoin_action(false, false)).
