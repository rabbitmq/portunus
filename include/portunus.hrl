%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%

%% The renewer's TTL floor: the renew interval is max(TTL/3, 1000 ms), so
%% 2000 ms keeps at least two renewals per TTL and one missed renewal does
%% not expire the lease (see `portunus_keepalive`).
-define(MIN_RENEWABLE_TTL_MS, 2000).

%% Guard: an options map either omits `ttl_ms` or carries one at or above
%% the renewable floor.
-define(IS_RENEWABLE_TTL_OPT(Opts),
        (not is_map_key(ttl_ms, Opts) orelse
         (is_integer(map_get(ttl_ms, Opts)) andalso
          map_get(ttl_ms, Opts) >= ?MIN_RENEWABLE_TTL_MS))).
