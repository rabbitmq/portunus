%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Team RabbitMQ <teamrabbitmq@gmail.com>. All Rights Reserved.
%%
-module(portunus_election_log_unit_SUITE).

-behaviour(portunus_election).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([raised_reason_stays_out_of_the_warning/1,
         bad_return_value_stays_out_of_the_warning/1]).
-export([elected/1, stepped_down/1]).
-export([log/2]).

-define(NAME, portunus_election_log_test).
-define(KEY, {election, log_hygiene}).
-define(LEASE, log_lease).
-define(MARKER, <<"amqp://user:secret@host">>).
-define(HANDLER, portunus_election_log_capture).

all() ->
    [raised_reason_stays_out_of_the_warning,
     bad_return_value_stays_out_of_the_warning].

init_per_testcase(_TC, Config) ->
    process_flag(trap_exit, true),
    KA = ensure_keepalive(),
    ok = meck:new(portunus, [passthrough, no_link]),
    ok = meck:new(portunus_batch_keepalive, [passthrough, no_link]),
    meck:expect(portunus_batch_keepalive, attach, fun(_N, _L, _T) -> ok end),
    meck:expect(portunus_batch_keepalive, detach, fun(_N, _L) -> ok end),
    meck:expect(portunus, grant_lease, fun(_N, _T) -> {ok, ?LEASE} end),
    meck:expect(portunus, revoke_lease, fun(_N, _L) -> ok end),
    meck:expect(portunus, acquire_or_join_succession_queue,
                fun(_N, _K, _L, _O, _Opts) -> {ok, 42} end),
    PrimaryLevel = maps:get(level, logger:get_primary_config()),
    ok = logger:set_primary_config(level, debug),
    ok = logger:add_handler(?HANDLER, ?MODULE,
                            #{config => self(), level => debug}),
    [{keepalive, KA}, {primary_level, PrimaryLevel} | Config].

end_per_testcase(_TC, Config) ->
    _ = logger:remove_handler(?HANDLER),
    ok = logger:set_primary_config(level, ?config(primary_level, Config)),
    catch meck:unload(portunus_batch_keepalive),
    catch meck:unload(portunus),
    stop_keepalive(?config(keepalive, Config)),
    ok.

%% Reuses an already-registered renewer (a full run leaves the real one
%% up); spawns an idle stand-in only when the name is free.
ensure_keepalive() ->
    case whereis(portunus_batch_keepalive) of
        undefined ->
            KA = spawn(fun idle/0),
            register(portunus_batch_keepalive, KA),
            KA;
        _Existing ->
            existing
    end.

stop_keepalive(KA) when is_pid(KA) ->
    catch exit(KA, kill),
    ok;
stop_keepalive(_) ->
    ok.

%% `elected/1` raises a reason carrying the marker: the warning names the
%% key and the class without the marker, and the debug line carries it.
raised_reason_stays_out_of_the_warning(_Config) ->
    {ok, E} = portunus_election:start_link(?NAME, ?KEY, ?MODULE,
                                           {raise, ?MARKER},
                                           #{ttl_ms => 2000}),
    {warning, Warning} = capture(warning),
    ?assertEqual(nomatch, binary:match(Warning, ?MARKER)),
    ?assertNotEqual(nomatch, binary:match(Warning, <<"error">>)),
    {debug, Debug} = capture(debug),
    ?assertNotEqual(nomatch, binary:match(Debug, ?MARKER)),
    stop(E).

%% Same for a bad return value that embeds the marker.
bad_return_value_stays_out_of_the_warning(_Config) ->
    {ok, E} = portunus_election:start_link(?NAME, ?KEY, ?MODULE,
                                           {return, ?MARKER},
                                           #{ttl_ms => 2000}),
    {warning, Warning} = capture(warning),
    ?assertEqual(nomatch, binary:match(Warning, ?MARKER)),
    ?assertNotEqual(nomatch, binary:match(Warning, <<"unexpected value">>)),
    {debug, Debug} = capture(debug),
    ?assertNotEqual(nomatch, binary:match(Debug, ?MARKER)),
    stop(E).

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

elected(#{args := {raise, Marker}}) ->
    error({start_failed, Marker});
elected(#{args := {return, Marker}}) ->
    {unexpected, Marker}.

stepped_down(_State) ->
    ok.

log(#{level := Level, msg := Msg}, #{config := Pid}) ->
    case format(Msg) of
        <<"portunus election", _/binary>> = Formatted ->
            Pid ! {log, Level, Formatted};
        _ ->
            ok
    end.

format({Fmt, Args}) when is_list(Fmt) ->
    unicode:characters_to_binary(io_lib:format(Fmt, Args));
format({string, S}) ->
    unicode:characters_to_binary(S);
format(_Report) ->
    <<>>.

capture(Level) ->
    receive
        {log, Level, Formatted} -> {Level, Formatted}
    after 5000 -> ct:fail({no_log_at, Level})
    end.

stop(E) ->
    _ = catch portunus_election:stop(E),
    ok.

idle() ->
    receive _ -> idle() end.
