PROJECT = portunus
PROJECT_DESCRIPTION = A Raft-based lock server for the Erlang ecosystem
PROJECT_VERSION = 0.12.0
PROJECT_MOD = portunus_app

dep_ra = hex 3.1.10
dep_seshat = hex 1.0.1
DEPS = ra seshat

dep_meck = hex 0.9.2
TEST_DEPS = proper meck

LOCAL_DEPS = sasl crypto

PLT_APPS += eunit proper meck syntax_tools erts kernel stdlib common_test ra seshat

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)
