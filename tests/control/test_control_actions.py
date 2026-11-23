"""Tests that each verb's `action` names a function of the CLI module whose keyword parameters are that verb's socket inputs.

A process that runs a verb without a terminal passes the validated request fields to the action as keyword arguments. The contracts pinned here:

- Exactly the verbs listed below have an action, and the CLI module has no other function with an action prefix: `do_` for a state-changing command, `get_` for a read command, whose action returns the data the terminal prints.
- An action takes keyword-only parameters; its public ones are the verb's socket inputs, required exactly where the input is required.
- An action has a confirmation callback exactly when the command asks for a confirmation, and a password callback exactly when it asks for the admin password; both default to None, which the action treats as "nobody to ask". A read command asks neither, so a getter has no private parameter at all. The one other private parameter is the run id of `do_run_start`, which the process that supervises a recorded run passes and no request field can set.

The registry is reached through the module (`verbs.<name>`) and the CLI as `import sdp`.
"""

from __future__ import annotations

import inspect

import pytest

import sdp
from social_data_pipeline.control import verbs

# Written out on purpose: giving a verb an action makes it runnable without a terminal, so that decision is made in two places.
ACTIONS = {
    "db.status": "get_db_status",
    "db.verify": "get_db_verify",
    "source.list": "get_source_list",
    "source.status": "get_source_status",
    "source.progress": "get_source_progress",
    "source.error_logs": "get_source_error_logs",
    "run.list": "get_run_list",
    "run.logs": "get_run_logs",
    "run.start": "do_run_start",
    "run.cancel": "do_run_cancel",
    "db.start": "do_db_start",
    "db.stop": "do_db_stop",
    "db.reset": "do_db_reset",
    "db.unsetup": "do_db_unsetup",
    "db.unsetup_mcp": "do_db_unsetup_mcp",
    "db.unsetup_jobs": "do_db_unsetup_jobs",
    "source.remove": "do_source_remove",
    "db.setup_mcp": "do_db_setup_mcp",
    "db.setup_jobs": "do_db_setup_jobs",
    "db.restart_needed": "get_db_restart_needed",
    "db.llm_settings": "get_db_llm_settings",
    "source.add": "do_source_add",
    "db.setup": "do_db_setup",
    "db.create_indexes": "do_db_create_indexes",
    "db.index_targets": "get_db_index_targets",
    "source.hf_inspect": "get_source_hf_inspect",
    "source.download": "do_source_download",
    "db.settings": "get_db_settings",
    "source.configure": "do_source_configure",
    "source.add_classifiers": "do_source_add_classifiers",
    "source.settings": "get_source_settings",
}

# The private parameter an action has for each thing its command asks on the terminal.
CALLBACK_FOR_PROMPT = {"confirmation": "_confirm", "admin_password": "_ask_password"}

# The private parameters an action has that are no callback for a prompt, written out per verb: `_run_id` is how the supervisor of a recorded run tells `do_run_start` which run the container belongs to.
OTHER_PRIVATE_PARAMETERS = {"run.start": {"_run_id"}}


def _parameters(verb_name):
    return inspect.signature(getattr(sdp, verbs.VERBS[verb_name].action)).parameters


class TestActionsMatchTheRegistry:
    def test_exactly_these_verbs_have_an_action(self):
        """Bug class: a verb made runnable without a terminal by accident (an action added to a verb whose inputs are not typed yet), or an action dropped from the registry so its verb silently stops being runnable."""
        assert {name: verb.action for name, verb in verbs.VERBS.items() if verb.action is not None} == ACTIONS

    def test_every_action_function_of_the_cli_is_in_the_registry(self):
        """Bug class: an action function added to the CLI without a verb, which no signature or no-prompt check would then cover. Both prefixes are swept: a `get_` function outside the registry would return data to a caller without typed inputs."""
        in_cli = {name for name, value in vars(sdp).items() if name.startswith(("do_", "get_")) and inspect.isfunction(value)}

        assert in_cli == set(ACTIONS.values())

    @pytest.mark.parametrize("verb_name", sorted(ACTIONS))
    def test_parameters_are_keyword_only(self, verb_name):
        """Bug class: a positional parameter, which lets a caller bind a request field to the wrong input by order."""
        kinds = {name: param.kind for name, param in _parameters(verb_name).items()}

        assert all(kind is inspect.Parameter.KEYWORD_ONLY for kind in kinds.values()), kinds

    @pytest.mark.parametrize("verb_name", sorted(ACTIONS))
    def test_public_parameters_are_the_socket_inputs(self, verb_name):
        """Bug class: drift between what the socket validates and what the action takes: a validated field the action does not accept (a TypeError at run time), or an action parameter no request can carry (a new CLI flag reaching the action without a typed input). A parameter is required exactly when the input is, so an absent optional field never needs an invented default."""
        socket_inputs = verbs.VERBS[verb_name].socket_inputs
        public = {name: param for name, param in _parameters(verb_name).items() if not name.startswith("_")}

        assert set(public) == set(socket_inputs)
        has_no_default = {name: param.default is inspect.Parameter.empty for name, param in public.items()}
        assert has_no_default == {name: spec.required for name, spec in socket_inputs.items()}

    @pytest.mark.parametrize("verb_name", sorted(ACTIONS))
    def test_callbacks_match_what_the_command_asks(self, verb_name):
        """Bug class: an action for a command that asks for a confirmation or the admin password without the matching callback (it could then only ask on a terminal it does not have, or skip the question), or a callback whose default is anything but None (a default that answers yes, or one that prompts). For a getter the expected set is empty: a private parameter there would be an input no request can carry and no type checks. Any other private parameter must be in the literal list above for its verb, and its default is None too, so a call without it is the terminal's call."""
        prompted = verbs.VERBS[verb_name].prompted_inputs
        private = {name: param for name, param in _parameters(verb_name).items() if name.startswith("_")}

        assert set(private) == {callback for prompt, callback in CALLBACK_FOR_PROMPT.items() if prompt in prompted} | OTHER_PRIVATE_PARAMETERS.get(verb_name, set())
        assert all(param.default is None for param in private.values())
