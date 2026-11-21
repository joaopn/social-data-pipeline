"""Host-side control boundary: the registry of verbs a client may request by name, and the one function that validates a request before anything acts on it.

A verb names one action of the host CLI. What a verb accepts from a client is a deny-by-default list of typed inputs that is narrower than the CLI's own inputs, so a new CLI flag is unreachable through a verb until someone lists it with a type. The package uses the standard library, PyYAML and `social_data_pipeline.setup.utils` only, and the CLI does not import it.
"""
