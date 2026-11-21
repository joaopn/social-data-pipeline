"""Host-side control boundary: the registry of verbs a client may request by name, and the one function that validates a request before anything acts on it.

A verb names one action of the host CLI. What a verb accepts from a client is a deny-by-default list of typed inputs that is narrower than the CLI's own inputs, so a new CLI flag is unreachable through a verb until someone lists it with a type. The package uses the standard library, PyYAML and `social_data_pipeline.setup.utils` only, and the CLI does not import it.

Three modules act on a validated request, each in a process of its own: `child` runs the action of one verb and ends, `supervise` keeps one recorded pipeline run alive and records its end, and `runs` starts such a run. They also use the run store, and import the CLI module only at the moment they call it, never when they are loaded; the registry itself (`verbs`, `types`, `names`, `errors`) imports neither.
"""

# The largest request, in bytes of JSON, that is handed from one process of this package to the next over standard input.
REQUEST_MAX_BYTES = 65536
