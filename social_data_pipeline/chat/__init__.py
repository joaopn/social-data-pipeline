"""The chat's runner: the small server inside the sandbox container that the web UI talks to over a unix socket (`runner`), with the Claude Code back end (`claude`). Run it with `python -m social_data_pipeline.chat`.

Standard library only, plus `social_data_pipeline.control.framing` and `errors` for the frames of the socket. Everything in the sandbox is treated as hostile by the outside; the runner keeps itself useful and bounded, and is not a security boundary.
"""
