"""The web UI: a page in the operator's browser that talks to the broker (`social_data_pipeline.control.broker`) through its socket, and does nothing else.

It runs in a container that holds only the broker's socket folder and its own certificate (`sdp webui start`, see `social_data_pipeline.webui_host`); every action goes through the broker's typed verbs. This version has the login and a read-only Overview.

Modules: `config` (the settings the container is started with), `guard` (the checks every request passes and the headers every response gets), `sessions` (the browser sessions, kept in memory only), `limiter` (failed logins per client address), `words` (the broker's error codes as sentences), `cleaning` (text from programs made fit to show), `overview` (the Overview's data), `app` (the routes), `probe` (asks the broker for `hello` from inside the container). `config`, `words`, `cleaning`, `limiter`, `sessions` and `probe` use the standard library and the broker's client only; `guard` and `app` also FastAPI, Starlette and Jinja2. Nothing here imports the CLI, the registry or PyYAML: the container has none of them.
"""
