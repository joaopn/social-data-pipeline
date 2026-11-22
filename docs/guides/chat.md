# The chat

The Chat window is to be an AI assistant that works in a sandbox container whose only network peer is a gateway container; the gateway holds the model's credential, so the sandbox never sees it. This version has two commands: `sdp.py chat setup`, which asks the settings and the credential, stores them and builds the two images, and `sdp.py chat unsetup`, which removes them. Starting the chat is not part of this version: nothing runs yet.

## Back ends

| Back end | What it uses | What setup asks |
|----------|--------------|-----------------|
| `claude-code` | Claude Code on your own Claude subscription login | The token: run `claude setup-token` on a machine where you are logged in to Claude Code and paste what it prints. This uses YOUR subscription, under the vendor's terms for it. |
| `openai-compatible` | An endpoint that speaks the OpenAI API, such as OpenRouter | The endpoint's address (default `https://openrouter.ai/api/v1`), the model name, and the endpoint's API key |

The address must be `https://host[:port]/path`: https only, and no user name, query, fragment, `..` or `%`. The model name is 1 to 200 visible ASCII characters without spaces.

## `sdp.py chat setup`

It asks, in this order:

1. The back end (1: `claude-code`, 2: `openai-compatible`).
2. For `openai-compatible`: the address and the model name.
3. The secret (the token or the API key), asked once and without echo, as `sdp.py broker passwd` asks its password. White space around it is dropped; it is refused when it is empty, longer than 4096 characters, or holds anything but visible ASCII characters (a space inside included). When a credential is already stored, setup first asks whether to replace it; the default keeps it and asks no secret. When you choose another back end than the saved one, that question is not asked and a new secret is required, because the stored one belongs to the other back end; the same holds when the settings are missing, cannot be read or name no back end, because then the stored secret's back end is not known.
4. The soft cap of the chats' work folder in GiB (a whole number from 1 to 10000, default 10).

On a second setup the earlier answers are the defaults. A refused address, model name or cap is asked again; a refused secret ends the setup with nothing saved.

Then it writes the settings and the credential and builds the two images: `sdp-gateway` (from `Dockerfile.gateway`: the gateway package on `python:3.13-slim`, as user 65534) and `sdp-sandbox` (from `Dockerfile.sandbox`: `python:3.13-slim` with pandas, numpy, matplotlib and pyarrow, and Claude Code at a pinned version, its binary downloaded directly from the vendor's download location and checked against a SHA-256 written in the Dockerfile before it is run, at `/usr/local/bin/claude`; as user 65534). Each image is built from a context cut down by its ignore file (`Dockerfile.gateway.dockerignore`, `Dockerfile.sandbox.dockerignore`) to the files that Dockerfile copies, so `.sdp` and the credential never reach Docker. It ends with a summary (the back end, the address and model, the cap) and says that nothing runs yet. The secret and its length are never printed, logged, passed on a command line or put in an environment.

It refuses a deployment folder whose path holds a comma or a line break. At its start it removes a leftover temporary file (a regular file only) of the credential (`model.token.<8 hex>.tmp`, left by an interrupted write) from `credential/`.

### What it stores

Everything is under `.sdp/chat/` in the deployment folder:

| Path | Mode | What it holds |
|------|------|---------------|
| `.sdp/chat/` | 0700 | The chat's state |
| `.sdp/chat/config.json` | 0600 | The back end; for `openai-compatible` the address (`upstream`) and the model (`model`); the work folder's cap (`work_cap_gib`); the Claude Code version of the sandbox image (`claude_code_version`) |
| `.sdp/chat/credential/` | 0700 | The credential's folder |
| `.sdp/chat/credential/model.token` | 0600 | The secret, and nothing else: a file only you can read |
| `.sdp/chat/work/` | 0700 | The chats' files (one folder per chat; empty in this version) |
| `.sdp/chat/socket/` | 0700 | Empty in this version |

The modes are set explicitly, whatever your umask. Each folder must be a real folder that belongs to you: a symbolic link or a folder of another user in its place is refused and left as it is. The settings and the secret are written to a temporary file made with mode 0600 and then renamed, so a file is never half written.

## `sdp.py chat unsetup`

It says what it removes (the images `sdp-gateway` and `sdp-sandbox`, and `.sdp/chat/config.json`, `.sdp/chat/credential/` and `.sdp/chat/socket/`) and asks to confirm (default no). Then it asks separately whether to also delete all chats and their files (`.sdp/chat/work/`, default no). Leftover temporary files of an interrupted setup beside `config.json` go too, and `.sdp/chat/` itself goes when nothing is left in it; when something else stayed in it, one line says how many entries stayed. Nothing is removed through a symbolic link: a link in place of a piece is refused and left; inside a removed folder, files, FIFOs, sockets and links are unlinked without being opened or followed, folders of any mode (000 included) and any depth are removed, one folder at a time. A folder on another file system, one of another user, and one that changes while it is removed are not entered and are left; the command then says how many entries could not be removed and exits with 1. A piece that is already gone is not an error. The exit code is 0, or 1 when something could not be removed.

## Commands

| Command | What it does |
|---------|--------------|
| `sdp.py chat setup` | Asks the back end, the address and model (`openai-compatible`), the secret (without echo; on a second setup only when you replace it) and the work folder's soft cap; writes `.sdp/chat/`; builds the images `sdp-gateway` and `sdp-sandbox`. Exit code 0, or 1 with one line. |
| `sdp.py chat unsetup` | After a confirmation, removes the two images, the settings and the credential; after a second one, also all chats and their files. |
