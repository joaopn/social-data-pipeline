"""The Hugging Face steps of `source add --hf` and `source download` without a terminal, with what the Hub answers checked before it is used.

The web UI reaches these through the broker: `inspect` is `sdp.get_source_hf_inspect` (the read verb `source.hf_inspect`), `download` is `sdp.do_source_download` (the task `source.download`). The terminal's commands are unchanged; these reuse their parts (`setup.hf`: the grouping by schema, the type mapping, `download_hf_files` and `organize_hf_downloads`).

What is added, because a request from a browser must not be able to reach further than the operator meant:

- Every reply of the Hub is read with a bound on its bytes (`INFO_MAX_BYTES`, `LIST_MAX_BYTES`), and its shape is checked before it is walked.
- `inspect` refuses a dataset whose configs or fields are not plain names (letters, digits, '_', '.' and '-'), or that has more groups, configs or fields than the browser takes (`GROUPS_MAX`, `CONFIGS_MAX`, `FIELDS_MAX`): such a dataset is added in a terminal.
- `download` checks every folder and file name it would build from the Hub's list of Parquet files before it writes anything (`check_parquet_urls`): the config and split names plain names, each file the last part of an https address on huggingface.co, a plain name ending in `.parquet`, none twice in one folder, and bounded counts. Only then are the files downloaded, as the terminal downloads them (into `<dumps>/<config>/<split>/`, each through a `.partial` file renamed when complete).

A refusal is `Refused`, whose one-line message holds no value of the Hub's reply or of the request.
"""

import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from social_data_pipeline.control.types import HfDatasetId
from social_data_pipeline.setup import hf, utils

# The bounds of what is read from the Hub and of what the browser is given.
INFO_MAX_BYTES = 4 * 1024 * 1024
LIST_MAX_BYTES = 16 * 1024 * 1024
GROUPS_MAX = 8
CONFIGS_MAX = 256
FIELDS_MAX = 256
HF_TYPE_MAX_CHARS = 64
# The bounds of a download's list: configs, splits per config, files in all, and the length of an address.
LIST_CONFIGS_MAX = 1024
LIST_SPLITS_MAX = 64
LIST_FILES_MAX = 20000
URL_MAX_CHARS = 2048
HUB_HOST = "huggingface.co"
PLAIN_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
PARQUET_FILE = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}\.parquet"
# The file `download_hf_files` writes beside the config folders.
README = "README.md"
TIMEOUT_SECONDS = 30


class Refused(ValueError):
    """What the Hub answered cannot be used, or could not be had. `message` is one line that holds no value of the reply."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def _plain(value):
    return isinstance(value, str) and re.fullmatch(PLAIN_NAME, value) is not None and value.strip(".") != ""


def _fetch_json(url, token, max_bytes):
    """The JSON the Hub answers at `url` (with the token as the terminal sends it), read up to `max_bytes`. Raises Refused."""
    headers = {"User-Agent": "social-data-pipeline/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = response.read(max_bytes + 1)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise Refused("the Hub has no such dataset, or it is private (a private dataset needs a token)") from None
        if error.code in (401, 403):
            raise Refused("the Hub refused access to the dataset: a private or gated dataset needs a token that has access to it") from None
        raise Refused(f"the Hub answered with an error (HTTP {int(error.code)})") from None
    except (socket.timeout, TimeoutError):
        raise Refused("the Hub did not answer in time") from None
    except (urllib.error.URLError, OSError):
        raise Refused("the Hub could not be reached (a network error): this runs on the host, which needs a connection to huggingface.co") from None
    if len(data) > max_bytes:
        raise Refused("the Hub's answer is larger than the browser takes: add the dataset in a terminal")
    try:
        return json.loads(data)
    except (ValueError, RecursionError):
        raise Refused("the Hub's answer is not JSON") from None


def _checked_id(dataset_id):
    try:
        return HfDatasetId().validate(dataset_id)
    except ValueError:
        raise Refused("the dataset id is not one the browser takes (<owner>/<name>, letters, digits, '_', '.' and '-')") from None


def _type_text(dtype):
    """The Hub's type as the terminal describes it (`hf._describe_hf_type`), cut to HF_TYPE_MAX_CHARS characters and without control characters."""
    try:
        described = hf._describe_hf_type(dtype)
    except Exception:
        described = "unknown"
    described = "".join(character if character.isprintable() else "?" for character in str(described))
    return described if len(described) <= HF_TYPE_MAX_CHARS else described[:HF_TYPE_MAX_CHARS - 3] + "..."


def _rows(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def inspect(dataset_id, token=None):
    """The schema of a dataset as `source add --hf` reads and groups it (`hf.extract_hf_defaults`): `{"dataset", "groups"}`, each group `{"configs": [{"name", "rows"}], "fields": [{"name", "sql_type", "hf_type"}]}` in the terminal's order (the groups as `hf.group_configs_by_schema` makes them, the fields of the group's first config in the Hub's order; `sql_type` None for a type without a mapping, which the terminal asks about). Raises Refused."""
    dataset_id = _checked_id(dataset_id)
    reply = _fetch_json(f"{hf.HF_DATASETS_SERVER}/info?dataset={dataset_id}", token, INFO_MAX_BYTES)
    server_info = reply.get("dataset_info", {}) if isinstance(reply, dict) else None
    if not isinstance(server_info, dict) or not all(isinstance(name, str) and isinstance(info, dict) and isinstance(info.get("features", {}), dict) and isinstance(info.get("splits", {}), (dict, list)) for name, info in server_info.items()):
        raise Refused("the Hub's answer does not have the expected shape")
    if not server_info:
        raise Refused("the dataset has no configs on the Hub, so its data types cannot be told (it may not be converted to Parquet yet)")
    if len(server_info) > GROUPS_MAX * CONFIGS_MAX:
        raise Refused(f"the dataset has more configs than the browser takes ({GROUPS_MAX * CONFIGS_MAX}): add it in a terminal")
    if not all(_plain(name) and all(_plain(field) for field in info.get("features", {})) for name, info in server_info.items()):
        raise Refused("the dataset has a config or field whose name is not a plain name (letters, digits, '_', '.' and '-'), which the browser does not take: add it in a terminal")
    try:
        configs = hf._configs_from_server_info(server_info)
    except (TypeError, AttributeError, ValueError):
        raise Refused("the Hub's answer does not have the expected shape") from None
    groups = hf.group_configs_by_schema(configs)
    if len(groups) > GROUPS_MAX:
        raise Refused(f"the dataset's configs fall into more schema groups than the browser takes ({GROUPS_MAX}): add it in a terminal")
    shown = []
    for group in groups:
        if len(group["configs"]) > CONFIGS_MAX or len(group["features"]) > FIELDS_MAX:
            raise Refused(f"a schema group of the dataset has more configs ({CONFIGS_MAX}) or fields ({FIELDS_MAX}) than the browser takes: add it in a terminal")
        fields = []
        for feature in group["features"]:
            name = feature.get("name", "")
            if not name:
                continue
            dtype = feature.get("dtype", feature)
            sql_type, _ = hf.map_hf_type_to_sql(dtype)
            fields.append({"name": name, "sql_type": sql_type, "hf_type": _type_text(dtype)})
        shown.append({"configs": [{"name": config["name"], "rows": _rows(config.get("num_rows"))} for config in group["configs"]], "fields": fields})
    return {"dataset": dataset_id, "groups": shown}


def _file_name(url):
    """The name `hf.download_hf_files` gives the file of `url`, or None when the address is not one of the Hub's https addresses."""
    if not isinstance(url, str) or not 1 <= len(url) <= URL_MAX_CHARS or any(not character.isprintable() or character.isspace() for character in url):
        return None
    try:
        parts = urllib.parse.urlsplit(url)
        port = parts.port
    except ValueError:
        return None
    if parts.scheme != "https" or parts.hostname != HUB_HOST or port is not None or parts.username is not None or parts.password is not None or parts.netloc != HUB_HOST:
        return None
    # An escape or a parent reference in the path is refused even where the last part alone would be plain: the Hub's own addresses need neither for plain config, split and file names.
    if "%" in parts.path or "\\" in parts.path or ".." in parts.path.split("/"):
        return None
    # The computation of `download_hf_files`, on the same address.
    name = Path(urllib.parse.unquote(urllib.parse.urlparse(url).path)).name
    return name if re.fullmatch(PARQUET_FILE, name) else None


def check_parquet_urls(parquet_urls):
    """`parquet_urls` (the Hub's answer to `/api/datasets/<id>/parquet`: {config: {split: [address, ...]}}) when every folder and file name `hf.download_hf_files` would build from it is safe; raises Refused before anything is written otherwise. Safe: a dict of at most LIST_CONFIGS_MAX configs and LIST_SPLITS_MAX splits each, config and split names plain names (no '/', never only dots, so no `..`), no config named README.md (the file written beside them), each address https on huggingface.co whose last part is a plain name ending in `.parquet`, no name twice in one folder, at most LIST_FILES_MAX files."""
    if not isinstance(parquet_urls, dict) or len(parquet_urls) > LIST_CONFIGS_MAX:
        raise Refused("the Hub's list of Parquet files does not have the expected shape, or names more configs than are taken: nothing was written")
    count = 0
    for config, splits in parquet_urls.items():
        if not _plain(config) or config == README:
            raise Refused("the Hub's list of Parquet files names a config that is not a plain name (letters, digits, '_', '.' and '-'), so no folder is made for it: nothing was written")
        if not isinstance(splits, dict) or len(splits) > LIST_SPLITS_MAX:
            raise Refused("the Hub's list of Parquet files does not have the expected shape: nothing was written")
        for split, urls in splits.items():
            if not _plain(split):
                raise Refused("the Hub's list of Parquet files names a split that is not a plain name (letters, digits, '_', '.' and '-'), so no folder is made for it: nothing was written")
            if not isinstance(urls, list):
                raise Refused("the Hub's list of Parquet files does not have the expected shape: nothing was written")
            names = set()
            for url in urls:
                name = _file_name(url)
                if name is None:
                    raise Refused("the Hub's list of Parquet files holds an address that is not an https address on huggingface.co ending in a plain file name with .parquet (no escape, no parent reference): nothing was written")
                if name in names:
                    raise Refused("the Hub's list of Parquet files names one file twice in one folder: nothing was written")
                names.add(name)
                count += 1
                if count > LIST_FILES_MAX:
                    raise Refused(f"the Hub's list of Parquet files names more than {LIST_FILES_MAX} files, more than the browser takes: download it in a terminal")
    return parquet_urls


def _at_root(path):
    return path if os.path.isabs(path) else os.path.join(str(utils.ROOT), path)


def download(source_name, token=None):
    """`source download <name>` without a terminal, for every data type (`--data-type` is not offered): the same lines as `sdp.cmd_source_download`, the Hub's list checked by `check_parquet_urls` before anything is written. The token is the one given, else `HF_TOKEN` of the environment, as in the terminal. Returns the exit code."""
    source_config = utils.load_source_config(source_name)
    if source_config is None:
        print(f"\n  Error: Source '{source_name}' not found in config/sources/\n", flush=True)
        return 1
    hf_dataset = source_config.get("hf_dataset")
    if not hf_dataset:
        print(f"\n  Error: Source '{source_name}' has no hf_dataset configured.", flush=True)
        print(f"  Use 'sdp source add {source_name} --hf <dataset_id>' to set up an HF source.\n", flush=True)
        return 1
    try:
        hf_dataset = _checked_id(hf_dataset)
    except Refused as refused:
        print(f"\n  Error: the hf_dataset of the source's platform.yaml: {refused.message}.\n", flush=True)
        return 1
    token = token or os.environ.get("HF_TOKEN")
    paths = source_config.get("paths", {}) if isinstance(source_config.get("paths", {}), dict) else {}
    dumps_dir, extracted_dir = paths.get("dumps"), paths.get("extracted")
    if not isinstance(dumps_dir, str) or not dumps_dir or not isinstance(extracted_dir, str) or not extracted_dir:
        print(f"\n  Error: No dumps/extracted path configured for source '{source_name}'.\n", flush=True)
        return 1
    config_map = source_config.get("hf_config_map", {})
    if not config_map or not isinstance(config_map, dict):
        print(f"\n  Error: Source '{source_name}' has no hf_config_map in platform.yaml.", flush=True)
        print(f"  Re-run 'sdp source add {source_name} --hf {hf_dataset}' to regenerate it.\n", flush=True)
        return 1

    print(f"\n  Source:  {source_name}")
    print(f"  Dataset: {hf_dataset}")
    print(f"  Dumps:   {dumps_dir}")
    print(f"  Extract: {extracted_dir}")
    print(f"  Data types: {', '.join(str(key) for key in config_map)}", flush=True)

    print("\n  --- Downloading from HF Hub ---\n", flush=True)
    try:
        parquet_urls = check_parquet_urls(_fetch_json(f"{hf.HF_API_BASE}/{hf_dataset}/parquet", token, LIST_MAX_BYTES))
    except Refused as refused:
        print(f"\n  Error: {refused.message}\n", flush=True)
        return 1
    live_configs = set(parquet_urls)
    saved_configs = {config for configs in config_map.values() if isinstance(configs, list) for config in configs}
    missing = sorted(str(config) for config in saved_configs - live_configs)
    if missing:
        print("\n  Error: saved hf_config_map references configs that no longer exist on HF.")
        print(f"  Missing configs: {', '.join(missing)}")
        print(f"  Available on HF: {', '.join(sorted(live_configs))}")
        print(f"  Re-run 'sdp source add {source_name} --hf {hf_dataset}' to regenerate the mapping.\n", flush=True)
        return 1
    hf.download_hf_files(parquet_urls, _at_root(dumps_dir), dataset_id=hf_dataset, token=token)
    print("\n  --- Organizing into data types ---\n", flush=True)
    hf.organize_hf_downloads(_at_root(dumps_dir), _at_root(extracted_dir), config_map)
    print(f"\n  Next step: python sdp.py run parse --source {source_name}\n", flush=True)
    return 0
