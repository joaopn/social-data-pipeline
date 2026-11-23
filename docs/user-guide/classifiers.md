---
title: "Language detection and classifiers"
parent: "User guide"
nav_order: 7
---
# Language detection and classifiers

The `lingua` and `ml` profiles run classifiers on parsed files (Parquet or CSV). `lingua` runs CPU-based Lingua language detection; `ml` runs GPU-based transformer classifiers. Each classifier detects the file format from the extension and writes its output in the same format, ready for the `postgres_ml` and `sr_ml` ingestion profiles.

## Running the classifiers

```bash
python sdp.py run lingua [--source <name>] [--filter <pattern>]   # CPU language detection
python sdp.py run ml [--source <name>] [--filter <pattern>]       # GPU transformer classifiers (requires an NVIDIA GPU)
```

Use `--filter` (`-f`) to restrict processing to files whose file id matches an fnmatch glob. The classifier's own skip logic (existing output files) still applies; `--filter` narrows the input set before that check:

```bash
python sdp.py run lingua --source reddit --filter "*2024*"    # Only classify 2024 months
```

The GPU profile requires the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html).

## Lingua language detection

Lingua is fast language detection through a Rust-based library (Rayon parallelism). It detects up to 54 languages, organized in 5 tiers by Reddit user volume. Before detection, the text is cleaned (see [Classifiers](../architecture/classifiers.md#text-cleaning-pipeline)); texts shorter than `min_chars` (default 5) after cleaning are skipped and get empty language values.

### Output columns

Lingua appends these columns to the original data, in the same format as the input (Parquet or CSV):

| Column | Description |
|--------|-------------|
| `lang` | ISO 639-1 code (for example `en`, `de`, `es`) |
| `lang_prob` | Confidence score (0.0 to 1.0) |
| `lang2` | Second most likely language |
| `lang2_prob` | Confidence for the second language |
| `lang_chars` | Character count of the cleaned text used for detection |

The output lands in `OUTPUT_PATH/<source>/lingua/<data_type>/<id>_lingua.{csv,parquet}`.

### Output modes

How lingua output reaches the databases is set by `prefer_lingua` in the ingestion profile config (`postgres_ingest` in `config/postgres/pipeline.yaml`, `sr_ingest` in `config/sr/pipeline.yaml`). It is a strict either/or, with no automatic fallback:

| `prefer_lingua` | What lingua writes | What ingestion does |
|-----------------|--------------------|---------------------|
| `true` (default) | All original columns plus the language columns | `postgres_ingest` and `sr_ingest` ingest only these enriched files, from the lingua output folder, into the main table. No separate lingua table is needed. If lingua has not run, ingestion finds no files to load. |
| `false` | The same files, plus a minimal `lingua_ingest` file with only `id, dataset, retrieved_utc` and the language columns | The main table gets the original parsed files (without language columns) through `postgres_ingest` / `sr_ingest`; `postgres_ml` / `sr_ml` ingest `lingua_ingest` as a separate table |

Lingua writes the `lingua_ingest` copy when either ingestion profile of the source has `prefer_lingua: false`. So with `prefer_lingua: true`, run `lingua` before `postgres_ingest` or `sr_ingest`.

### Lingua configuration

**Config file:** `config/lingua/cpu_classifiers.yaml`

Global settings:

| Option | Description | Default |
|--------|-------------|---------|
| `text_columns` | Columns to classify per data type | submissions: `[title, selftext]`, comments: `[body]` |
| `remove_strings` | Exact strings to remove before classification | `[deleted]`, `[removed]`, `[unavailable]` |
| `remove_patterns` | Regex patterns to remove | URLs, `r/subreddit`, `u/user` |
| `fields` | Extra columns in the `lingua_ingest` output | `[]` (only mandatory fields) |

Lingua settings:

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename suffix | `"_lingua"` |
| `min_chars` | Minimum character count after text cleaning | `5` |
| `low_accuracy` | Faster but less accurate | `false` |
| `workers` | Total parallel workers (divided among `file_workers`) | `16` |
| `file_workers` | Files processed in parallel | `2` |
| `batch_size` | Rows per batch | `2000000` |
| `languages` | Lingua enum names to detect | 54 languages in 5 tiers |

### Language tiers

Languages are organized by Reddit user volume. Edit the `languages` list in `cpu_classifiers.yaml` to add or remove languages.

| Tier | Languages |
|------|-----------|
| 1 (top 10 countries) | English, German, Spanish, Portuguese, French, Italian, Russian, Hindi, Tagalog, Turkish |
| 2 (high volume) | Dutch, Swedish, Polish, Malay, Arabic, Vietnamese, Thai, Chinese, Japanese, Korean, Romanian, Greek, Czech, Danish, Finnish, Norwegian (Bokmal), Hebrew |
| 3 (moderate) | Ukrainian, Hungarian, Slovak, Croatian, Serbian, Bulgarian, Slovene, Lithuanian, Latvian, Estonian, Bosnian, Indonesian, Persian |
| 4 (Indian regional) | Bengali, Tamil, Telugu, Marathi, Gujarati, Punjabi, Urdu |
| 5 (low volume) | Commented out by default: Afrikaans, Albanian, Armenian, Azerbaijani, Basque, Belarusian, Catalan, Esperanto, and others. Uncomment them in `cpu_classifiers.yaml` to enable. |

## Transformer classifiers

The `ml` profile runs GPU text classification with Hugging Face models on ONNX FP16, ONNX or PyTorch backends, and spreads the work over several GPUs. Models are downloaded from Hugging Face and cached in `$HF_HOME/sdb_models/`. How a text goes from cleaning through tokenization, chunking and inference to scores is in [Classifiers](../architecture/classifiers.md#how-the-transformer-classifiers-work).

### Output

Each classifier writes a file per input file, in the same format as the input (Parquet or CSV), to `OUTPUT_PATH/<source>/<classifier>/`. It holds the mandatory fields (`id`, `dataset`, `retrieved_utc`), the configured extra `fields`, and the classifier's output columns. The output columns are derived from the model's `config.id2label`.

| Activation | Meaning of the scores |
|------------|-----------------------|
| `sigmoid` | Multi-label: each label independent, probabilities 0 to 1 |
| `softmax` | Single-label: the scores sum to 1 |

When `use_lingua: true`, a classifier only processes texts whose `lang` matches its `supported_languages`; with `lang2_fallback: true` it also checks `lang2`. Texts with fewer than `min_tokens` tokens after cleaning are skipped.

### Default classifiers

| Classifier | Model | Type | Activation | Languages | Output | Strategy |
|------------|-------|------|------------|-----------|--------|----------|
| `toxic_roberta` | `joaopn/unbiased-toxic-roberta-onnx-fp16` | ONNX FP16 | sigmoid (multi-label) | English only | Toxicity labels: toxic, severe_toxic, obscene, threat, insult, identity_hate | chunk with stride=256, top_k=1 (max-pooling) |
| `go_emotions` | `joaopn/roberta-base-go_emotions-onnx-fp16` | ONNX FP16 | sigmoid (multi-label) | English only | 28 emotion labels: admiration, amusement, anger, annoyance, approval, caring, confusion, curiosity, desire, disappointment, disapproval, disgust, embarrassment, excitement, fear, gratitude, grief, joy, love, nervousness, optimism, pride, realization, relief, remorse, sadness, surprise, neutral | chunk with stride=256, top_k=3 |

More bundled classifiers (`multilingual_sentiment`, `xlmr_toxicity`) are listed in [Configuration reference](configuration.md#ml-gpu-profile).

### Transformer options reference

Per-classifier options, set under the classifier's key in `config/ml/gpu_classifiers.yaml`:

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename and table suffix | *(required)* |
| `type` | Model backend: `onnx_fp16`, `onnx`, `pytorch` | `onnx_fp16` |
| `model` | Hugging Face model ID | *(required)* |
| `file_name` | ONNX model filename | `model.onnx` |
| `activation` | `sigmoid` (multi-label) or `softmax` (single-label) | `softmax` |
| `supported_languages` | Filter by the `lang` column (requires `use_lingua: true`) | *(all languages)* |
| `lang2_fallback` | Also check the `lang2` column for a language match | `false` |
| `min_tokens` | Minimum tokens after cleaning (overrides the global value) | global value |
| `fields` | Extra columns to keep from the input (overrides the global value) | global value |
| `classifier_batch_size` | Batch size per GPU | `32` |
| `max_length` | Maximum token length | `512` |
| `chunking_strategy` | `truncate` or `chunk` | `truncate` |
| `stride` | Overlap between chunks (for `chunk`) | `64` |
| `top_k` | Top-k chunks for aggregation (1 = max-pooling) | `2` |
| `gpu_ids` | Override the global GPU list | global value |
| `batch_size` | Rows per I/O batch | global value |

Global settings that can be overridden per classifier: `gpu_ids`, `file_workers`, `tokenize_workers`, `batch_size`, `classifier_batch_size`, `use_lingua`, `lang2_fallback`, `min_tokens`, `fields`. The global settings and their defaults are in [Configuration reference](configuration.md#ml-gpu-profile).

## Running a single classifier

Use the `CLASSIFIER` environment variable to run only one GPU classifier:

```bash
CLASSIFIER=toxic_roberta docker compose --profile ml up
```

This is a per-run override. For standard usage, configure classifiers with `python sdp.py source add-classifiers <name>` (or **Add classifiers…** on the source's page in the [web UI](webui.md#reconfiguring-a-source-and-its-classifiers)) and run them with `python sdp.py run ml`.

## Reprocessing

Both profiles skip an input file whose output file already exists, so a rerun continues where the last one stopped. To reprocess, delete the specific output file or the whole classifier output folder:

```bash
rm -rf data/output/<source>/lingua/          # Reprocess lingua only
rm -rf data/output/<source>/toxic_roberta/   # Reprocess toxic_roberta only
rm -rf data/output/<source>/                 # Reprocess all classifiers
```

## Watch mode

Set `watch_interval` to a value above 0 in the profile's `pipeline.yaml` to check for new parsed files every N minutes.

## Adding your own classifier

See [Custom classifiers](custom-classifiers.md) for the config-only approach, and [Classifiers](../architecture/classifiers.md#custom-python-classifiers) for custom Python classifiers.
