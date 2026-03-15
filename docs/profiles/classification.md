# Classification Profiles

The `lingua` and `ml` profiles run classifiers on parsed files (Parquet or CSV). The `lingua` profile runs CPU-based Lingua language detection, while the `ml` profile runs GPU-based transformer classifiers. Classifiers detect file format from the extension and produce output in the same format.

## Running

```bash
# CPU language detection
python sdb.py run lingua [--source <name>]

# GPU transformer classifiers (requires NVIDIA GPU)
python sdb.py run ml [--source <name>]

# Run a single GPU classifier
CLASSIFIER=toxic_roberta docker compose --profile ml up
```

---

## Lingua Language Detection (lingua)

### Overview

Lingua provides fast language detection using a Rust-based library (Rayon parallelism). It detects up to 54 languages organized in 5 tiers by Reddit user volume.

### How It Works

1. **Text Cleaning**: Removes configured strings (`[deleted]`, `[removed]`, `[unavailable]`) and patterns (URLs, subreddit/user mentions), then normalizes whitespace.

2. **Text Validity Filtering**: Texts shorter than `min_chars` (default 5) after cleaning are skipped and get empty language values.

3. **Detection**: Texts are sorted by length (descending) for optimal Rayon parallel utilization. Detection runs in configurable batches (default 2M rows) to manage memory.

4. **Output**: Appends language columns to the original data (same format as input — Parquet or CSV).

### Output Columns

| Column | Description |
|--------|-------------|
| `lang` | ISO 639-1 code (e.g., `en`, `de`, `es`) |
| `lang_prob` | Confidence score (0.0 - 1.0) |
| `lang2` | Second most likely language |
| `lang2_prob` | Confidence for second language |
| `lang_chars` | Character count of cleaned text used for detection |

### Output Modes

Lingua has two output modes controlled by `prefer_lingua` in the postgres profile:

- **`prefer_lingua: true`** (default): Lingua output includes all original columns + language columns. The `postgres_ingest` profile ingests these enriched files directly into the main table. No separate lingua table is needed.

- **`prefer_lingua: false`**: Lingua additionally generates a minimal `lingua_ingest` file containing only `id, dataset, retrieved_utc` + language columns. The `postgres_ml` profile ingests this as a separate table. Original parsed files (without lingua columns) go to the main table via `postgres_ingest`.

### Configuration

**Config file:** `config/lingua/cpu_classifiers.yaml`

<details>
<summary><strong>Global settings</strong></summary>

| Option | Description | Default |
|--------|-------------|---------|
| `text_columns` | Columns to classify per data type | submissions: [title, selftext], comments: [body] |
| `remove_strings` | Exact strings to remove before classification | [deleted], [removed], [unavailable] |
| `remove_patterns` | Regex patterns to remove | URLs, r/subreddit, u/user |
| `fields` | Extra columns in lingua_ingest output | `[]` (only mandatory fields) |

</details>

<details>
<summary><strong>Lingua settings</strong></summary>

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename suffix | `"_lingua"` |
| `min_chars` | Minimum character count after text cleaning | `5` |
| `low_accuracy` | Faster but less accurate | `false` |
| `workers` | Total parallel workers (divided among file_workers) | `16` |
| `file_workers` | Files processed in parallel | `2` |
| `batch_size` | Rows per batch | `2000000` |
| `languages` | Lingua enum names to detect | 54 languages in 5 tiers |

</details>

### Language Tiers

Languages are organized by Reddit user volume:
- **Tier 1** (Top 10 countries): English, German, Spanish, Portuguese, French, Italian, Russian, Hindi, Tagalog, Turkish
- **Tier 2** (High volume): Dutch, Swedish, Polish, Malay, Arabic, Vietnamese, Thai, Chinese, Japanese, Korean, Romanian, Greek, Czech, Danish, Finnish, Norwegian (Bokmal), Hebrew
- **Tier 3** (Moderate): Ukrainian, Hungarian, Slovak, Croatian, Serbian, Bulgarian, Slovene, Lithuanian, Latvian, Estonian, Bosnian, Indonesian, Persian
- **Tier 4** (Indian regional): Bengali, Tamil, Telugu, Marathi, Gujarati, Punjabi, Urdu
- **Tier 5** (Low volume): Commented out by default — Afrikaans, Albanian, Armenian, etc.

Edit the `languages` list in `cpu_classifiers.yaml` to add or remove languages.

---

## Transformer Classifiers (ml)

### Overview

GPU-based text classification using HuggingFace models with ONNX FP16, ONNX, or PyTorch backends. Supports multi-GPU parallelization.

### How It Works

1. **Model Loading**: Models are downloaded from HuggingFace and cached in `$HF_HOME/sdb_models/`. Each GPU worker thread loads models once at startup and reuses them across files (persistent worker architecture).

2. **Text Cleaning**: Same pipeline as Lingua — removes configured strings and patterns, normalizes whitespace.

3. **Tokenization**: Parallel tokenization using HuggingFace tokenizers with `tokenize_workers` threads.

4. **Filtering**:
   - **Language filter** (optional): When `use_lingua: true`, only processes texts where the `lang` column matches the classifier's `supported_languages`. If `lang2_fallback: true`, also checks `lang2`.
   - **Min tokens filter**: Skips texts with fewer than `min_tokens` tokens (applied after cleaning and tokenization).

5. **Chunking**: For texts longer than `max_length`:
   - **`truncate`** (default): Single chunk, truncated to max_length.
   - **`chunk`**: Sliding window with configurable `stride` (overlap). Produces multiple chunks per text.

6. **Inference**: Batched inference on GPU(s) with configurable `classifier_batch_size`.

7. **Chunk Aggregation**: When using `chunk` strategy, each label independently selects its top-k chunks by logit value, then averages them. This per-label top-k pooling means different labels can focus on different parts of a long text. With `top_k: 1`, this is equivalent to max-pooling.

8. **Activation**:
   - `sigmoid`: Multi-label classification (each label independent, outputs probabilities 0-1)
   - `softmax`: Single-label classification (outputs sum to 1)

9. **Output**: File (same format as input — Parquet or CSV) with mandatory fields (id, dataset, retrieved_utc) + configured extra `fields` + classifier output columns.

### Multi-GPU Architecture

The ml profile uses persistent worker threads for GPU inference:
- One executor thread per GPU, with models loaded into thread-local storage
- `file_workers` controls how many files are processed in parallel (each file uses all GPUs)
- Models are loaded once at startup and reused across files
- Thread affinity ensures each worker only processes on its assigned GPU

### Default Classifiers

<details>
<summary><strong>toxic_roberta</strong></summary>

- **Model**: `joaopn/unbiased-toxic-roberta-onnx-fp16`
- **Type**: ONNX FP16
- **Activation**: sigmoid (multi-label)
- **Languages**: English only
- **Output**: Toxicity labels (toxic, severe_toxic, obscene, threat, insult, identity_hate)
- **Strategy**: chunk with stride=256, top_k=1 (max-pooling)

</details>

<details>
<summary><strong>go_emotions</strong></summary>

- **Model**: `joaopn/roberta-base-go_emotions-onnx-fp16`
- **Type**: ONNX FP16
- **Activation**: sigmoid (multi-label)
- **Languages**: English only
- **Output**: 28 emotion labels (admiration, amusement, anger, annoyance, approval, caring, confusion, curiosity, desire, disappointment, disapproval, disgust, embarrassment, excitement, fear, gratitude, grief, joy, love, nervousness, optimism, pride, realization, relief, remorse, sadness, surprise, neutral)
- **Strategy**: chunk with stride=256, top_k=3

</details>

### Transformer Options Reference

<details>
<summary><strong>Full options table</strong></summary>

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename/table suffix | *(required)* |
| `type` | Model backend: `onnx_fp16`, `onnx`, `pytorch` | `onnx_fp16` |
| `model` | HuggingFace model ID | *(required)* |
| `file_name` | ONNX model filename | `model.onnx` |
| `activation` | `sigmoid` (multi-label) or `softmax` (single-label) | `softmax` |
| `supported_languages` | Filter by lang column (requires `use_lingua: true`) | *(all languages)* |
| `lang2_fallback` | Also check lang2 column for language match | `false` |
| `min_tokens` | Minimum tokens after cleaning (overrides global) | global value |
| `fields` | Extra columns to keep from input (overrides global) | global value |
| `classifier_batch_size` | Batch size per GPU | `32` |
| `max_length` | Maximum token length | `512` |
| `chunking_strategy` | `truncate` or `chunk` | `truncate` |
| `stride` | Overlap between chunks (for `chunk`) | `64` |
| `top_k` | Top-k chunks for aggregation (1 = max-pooling) | `2` |
| `gpu_ids` | Override global GPU list | global value |
| `batch_size` | Rows per I/O batch | global value |

Global settings that can be overridden per-classifier: `gpu_ids`, `file_workers`, `tokenize_workers`, `batch_size`, `classifier_batch_size`, `use_lingua`, `lang2_fallback`, `min_tokens`, `fields`.

</details>

---

## Shared Concepts

### Text Cleaning Pipeline

Both profiles share the same text cleaning approach:
1. Concatenate configured text columns (e.g., title + selftext for submissions)
2. Remove exact strings: `[deleted]`, `[removed]`, `[unavailable]`
3. Remove regex patterns: URLs (`https?://\S+`), subreddit mentions (`r/\w+`), user mentions (`u/\w+`)
4. Normalize whitespace

Configure via `text_columns`, `remove_strings`, and `remove_patterns` in the respective classifier config files.

### Resume Behavior

Both profiles check for existing output files before processing:
- If the output file exists, the input file is skipped
- To reprocess: delete the specific output file or the entire classifier output directory

```bash
rm -rf data/output/<source>/lingua/          # Reprocess lingua only
rm -rf data/output/<source>/toxic_roberta/   # Reprocess toxic_roberta only
rm -rf data/output/<source>/                 # Reprocess all classifiers
```

### Running a Single Classifier

Use the `CLASSIFIER` environment variable to run only one GPU classifier:

```bash
CLASSIFIER=toxic_roberta docker compose --profile ml up
```

> [!NOTE]
> This is a per-run override. For standard usage, configure classifiers during `python sdb.py source add-classifiers <name>` and run with `python sdb.py run ml`.

### Watch Mode

Set `watch_interval` to a value > 0 in pipeline.yaml to continuously check for new parsed files every N minutes.

### Adding Custom Classifiers

See [Custom Classifiers Guide](../guides/custom-classifiers.md) for config-only and Python approaches.
