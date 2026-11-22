---
title: "Classifiers"
parent: "Architecture"
nav_order: 4
---
# Classifiers

The classifier profiles read parsed files and write one output file per input file into `output/<source>/<classifier>/`: `lingua` on the CPU, `ml` on one or more GPUs. This page describes how each works, the text cleaning both share, how they resume, and how a custom Python classifier plugs into the registry. Running and configuring them is on [Language detection and classifiers](../user-guide/classifiers.md).

## Text cleaning pipeline

Both profiles clean text the same way before classifying it:

1. Concatenate the configured text columns (for example title + selftext for submissions).
2. Remove exact strings: `[deleted]`, `[removed]`, `[unavailable]`.
3. Remove regex patterns: URLs (`https?://\S+`), subreddit mentions (`r/\w+`), user mentions (`u/\w+`).
4. Normalize whitespace.

The cleaning is configured with `text_columns`, `remove_strings` and `remove_patterns` in each profile's classifier config file.

## How lingua works

1. **Text cleaning**: removes the configured strings (`[deleted]`, `[removed]`, `[unavailable]`) and patterns (URLs, subreddit and user mentions), then normalizes whitespace.
2. **Text validity filtering**: texts shorter than `min_chars` (default 5) after cleaning are skipped and get empty language values.
3. **Detection**: texts are sorted by length (descending) for the best Rayon parallel use. Detection runs in configurable batches (default 2M rows) to bound memory.
4. **Output**: appends the language columns to the original data, in the same format as the input (Parquet or CSV).

## How the transformer classifiers work

1. **Model loading**: models are downloaded from Hugging Face and cached in `$HF_HOME/sdb_models/`. Each GPU worker thread loads its models once at startup and reuses them across files (persistent workers).
2. **Text cleaning**: the same pipeline as lingua.
3. **Tokenization**: parallel tokenization with Hugging Face tokenizers, on `tokenize_workers` threads.
4. **Filtering**:
   - **Language filter** (optional): when `use_lingua: true`, only texts whose `lang` column matches the classifier's `supported_languages` are processed. With `lang2_fallback: true`, `lang2` is checked too.
   - **Min tokens filter**: texts with fewer than `min_tokens` tokens are skipped (applied after cleaning and tokenization).
5. **Chunking**, for texts longer than `max_length`:
   - **`truncate`** (default): a single chunk, truncated to `max_length`.
   - **`chunk`**: a sliding window with a configurable `stride` (overlap), producing several chunks per text.
6. **Inference**: batched inference on the GPU(s), with a configurable `classifier_batch_size`.
7. **Chunk aggregation**: with the `chunk` strategy, each label independently selects its top-k chunks by logit value and averages them. This per-label top-k pooling lets different labels focus on different parts of a long text. With `top_k: 1` it is max-pooling.
8. **Activation**:
   - `sigmoid`: multi-label classification (each label independent, probabilities 0 to 1).
   - `softmax`: single-label classification (outputs sum to 1).
9. **Output**: a file in the input's format (Parquet or CSV) with the mandatory fields (`id`, `dataset`, `retrieved_utc`), the configured extra `fields`, and the classifier output columns.

## Multi-GPU architecture

The `ml` profile uses persistent worker threads for GPU inference:

- One executor thread per GPU, with its models loaded into thread-local storage.
- `file_workers` controls how many files are processed in parallel; each file uses all GPUs.
- Models are loaded once at startup and reused across files.
- Thread affinity makes each worker process only on its assigned GPU.

## How classifiers resume

Both profiles check for an existing output file before processing an input file. If the output file exists, the input is skipped. Deleting an output file, or a whole classifier output folder, makes the next run process those inputs again. `--filter` narrows the input set before this check.

## Custom Python classifiers

For classifiers that need custom logic (non-Hugging Face models, special preprocessing and so on), write a Python classifier class. The config-only approach for Hugging Face models is on [Custom classifiers](../user-guide/custom-classifiers.md).

**1. Create the classifier module.** Write `social_data_pipeline/classifiers/my_classifier.py`:

```python
from .base import register_classifier


@register_classifier('my_classifier')
class MyClassifier:
    def __init__(self, name, classifier_config, global_config):
        """
        Initialize the classifier.

        Args:
            name: Classifier name (e.g., 'my_classifier')
            classifier_config: Per-classifier config from cpu/gpu_classifiers.yaml
            global_config: Full config including global settings
        """
        self.name = name
        self.config = classifier_config
        # Load your model, initialize resources, etc.

    def process_csv(self, input_csv, output_csv, data_type, config):
        """
        Process a single CSV file.

        Args:
            input_csv: Path to input CSV file
            output_csv: Path to write output CSV
            data_type: Data type being processed (e.g., 'submissions')
            config: Full pipeline configuration
        """
        # Read input CSV
        # Run classification
        # Write output CSV with mandatory fields (id, dataset, retrieved_utc)
        #   + any extra fields + classifier output columns
        pass
```

**2. Register it in the configuration.** Add it to `config/ml/gpu_classifiers.yaml` (or `cpu_classifiers.yaml` for CPU classifiers):

```yaml
my_classifier:
  suffix: "_my_output"
  # Add any custom config your classifier needs
  my_option: value
```

Then add it to the matching `pipeline.yaml`:

```yaml
gpu_classifiers:   # or cpu_classifiers
  - my_classifier
```

### How the registry works

The `@register_classifier('name')` decorator registers the class in a global registry. When the pipeline meets a classifier name, it:

1. Checks the registry for a custom classifier.
2. If found, instantiates it with the config.
3. If not found and the config has a `model` key, falls back to the built-in `TransformerClassifier`.
4. If neither, skips the classifier.

### Output requirements

The `process_csv` output must include:

| Columns | Why |
|---------|-----|
| Mandatory fields: `id`, `dataset`, `retrieved_utc` | Needed for database ingestion with ON CONFLICT |
| Classifier output columns | Your classification results |
| Optional extra fields | Any additional columns from the input, controlled by the `fields` config |
