---
title: "Custom classifiers"
parent: "User guide"
nav_order: 8
---
# Custom classifiers

You can add your own classifier in two ways: config-only, for Hugging Face transformer models, and custom Python, for any classification logic. This page walks through the config-only approach; you end up with a classifier that runs in the `ml` profile and is ingested into the databases like the built-in ones.

For custom Python classifiers (non-Hugging Face models, special preprocessing), see [Classifiers](../architecture/classifiers.md#custom-python-classifiers).

## Adding a Hugging Face model

**1. Add the classifier definition.** Add it to `config/ml/gpu_classifiers.yaml`:

```yaml
my_classifier:
  suffix: "_my_output"
  type: onnx_fp16                    # onnx_fp16, onnx, or pytorch
  model: "org/model-name"            # HuggingFace model ID
  file_name: "model.onnx"            # ONNX model filename (for ONNX types)
  activation: sigmoid                # sigmoid (multi-label) or softmax (single-label)
  supported_languages: [en]          # Optional: filter by language
  classifier_batch_size: 32          # Batch size per GPU
  max_length: 512                    # Max token length
  # Optional: chunking for long texts
  chunking_strategy: chunk           # truncate or chunk
  stride: 128                        # Overlap between chunks
  top_k: 2                           # Top-k chunks to average
```

**2. Enable it in the pipeline.** Add it to `config/ml/pipeline.yaml`, or per source to `config/sources/<source>/ml.yaml`:

```yaml
gpu_classifiers:
  - toxic_roberta                    # bare string = runs on every processing.data_types entry
  - go_emotions
  - name: my_classifier              # dict form scopes to a subset of data_types
    data_types: [comments]
```

Each entry is either a string (runs on all configured `data_types`) or a `{name, data_types}` dict that limits the classifier to the listed data types. Unknown `data_types` raise a config error.

`python sdp.py source add-classifiers <source>` does this interactively: for every selected classifier it asks which data types it should run on, when the source has more than one. The web UI's **Add classifiers…** on the source's Configuration tab asks the same questions, with the current answers filled in (see [Web UI](webui.md#reconfiguring-a-source-and-its-classifiers)).

**3. Run it.**

```bash
python sdp.py run ml                                  # Run all classifiers
CLASSIFIER=my_classifier docker compose --profile ml up   # Run only your classifier
```

**4. Ingest it.** Once a classifier is in your source's `ml.yaml`, ingestion is automatic: `postgres_ml` and `sr_ml` take the classifier list and each classifier's `suffix` from the same merged ml profile config, so nothing is duplicated.

`config/postgres_ml/services.yaml` and `config/sr_ml/services.yaml` (and their per-source overrides at `config/sources/<name>/postgres_ml.yaml` / `sr_ml.yaml`) hold ingestion-only overrides, all optional:

```yaml
classifiers:
  my_classifier:
    enabled: false              # ran in ml profile, but skip ingestion
    source_dir: custom_outputs  # non-default output directory
    column_overrides:
      confidence: REAL          # force a specific PG/SR column type
```

## Options

The options used above:

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename and table suffix | *(required)* |
| `type` | Model backend: `onnx_fp16`, `onnx`, `pytorch` | `onnx_fp16` |
| `model` | Hugging Face model ID | *(required)* |
| `file_name` | ONNX model filename | `model.onnx` |
| `activation` | `sigmoid` (multi-label) or `softmax` (single-label) | `softmax` |
| `supported_languages` | Filter by the `lang` column (requires `use_lingua: true`) | *(all languages)* |
| `classifier_batch_size` | Batch size per GPU | `32` |
| `max_length` | Maximum token length | `512` |
| `chunking_strategy` | `truncate` or `chunk` | `truncate` |
| `stride` | Overlap between chunks (for `chunk`) | `64` |
| `top_k` | Top-k chunks to average (1 = max-pooling) | `2` |

See the full [Transformer options reference](classifiers.md#transformer-options-reference) for every per-classifier option.

## Model requirements

| Requirement | Detail |
|-------------|--------|
| Labels | Output columns are derived from `model.config.id2label` |
| Compatibility | The model must work with Hugging Face's `AutoModelForSequenceClassification` (for PyTorch) or provide an ONNX export |
| Performance | ONNX FP16 models are recommended for the best performance |
