# Adding Custom Classifiers

Social Data Bridge supports two approaches for adding custom classifiers: config-only (for HuggingFace transformer models) and custom Python (for any classification logic).

## Config-Only Approach

For HuggingFace models that follow standard classification patterns, you can add a classifier entirely through configuration.

### 1. Add Classifier Definition

Add to `config/ml/gpu_classifiers.yaml`:

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

### 2. Enable in Pipeline

Add to `config/ml/pipeline.yaml`:

```yaml
gpu_classifiers:
  - toxic_roberta
  - go_emotions
  - my_classifier                    # Add your classifier
```

### 3. Run

```bash
# Run all classifiers
python sdb.py run ml

# Run only your classifier
CLASSIFIER=my_classifier docker compose --profile ml up
```

### 4. Configure Database Ingestion (Optional)

Add to `config/postgres_ml/services.yaml`:

```yaml
classifiers:
  # ... existing classifiers ...
  my_classifier:
    enabled: true
    source_dir: my_classifier
    suffix: "_my_output"
```

### Model Requirements

- Output columns are auto-derived from `model.config.id2label`
- The model must be compatible with HuggingFace's `AutoModelForSequenceClassification` (for PyTorch) or provide an ONNX export
- ONNX FP16 models are recommended for best performance

### Available Options

See the full [Transformer Options Reference](../profiles/classification.md#transformer-options-reference) for all per-classifier options.

## Custom Python Approach

For classifiers that need custom logic (non-HuggingFace models, special preprocessing, etc.), create a Python classifier class.

### 1. Create Classifier Module

Create `social_data_bridge/classifiers/my_classifier.py`:

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

### 2. Register in Configuration

Add to `config/ml/gpu_classifiers.yaml` (or `cpu_classifiers.yaml` for CPU classifiers):

```yaml
my_classifier:
  suffix: "_my_output"
  # Add any custom config your classifier needs
  my_option: value
```

Add to the appropriate pipeline.yaml:

```yaml
gpu_classifiers:   # or cpu_classifiers
  - my_classifier
```

### How the Registry Works

The `@register_classifier('name')` decorator registers your class in a global registry. When the pipeline encounters a classifier name:

1. Checks the registry for a custom classifier
2. If found, instantiates it with the config
3. If not found and config has a `model` key, falls back to the built-in `TransformerClassifier`
4. If neither, skips the classifier

### Output Requirements

Your `process_csv` output must include:
- **Mandatory fields**: `id`, `dataset`, `retrieved_utc` (needed for database ingestion with ON CONFLICT)
- **Classifier output columns**: Your classification results
- **Optional extra fields**: Any additional columns from the input (controlled by `fields` config)
