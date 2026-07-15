"""
Classifier registry and base utilities for social_data_pipeline pipeline.

Provides:
- CLASSIFIER_REGISTRY: Dictionary of registered custom classifiers
- register_classifier: Decorator to register custom classifier classes
- get_classifier: Factory function to get classifier instances
"""

import re
from typing import Dict, Optional, Any

# Registry for custom classifier classes
CLASSIFIER_REGISTRY: Dict[str, type] = {}


def normalize_score_name(name: str) -> str:
    """Canonical form of a classifier output (score) column name.

    Model id2label labels vary in style ("Very Negative", "LABEL_0",
    "toxic"); this maps them to a single snake_case convention so every
    classifier's output columns are consistent and need no quoting in SQL:
    non-alphanumeric runs collapse to a single underscore, edges are
    trimmed, everything is lowercased. Already-snake names are unchanged
    ("toxic" -> "toxic", "not_toxic" -> "not_toxic").

    This is the single source of truth for output-column naming. The
    transformer classifier applies it when writing, and the offline
    CSV->parquet converter applies it when reformatting historical outputs,
    so converted files and freshly-produced files carry identical columns.
    """
    s = re.sub(r"[^0-9A-Za-z]+", "_", name.strip())
    return re.sub(r"_+", "_", s).strip("_").lower()


def register_classifier(name: str):
    """
    Decorator to register a custom classifier class.
    
    Usage:
        @register_classifier('my_classifier')
        class MyClassifier:
            def __init__(self, name: str, classifier_config: Dict, global_config: Dict):
                ...
            
            def process_csv(self, input_csv: str, output_csv: str, data_type: str, config: Dict):
                ...
    
    Args:
        name: Unique identifier for the classifier (used in config files)
    """
    def decorator(cls):
        CLASSIFIER_REGISTRY[name] = cls
        return cls
    return decorator


def get_classifier(name: str, classifier_config: Dict, global_config: Dict) -> Optional[Any]:
    """
    Get a classifier instance by name.
    
    Resolution order:
    1. Check CLASSIFIER_REGISTRY for custom registered classifiers
    2. If config has 'model' key, create TransformerClassifier instance
    3. Return None if not found (caller handles legacy fallback)
    
    Args:
        name: Classifier name (from pipeline.yaml classifiers list)
        classifier_config: Per-classifier config from classifiers.yaml
        global_config: Global settings from classifiers.yaml (text_columns, gpu_ids, etc.)
    
    Returns:
        Classifier instance or None if not found
    """
    # Check registry for custom classifiers
    if name in CLASSIFIER_REGISTRY:
        cls = CLASSIFIER_REGISTRY[name]
        return cls(name, classifier_config, global_config)
    
    # Check if this is a config-defined transformer classifier
    if isinstance(classifier_config, dict) and 'model' in classifier_config:
        from .transformer import TransformerClassifier
        return TransformerClassifier(name, classifier_config, global_config)
    
    # Not found - caller should handle legacy fallback (e.g., lingua)
    return None
