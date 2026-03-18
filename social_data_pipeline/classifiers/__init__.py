"""
Classifier modules for text classification.
"""

from .base import get_classifier, register_classifier
from . import lingua

__all__ = [
    'get_classifier',
    'register_classifier',
    'lingua',
]
