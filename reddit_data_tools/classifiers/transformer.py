"""
Config-driven transformer classifier for reddit_data_tools pipeline.

Supports:
- ONNX-fp16, ONNX, and PyTorch model backends
- Multi-GPU parallelization with persistent worker threads
- Chunking with overlapping windows for long texts
- Top-k logit pooling for chunk aggregation
- Language filtering via 'lang' column from Lingua

Architecture:
- Each GPU gets a dedicated worker thread that loads the model once
- Workers stay alive across files, eliminating model reload overhead
- Thread-local storage holds model/tokenizer per worker

Note: All heavy dependencies (numpy, scipy, torch, transformers) are lazy-imported
to allow this module to be loaded in CPU-only environments without failing.
"""

from __future__ import annotations  # Defer type annotation evaluation

import os
import time
import threading
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor

# Lazy imports for GPU dependencies (only loaded when actually processing)
np = None
scipy_softmax = None
pl = None
torch = None
AutoTokenizer = None

# Thread-local storage for worker models
_thread_local = threading.local()

# Mandatory fields always included in output (required for ON CONFLICT resolution)
# Same as base table ingestion - ensures consistent duplicate handling
MANDATORY_FIELDS = ['id', 'dataset', 'retrieved_utc']


def _lazy_import_deps():
    """Lazy import all heavy dependencies to avoid loading in CPU-only containers."""
    global np, scipy_softmax, pl, torch, AutoTokenizer
    
    if np is None:
        import numpy as _np
        from scipy.special import softmax as _scipy_softmax
        import polars as _pl
        import torch as _torch
        from transformers import AutoTokenizer as _AutoTokenizer
        
        np = _np
        scipy_softmax = _scipy_softmax
        pl = _pl
        torch = _torch
        AutoTokenizer = _AutoTokenizer


def _worker_init(gpu_id: int, model_id: str, model_type: str, file_name: str, quiet: bool = False):
    """
    Initialize a worker thread by loading the model for its assigned GPU.
    Called once per worker when the executor is created.
    
    Stores model, tokenizer, gpu_id, and device in thread-local storage.
    """
    try:
        _lazy_import_deps()
        
        # Check CUDA availability
        if not torch.cuda.is_available():
            raise RuntimeError(
                f"CUDA is not available. "
                f"torch.cuda.is_available()=False. "
                f"Check NVIDIA drivers and CUDA installation."
            )
        
        cuda_device_count = torch.cuda.device_count()
        if gpu_id >= cuda_device_count:
            raise RuntimeError(
                f"GPU {gpu_id} requested but only {cuda_device_count} GPU(s) available. "
                f"Available GPUs: {[i for i in range(cuda_device_count)]}"
            )
        
        if not quiet:
            print(f"[GPU {gpu_id}] Loading model: {model_id}")
        device = torch.device(f"cuda:{gpu_id}")
        
        # Load model based on type
        if model_type in ('onnx_fp16', 'onnx'):
            try:
                from optimum.onnxruntime import ORTModelForSequenceClassification
            except ImportError:
                # Fallback for different optimum versions
                from optimum.onnxruntime.modeling import ORTModelForSequenceClassification
            
            # Check ONNX Runtime CUDA provider availability
            import onnxruntime as ort
            available_providers = ort.get_available_providers()
            if 'CUDAExecutionProvider' not in available_providers:
                raise RuntimeError(
                    f"CUDAExecutionProvider not available in ONNX Runtime. "
                    f"Available providers: {available_providers}. "
                    f"Reinstall onnxruntime-gpu or check CUDA installation."
                )
            
            model = ORTModelForSequenceClassification.from_pretrained(
                model_id,
                file_name=file_name,
                provider="CUDAExecutionProvider",
                provider_options={'device_id': gpu_id}
            )
        else:  # pytorch
            from transformers import AutoModelForSequenceClassification
            model = AutoModelForSequenceClassification.from_pretrained(model_id)
            model = model.to(device)
            model.eval()
        
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        
        # Store in thread-local storage
        _thread_local.gpu_id = gpu_id
        _thread_local.model = model
        _thread_local.tokenizer = tokenizer
        _thread_local.device = device
        _thread_local.model_type = model_type
        
    except Exception as e:
        # Log extensive diagnostics only on failure
        print(f"[GPU {gpu_id}] FATAL: Worker initialization failed: {e}")
        print(f"[GPU {gpu_id}] Diagnostics:")
        print(f"[GPU {gpu_id}]   model_id: {model_id}")
        print(f"[GPU {gpu_id}]   model_type: {model_type}")
        print(f"[GPU {gpu_id}]   file_name: {file_name}")
        try:
            print(f"[GPU {gpu_id}]   torch.cuda.is_available(): {torch.cuda.is_available()}")
            print(f"[GPU {gpu_id}]   torch.cuda.device_count(): {torch.cuda.device_count()}")
            if torch.cuda.is_available():
                for i in range(torch.cuda.device_count()):
                    print(f"[GPU {gpu_id}]   GPU {i}: {torch.cuda.get_device_name(i)}")
        except Exception as cuda_err:
            print(f"[GPU {gpu_id}]   CUDA query failed: {cuda_err}")
        try:
            import onnxruntime as ort
            print(f"[GPU {gpu_id}]   ONNX Runtime providers: {ort.get_available_providers()}")
        except Exception as ort_err:
            print(f"[GPU {gpu_id}]   ONNX Runtime query failed: {ort_err}")
        import traceback
        traceback.print_exc()
        raise


def create_token_chunks(tokens: List[int], max_length: int, stride: int) -> List[List[int]]:
    """
    Split token IDs into overlapping chunks.
    
    Args:
        tokens: List of token IDs (without special tokens)
        max_length: Maximum length in tokens
        stride: Stride between windows
    
    Returns:
        List of token ID chunks (each chunk is a list of token IDs)
    """
    # Account for special tokens (CLS and SEP)
    max_content_length = max_length - 2
    
    if len(tokens) <= max_content_length:
        return [tokens]
    
    chunks = []
    for i in range(0, len(tokens), stride):
        chunk_tokens = tokens[i:i + max_content_length]
        chunks.append(chunk_tokens)
        
        # Stop if this chunk reaches the end
        if i + max_content_length >= len(tokens):
            break
    
    return chunks


def _infer_worker(
    chunk_token_ids_list: List[List[int]],
    classifier_batch_size: int,
    max_length: int,
    quiet: bool = False,
) -> List[np.ndarray]:
    """
    Run inference using this worker's pre-loaded model (from thread-local storage).
    
    Args:
        chunk_token_ids_list: List of token ID lists (pre-tokenized chunks)
        classifier_batch_size: Batch size for GPU inference
        max_length: Maximum sequence length
        quiet: Suppress per-GPU logging (for parallel file processing)
    
    Returns:
        List of logits as numpy arrays
    """
    # Get model from thread-local storage (loaded by _worker_init)
    try:
        gpu_id = _thread_local.gpu_id
        model = _thread_local.model
        tokenizer = _thread_local.tokenizer
        device = _thread_local.device
        model_type = _thread_local.model_type
    except AttributeError as e:
        print(f"[INFERENCE] FATAL: Worker not initialized. Thread-local missing: {e}")
        print(f"[INFERENCE] This usually means _worker_init failed silently or was never called.")
        raise RuntimeError(f"Worker thread-local storage not initialized: {e}")
    
    all_logits = []
    total_chunks = len(chunk_token_ids_list)
    start_time = time.time()
    
    try:
        for i in range(0, len(chunk_token_ids_list), classifier_batch_size):
            batch_token_ids = chunk_token_ids_list[i:i + classifier_batch_size]
            
            # Add special tokens and pad
            batch_input_ids = []
            for token_ids in batch_token_ids:
                input_ids = [tokenizer.cls_token_id] + token_ids + [tokenizer.sep_token_id]
                batch_input_ids.append(input_ids)
            
            # Pad sequences to same length
            max_len = min(max(len(ids) for ids in batch_input_ids), max_length)
            padded_input_ids = []
            attention_masks = []
            
            for input_ids in batch_input_ids:
                if len(input_ids) > max_len:
                    input_ids = input_ids[:max_len]
                
                attention_mask = [1] * len(input_ids)
                padding_length = max_len - len(input_ids)
                input_ids = input_ids + [tokenizer.pad_token_id] * padding_length
                attention_mask = attention_mask + [0] * padding_length
                
                padded_input_ids.append(input_ids)
                attention_masks.append(attention_mask)
            
            # Convert to tensors
            batch_inputs = {
                'input_ids': torch.tensor(padded_input_ids, dtype=torch.long).to(device),
                'attention_mask': torch.tensor(attention_masks, dtype=torch.long).to(device)
            }
            
            with torch.inference_mode():
                if model_type == 'pytorch':
                    with torch.amp.autocast('cuda'):
                        outputs = model(**batch_inputs)
                else:
                    outputs = model(**batch_inputs)
                
                logits = outputs.logits
                all_logits.extend(logits.cpu().numpy())
                
    except Exception as e:
        print(f"[GPU {gpu_id}] FATAL: Inference failed: {e}")
        print(f"[GPU {gpu_id}] Diagnostics:")
        print(f"[GPU {gpu_id}]   total_chunks: {total_chunks}")
        print(f"[GPU {gpu_id}]   classifier_batch_size: {classifier_batch_size}")
        print(f"[GPU {gpu_id}]   max_length: {max_length}")
        print(f"[GPU {gpu_id}]   model_type: {model_type}")
        print(f"[GPU {gpu_id}]   device: {device}")
        try:
            print(f"[GPU {gpu_id}]   torch.cuda.is_available(): {torch.cuda.is_available()}")
            print(f"[GPU {gpu_id}]   torch.cuda.current_device(): {torch.cuda.current_device()}")
            print(f"[GPU {gpu_id}]   torch.cuda.memory_allocated({gpu_id}): {torch.cuda.memory_allocated(gpu_id) / 1e9:.2f} GB")
        except Exception as cuda_err:
            print(f"[GPU {gpu_id}]   CUDA query failed: {cuda_err}")
        import traceback
        traceback.print_exc()
        raise
    
    # Only log if not in quiet mode (parallel file workers have their own summary)
    if not quiet:
        elapsed = time.time() - start_time
        speed = total_chunks / elapsed if elapsed > 0 else 0
        print(f"[GPU {gpu_id}] {total_chunks:,} chunks in {elapsed:.1f}s ({speed:.0f} chunks/s)")
    
    return all_logits


def aggregate_chunk_logits(
    chunk_metadata_list: List[Tuple[int, int]],
    all_logits: List[np.ndarray],
    num_texts: int,
    top_k: int,
    chunking_strategy: str,
    activation: str
) -> Tuple[np.ndarray, List[int]]:
    """
    Aggregate chunk logits back to per-text predictions using per-label top-k pooling.
    
    For multi-label classifiers, each label is aggregated independently:
    - For each label, select the top-k chunks with highest logits for that label
    - Average those k chunks to get the final logit for that label
    
    This ensures that each label's prediction uses its most relevant chunks,
    rather than using the same chunks for all labels.
    
    Args:
        chunk_metadata_list: List of (text_idx, chunk_id) for each chunk
        all_logits: Flat list of logits for all chunks (numpy arrays)
        num_texts: Total number of original texts
        top_k: Number of top chunks to average per label (1 = max pooling)
        chunking_strategy: 'truncate' or 'chunk'
        activation: 'sigmoid' or 'softmax'
    
    Returns:
        Tuple of (probabilities array shape [num_texts, num_labels], skip_indices list)
    """
    if not all_logits:
        return np.array([]), []
    
    num_labels = all_logits[0].shape[0] if len(all_logits[0].shape) > 0 else 1
    
    # Group logits by text_idx
    text_logits: List[List[np.ndarray]] = [[] for _ in range(num_texts)]
    for (text_idx, chunk_id), logits in zip(chunk_metadata_list, all_logits):
        text_logits[text_idx].append(logits)
    
    # Aggregate and apply activation
    all_probs = []
    skip_indices = []
    
    for text_idx, logits_list in enumerate(text_logits):
        if not logits_list:
            # No chunks for this text (filtered out)
            skip_indices.append(text_idx)
            all_probs.append(np.zeros(num_labels))
            continue
        
        if len(logits_list) == 1 or chunking_strategy == 'truncate':
            # Single chunk or truncate mode
            logits = logits_list[0]
        else:
            # Multiple chunks: apply top-k pooling PER LABEL (important for multi-label)
            logits_array = np.array(logits_list)  # Shape: (num_chunks, num_labels)
            num_chunks, n_labels = logits_array.shape
            k = min(top_k, num_chunks)
            
            # For each label, select top-k chunks based on that label's logits
            # and average them independently
            logits = np.zeros(n_labels)
            for label_idx in range(n_labels):
                label_logits = logits_array[:, label_idx]  # Shape: (num_chunks,)
                top_k_indices = np.argpartition(label_logits, -k)[-k:]
                logits[label_idx] = np.mean(label_logits[top_k_indices])
        
        # Apply activation
        if activation == 'sigmoid':
            probs = 1 / (1 + np.exp(-logits))  # Sigmoid
        else:
            probs = scipy_softmax(logits)
        
        all_probs.append(probs)
    
    return np.array(all_probs), skip_indices


class TransformerClassifier:
    """
    Config-driven transformer classifier with multi-GPU support.
    
    Handles ONNX-fp16, ONNX, and PyTorch models with:
    - Persistent worker threads: one per GPU, models loaded once at startup
    - Chunking with overlapping windows for long texts
    - Top-k logit pooling for chunk aggregation
    - Language filtering via 'lang' column
    
    Architecture:
    - Each GPU gets a dedicated ThreadPoolExecutor with 1 worker
    - Worker loads model once via initializer, stores in thread-local
    - Executors stay alive across files, reused for all process_csv calls
    """
    
    def __init__(self, name: str, classifier_config: Dict, global_config: Dict, worker_id: Optional[int] = None):
        """
        Initialize transformer classifier from config.
        Creates persistent worker threads that load models on their GPUs.
        
        Args:
            name: Classifier name (for logging)
            classifier_config: Per-classifier config from classifiers.yaml
            global_config: Global settings (text_columns, gpu_ids, etc.)
            worker_id: Optional worker ID for logging (used in parallel file processing)
        """
        self.name = name
        self.worker_id = worker_id
        self.model_id = classifier_config['model']
        self.model_type = classifier_config.get('type', 'onnx_fp16')
        self.file_name = classifier_config.get('file_name', 'model.onnx')
        self.activation = classifier_config.get('activation', 'softmax')
        self.suffix = classifier_config.get('suffix', f'_{name}')
        
        # Inference settings (per-classifier, else global, else default)
        self.classifier_batch_size = classifier_config.get('classifier_batch_size', 
                                                            global_config.get('classifier_batch_size', 32))
        self.max_length = classifier_config.get('max_length', 512)
        
        # Disk I/O batch size (rows per batch for reading/writing CSV)
        # Per-classifier, else global, else 2M rows to match Lingua behavior
        self.batch_size = classifier_config.get('batch_size', 
                                                 global_config.get('batch_size', 2_000_000))
        
        # Minimum token count - per-classifier, else global, else 0 (no filter)
        # Filtering happens AFTER text cleaning (removing strings/patterns) and tokenization
        self.min_tokens = classifier_config.get('min_tokens', global_config.get('min_tokens', 0))
        
        # GPU: per-classifier override, else global, else [0]
        self.gpu_ids = classifier_config.get('gpu_ids', global_config.get('gpu_ids', [0]))
        if isinstance(self.gpu_ids, int):
            self.gpu_ids = [self.gpu_ids]
        
        # Tokenization parallelization: global setting (HuggingFace tokenizer workers)
        self.tokenize_workers = global_config.get('tokenize_workers', 0)  # 0 = no parallelization
        
        # Language filtering (only applies if use_lingua is True globally)
        self.use_lingua = global_config.get('use_lingua', True)
        self.supported_languages = classifier_config.get('supported_languages', None)
        # If True, classify if either lang OR lang2 matches supported_languages
        # If False (default), only check lang column
        self.lang2_fallback = classifier_config.get('lang2_fallback', 
                                                     global_config.get('lang2_fallback', False))
        
        # Chunking for long texts
        self.chunking_strategy = classifier_config.get('chunking_strategy', 'truncate')
        self.stride = classifier_config.get('stride', 64)
        self.top_k = classifier_config.get('top_k', 2)
        
        # Output field control: fields list specifies which input columns to keep
        # If not specified or empty, keeps all input columns
        self.fields = classifier_config.get('fields', global_config.get('fields', None))
        
        # Store global config for text column lookup
        self.global_config = global_config
        
        # Persistent executors: one per GPU, created lazily on first use
        self._executors: Dict[int, ThreadPoolExecutor] = {}
        self._tokenizer = None  # Cached tokenizer for main thread
    
    def _ensure_executors(self, quiet: bool = False):
        """
        Create persistent worker executors if not already created.
        Each GPU gets one executor with one worker that loads the model.
        Called lazily on first process_csv to allow GPU-only container startup.
        
        Args:
            quiet: Suppress per-GPU loading messages (for parallel file workers)
        """
        if self._executors:
            return
        
        _lazy_import_deps()
        
        # Create one executor per GPU, each with initializer that loads model
        for gpu_id in self.gpu_ids:
            executor = ThreadPoolExecutor(
                max_workers=1,
                initializer=_worker_init,
                initargs=(gpu_id, self.model_id, self.model_type, self.file_name, quiet),
            )
            self._executors[gpu_id] = executor
        
        # Warm up executors by submitting a no-op (triggers initializer)
        # This will raise if initializer failed (e.g., ImportError)
        warmup_futures = []
        for gpu_id, executor in self._executors.items():
            future = executor.submit(lambda: None)
            warmup_futures.append((gpu_id, future))
        
        # Wait for all workers to initialize (models loaded)
        # Collect any errors and raise them properly
        errors = []
        for gpu_id, f in warmup_futures:
            try:
                f.result(timeout=300)  # 5 min timeout for model loading
            except Exception as e:
                errors.append(f"GPU {gpu_id}: {e}")
        
        if errors:
            # Clean up broken executors
            for executor in self._executors.values():
                executor.shutdown(wait=False)
            self._executors.clear()
            raise RuntimeError(f"Failed to initialize GPU workers: {'; '.join(errors)}")
        
        # Cache tokenizer for main thread (from first worker's thread-local)
        # We need our own copy since thread-local is per-thread
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_id)
    
    def shutdown(self):
        """Shutdown all worker executors. Call when done with classifier."""
        for executor in self._executors.values():
            executor.shutdown(wait=True)
        self._executors.clear()
    
    def __del__(self):
        """Cleanup executors on garbage collection."""
        if hasattr(self, '_executors') and self._executors:
            self.shutdown()
    
    def _get_text_columns(self, data_type: str) -> List[str]:
        """Get text columns for the given data type."""
        text_columns_config = self.global_config.get('text_columns', {})
        defaults = {'submissions': ['title', 'selftext'], 'comments': ['body']}
        return text_columns_config.get(data_type, defaults.get(data_type, []))
    
    def _build_text_expr(self, text_columns: List[str]) -> pl.Expr:
        """Build Polars expression for text concatenation."""
        remove_strings = self.global_config.get('remove_strings', [])
        remove_patterns = self.global_config.get('remove_patterns', [])
        
        if len(text_columns) == 1:
            text_expr = pl.col(text_columns[0]).fill_null("")
        else:
            col_exprs_nullable = [
                pl.when(pl.col(c).fill_null("") == "")
                .then(None)
                .otherwise(pl.col(c))
                for c in text_columns
            ]
            text_expr = pl.concat_str(col_exprs_nullable, separator=" ", ignore_nulls=True)
        
        # Remove exact strings
        for s in remove_strings:
            text_expr = text_expr.str.replace_all(s, "", literal=True)
        
        # Remove regex patterns
        for pattern in remove_patterns:
            text_expr = text_expr.str.replace_all(pattern, "")
        
        # Clean whitespace
        text_expr = text_expr.str.replace_all("\n", " ", literal=True)
        text_expr = text_expr.str.replace_all("\\n", " ", literal=True)
        text_expr = text_expr.str.replace_all(r"\s+", " ")
        text_expr = text_expr.str.strip_chars()
        
        return text_expr
    
    def process_csv(self, input_csv: str, output_csv: str, data_type: str, config: Dict, quiet_gpu: bool = False) -> int:
        """
        Process CSV file with transformer classification.
        
        Uses batched CSV reading/writing to handle very large files (50GB+).
        Uses a .temp file during writing and renames to final name on success.
        This ensures partial files from interrupted runs are not mistaken as complete.
        
        Args:
            input_csv: Input CSV path
            output_csv: Output CSV path
            data_type: 'submissions' or 'comments'
            config: Full classifier config (merged global + per-classifier)
            quiet_gpu: Suppress per-GPU loading logs (for parallel file workers)
        
        Returns:
            Number of rows processed
        """
        _lazy_import_deps()
        
        start_time = time.time()
        input_path = Path(input_csv)
        output_path = Path(output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use temp file during writing, rename on success
        temp_path = output_path.with_suffix(output_path.suffix + '.temp')
        
        # Build worker suffix for logging
        worker_suffix = f" (worker {self.worker_id})" if self.worker_id is not None else ""
        
        # Clean up any leftover temp file from interrupted run
        if temp_path.exists():
            print(f"[{self.name.upper()}] [{input_path.stem}] Removing incomplete temp file: {temp_path.name}{worker_suffix}")
            temp_path.unlink()
        
        # Get model config for id2label (needed for output columns)
        from transformers import AutoConfig
        model_config = AutoConfig.from_pretrained(self.model_id)
        num_labels = model_config.num_labels if hasattr(model_config, 'num_labels') else 2
        id2label = model_config.id2label if hasattr(model_config, 'id2label') else {i: f'label_{i}' for i in range(num_labels)}
        
        # Build text expression once (reused per batch)
        text_columns = self._get_text_columns(data_type)
        text_expr = self._build_text_expr(text_columns)
        
        # Ensure GPU models are loaded before processing
        self._ensure_executors(quiet=quiet_gpu)
        
        # Use cached tokenizer
        tokenizer = self._tokenizer
        tokenizer.model_max_length = int(1e30)  # Disable warning, we handle chunking
        
        # Set tokenizer parallelism if configured
        if self.tokenize_workers > 0:
            os.environ['TOKENIZERS_PARALLELISM'] = 'true'
            os.environ['RAYON_NUM_THREADS'] = str(self.tokenize_workers)
        
        # Initialize counters and timing
        total_rows = 0
        total_classified = 0
        tok_time = 0.0
        inf_time = 0.0
        first_batch = True
        
        # Use batched CSV reader for streaming large files
        reader = pl.read_csv_batched(input_csv, batch_size=self.batch_size)
        
        print(f"[{self.name.upper()}] [{input_path.stem}] Starting{worker_suffix}")
        
        while True:
            # Read batches until we have enough rows or EOF
            accumulated = []
            accumulated_rows = 0
            while accumulated_rows < self.batch_size:
                batches = reader.next_batches(1)
                if not batches:
                    break
                accumulated.append(batches[0])
                accumulated_rows += len(batches[0])
            
            if not accumulated:
                break
            
            # Concatenate accumulated batches
            if len(accumulated) == 1:
                df = accumulated[0]
            else:
                df = pl.concat(accumulated)
            
            batch_rows = len(df)
            total_rows += batch_rows
            
            # Build text column
            df = df.with_columns([
                text_expr.alias("_text"),
            ])
            
            # Language filtering (if enabled)
            rows_to_classify_mask = pl.lit(True)
            if self.use_lingua and self.supported_languages:
                if 'lang' in df.columns:
                    lang_match = pl.col('lang').is_in(self.supported_languages)
                    if self.lang2_fallback and 'lang2' in df.columns:
                        # Classify if either lang OR lang2 matches
                        lang2_match = pl.col('lang2').is_in(self.supported_languages)
                        rows_to_classify_mask = lang_match | lang2_match
                    else:
                        rows_to_classify_mask = lang_match
            
            # Non-empty text filter (must have at least 1 character after cleaning)
            non_empty_mask = pl.col("_text").str.len_chars() > 0
            
            df = df.with_columns([
                rows_to_classify_mask.alias("_lang_ok"),
                non_empty_mask.alias("_text_ok"),
            ])
            
            # First-pass filter: language OK and non-empty text
            candidate_mask = pl.col("_lang_ok") & pl.col("_text_ok")
            candidate_df = df.filter(candidate_mask)
            candidate_texts = candidate_df["_text"].to_list()
            candidate_batch_indices = df.with_row_index("_idx").filter(candidate_mask)["_idx"].to_list()
            
            # Initialize output columns with empty values
            output_col_values = {id2label[lid]: [""] * batch_rows for lid in sorted(id2label.keys())}
            
            # Initialize valid_indices for cases where candidate_texts is empty
            valid_indices = []
            
            if candidate_texts:
                # Tokenize all candidate texts
                tok_start = time.time()
                tokenize_batch_size = 50000
                all_token_ids = []
                
                for i in range(0, len(candidate_texts), tokenize_batch_size):
                    batch_texts = candidate_texts[i:i + tokenize_batch_size]
                    batch_encoded = tokenizer(
                        batch_texts,
                        add_special_tokens=False,
                        truncation=False,
                        return_attention_mask=False,
                        return_token_type_ids=False,
                    )
                    all_token_ids.extend(batch_encoded['input_ids'])
                
                tok_time += time.time() - tok_start
                
                # Second-pass filter: apply min_tokens threshold (vectorized)
                # Get token counts as numpy array for fast filtering
                token_lengths = np.array([len(t) for t in all_token_ids], dtype=np.int32)
                valid_mask = token_lengths >= self.min_tokens
                valid_candidate_indices = np.nonzero(valid_mask)[0]
                
                # Map candidate indices to batch indices
                candidate_batch_indices_arr = np.array(candidate_batch_indices, dtype=np.int64)
                valid_indices = candidate_batch_indices_arr[valid_candidate_indices].tolist()
                
                # Create chunks only for texts that pass min_tokens filter
                chunk_metadata_list = []
                chunk_token_ids_list = []
                
                # Pre-compute chunking parameters
                max_content_length = self.max_length - 2  # Account for CLS/SEP
                use_chunking = self.chunking_strategy == 'chunk'
                
                for text_idx, candidate_idx in enumerate(valid_candidate_indices):
                    tokens = all_token_ids[candidate_idx]
                    
                    if use_chunking and len(tokens) > max_content_length:
                        # Chunking with overlap
                        chunk_id = 0
                        for chunk_start in range(0, len(tokens), self.stride):
                            chunk_tokens = tokens[chunk_start:chunk_start + max_content_length]
                            chunk_metadata_list.append((text_idx, chunk_id))
                            chunk_token_ids_list.append(chunk_tokens)
                            chunk_id += 1
                            if chunk_start + max_content_length >= len(tokens):
                                break
                    else:
                        # Single chunk (truncate if needed)
                        chunk_metadata_list.append((text_idx, 0))
                        chunk_token_ids_list.append(tokens[:max_content_length])
                
                # Split chunks across GPUs
                num_gpus = len(self.gpu_ids)
                chunk_splits = [chunk_token_ids_list[i::num_gpus] for i in range(num_gpus)]
                metadata_splits = [chunk_metadata_list[i::num_gpus] for i in range(num_gpus)]
                
                # Run inference
                inf_start = time.time()
                futures = []
                for i, gpu_id in enumerate(self.gpu_ids):
                    future = self._executors[gpu_id].submit(
                        _infer_worker,
                        chunk_splits[i],
                        self.classifier_batch_size,
                        self.max_length,
                        True,
                    )
                    futures.append(future)
                
                results = [f.result() for f in futures]
                inf_time += time.time() - inf_start
                
                # Reconstruct logits
                all_logits = []
                reconstructed_metadata = []
                max_chunks = max(len(r) for r in results) if results else 0
                for i in range(max_chunks):
                    for gpu_idx in range(num_gpus):
                        if i < len(results[gpu_idx]):
                            all_logits.append(results[gpu_idx][i])
                            reconstructed_metadata.append(metadata_splits[gpu_idx][i])
                
                # Aggregate chunk logits
                probs_array, skip_indices = aggregate_chunk_logits(
                    reconstructed_metadata,
                    all_logits,
                    len(valid_candidate_indices),  # Number of texts that passed min_tokens filter
                    self.top_k,
                    self.chunking_strategy,
                    self.activation
                )
                
                # Assign results to output columns at correct batch indices
                for text_i, batch_idx in enumerate(valid_indices):
                    if text_i not in skip_indices and text_i < len(probs_array):
                        probs = probs_array[text_i]
                        for label_id in sorted(id2label.keys()):
                            label_name = id2label[label_id]
                            label_idx = list(sorted(id2label.keys())).index(label_id)
                            if label_idx < len(probs):
                                output_col_values[label_name][batch_idx] = f"{probs[label_idx]:.4f}"
                
                total_classified += len(valid_candidate_indices) - len(skip_indices)
            
            # Build output dataframe: filter to valid rows only (those that passed min_tokens filter)
            # valid_indices contains batch indices of texts that passed language + min_tokens filters
            if candidate_texts and valid_indices:
                df_indexed = df.with_row_index("_idx")
                df_out = df_indexed.filter(pl.col("_idx").is_in(valid_indices))
                
                # Add result columns (filtered to valid rows only)
                for label_name, col_values in output_col_values.items():
                    filtered_values = [col_values[i] for i in valid_indices]
                    df_out = df_out.with_columns(pl.Series(label_name, filtered_values))
                
                # Drop temporary columns
                df_out = df_out.drop(["_text", "_lang_ok", "_text_ok", "_idx"])
            else:
                # No valid texts - create empty output with proper columns
                df_out = df.filter(pl.lit(False))
                for label_name in output_col_values.keys():
                    df_out = df_out.with_columns(pl.Series(label_name, []))
                df_out = df_out.drop(["_text", "_lang_ok", "_text_ok"])
            
            # Filter to specified fields if configured
            # Always keep: mandatory fields (id, dataset, retrieved_utc) for ON CONFLICT resolution
            # Plus: specified fields + classifier output columns (id2label values)
            classifier_cols = set(id2label.values())
            
            # Start with mandatory fields (same as base table for consistent duplicate handling)
            cols_to_keep = [f for f in MANDATORY_FIELDS if f in df_out.columns]
            
            # Add configured fields (excluding mandatory to avoid duplicates)
            if self.fields:
                for f in self.fields:
                    if f in df_out.columns and f not in cols_to_keep:
                        cols_to_keep.append(f)
            
            # Add classifier output columns
            cols_to_keep += [c for c in df_out.columns if c in classifier_cols]
            df_out = df_out.select(cols_to_keep)
            
            # Write batch to temp file
            if first_batch:
                df_out.write_csv(temp_path)
                first_batch = False
            else:
                with open(temp_path, 'ab') as f:
                    df_out.write_csv(f, include_header=False)
        
        # Rename temp file to final output path on success
        if temp_path.exists():
            temp_path.rename(output_path)
        
        # Finished summary
        elapsed = time.time() - start_time
        rate = total_classified / elapsed if elapsed > 0 else 0
        skipped = total_rows - total_classified
        skip_pct = (skipped / total_rows * 100) if total_rows > 0 else 0
        print(f"[{self.name.upper()}] [{input_path.stem}] Finished ({skip_pct:.0f}% skipped). tok={tok_time:.1f}s, inf={inf_time:.1f}s, total={elapsed:.1f}s ({rate:.0f} msg/s){worker_suffix}")
        
        return total_classified
