"""
ML profile orchestrator for social_data_pipeline.
Handles running classifiers (Lingua for CPU, transformers for GPU).
Expects CSV files to already exist (run parse profile first).
"""

import os
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from ..core.config import (
    load_profile_config,
    load_platform_config,
    get_required,
    get_optional,
    validate_processing_config,
    validate_classifier_config,
    ConfigurationError,
)


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def _process_lingua_worker(args: Tuple[str, str, str, str, Dict, int]) -> Tuple[str, bool, str]:
    """
    Worker function for parallel Lingua file processing.
    Must be at module level for pickling.
    """
    input_csv, output_csv, data_type, classifier_name, config, workers_per_file = args
    
    os.environ['RAYON_NUM_THREADS'] = str(workers_per_file)
    os.environ['POLARS_MAX_THREADS'] = str(workers_per_file)
    
    try:
        from ..classifiers import lingua as classifier_module
        
        classifier_module.process_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            data_type=data_type,
            config=config
        )
        return (Path(input_csv).stem, True, "")
    except Exception as e:
        return (Path(input_csv).stem, False, str(e))


def _process_transformer_batch(args: Tuple[List[Tuple[str, str, str]], str, Dict, Dict, int]) -> List[Tuple[str, bool, str]]:
    """
    Worker function for parallel transformer file processing.
    Each worker loads models on ALL GPUs, then processes its assigned files.
    """
    file_list, classifier_name, classifier_config, global_config, worker_id = args
    
    results = []
    
    try:
        from ..classifiers.transformer import TransformerClassifier
        
        classifier = TransformerClassifier(classifier_name, classifier_config, global_config, worker_id=worker_id)
        
        for input_csv, output_csv, data_type in file_list:
            try:
                classifier.process_csv(
                    input_csv=input_csv,
                    output_csv=output_csv,
                    data_type=data_type,
                    config=classifier_config,
                    quiet_gpu=True,
                )
                results.append((Path(input_csv).stem, True, ""))
            except Exception as e:
                results.append((Path(input_csv).stem, False, str(e)))
        
        classifier.shutdown()
        
    except Exception as e:
        for input_csv, _, _ in file_list:
            results.append((Path(input_csv).stem, False, str(e)))
    
    return results


def load_config(config_dir: str = "/app/config", profile: str = "lingua", quiet: bool = False) -> Dict:
    """Load ML profile configuration."""
    if profile not in ('lingua', 'ml'):
        raise ConfigurationError(f"Invalid ML profile: {profile}. Must be 'lingua' or 'ml'")
    
    config = load_profile_config(profile, config_dir, source=SOURCE, quiet=quiet)
    validate_processing_config(config, profile)
    return config


def detect_parsed_files(parsed_dir: str, data_types: List[str], file_patterns: Dict = None, file_format: str = 'csv') -> List[Tuple[str, str, str]]:
    """
    Detect parsed files (Parquet or CSV) in the parsed directory.

    Args:
        parsed_dir: Directory containing parsed files
        data_types: List of data types to search for
        file_patterns: Optional dict of file patterns per data type.
                      If None, uses simple glob for each data type.
        file_format: File format ('csv' or 'parquet')
    """
    parsed_base = Path(parsed_dir)
    files = []

    ext = 'parquet' if file_format == 'parquet' else 'csv'

    # Build patterns from platform config if provided
    patterns = {}
    if file_patterns:
        for data_type in data_types:
            if data_type in file_patterns and file_format in file_patterns[data_type]:
                patterns[data_type] = re.compile(file_patterns[data_type][file_format])

    for data_type in data_types:
        type_dir = parsed_base / data_type
        if not type_dir.is_dir():
            continue

        for filepath in type_dir.glob(f"*.{ext}"):
            filename = filepath.name

            # If we have a pattern for this data type, use it
            if data_type in patterns:
                match = patterns[data_type].match(filename)
                if match:
                    file_id = filepath.stem
                    files.append((str(filepath), file_id, data_type, filename))
            else:
                # No pattern - accept all files in the data type directory
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def run_pipeline(profile: str = "lingua", config_dir: str = "/app/config", target_classifier: Optional[str] = None):
    """Run the ML classifier pipeline."""
    config = load_config(config_dir=config_dir, profile=profile)
    
    data_types = get_required(config, 'processing', 'data_types')
    
    if profile == "lingua":
        classifiers_to_run = get_required(config, 'cpu_classifiers')
    else:
        classifiers_to_run = get_required(config, 'gpu_classifiers')
    
    single_classifier = os.environ.get('CLASSIFIER', '')
    if single_classifier:
        if single_classifier not in classifiers_to_run:
            print(f"[sdp] CLASSIFIER='{single_classifier}' not in classifiers list: {classifiers_to_run}")
            sys.exit(1)
        classifiers_to_run = [single_classifier]
        print(f"[sdp] Running single classifier: {single_classifier}")
    
    global_config = {
        'text_columns': get_required(config, 'text_columns'),
        'remove_strings': get_required(config, 'remove_strings'),
        'remove_patterns': get_required(config, 'remove_patterns'),
    }
    
    if profile == "lingua":
        try:
            postgres_config = load_profile_config('postgres_ingest', config_dir, source=SOURCE, quiet=True)
            prefer_lingua = postgres_config.get('processing', {}).get('prefer_lingua', True)
        except Exception:
            prefer_lingua = True
        
        global_config.update({
            'prefer_lingua': prefer_lingua,
            'fields': get_optional(config, 'fields', default=[]),
        })
        print(f"[sdp] Prefer lingua: {prefer_lingua}")
    
    if profile == "ml":
        global_config.update({
            'batch_size': get_required(config, 'batch_size'),
            'classifier_batch_size': get_required(config, 'classifier_batch_size'),
            'gpu_ids': get_required(config, 'gpu_ids'),
            'file_workers': get_required(config, 'file_workers'),
            'use_lingua': get_required(config, 'use_lingua'),
            'lang2_fallback': get_required(config, 'lang2_fallback'),
            'min_tokens': get_required(config, 'min_tokens'),
            'tokenize_workers': get_required(config, 'tokenize_workers'),
            'fields': get_optional(config, 'fields', default=None),
        })
    
    enabled_classifiers = []
    for name in classifiers_to_run:
        if name not in config:
            print(f"[sdp] Classifier '{name}' not configured, skipping")
            continue
        
        cfg = config[name]
        if not isinstance(cfg, dict):
            cfg = {}
        
        validate_classifier_config(cfg, name, profile)
        merged_cfg = {**global_config, **cfg}
        enabled_classifiers.append((name, merged_cfg))
        
        if target_classifier and name == target_classifier:
            break
    
    # Load platform config for file format
    platform_cfg = load_platform_config(config_dir, PLATFORM, source=SOURCE)
    file_format = platform_cfg.get('file_format', 'csv')
    ext = 'parquet' if file_format == 'parquet' else 'csv'

    print(f"[sdp] Profile: {profile}")
    print(f"[sdp] Data types: {data_types}")
    print(f"[sdp] Classifiers: {[name for name, _ in enabled_classifiers]}")

    if profile == "ml":
        print(f"[sdp] GPUs: {global_config['gpu_ids']}, file_workers: {global_config['file_workers']}")

    parsed_dir = "/data/parsed"
    output_dir = "/data/output"
    
    print("\n" + "="*60)
    print("INPUT DETECTION")
    print("="*60)
    
    use_lingua = profile == "ml" and global_config.get('use_lingua', False)
    if use_lingua:
        lingua_config = config.get('lingua', {})
        lingua_suffix = lingua_config.get('suffix', '_lingua')
        lingua_output_dir = Path(output_dir) / 'lingua'
        files_with_name = []
        for data_type in data_types:
            type_dir = lingua_output_dir / data_type
            if type_dir.is_dir():
                for filepath in type_dir.glob(f"*{lingua_suffix}.{ext}"):
                    file_id = filepath.stem
                    if file_id.endswith(lingua_suffix):
                        file_id = file_id[:-len(lingua_suffix)]
                    files_with_name.append((str(filepath), file_id, data_type, filepath.name))
        type_order = {dt: i for i, dt in enumerate(data_types)}
        files_with_name.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
        parsed_files = [(f[0], f[1], f[2]) for f in files_with_name]
        print(f"[sdp] Found {len(parsed_files)} lingua files")
    else:
        parsed_files = detect_parsed_files(parsed_dir, data_types, file_format=file_format)
        print(f"[sdp] Found {len(parsed_files)} {ext.upper()} files")

    if not parsed_files:
        print("\n[sdp] No input files found. Run 'parse' profile first" + ("; use 'lingua' for language detection." if use_lingua else "."))
        return
    
    if not enabled_classifiers:
        print("\n[sdp] No classifiers enabled. Exiting.")
        return
    
    total_timings = {'classifiers': 0.0}
    success_count = 0
    fail_count = 0
    
    print("\n" + "="*60)
    print("RUNNING CLASSIFIERS")
    print("="*60)
    
    t_start = time.time()
    
    for classifier_name, classifier_config in enabled_classifiers:
        is_lingua = classifier_name == 'lingua'
        
        if is_lingua:
            workers = classifier_config['workers']
            os.environ['RAYON_NUM_THREADS'] = str(workers)
        
        classifier_output_dir = Path(output_dir) / classifier_name
        suffix = classifier_config['suffix']
        
        if is_lingua:
            input_files = parsed_files
            input_source = "parsed/"
        else:
            use_lingua = classifier_config.get('use_lingua', global_config['use_lingua'])
            supported_languages = classifier_config.get('supported_languages', None)

            if not use_lingua or not supported_languages:
                input_files = parsed_files
                input_source = "parsed/"
            else:
                # Use lingua files - look them up based on parsed file list
                lingua_config = config.get('lingua', {})
                lingua_suffix = lingua_config.get('suffix', '_lingua')
                lingua_output_dir = Path(output_dir) / 'lingua'
                input_files = []
                for file_path, file_id, data_type in parsed_files:
                    lingua_file = lingua_output_dir / data_type / f"{file_id}{lingua_suffix}.{ext}"
                    if lingua_file.exists():
                        input_files.append((str(lingua_file), file_id, data_type))
                input_source = "output/lingua/"
        
        files_for_classifier = []
        skipped_count = 0
        for file_path, file_id, data_type in input_files:
            output_path = classifier_output_dir / data_type / f"{file_id}{suffix}.{ext}"
            if output_path.exists():
                skipped_count += 1
            else:
                files_for_classifier.append((file_path, file_id, data_type))
        
        if not files_for_classifier:
            print(f"[sdp] {classifier_name}: No new files (skipped {skipped_count})")
            continue
        
        print(f"[sdp] {classifier_name}: {len(files_for_classifier)} files ({skipped_count} skipped)")
        
        if is_lingua:
            languages = classifier_config['languages']
            workers = classifier_config['workers']
            file_workers = classifier_config['file_workers']
            print(f"[sdp] Lingua: {len(languages)} languages, {workers} workers, {file_workers} file_workers")
            
            from ..classifiers import lingua as classifier_module
            
            if file_workers > 1 and len(files_for_classifier) > 1:
                workers_per_file = max(1, workers // file_workers)
                print(f"[sdp] Parallel: {file_workers} files × {workers_per_file} threads")
                
                worker_args = []
                for file_path, file_id, data_type in files_for_classifier:
                    output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.{ext}")
                    worker_args.append((file_path, output_csv, data_type, classifier_name, classifier_config, workers_per_file))
                
                with ProcessPoolExecutor(max_workers=file_workers) as executor:
                    results = list(executor.map(_process_lingua_worker, worker_args))
                
                for file_id, success, error_msg in results:
                    if success:
                        success_count += 1
                    else:
                        print(f"[sdp] Lingua ERROR {file_id}: {error_msg}")
                        fail_count += 1
            else:
                for file_path, file_id, data_type in files_for_classifier:
                    try:
                        output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.{ext}")
                        classifier_module.process_csv(
                            input_csv=file_path,
                            output_csv=output_csv,
                            data_type=data_type,
                            config=classifier_config
                        )
                        success_count += 1
                    except Exception as e:
                        print(f"[sdp] Lingua ERROR {file_id}: {e}")
                        fail_count += 1
        else:
            from ..classifiers.base import get_classifier
            
            gpu_ids = classifier_config.get('gpu_ids', global_config['gpu_ids'])
            model_id = classifier_config['model']
            file_workers = classifier_config.get('file_workers', global_config['file_workers'])
            
            print(f"[sdp] {classifier_name}: Model {model_id}, GPUs {gpu_ids}")
            
            if file_workers > 1 and len(files_for_classifier) > 1:
                actual_workers = min(file_workers, len(files_for_classifier))
                
                worker_batches: List[List[Tuple[str, str, str]]] = [[] for _ in range(actual_workers)]
                for idx, (file_path, file_id, data_type) in enumerate(files_for_classifier):
                    output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.{ext}")
                    worker_idx = idx % actual_workers
                    worker_batches[worker_idx].append((file_path, output_csv, data_type))
                
                worker_args = [
                    (batch, classifier_name, classifier_config, global_config, i)
                    for i, batch in enumerate(worker_batches)
                ]
                
                with ProcessPoolExecutor(max_workers=actual_workers) as executor:
                    batch_results = list(executor.map(_process_transformer_batch, worker_args))
                
                for results in batch_results:
                    for file_id, success, error_msg in results:
                        if success:
                            success_count += 1
                        else:
                            print(f"[sdp] {classifier_name} ERROR {file_id}: {error_msg}")
                            fail_count += 1
            else:
                classifier_instance = get_classifier(classifier_name, classifier_config, global_config)
                
                if classifier_instance is None:
                    print(f"[sdp] {classifier_name}: Could not create classifier")
                    fail_count += len(files_for_classifier)
                    continue
                
                for file_path, file_id, data_type in files_for_classifier:
                    try:
                        output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.{ext}")
                        classifier_instance.process_csv(
                            input_csv=file_path,
                            output_csv=output_csv,
                            data_type=data_type,
                            config=classifier_config
                        )
                        success_count += 1
                    except Exception as e:
                        print(f"[sdp] {classifier_name} ERROR {file_id}: {e}")
                        fail_count += 1
    
    total_timings['classifiers'] = time.time() - t_start
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"[sdp] Successful: {success_count}")
    print(f"[sdp] Failed: {fail_count}")
    print(f"[sdp] Timing: {total_timings['classifiers'] / 3600:.2f} hours")


def main():
    """Main entry point."""
    config_dir = "/app/config"
    
    profile = os.environ.get('PROFILE', 'lingua')
    if profile not in ('lingua', 'ml'):
        print(f"[sdp] PROFILE env var must be 'lingua' or 'ml', got: '{profile}'")
        sys.exit(1)
    
    target_classifier = None
    if len(sys.argv) > 1:
        target_classifier = sys.argv[1]
    
    config = load_config(config_dir=config_dir, profile=profile)
    watch_interval = get_required(config, 'processing', 'watch_interval')
    
    if watch_interval > 0:
        print(f"[sdp] Watch mode: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline(profile=profile, config_dir=config_dir, target_classifier=target_classifier)
            except Exception as e:
                print(f"[sdp] Pipeline error: {e}")
                print("[sdp] Will retry next interval...")
            
            print(f"\n[sdp] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline(profile=profile, config_dir=config_dir, target_classifier=target_classifier)


if __name__ == "__main__":
    main()
