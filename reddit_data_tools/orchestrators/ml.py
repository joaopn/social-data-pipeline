"""
ML profile orchestrator for reddit_data_tools.
Handles running classifiers (Lingua for CPU, transformers for GPU).
Expects CSV files to already exist (run parse profile first).
"""

import os
import re
import sys
import time
import yaml
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from ..core.state import PipelineState


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


def load_config(config_dir: str = "/app/config", profile: str = "ml_cpu", quiet: bool = False) -> Tuple[Dict, Dict]:
    """Load pipeline and classifier configuration for the given profile."""
    config_dir = Path(config_dir)
    
    # Determine config paths based on profile
    if profile == "ml_cpu":
        pipeline_path = config_dir / "ml_cpu" / "pipeline.yaml"
        classifiers_path = config_dir / "ml_cpu" / "cpu_classifiers.yaml"
    else:  # ml (GPU)
        pipeline_path = config_dir / "ml" / "pipeline.yaml"
        classifiers_path = config_dir / "ml" / "gpu_classifiers.yaml"
    
    # Load pipeline config with .local override
    local_pipeline = pipeline_path.with_suffix('.local.yaml')
    if local_pipeline.exists():
        pipeline_path = local_pipeline
        if not quiet:
            print(f"[CONFIG] Using local override: {local_pipeline.name}")
    
    with open(pipeline_path, 'r') as f:
        pipeline_config = yaml.safe_load(f)
    
    # Load classifiers config with .local override
    local_classifiers = classifiers_path.with_suffix('.local.yaml')
    if local_classifiers.exists():
        classifiers_path = local_classifiers
        if not quiet:
            print(f"[CONFIG] Using local override: {local_classifiers.name}")
    
    with open(classifiers_path, 'r') as f:
        classifiers_config = yaml.safe_load(f) or {}
    
    return pipeline_config, classifiers_config


def detect_parsed_csv_files(csv_dir: str, data_types: List[str]) -> List[Tuple[str, str, str]]:
    """Detect parsed CSV files in the CSV directory."""
    csv_base = Path(csv_dir)
    files = []
    
    patterns = {
        'submissions': re.compile(r'^RS_(\d{4}-\d{2})\.csv$'),
        'comments': re.compile(r'^RC_(\d{4}-\d{2})\.csv$')
    }
    
    for data_type in data_types:
        if data_type not in patterns:
            continue
            
        type_dir = csv_base / data_type
        if not type_dir.is_dir():
            continue
            
        for filepath in type_dir.glob("*.csv"):
            filename = filepath.name
            match = patterns[data_type].match(filename)
            if match:
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {'submissions': 0, 'comments': 1}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def run_pipeline(profile: str = "ml_cpu", target_classifier: Optional[str] = None):
    """
    Run the ML classifier pipeline.
    
    Args:
        profile: 'ml_cpu' for Lingua only, 'ml' for GPU transformers
        target_classifier: If set, run only up to this classifier
    """
    # Load configuration
    pipeline_config, classifiers_config = load_config(profile=profile)
    
    proc_config = pipeline_config['processing']
    data_types = proc_config['data_types']
    
    # Get list of classifiers to run
    if profile == "ml_cpu":
        classifiers_to_run = pipeline_config.get('cpu_classifiers', [])
    else:
        classifiers_to_run = pipeline_config.get('gpu_classifiers', [])
    
    # Optional: CLASSIFIER env var to run only a single classifier
    single_classifier = os.environ.get('CLASSIFIER', '')
    if single_classifier:
        if single_classifier not in classifiers_to_run:
            print(f"[ERROR] CLASSIFIER='{single_classifier}' not in classifiers list: {classifiers_to_run}")
            sys.exit(1)
        classifiers_to_run = [single_classifier]
        print(f"[CONFIG] Running single classifier: {single_classifier}")
    
    # Extract global settings
    global_text_columns = classifiers_config.pop('text_columns', {})
    global_remove_strings = classifiers_config.pop('remove_strings', [])
    global_remove_patterns = classifiers_config.pop('remove_patterns', [])
    global_batch_size = classifiers_config.pop('batch_size', 2_000_000)
    global_classifier_batch_size = classifiers_config.pop('classifier_batch_size', 32)
    global_gpu_ids = classifiers_config.pop('gpu_ids', [0])
    global_file_workers = classifiers_config.pop('file_workers', 1)
    global_use_lingua = classifiers_config.pop('use_lingua', True)
    global_lang2_fallback = classifiers_config.pop('lang2_fallback', False)
    global_min_tokens = classifiers_config.pop('min_tokens', 0)
    global_tokenize_workers = classifiers_config.pop('tokenize_workers', 0)
    global_minimal_fields = classifiers_config.pop('minimal_fields', False)
    
    global_config = {
        'text_columns': global_text_columns,
        'remove_strings': global_remove_strings,
        'remove_patterns': global_remove_patterns,
        'batch_size': global_batch_size,
        'classifier_batch_size': global_classifier_batch_size,
        'gpu_ids': global_gpu_ids,
        'file_workers': global_file_workers,
        'use_lingua': global_use_lingua,
        'lang2_fallback': global_lang2_fallback,
        'min_tokens': global_min_tokens,
        'tokenize_workers': global_tokenize_workers,
        'minimal_fields': global_minimal_fields,
    }
    
    # Build list of classifiers with merged config
    enabled_classifiers = []
    for name in classifiers_to_run:
        if name not in classifiers_config:
            print(f"[WARNING] Classifier '{name}' not configured, skipping")
            continue
        
        cfg = classifiers_config[name]
        if not isinstance(cfg, dict):
            cfg = {}
        
        merged_cfg = {**global_config, **cfg}
        enabled_classifiers.append((name, merged_cfg))
        
        if target_classifier and name == target_classifier:
            break
    
    print(f"[CONFIG] Profile: {profile}")
    print(f"[CONFIG] Data types: {data_types}")
    print(f"[CONFIG] Classifiers: {[name for name, _ in enabled_classifiers]}")
    
    if profile == "ml":
        print(f"[CONFIG] GPUs: {global_gpu_ids}, file_workers: {global_file_workers}")
    
    # Initialize state manager
    state_file = f"/data/output/{profile}_state.json"
    state = PipelineState(state_file=state_file)
    
    stats = state.get_stats()
    print(f"[STATE] Previously processed: {stats['processed_count']} files")
    
    # Paths
    csv_dir = "/data/csv"
    output_dir = "/data/output"
    
    # Detect input CSV files
    print("\n" + "="*60)
    print("INPUT DETECTION")
    print("="*60)
    
    parsed_csv_files = detect_parsed_csv_files(csv_dir, data_types)
    print(f"[DETECT] Found {len(parsed_csv_files)} CSV files in csv directory")
    
    if not parsed_csv_files:
        print("\n[PIPELINE] No input files found. Run 'parse' profile first.")
        return
    
    if not enabled_classifiers:
        print("\n[PIPELINE] No classifiers enabled. Exiting.")
        return
    
    total_timings = {'classifiers': 0.0}
    success_count = 0
    fail_count = 0
    
    # Run classifiers
    print("\n" + "="*60)
    print("RUNNING CLASSIFIERS")
    print("="*60)
    
    t_start = time.time()
    
    for classifier_name, classifier_config in enabled_classifiers:
        is_lingua = classifier_name == 'lingua'
        
        # Set RAYON_NUM_THREADS before importing Lingua
        if is_lingua:
            workers = classifier_config.get('workers', 8)
            os.environ['RAYON_NUM_THREADS'] = str(workers)
        
        classifier_output_dir = Path(output_dir) / classifier_name
        suffix = classifier_config.get('suffix', f'_{classifier_name}')
        
        # Determine input source for GPU classifiers
        use_lingua = classifier_config.get('use_lingua', global_config.get('use_lingua', True))
        supported_languages = classifier_config.get('supported_languages', None)
        
        if is_lingua or not use_lingua or not supported_languages:
            input_files = parsed_csv_files
            input_source = "csv/"
        else:
            # Read from Lingua output
            lingua_suffix = classifiers_config.get('lingua', {}).get('suffix', '_lingua')
            lingua_output_dir = Path(output_dir) / 'lingua'
            input_files = []
            for csv_path, file_id, data_type in parsed_csv_files:
                lingua_file = lingua_output_dir / data_type / f"{file_id}{lingua_suffix}.csv"
                if lingua_file.exists():
                    input_files.append((str(lingua_file), file_id, data_type))
            input_source = "output/lingua/"
        
        # Find files to process (skip existing outputs)
        files_for_classifier = []
        skipped_count = 0
        for csv_path, file_id, data_type in input_files:
            output_path = classifier_output_dir / data_type / f"{file_id}{suffix}.csv"
            if output_path.exists():
                skipped_count += 1
            else:
                files_for_classifier.append((csv_path, file_id, data_type))
        
        if not files_for_classifier:
            print(f"[{classifier_name.upper()}] No new files to process (skipped {skipped_count})")
            continue
        
        print(f"[{classifier_name.upper()}] {len(files_for_classifier)} files to process ({skipped_count} skipped)")
        
        if is_lingua:
            # Run Lingua
            languages = classifier_config.get('languages', [])
            workers = classifier_config.get('workers', 8)
            file_workers = classifier_config.get('file_workers', 1)
            print(f"[LINGUA] Languages: {len(languages)}, workers: {workers}, file_workers: {file_workers}")
            
            from ..classifiers import lingua as classifier_module
            
            if file_workers > 1 and len(files_for_classifier) > 1:
                workers_per_file = max(1, workers // file_workers)
                print(f"[LINGUA] Parallel: {file_workers} files × {workers_per_file} threads")
                
                worker_args = []
                for csv_path, file_id, data_type in files_for_classifier:
                    output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.csv")
                    worker_args.append((csv_path, output_csv, data_type, classifier_name, classifier_config, workers_per_file))
                
                with ProcessPoolExecutor(max_workers=file_workers) as executor:
                    results = list(executor.map(_process_lingua_worker, worker_args))
                
                for file_id, success, error_msg in results:
                    if success:
                        success_count += 1
                    else:
                        print(f"[LINGUA] ERROR {file_id}: {error_msg}")
                        fail_count += 1
            else:
                for csv_path, file_id, data_type in files_for_classifier:
                    try:
                        output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.csv")
                        classifier_module.process_csv(
                            input_csv=csv_path,
                            output_csv=output_csv,
                            data_type=data_type,
                            config=classifier_config
                        )
                        success_count += 1
                    except Exception as e:
                        print(f"[LINGUA] ERROR {file_id}: {e}")
                        fail_count += 1
        else:
            # Run transformer classifier
            from ..classifiers.base import get_classifier
            
            gpu_ids = classifier_config.get('gpu_ids', global_config.get('gpu_ids', [0]))
            model_id = classifier_config.get('model', 'unknown')
            file_workers = classifier_config.get('file_workers', global_config.get('file_workers', 1))
            
            print(f"[{classifier_name.upper()}] Model: {model_id}")
            print(f"[{classifier_name.upper()}] GPUs: {gpu_ids}, file_workers: {file_workers}")
            
            if file_workers > 1 and len(files_for_classifier) > 1:
                # Parallel file processing
                actual_workers = min(file_workers, len(files_for_classifier))
                
                worker_batches: List[List[Tuple[str, str, str]]] = [[] for _ in range(actual_workers)]
                for idx, (csv_path, file_id, data_type) in enumerate(files_for_classifier):
                    output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.csv")
                    worker_idx = idx % actual_workers
                    worker_batches[worker_idx].append((csv_path, output_csv, data_type))
                
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
                            print(f"[{classifier_name.upper()}] ERROR {file_id}: {error_msg}")
                            fail_count += 1
            else:
                classifier_instance = get_classifier(classifier_name, classifier_config, global_config)
                
                if classifier_instance is None:
                    print(f"[{classifier_name.upper()}] Error: Could not create classifier instance")
                    fail_count += len(files_for_classifier)
                    continue
                
                for csv_path, file_id, data_type in files_for_classifier:
                    try:
                        output_csv = str(classifier_output_dir / data_type / f"{file_id}{suffix}.csv")
                        classifier_instance.process_csv(
                            input_csv=csv_path,
                            output_csv=output_csv,
                            data_type=data_type,
                            config=classifier_config
                        )
                        success_count += 1
                    except Exception as e:
                        print(f"[{classifier_name.upper()}] ERROR {file_id}: {e}")
                        fail_count += 1
    
    total_timings['classifiers'] = time.time() - t_start
    
    # Final summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    
    print(f"\nTiming (hours):")
    print(f"  Classifiers: {total_timings['classifiers'] / 3600:.2f}")


def main():
    """Main entry point."""
    # Determine profile from PROFILE env var
    profile = os.environ.get('PROFILE', 'ml_cpu')
    if profile not in ('ml_cpu', 'ml'):
        print(f"[ERROR] PROFILE env var must be 'ml_cpu' or 'ml', got: '{profile}'")
        sys.exit(1)
    
    target_classifier = None
    if len(sys.argv) > 1:
        target_classifier = sys.argv[1]
    
    pipeline_config, _ = load_config(profile=profile)
    watch_interval = pipeline_config.get('processing', {}).get('watch_interval', 0)
    
    if watch_interval > 0:
        print(f"[WATCH] Watch mode enabled: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline(profile=profile, target_classifier=target_classifier)
            except Exception as e:
                print(f"[WATCH] Pipeline error: {e}")
                print("[WATCH] Will retry next interval...")
            
            print(f"\n[WATCH] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline(profile=profile, target_classifier=target_classifier)


if __name__ == "__main__":
    main()
