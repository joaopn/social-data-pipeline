"""
Unified configuration loader for social_data_pipeline.

Supports profile-based configuration with source-specific overrides.
Each source can have per-profile override files in config/sources/<source>/.

Source overrides are loaded from config/sources/<source>/<profile_key>.yaml
and scoped by filename key (same as legacy user.yaml):
    parse.yaml:
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16

Legacy user.yaml overrides in config/<profile>/user.yaml are still supported
as a fallback when no source is specified.

List values in overrides fully replace base values (no merging).

No hardcoded defaults - missing required config values will raise errors.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from copy import deepcopy


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


def deep_merge(base: Dict, override: Dict, replace_lists: bool = True) -> Dict:
    """
    Deep merge two dictionaries. Override values take precedence.
    
    Args:
        base: Base dictionary
        override: Dictionary with override values
        replace_lists: If True, lists in override fully replace base lists.
                      If False, lists would be merged (not recommended for config).
        
    Returns:
        Merged dictionary (new copy, originals unchanged)
    """
    result = deepcopy(base)
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value, replace_lists)
        elif replace_lists and isinstance(value, list):
            # Lists fully replace, not merge
            result[key] = deepcopy(value)
        else:
            result[key] = deepcopy(value)
    
    return result


def load_yaml_file(file_path: Path) -> Optional[Dict]:
    """
    Load a single YAML file.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Parsed YAML content, or None if file doesn't exist
        
    Raises:
        ConfigurationError: If file exists but cannot be parsed
    """
    if not file_path.exists():
        return None
    
    with open(file_path, 'r') as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Failed to parse {file_path}: {e}")


def get_config_key(filename: str) -> str:
    """
    Get the user.yaml key for a config filename.
    
    Strips the .yaml extension to get the key name.
    e.g., 'pipeline.yaml' -> 'pipeline'
         'gpu_classifiers.yaml' -> 'gpu_classifiers'
    """
    return filename.replace('.yaml', '')


def load_profile_config(
    profile: str,
    config_dir: str = "/app/config",
    source: str = None,
    quiet: bool = False
) -> Dict[str, Any]:
    """
    Load configuration for a profile with source-specific overrides.

    Loads all base config files for the profile, then applies overrides.

    Override resolution order:
        1. If source is provided: config/sources/<source>/<profile_key>.yaml
        2. Fallback: config/<profile_folder>/user.yaml (legacy)

    Override structure (scoped by config filename key):
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16
        gpu_classifiers:    # Overrides gpu_classifiers.yaml
            batch_size: 1000000

    List values in overrides fully replace base values (no merging).

    Args:
        profile: Profile name ('parse', 'lingua', 'ml', 'postgres_ingest', 'postgres_ml')
        config_dir: Base configuration directory
        source: Source name. If provided, loads overrides from config/sources/<source>/
        quiet: If True, suppress informational output

    Returns:
        Merged configuration dictionary

    Raises:
        ConfigurationError: If required config files are missing
    """
    # Map profile names to config folder names
    profile_folders = {
        'postgres_ingest': 'postgres',
        'mongo_ingest': 'mongo',
        'sr_ingest': 'sr',
        'sr_ml': 'sr_ml',
    }
    folder_name = profile_folders.get(profile, profile)
    config_path = Path(config_dir) / folder_name

    if not config_path.exists():
        raise ConfigurationError(f"Config directory not found: {config_path}")

    # Define base config files per profile
    profile_configs = {
        'parse': ['pipeline.yaml'],
        'lingua': ['pipeline.yaml', 'cpu_classifiers.yaml'],
        'ml': ['pipeline.yaml', 'gpu_classifiers.yaml'],
        'postgres_ingest': ['pipeline.yaml'],
        'postgres_ml': ['pipeline.yaml', 'services.yaml'],
        'mongo_ingest': ['pipeline.yaml'],
        'sr_ingest': ['pipeline.yaml'],
        'sr_ml': ['pipeline.yaml', 'services.yaml'],
    }

    if profile not in profile_configs:
        raise ConfigurationError(f"Unknown profile: {profile}")

    # Map profiles to source override filenames
    source_override_files = {
        'parse': 'parse.yaml',
        'lingua': 'lingua.yaml',
        'ml': 'ml.yaml',
        'postgres_ingest': 'postgres.yaml',
        'postgres_ml': 'postgres_ml.yaml',
        'mongo_ingest': 'mongo.yaml',
        'sr_ingest': 'starrocks.yaml',
        'sr_ml': 'sr_ml.yaml',
    }

    # Try source-specific override first, then fall back to legacy user.yaml
    user_config = None
    override_label = None

    if source:
        source_override_path = Path(config_dir) / "sources" / source / source_override_files[profile]
        user_config = load_yaml_file(source_override_path)
        if user_config is not None:
            override_label = f"sources/{source}/{source_override_files[profile]}"

    if user_config is None:
        # Legacy fallback: config/<profile_folder>/user.yaml
        user_config_path = config_path / 'user.yaml'
        user_config = load_yaml_file(user_config_path)
        if user_config is not None:
            override_label = f"{folder_name}/user.yaml"

    has_user_config = user_config is not None

    if has_user_config and not quiet:
        print(f"[sdp] Using override: {override_label}")

    # Load each base config file and apply user overrides
    merged_config = {}
    for config_file in profile_configs[profile]:
        file_path = config_path / config_file
        config = load_yaml_file(file_path)

        if config is None:
            raise ConfigurationError(f"Required config file not found: {file_path}")

        # Apply user overrides for this specific file
        if has_user_config:
            config_key = get_config_key(config_file)
            if config_key in user_config:
                config = deep_merge(config, user_config[config_key])

        # Merge into final config
        merged_config = deep_merge(merged_config, config)

    return merged_config


def get_required(config: Dict, *keys: str, error_msg: str = None) -> Any:
    """
    Get a required configuration value, raising error if missing.
    
    Args:
        config: Configuration dictionary
        *keys: Path of keys to traverse (e.g., 'processing', 'data_types')
        error_msg: Custom error message (optional)
        
    Returns:
        Configuration value
        
    Raises:
        ConfigurationError: If value is missing
    """
    value = config
    path = []
    
    for key in keys:
        path.append(key)
        if not isinstance(value, dict) or key not in value:
            key_path = '.'.join(path)
            msg = error_msg or f"Required configuration missing: {key_path}"
            raise ConfigurationError(msg)
        value = value[key]
    
    return value


def get_optional(config: Dict, *keys: str, default: Any = None) -> Any:
    """
    Get an optional configuration value with a default.

    Args:
        config: Configuration dictionary
        *keys: Path of keys to traverse
        default: Default value if not found

    Returns:
        Configuration value or default
    """
    value = config

    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]

    return value


def validate_processing_config(config: Dict, profile: str) -> None:
    """
    Validate that required processing config exists.
    
    Args:
        config: Configuration dictionary
        profile: Profile name for error messages
        
    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['data_types']
    
    for key in required_keys:
        if 'processing' not in config or key not in config['processing']:
            raise ConfigurationError(
                f"[{profile}] Required config missing: processing.{key}"
            )


def validate_database_config(config: Dict) -> None:
    """
    Validate that required database config exists for postgres profiles.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['host', 'port', 'name', 'schema', 'user']
    
    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[postgres] Required config missing: database.{key}"
            )


def validate_mongo_config(config: Dict) -> None:
    """
    Validate that required MongoDB config exists for mongo_ingest profile.

    Args:
        config: Configuration dictionary

    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['host', 'port']

    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[mongo] Required config missing: database.{key}"
            )


# StarRocks-supported table compression codecs (used by CREATE TABLE
# PROPERTIES). LZ4 is StarRocks' own default when no codec is specified.
VALID_STARROCKS_COMPRESSION = {'LZ4', 'ZSTD', 'ZLIB', 'SNAPPY'}


def validate_starrocks_config(config: Dict) -> None:
    """
    Validate that required StarRocks config exists for sr_ingest profile.

    Args:
        config: Configuration dictionary

    Raises:
        ConfigurationError: If required config is missing or invalid
    """
    required_keys = ['host', 'port', 'user']

    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[starrocks] Required config missing: database.{key}"
            )

    compression = config['database'].get('compression')
    if compression is not None and compression not in VALID_STARROCKS_COMPRESSION:
        raise ConfigurationError(
            f"[starrocks] Invalid compression '{compression}'. "
            f"Allowed: {', '.join(sorted(VALID_STARROCKS_COMPRESSION))}"
        )


def normalize_classifier_entries(entries: List, data_types: List[str], profile: str) -> List[Dict]:
    """Normalize a classifier list to [{name, data_types}].

    Accepts two forms per entry:
      - 'name'                              -> runs on all data_types
      - {'name': ..., 'data_types': [...]}  -> runs only on listed data_types

    data_types=None in the normalized form means 'all'. Unknown data_types
    in a scoped entry raise ConfigurationError.
    """
    normalized = []
    known = set(data_types)
    for entry in entries:
        if isinstance(entry, str):
            normalized.append({'name': entry, 'data_types': None})
        elif isinstance(entry, dict):
            name = entry.get('name')
            if not name:
                raise ConfigurationError(
                    f"[{profile}] Classifier entry missing 'name': {entry}"
                )
            scope = entry.get('data_types')
            if scope is not None:
                if not isinstance(scope, list) or not scope:
                    raise ConfigurationError(
                        f"[{profile}] Classifier '{name}' data_types must be a non-empty list"
                    )
                unknown = [dt for dt in scope if dt not in known]
                if unknown:
                    raise ConfigurationError(
                        f"[{profile}] Classifier '{name}' scoped to unknown data_types {unknown} "
                        f"(known: {sorted(known)})"
                    )
            normalized.append({'name': name, 'data_types': scope})
        else:
            raise ConfigurationError(
                f"[{profile}] Invalid classifier entry (must be string or dict): {entry!r}"
            )
    return normalized


def load_classifier_scopes(
    config_dir: str,
    source: str,
    profile: str = 'ml',
) -> List[Dict]:
    """Resolve the source's classifier scopes from its ml/lingua profile config.

    Returns list of dicts: [{name, suffix, data_types}, ...]. data_types is
    None when the classifier should run on every configured data_type.

    The 'what' (which classifiers to run, with optional data_types scope)
    comes from the source's pipeline.gpu_classifiers (or cpu_classifiers
    for the lingua profile). The 'how' (suffix and other per-classifier
    settings) comes from the same merged profile config — i.e. the source
    can override any classifier setting in its own ml.yaml/lingua.yaml.

    Args:
        config_dir: Base configuration directory
        source: Source name to load overrides for
        profile: 'ml' or 'lingua'

    Raises:
        ConfigurationError: If the profile config is missing required keys
            or a referenced classifier lacks a 'suffix'.
    """
    if profile not in ('ml', 'lingua'):
        raise ConfigurationError(
            f"load_classifier_scopes only supports 'ml' or 'lingua', got '{profile}'"
        )
    cfg = load_profile_config(profile, config_dir, source=source, quiet=True)
    data_types = get_required(cfg, 'processing', 'data_types')
    list_key = 'cpu_classifiers' if profile == 'lingua' else 'gpu_classifiers'
    raw = get_required(cfg, list_key)

    out = []
    for entry in normalize_classifier_entries(raw, data_types, profile):
        name = entry['name']
        cls_cfg = cfg.get(name)
        if not isinstance(cls_cfg, dict) or 'suffix' not in cls_cfg:
            raise ConfigurationError(
                f"[{profile}] Classifier '{name}' missing 'suffix'. "
                f"Define it in config/{profile}/ or override in config/sources/{source}/{profile}.yaml."
            )
        out.append({
            'name': name,
            'suffix': cls_cfg['suffix'],
            'data_types': entry['data_types'],
        })
    return out


def resolve_classifier_runs(
    config_dir: str,
    source: str,
    ingestion_overrides: Dict,
    prefer_lingua: bool,
) -> List[Dict]:
    """Build the ordered list of classifier ingestion runs for a source.

    Used by postgres_ml and sr_ml — both compose runs the same way:
      - lingua profile (cpu_classifiers list) -> lingua classifier (only
        when prefer_lingua=False; otherwise lingua data is already in the
        main table via the parent ingestion profile).
      - ml profile (gpu_classifiers list) -> non-lingua classifiers.
      - ingestion_overrides (services.yaml + source override): per-classifier
        {enabled, source_dir, source_dir_ingest, column_overrides}.
        enabled=False skips the classifier at ingest time without affecting
        the ml profile.

    Each run is a dict: {name, suffix, source_dir, data_types,
    column_overrides}. data_types=None means 'all configured data_types'.
    """
    runs = []

    if not prefer_lingua:
        try:
            lingua_scopes = load_classifier_scopes(config_dir, source=source, profile='lingua')
        except ConfigurationError:
            lingua_scopes = []
        for scope in lingua_scopes:
            if scope['name'] != 'lingua':
                continue
            ovr = ingestion_overrides.get('lingua', {}) or {}
            if not ovr.get('enabled', True):
                continue
            runs.append({
                'name': 'lingua',
                'suffix': scope['suffix'],
                'source_dir': ovr.get('source_dir_ingest', 'lingua_ingest'),
                'data_types': scope['data_types'],
                'column_overrides': ovr.get('column_overrides', {}),
            })

    try:
        ml_scopes = load_classifier_scopes(config_dir, source=source, profile='ml')
    except ConfigurationError:
        ml_scopes = []
    for scope in ml_scopes:
        ovr = ingestion_overrides.get(scope['name'], {}) or {}
        if not ovr.get('enabled', True):
            print(f"[sdp] {scope['name']}: Skipped (enabled=false in ingestion overrides)")
            continue
        runs.append({
            'name': scope['name'],
            'suffix': scope['suffix'],
            'source_dir': ovr.get('source_dir', scope['name']),
            'data_types': scope['data_types'],
            'column_overrides': ovr.get('column_overrides', {}),
        })

    return runs


#: StarRocks index mechanisms. A plain YAML list means BITMAP — the historical
#: meaning, and configs in the wild rely on it.
SR_INDEX_TYPES = ('bitmap', 'bloomfilter')

#: Column types StarRocks accepts for bloom filter indexes. FLOAT, DOUBLE,
#: TINYINT, BOOLEAN and DECIMAL are NOT supported — notably `lang_prob` (FLOAT)
#: in every lingua classifier table.
#:
#: Lives here rather than in db/starrocks/ingest.py because sdp.py runs on the
#: host, where mysql-connector is not installed; importing the driver module
#: just to read a constant would break `sdp db create-indexes`.
BLOOM_FILTER_TYPES = frozenset({
    'smallint', 'int', 'bigint', 'largeint',
    'char', 'varchar', 'string',
    'date', 'datetime',
})

#: Column types StarRocks accepts for BITMAP indexes. Wider than the bloom
#: filter set (TINYINT, BOOLEAN and DECIMAL are allowed here) but it also
#: excludes FLOAT and DOUBLE — so a float column can carry NEITHER index type
#: and must be dropped from an index plan rather than rerouted.
BITMAP_INDEX_TYPES = frozenset({
    'tinyint', 'smallint', 'int', 'bigint', 'largeint', 'decimal', 'boolean',
    'char', 'varchar', 'string',
    'date', 'datetime', 'hll',
})


def normalize_sr_column_type(data_type: str) -> str:
    """Reduce an information_schema data_type to its bare type name.

    Handles parameterized spellings (`varchar(65533)`) and the decimal variants
    StarRocks reports (`decimal64`, `decimal128`).
    """
    base = (data_type or '').strip().lower().split('(')[0].strip()
    if base.startswith('decimal'):
        return 'decimal'
    return base


def sr_index_types_for_column(data_type: str) -> Dict[str, bool]:
    """Which StarRocks index mechanisms a column type supports.

    An unknown/empty type is treated as supporting both: the type list is a
    guard against known-bad builds, not an allowlist, and StarRocks itself
    rejects anything genuinely unsupported.
    """
    base = normalize_sr_column_type(data_type)
    if not base:
        return {'bitmap': True, 'bloomfilter': True}
    known = base in BITMAP_INDEX_TYPES or base in BLOOM_FILTER_TYPES
    if not known and base not in ('float', 'double'):
        return {'bitmap': True, 'bloomfilter': True}
    return {
        'bitmap': base in BITMAP_INDEX_TYPES,
        'bloomfilter': base in BLOOM_FILTER_TYPES,
    }


def normalize_index_spec(spec, where: str = 'index spec') -> Dict[str, List[str]]:
    """
    Normalize one table's StarRocks index spec to {'bitmap': [...], 'bloomfilter': [...]}.

    Accepts either shape:
        [a, b]                              -> all BITMAP (legacy; never reinterpreted)
        {bitmap: [a], bloomfilter: [b]}     -> explicit, either sub-key optional

    Args:
        spec: List, dict, or None
        where: Context for error messages (e.g. "sr_ml_indexes['comments_lingua']")

    Raises:
        ConfigurationError: On an unknown sub-key (catches `bloom_filter` /
            `bloomFilter` typos that would otherwise silently index nothing) or
            a non-list value for a known sub-key.
    """
    out = {t: [] for t in SR_INDEX_TYPES}

    if spec is None:
        return out

    if isinstance(spec, list):
        out['bitmap'] = list(spec)
        return out

    if isinstance(spec, dict):
        unknown = [k for k in spec if k not in SR_INDEX_TYPES]
        if unknown:
            raise ConfigurationError(
                f"{where}: unknown index type(s) {unknown}. "
                f"Valid types: {list(SR_INDEX_TYPES)}. A plain list means bitmap."
            )
        for index_type in SR_INDEX_TYPES:
            fields = spec.get(index_type) or []
            if not isinstance(fields, list):
                raise ConfigurationError(
                    f"{where}: '{index_type}' must be a list of column names, "
                    f"got {type(fields).__name__}."
                )
            # Dedupe within a type; a column may legitimately appear under both
            # types, since bitmap and bloom filter are independent mechanisms.
            seen = set()
            out[index_type] = [f for f in fields if not (f in seen or seen.add(f))]
        return out

    raise ConfigurationError(
        f"{where}: expected a list of column names or a "
        f"{{bitmap: [...], bloomfilter: [...]}} mapping, got {type(spec).__name__}."
    )


def build_sr_index_plan(
    indexed_tables,
    profile_config: Dict,
    platform_config: Dict,
    fallback_keys,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Build the {table: {'bitmap': [...], 'bloomfilter': [...]}} plan for StarRocks.

    Args:
        indexed_tables: Iterable of table names (sr_ingest passes data types,
            sr_ml passes classifier table names — this is name-agnostic)
        profile_config: Merged profile config (sr_ingest / sr_ml)
        platform_config: Source platform config
        fallback_keys: Config keys to try in order. sr_ingest passes
            ('sr_indexes', 'indexes') to preserve its historical fallback;
            sr_ml passes ('sr_ml_indexes',) — no fallback, since base index
            fields name columns classifier tables do not have.

    Returns:
        Tables in sorted order, excluding those with no fields of either type.

    The `indexes` key is read list-only: it is PostgreSQL's, and postgres_ingest
    would silently try to index columns named 'bitmap'/'bloomfilter' if a nested
    dict were written there. A nested value under `indexes` raises instead.
    """
    index_config = {}
    source_key = None
    for key in fallback_keys:
        index_config = profile_config.get(key) or platform_config.get(key) or {}
        if index_config:
            source_key = key
            break

    plan = {}
    for table in sorted(indexed_tables):
        spec = index_config.get(table)
        if spec is None:
            continue
        if source_key == 'indexes' and not isinstance(spec, list):
            raise ConfigurationError(
                f"indexes['{table}'] must be a plain list — it is also read by "
                f"postgres_ingest. Put per-type StarRocks indexes in sr_indexes."
            )
        normalized = normalize_index_spec(spec, where=f"{source_key}['{table}']")
        if any(normalized[t] for t in SR_INDEX_TYPES):
            plan[table] = normalized
    return plan


def build_ml_index_plan(
    indexed_tables,
    profile_config: Dict,
    platform_config: Dict,
    key: str,
) -> Dict[str, List[str]]:
    """
    Build the {table: [fields]} index plan for classifier tables.

    Args:
        indexed_tables: Iterable of classifier table names (postgres_ml passes a
            table -> data_type dict, sr_ml passes a set; only the names are used)
        profile_config: Merged profile config (postgres_ml / sr_ml)
        platform_config: Source platform config
        key: 'ml_indexes' (postgres_ml) or 'sr_ml_indexes' (sr_ml)

    Returns:
        Tables in sorted order, excluding any with no configured fields.

    The profile-level map wins over the platform one. There is deliberately no
    fallback to the base `indexes` / `sr_indexes` maps: those name base-table
    columns (author, subreddit, domain) that classifier tables do not have.
    """
    index_config = profile_config.get(key) or platform_config.get(key, {}) or {}

    plan = {}
    for table in sorted(indexed_tables):
        fields = index_config.get(table, [])
        if not fields:
            continue
        if not isinstance(fields, list):
            # PostgreSQL has one index mechanism, so the per-type StarRocks
            # shape is meaningless here. Without this guard list(dict) yields
            # ['bitmap', 'bloomfilter'] and postgres_ml indexes columns by
            # those literal names, with no error.
            raise ConfigurationError(
                f"{key}['{table}'] must be a plain list of column names for "
                f"PostgreSQL, got {type(fields).__name__}. Per-type index specs "
                f"are StarRocks-only (sr_indexes / sr_ml_indexes)."
            )
        plan[table] = list(fields)
    return plan


def validate_classifier_config(config: Dict, classifier_name: str, profile: str) -> None:
    """
    Validate that required classifier config exists.
    
    Args:
        config: Classifier configuration dictionary
        classifier_name: Name of the classifier
        profile: Profile name for error messages
        
    Raises:
        ConfigurationError: If required config is missing
    """
    if profile == 'lingua' and classifier_name == 'lingua':
        required_keys = ['suffix', 'languages']
    else:
        # GPU classifiers
        required_keys = ['suffix', 'model']
    
    for key in required_keys:
        if key not in config:
            raise ConfigurationError(
                f"[{profile}] Required config missing for {classifier_name}: {key}"
            )


def load_platform_config(
    config_dir: str = "/app/config",
    platform: str = None,
    source: str = None
) -> Dict[str, Any]:
    """
    Load platform-specific configuration from config/sources/<source>/platform.yaml.

    Args:
        config_dir: Base configuration directory
        platform: Platform name (unused, kept for signature compatibility)
        source: Source name. Loads from config/sources/<source>/platform.yaml

    Returns:
        Platform configuration dictionary

    Raises:
        ConfigurationError: If config file is not found
    """
    if not source:
        source = os.environ.get('SOURCE') or os.environ.get('PLATFORM', 'reddit')

    source_path = Path(config_dir) / "sources" / source / "platform.yaml"
    config = load_yaml_file(source_path)
    if config is not None:
        return config

    raise ConfigurationError(
        f"Platform config not found: {source_path}\n"
        f"Run 'python sdp.py source add {source}' to configure this source."
    )


def get_platform_fields(platform_config: Dict, data_type: str) -> List[str]:
    """
    Get the field list for a data type from platform config.

    Args:
        platform_config: Loaded platform configuration
        data_type: Data type key (e.g., 'submissions', 'comments')

    Returns:
        List of field names

    Raises:
        ConfigurationError: If no fields are configured for the data type
    """
    fields = platform_config.get('fields', {}).get(data_type, [])
    if not fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    return fields


def get_platform_field_types(platform_config: Dict) -> Dict[str, Any]:
    """
    Get the field type definitions from platform config.

    Args:
        platform_config: Loaded platform configuration

    Returns:
        Dictionary mapping field names to type definitions

    Raises:
        ConfigurationError: If no field_types are configured
    """
    field_types = platform_config.get('field_types', {})
    if not field_types:
        raise ConfigurationError("No field_types configured in platform config")
    return field_types


def load_db_config(
    db_type: str,
    config_dir: str = "/app/config"
) -> Optional[Dict[str, Any]]:
    """
    Load global database configuration.

    Loads from config/db/<db_type>.yaml (e.g., config/db/postgres.yaml).

    Args:
        db_type: Database type ('postgres' or 'mongo')
        config_dir: Base configuration directory

    Returns:
        Database configuration dictionary, or None if not found
    """
    config_path = Path(config_dir) / "db" / f"{db_type}.yaml"
    return load_yaml_file(config_path)


def apply_env_overrides(config: Dict, profile: str) -> Dict:
    """
    Apply environment variable overrides to configuration.
    
    For postgres profiles, environment variables override database settings.
    
    Args:
        config: Configuration dictionary
        profile: Profile name
        
    Returns:
        Configuration with env overrides applied
    """
    result = deepcopy(config)
    
    if profile in ('postgres_ingest', 'postgres_ml'):
        if 'database' not in result:
            result['database'] = {}

        if 'POSTGRES_PORT' in os.environ:
            result['database']['port'] = int(os.environ['POSTGRES_PORT'])
        if 'DB_NAME' in os.environ:
            result['database']['name'] = os.environ['DB_NAME']
        if 'DB_SCHEMA' in os.environ:
            result['database']['schema'] = os.environ['DB_SCHEMA']
        if os.environ.get('POSTGRES_PASSWORD'):
            result['database']['password'] = os.environ['POSTGRES_PASSWORD']

    if profile == 'mongo_ingest':
        if 'database' not in result:
            result['database'] = {}

        if 'MONGO_PORT' in os.environ:
            result['database']['port'] = int(os.environ['MONGO_PORT'])
        if os.environ.get('MONGO_ADMIN_USER'):
            result['database']['user'] = os.environ['MONGO_ADMIN_USER']
        if os.environ.get('MONGO_ADMIN_PASSWORD'):
            result['database']['password'] = os.environ['MONGO_ADMIN_PASSWORD']

    if profile in ('sr_ingest', 'sr_ml'):
        if 'database' not in result:
            result['database'] = {}

        if 'STARROCKS_PORT' in os.environ:
            result['database']['port'] = int(os.environ['STARROCKS_PORT'])
        if 'STARROCKS_FE_HTTP_PORT' in os.environ:
            result['database']['fe_http_port'] = int(os.environ['STARROCKS_FE_HTTP_PORT'])
        if os.environ.get('STARROCKS_ROOT_PASSWORD'):
            result['database']['password'] = os.environ['STARROCKS_ROOT_PASSWORD']

    return result
