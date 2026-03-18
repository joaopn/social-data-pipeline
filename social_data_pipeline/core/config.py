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

    return result
