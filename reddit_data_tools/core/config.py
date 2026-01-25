"""
Unified configuration loader for reddit_data_tools.

Supports profile-based configuration with user overrides.
Each profile can have a user.yaml that overrides base config values.
Shared configs also support a user.yaml.

User overrides are scoped by filename:
    user.yaml:
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16
        gpu_classifiers:    # Overrides gpu_classifiers.yaml
            batch_size: 1000000

List values in user.yaml fully replace base values (no merging).

No hardcoded defaults - missing required config values will raise errors.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
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
    quiet: bool = False
) -> Dict[str, Any]:
    """
    Load configuration for a profile with user.yaml overrides.
    
    Loads all base config files for the profile, then applies
    user.yaml overrides scoped by filename key.
    
    user.yaml structure:
        pipeline:           # Overrides pipeline.yaml
            processing:
                workers: 16
        gpu_classifiers:    # Overrides gpu_classifiers.yaml
            batch_size: 1000000
    
    List values in user.yaml fully replace base values (no merging).
    
    Args:
        profile: Profile name ('parse', 'ml_cpu', 'ml', 'db_pg')
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config files are missing
    """
    config_path = Path(config_dir) / profile
    
    if not config_path.exists():
        raise ConfigurationError(f"Config directory not found: {config_path}")
    
    # Define base config files per profile
    profile_configs = {
        'parse': ['pipeline.yaml'],
        'ml_cpu': ['pipeline.yaml', 'cpu_classifiers.yaml'],
        'ml': ['pipeline.yaml', 'gpu_classifiers.yaml'],
        'db_pg': ['pipeline.yaml', 'services.yaml'],
    }
    
    if profile not in profile_configs:
        raise ConfigurationError(f"Unknown profile: {profile}")
    
    # Load user.yaml if it exists
    user_config_path = config_path / 'user.yaml'
    user_config = load_yaml_file(user_config_path)
    has_user_config = user_config is not None
    
    if has_user_config and not quiet:
        print(f"[CONFIG] Using user override: {profile}/user.yaml")
    
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


def load_shared_config(
    config_dir: str = "/app/config",
    ml_fields: bool = False,
    quiet: bool = False
) -> Tuple[Dict, Dict]:
    """
    Load shared configuration files (field list and field types).
    
    user.yaml structure for shared config:
        reddit_field_list:      # Overrides reddit_field_list.yaml
            submissions:
                - field1
                - field2
        reddit_field_list_ml:   # Overrides reddit_field_list_ml.yaml
            submissions:
                - field1
        reddit_field_types:     # Overrides reddit_field_types.yaml
            my_field: text
    
    List values in user.yaml fully replace base values (no merging).
    
    Args:
        config_dir: Base configuration directory
        ml_fields: If True, use reddit_field_list_ml.yaml instead of reddit_field_list.yaml
        quiet: If True, suppress informational output
        
    Returns:
        Tuple of (field_list, field_types) dictionaries
        
    Raises:
        ConfigurationError: If required config files are missing
    """
    shared_path = Path(config_dir) / 'shared'
    
    if not shared_path.exists():
        raise ConfigurationError(f"Shared config directory not found: {shared_path}")
    
    # Load user.yaml if it exists
    user_config_path = shared_path / 'user.yaml'
    user_config = load_yaml_file(user_config_path)
    has_user_config = user_config is not None
    
    if has_user_config and not quiet:
        print(f"[CONFIG] Using user override: shared/user.yaml")
    
    # Determine which field list to use
    if ml_fields:
        field_list_file = 'reddit_field_list_ml.yaml'
    else:
        field_list_file = 'reddit_field_list.yaml'
    
    # Load field list
    field_list_path = shared_path / field_list_file
    field_list = load_yaml_file(field_list_path)
    if field_list is None:
        raise ConfigurationError(f"Required config file not found: {field_list_path}")
    
    # Apply user overrides for field list
    if has_user_config:
        field_list_key = get_config_key(field_list_file)
        if field_list_key in user_config:
            field_list = deep_merge(field_list, user_config[field_list_key])
    
    # Load field types
    field_types_file = 'reddit_field_types.yaml'
    field_types_path = shared_path / field_types_file
    field_types = load_yaml_file(field_types_path)
    if field_types is None:
        raise ConfigurationError(f"Required config file not found: {field_types_path}")
    
    # Apply user overrides for field types
    if has_user_config:
        field_types_key = get_config_key(field_types_file)
        if field_types_key in user_config:
            field_types = deep_merge(field_types, user_config[field_types_key])
    
    return field_list, field_types


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
    Validate that required database config exists for db_pg profile.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    required_keys = ['host', 'port', 'name', 'schema', 'user']
    
    for key in required_keys:
        if 'database' not in config or key not in config['database']:
            raise ConfigurationError(
                f"[db_pg] Required config missing: database.{key}"
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
    if profile == 'ml_cpu' and classifier_name == 'lingua':
        required_keys = ['suffix', 'languages']
    else:
        # GPU classifiers
        required_keys = ['suffix', 'model']
    
    for key in required_keys:
        if key not in config:
            raise ConfigurationError(
                f"[{profile}] Required config missing for {classifier_name}: {key}"
            )


def apply_env_overrides(config: Dict, profile: str) -> Dict:
    """
    Apply environment variable overrides to configuration.
    
    For db_pg profile, environment variables override database settings.
    
    Args:
        config: Configuration dictionary
        profile: Profile name
        
    Returns:
        Configuration with env overrides applied
    """
    result = deepcopy(config)
    
    if profile == 'db_pg':
        if 'database' not in result:
            result['database'] = {}
        
        if 'POSTGRES_PORT' in os.environ:
            result['database']['port'] = int(os.environ['POSTGRES_PORT'])
        if 'DB_NAME' in os.environ:
            result['database']['name'] = os.environ['DB_NAME']
        if 'DB_SCHEMA' in os.environ:
            result['database']['schema'] = os.environ['DB_SCHEMA']
    
    return result
