import yaml


def load_config_from_file(cls, file: str):
    """Load configuration data from a YAML file."""
    with open(file, "r", encoding="utf-8") as file_handle:
        metadata = yaml.safe_load(file_handle)
        return cls(**metadata)
