import logging
import os
import yaml
from typing import Dict

logger = logging.getLogger(__name__)


def read_config() -> Dict:
    """
    function to open, read YAML file
    :return: dtypes dictionary
    """
    logger.info("Reading config.YAML file")
    with open(os.environ.get("CONFIG_FILE_PATH", "config_file/config.yaml")) as file:
        config = yaml.full_load(file)["input"]
        return config


