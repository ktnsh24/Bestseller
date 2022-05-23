import logging
import shutil

from configuration import read_config

logger = logging.getLogger(__name__)
config = read_config()


def delete_folder() -> None:
    """
    Delete local folder when the files are processed
    and moved to database
    """
    try:
        shutil.rmtree(config['bucket_source_folder'])
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
    return
