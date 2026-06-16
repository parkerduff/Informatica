"""Replaces ``Maintenance Scripts/remove_file``.

Original ksh::

    cd $inputdir
    if [ -f $filename ]; then rm $filename; fi
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def remove_file(input_dir: str, filename: str) -> bool:
    """Delete ``input_dir/filename`` if it exists. Returns True when removed."""
    path = os.path.join(input_dir, filename)
    if os.path.isfile(path):
        os.remove(path)
        logger.info("Removed %s", path)
        return True
    logger.info("Nothing to remove at %s", path)
    return False
