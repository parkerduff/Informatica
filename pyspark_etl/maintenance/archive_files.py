"""Replaces ``Maintenance Scripts/archive_files``.

Original ksh::

    cd $inputdir
    for file in *; do
        fileprefix=$(basename $file .txt)
        mv "$file" "$filedest/${fileprefix}_P${pay_period}.txt"
    done

Each file in ``input_dir`` is moved to ``dest_dir`` with the pay period appended
(``<prefix>_P<pay_period>.txt``).
"""
from __future__ import annotations

import logging
import os
import shutil
from typing import List

logger = logging.getLogger(__name__)


def archive_files(input_dir: str, dest_dir: str, pay_period: str) -> List[str]:
    """Move every file from ``input_dir`` to ``dest_dir`` with the PP suffix.

    Returns the list of destination paths.
    """
    os.makedirs(dest_dir, exist_ok=True)
    moved: List[str] = []
    for name in os.listdir(input_dir):
        src = os.path.join(input_dir, name)
        if not os.path.isfile(src):
            continue
        prefix = name[:-4] if name.endswith(".txt") else name
        dest = os.path.join(dest_dir, f"{prefix}_P{pay_period}.txt")
        shutil.move(src, dest)
        moved.append(dest)
        logger.info("Archived %s -> %s", src, dest)
    return moved
