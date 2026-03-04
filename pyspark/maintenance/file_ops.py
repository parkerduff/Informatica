"""
File maintenance utilities for BIISINT.

Replaces:
  - Maintenance Scripts/archive_files
  - Maintenance Scripts/remove_file

archive_files: Moves all files in an input directory to a destination
directory, renaming them with a pay period suffix.

remove_file: Removes a specific file from a directory if it exists.
"""

import logging
import os
import shutil

logger = logging.getLogger(__name__)


def archive_files(input_dir: str, dest_dir: str, pay_period: str) -> int:
    """Archive files by moving them to a destination with pay period suffix.

    Replaces: Maintenance Scripts/archive_files

    Original KSH logic:
        cd $inputdir
        for file in *; do
            fileprefix=$(basename $file .txt)
            mv "${file}" "${filedest}"/"${fileprefix}"_P"${pay_period}".txt
        done

    Args:
        input_dir: Source directory containing files to archive.
        dest_dir: Destination directory for archived files.
        pay_period: Pay period string appended to filenames
            (e.g. "202601").

    Returns:
        Number of files archived.
    """
    if not os.path.isdir(input_dir):
        logger.warning("Input directory does not exist: %s", input_dir)
        return 0

    os.makedirs(dest_dir, exist_ok=True)

    count = 0
    for filename in os.listdir(input_dir):
        src_path = os.path.join(input_dir, filename)
        if not os.path.isfile(src_path):
            continue

        # Strip .txt extension if present, add pay period suffix
        base = filename
        if base.endswith(".txt"):
            base = base[:-4]

        dest_filename = f"{base}_P{pay_period}.txt"
        dest_path = os.path.join(dest_dir, dest_filename)

        shutil.move(src_path, dest_path)
        logger.info("Archived: %s -> %s", filename, dest_filename)
        count += 1

    logger.info("Archived %d files from %s to %s", count, input_dir, dest_dir)
    return count


def remove_file(input_dir: str, filename: str) -> bool:
    """Remove a specific file from a directory.

    Replaces: Maintenance Scripts/remove_file

    Original KSH logic:
        cd $inputdir
        if [ -f $filename ]; then
            rm $filename
        fi

    Args:
        input_dir: Directory containing the file.
        filename: Name of the file to remove.

    Returns:
        True if the file was removed, False if it did not exist.
    """
    file_path = os.path.join(input_dir, filename)

    if os.path.isfile(file_path):
        os.remove(file_path)
        logger.info("Removed: %s", file_path)
        return True

    logger.info("File not found (no action taken): %s", file_path)
    return False


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """Command-line interface for file maintenance operations.

    Usage:
        python -m pyspark.maintenance.file_ops archive <input_dir> <dest_dir> <pay_period>
        python -m pyspark.maintenance.file_ops remove <input_dir> <filename>
    """
    import sys

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        print("Usage:")
        print("  archive <input_dir> <dest_dir> <pay_period>")
        print("  remove <input_dir> <filename>")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "archive":
        if len(sys.argv) < 5:
            print("Usage: archive <input_dir> <dest_dir> <pay_period>")
            sys.exit(1)
        count = archive_files(sys.argv[2], sys.argv[3], sys.argv[4])
        print(f"Archived {count} files")

    elif command == "remove":
        if len(sys.argv) < 4:
            print("Usage: remove <input_dir> <filename>")
            sys.exit(1)
        removed = remove_file(sys.argv[2], sys.argv[3])
        print(f"File {'removed' if removed else 'not found'}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
