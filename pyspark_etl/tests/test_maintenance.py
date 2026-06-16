import os

from pyspark_etl.config.schemas import FixedField
from pyspark_etl.maintenance.archive_files import archive_files
from pyspark_etl.maintenance.remove_file import remove_file
from pyspark_etl.transforms.fixed_width_writer import to_fixed_width_line


def test_archive_files(tmp_path):
    src = tmp_path / "in"
    dest = tmp_path / "out"
    src.mkdir()
    (src / "cpm_nih.txt").write_text("data")
    (src / "cpm_cdc.txt").write_text("data")

    moved = archive_files(str(src), str(dest), "12")

    names = sorted(os.path.basename(p) for p in moved)
    assert names == ["cpm_cdc_P12.txt", "cpm_nih_P12.txt"]
    assert not list(src.iterdir())  # all moved


def test_remove_file(tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("y")
    assert remove_file(str(tmp_path), "x.txt") is True
    assert not f.exists()
    assert remove_file(str(tmp_path), "missing.txt") is False


def test_fixed_width_line():
    layout = [
        FixedField("A", 0, 3),
        FixedField("N", 3, 5, "number"),
    ]
    line = to_fixed_width_line({"A": "X", "N": "42"}, layout)
    assert line == "X  " + "00042"
    assert len(line) == 8
