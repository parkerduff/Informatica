"""Unit tests for transfers/sftp_transfer.py."""
import pytest

from transfers import sftp_transfer
from utils import notifications
from utils.config import get_config


def test_all_agency_routes_valid():
    expected = {"nih_cpm", "nih_les", "oig", "cdc", "afps", "fda"}
    assert set(sftp_transfer.AGENCY_ROUTES) == expected
    for agency in expected:
        route = sftp_transfer.get_route(agency)
        assert route.host == "m1csv301.hhs.gov"
        assert route.account == "sa-cdirect"
        assert route.remote_dir.endswith("/outbound")


def test_unknown_route_raises():
    with pytest.raises(ValueError):
        sftp_transfer.get_route("nope")


def test_file_not_found_raises_and_notifies(tmp_path):
    notifications.reset()
    cfg = get_config("test")
    missing = str(tmp_path / "nope.txt")
    with pytest.raises(FileNotFoundError):
        sftp_transfer.transfer_file("oig", missing, cfg)
    assert any("not found" in n.subject.lower() for n in notifications.SENT)


def test_archive_renames_with_pay_period(tmp_path):
    src = tmp_path / "biisfile.txt"
    src.write_text("payload")
    dest_dir = tmp_path / "archive"
    out = sftp_transfer.archive_file(str(src), str(dest_dir), "202603")
    assert out.endswith("biisfile_P202603.txt")
    assert not src.exists()


def test_remove_existing_and_nonexistent(tmp_path):
    f = tmp_path / "x.txt"
    f.write_text("y")
    assert sftp_transfer.remove_file(str(f)) is True
    assert sftp_transfer.remove_file(str(f)) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
