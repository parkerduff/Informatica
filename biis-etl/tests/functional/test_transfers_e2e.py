"""End-to-end functional test for the SFTP transfer flow.

Uses a fake SFTP client (writing to a local directory) so the test runs without
a live SFTP server, while still exercising ``transfer_file`` end-to-end.  The
docker-compose ``sftp-mock`` service / CI ``sftp`` service cover the real
network path.
"""
import os
import shutil

import pytest

from transfers import sftp_transfer
from utils import notifications
from utils.config import get_config


class _FakeSFTP:
    def __init__(self, root):
        self.root = root

    def put(self, local, remote):
        dest = os.path.join(self.root, os.path.basename(remote))
        shutil.copy(local, dest)

    def close(self):
        pass


class _FakeTransport:
    def close(self):
        pass


def test_transfer_file_sends_to_remote(tmp_path, monkeypatch):
    notifications.reset()
    remote_root = tmp_path / "remote"
    remote_root.mkdir()
    monkeypatch.setattr(sftp_transfer, "_sftp_client",
                        lambda cfg, route: (_FakeSFTP(str(remote_root)), _FakeTransport()))

    local = tmp_path / "nih_cpm.txt"
    local.write_text("payload")
    remote_path = sftp_transfer.transfer_file("nih_cpm", str(local), get_config("test"))

    assert remote_path.endswith("/nih_cpm.txt")
    assert (remote_root / "nih_cpm.txt").exists()
    assert any("transferred" in n.subject.lower() for n in notifications.SENT)


def test_transfer_missing_file_aborts(tmp_path):
    notifications.reset()
    with pytest.raises(FileNotFoundError):
        sftp_transfer.transfer_file("oig", str(tmp_path / "absent.txt"), get_config("test"))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
