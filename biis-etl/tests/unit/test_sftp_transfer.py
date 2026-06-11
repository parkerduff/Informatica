import os

import pytest

from transfers import sftp_transfer


def _config(tmp_path):
    return {
        "sftp": {"host": "localhost", "port": 2222, "user": "u", "password": "p",
                 "routes": {"nih_cpm": "/outbound"}},
        "paths": {"staging": str(tmp_path)},
        "notifications": {"provider": "log"},
    }


def test_transfer_file_unknown_agency(tmp_path):
    with pytest.raises(ValueError):
        sftp_transfer.transfer_file("nope", "f.txt", _config(tmp_path))


def test_transfer_file_missing_file_notifies(monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(sftp_transfer, "send_notification",
                        lambda s, b, c: sent.append(s))
    with pytest.raises(FileNotFoundError):
        sftp_transfer.transfer_file("nih_cpm", "absent.txt", _config(tmp_path))
    assert sent == ["Aborting: File absent.txt not found!"]


def test_transfer_file_success(monkeypatch, tmp_path):
    (tmp_path / "f.txt").write_text("data")
    puts = []

    class FakeSFTP:
        def put(self, local, remote):
            puts.append((local, remote))

        def close(self):
            pass

    monkeypatch.setattr(sftp_transfer, "_connect", lambda c: FakeSFTP())
    sent = []
    monkeypatch.setattr(sftp_transfer, "send_notification",
                        lambda s, b, c: sent.append(s))
    remote = sftp_transfer.transfer_file("nih_cpm", "f.txt", _config(tmp_path))
    assert remote == "/outbound/f.txt"
    assert puts and puts[0][1] == "/outbound/f.txt"
    assert "transferred successfully" in sent[0]


def test_connect_uses_reject_policy(monkeypatch):
    policies = []

    class FakeClient:
        def load_host_keys(self, path):
            pass

        def set_missing_host_key_policy(self, policy):
            policies.append(policy)

        def connect(self, **kwargs):
            pass

        def open_sftp(self):
            return "sftp"

    monkeypatch.setattr(sftp_transfer.paramiko, "SSHClient", FakeClient)
    sftp_transfer._connect({"sftp": {"host": "h", "port": 22, "user": "u", "password": "p"}})
    assert len(policies) == 1
    assert isinstance(policies[0], sftp_transfer.paramiko.RejectPolicy)


def test_archive_files(tmp_path):
    src = tmp_path / "in"
    dest = tmp_path / "out"
    src.mkdir()
    (src / "a.txt").write_text("1")
    (src / "b.dat").write_text("2")
    moved = sftp_transfer.archive_files(str(src), str(dest), "202612")
    assert sorted(os.path.basename(m) for m in moved) == ["a_P202612.txt", "b_P202612.dat"]
    assert not os.listdir(src)


def test_remove_file(tmp_path):
    (tmp_path / "x.txt").write_text("1")
    assert sftp_transfer.remove_file(str(tmp_path), "x.txt") is True
    assert sftp_transfer.remove_file(str(tmp_path), "x.txt") is False
