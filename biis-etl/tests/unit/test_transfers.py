"""Unit tests for the SFTP transfer + maintenance helpers."""
import types

import pytest

from transfers import sftp_transfer

pytestmark = pytest.mark.unit


def _config(tmp_path):
    sftp = types.SimpleNamespace(
        host="h", port=22, user="u", password="p",
        routes={"nih_cpm": "/outbound"},
    )
    paths = types.SimpleNamespace(staging=str(tmp_path))
    return types.SimpleNamespace(sftp=sftp, paths=paths)


def test_transfer_file_unknown_agency_raises(tmp_path):
    with pytest.raises(KeyError):
        sftp_transfer.transfer_file("nope", "f.dat", _config(tmp_path))


def test_transfer_file_missing_file_notifies_and_raises(tmp_path, monkeypatch):
    notes = []
    monkeypatch.setattr(sftp_transfer, "send_notification",
                        lambda s, b, c: notes.append((s, b)))
    with pytest.raises(FileNotFoundError):
        sftp_transfer.transfer_file("nih_cpm", "missing.dat", _config(tmp_path))
    assert notes and "FAILED" in notes[0][0]


def test_transfer_file_uploads_to_configured_route(tmp_path, monkeypatch):
    f = tmp_path / "payroll.dat"
    f.write_text("data")

    puts = []

    class FakeSftp:
        def put(self, local, remote):
            puts.append((local, remote))

        def close(self):
            pass

    class FakeClient:
        def open_sftp(self):
            return FakeSftp()

        def close(self):
            pass

    monkeypatch.setattr(sftp_transfer, "_client", lambda config: FakeClient())
    monkeypatch.setattr(sftp_transfer, "send_notification", lambda *a: None)

    remote = sftp_transfer.transfer_file("nih_cpm", "payroll.dat", _config(tmp_path))
    assert remote == "/outbound/payroll.dat"
    assert puts[0][1] == "/outbound/payroll.dat"


def test_archive_files_adds_pay_period_suffix(tmp_path):
    src = tmp_path / "in"
    dest = tmp_path / "arch"
    src.mkdir()
    (src / "a.dat").write_text("x")
    (src / "b.txt").write_text("y")
    moved = sftp_transfer.archive_files(str(src), str(dest), "202612")
    assert moved == 2
    names = sorted(p.name for p in dest.iterdir())
    assert names == ["a_202612.dat", "b_202612.txt"]


def test_remove_file(tmp_path):
    (tmp_path / "x.dat").write_text("z")
    assert sftp_transfer.remove_file(str(tmp_path), "x.dat") is True
    assert sftp_transfer.remove_file(str(tmp_path), "x.dat") is False
