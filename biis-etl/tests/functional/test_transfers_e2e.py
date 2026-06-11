"""End-to-end SFTP transfer test against the Docker atmoz/sftp container."""
import socket

import pytest

from transfers.sftp_transfer import transfer_file

pytestmark = pytest.mark.functional


def _sftp_up(config) -> bool:
    try:
        with socket.create_connection((config.sftp.host, config.sftp.port), timeout=3):
            return True
    except OSError:
        return False


def test_transfer_file_to_sftp(config, tmp_path, monkeypatch):
    if not _sftp_up(config):
        pytest.skip("SFTP container not reachable (start it with `make db-up`)")

    import paramiko

    from transfers import sftp_transfer

    # The atmoz/sftp test container presents an ad-hoc host key; auto-accept it
    # for the test only (production keeps RejectPolicy).
    def _test_client(cfg):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=cfg.sftp.host, port=cfg.sftp.port,
                       username=cfg.sftp.user, password=cfg.sftp.password)
        return client

    monkeypatch.setattr(sftp_transfer, "_client", _test_client)

    f = tmp_path / "nihtest_NIH_PAYROLL_MASTER.dat"
    f.write_text("H  \nrec\nT000000001\n")
    remote = transfer_file("nih_cpm", f.name, config, local_dir=str(tmp_path))
    assert remote.endswith("nihtest_NIH_PAYROLL_MASTER.dat")
