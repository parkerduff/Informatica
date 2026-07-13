"""Tests for generated orchestration ASL and static IaC presence."""
import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.orchestration import generate_orchestration as ORCH

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ASL_DIR = os.path.join(REPO_ROOT, "migration", "orchestration", "statemachines")
IAC_DIR = os.path.join(REPO_ROOT, "migration", "iac")


def test_master_has_parallel_branches():
    m = ORCH.master_machine()
    top = m["States"]["AllFamilies"]
    assert top["Type"] == "Parallel"
    assert len(top["Branches"]) == len(ORCH.FAMILIES)


def test_agency_family_runs_feeds_in_parallel():
    fam = ORCH.family_machine("agency_feeds", ORCH.FAMILIES["agency_feeds"])
    run = fam["States"]["RunFeeds"]
    assert run["Type"] == "Parallel"
    assert len(run["Branches"]) == 4  # NIH/OIG/CDC/FDA


def test_ehrp_branch_sequences_pre_job_after_sftp():
    branch = ORCH.branch_for_folder("EHRP2BIIS_UPDATE")
    names = " ".join(branch["States"].keys()).lower()
    assert "preload" in names and "afterload" in names and "sftp" in names


def test_generated_asl_files_valid_json():
    ORCH.main()
    for fn in os.listdir(ASL_DIR):
        if fn.endswith(".asl.json"):
            with open(os.path.join(ASL_DIR, fn)) as fh:
                json.load(fh)  # raises on invalid


def test_iac_files_present():
    for fn in ["main.tf", "variables.tf", "glue.tf", "stepfunctions.tf", "outputs.tf"]:
        assert os.path.exists(os.path.join(IAC_DIR, fn))


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
