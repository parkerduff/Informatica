"""Create the biis_test database/schema and seed the PAY_PERIOD fixture."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import config as cfg  # noqa: E402
from utils import db  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DDL_PATH = os.path.join(ROOT, "sql", "ddl.sql")
SEED_PATH = os.path.join(ROOT, "tests", "fixtures", "pay_calendar_seed.sql")


def main(env: str = "test") -> None:
    config = cfg.load_config(env)
    with open(DDL_PATH, "r", encoding="utf-8") as fh:
        ddl = fh.read()
    print("Applying DDL ...")
    db.execute_sql(config, ddl, database="master")
    print("DDL applied. Seeding PAY_PERIOD ...")
    with open(SEED_PATH, "r", encoding="utf-8") as fh:
        seed = fh.read()
    db.execute_sql(config, seed)
    count = db.execute_scalar(config, "SELECT COUNT(*) FROM PAY_PERIOD")
    print(f"Seeded PAY_PERIOD rows: {count}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "test")
