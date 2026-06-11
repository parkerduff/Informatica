"""CPM OIG extract — migration of workflow wf_CPM_OIG.

Reads CPM_NEWPAY_TBL for the current pay period, renders the NIH payroll
master fixed-width file (layout: oigsgndec_SKPAYROLL_MASTER) with header from
the OIG layout (signed decimals use trailing sign bytes), writes the file to the staging directory and mirrors the
records into CPM_OIG_STG_TBL.
"""
import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from jobs.cpm import cpm_common  # noqa: E402
from utils import schemas  # noqa: E402
from jobs.spark_common import get_spark  # noqa: E402
from utils.notifications import send_notification  # noqa: E402
from utils.secrets import get_secret, load_config  # noqa: E402

logger = logging.getLogger("cpm.oig")

AGENCY = "OIG"
MODULE = "CPM_OIG"
MASTER_DEF = "oigsgndec_SKPAYROLL_MASTER"
STAGING_TABLE = "CPM_OIG_STG_TBL"
OUTPUT_FILE = "cpm_oig_payroll.txt"


def run(env: str = None, run_date: str = None) -> int:
    config = load_config(env)
    secret = get_secret("biis", config)
    spark = get_spark("biis-cpm-oig")
    try:
        pay_period = cpm_common.get_pay_period(spark, config, secret)
        layout = cpm_common.agency_layout(MODULE, MASTER_DEF)
        signed = {
            f["name"]: int(f.get("scale") or 0)
            for f in schemas.get_table_fields(MODULE, MASTER_DEF, "targets")
            if (f.get("datatype") or "").startswith("number") and int(f.get("scale") or 0) > 0
        }
        df = cpm_common.read_newpay(spark, config, secret)
        df = df.filter(
            (df.PP_NUM == pay_period["pp_num"])
            & (df.PP_END_YEAR == pay_period["pp_end_year"])
        ).orderBy("DFAS_PSEUDO_SSN", "LINE_TYPE")
        rows = [r.asDict() for r in df.collect()]

        lines = [cpm_common.build_header(pay_period, AGENCY)]
        types = ["H"]
        for row in rows:
            lines.append(cpm_common.render_fixed_width(row, layout, signed_decimal_fields=signed))
            types.append("D")
        lines.append(cpm_common.build_trailer(len(rows), AGENCY))
        types.append("T")

        staging_dir = config["paths"]["staging"]
        os.makedirs(staging_dir, exist_ok=True)
        out_path = os.path.join(staging_dir, OUTPUT_FILE)
        with open(out_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        n = cpm_common.load_staging_table(config, secret, STAGING_TABLE, lines, types)
        send_notification(
            f"CPM {AGENCY} extract completed",
            f"Wrote {len(rows)} payroll records to {out_path} ({n} staging rows)",
            config,
        )
        return len(rows)
    except Exception as exc:
        send_notification(f"CPM {AGENCY} extract FAILED", str(exc), config)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args()
    run(args.env, args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
