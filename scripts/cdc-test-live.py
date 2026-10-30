"""Run opt-in live tests without evaluating a secret file as shell code."""

import argparse
import os
import shlex
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--env-file", type=Path, default=Path(".env.local"))
parser.add_argument("--run", default="^TestLambdaDBLive", help="Go test name pattern")
args = parser.parse_args()
env = os.environ.copy()
allowed = {"LAMBDADB_BASE_URL", "LAMBDADB_PROJECT_NAME", "LAMBDADB_PROJECT_API_KEY"}
for line in args.env_file.read_text().splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
        continue
    name, value = line.split("=", 1)
    name = name.strip().removeprefix("export ")
    if name in allowed:
        values = shlex.split(value, comments=True)
        if len(values) != 1:
            raise SystemExit(f"Expected one literal value for {name}")
        env[name] = values[0]
if not env.get("CDC_TEST_POSTGRES_DSN"):
    raise SystemExit("CDC_TEST_POSTGRES_DSN must name an isolated localhost source")
env["CDC_TEST_LIVE"] = "1"
raise SystemExit(
    subprocess.call(
        [
            "go",
            "test",
            "-v",
            "-race",
            "-count=1",
            "-timeout",
            "18m",
            "-run",
            args.run,
            "./internal/postgrescdc",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
    )
)
