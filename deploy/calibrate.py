#!/usr/bin/env python3
"""Offline strict-production calibration entry point; never applies a config patch."""

import argparse

from tank.calibration import write_report

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("--output-dir", required=True)
args = parser.parse_args()
report = write_report(args.input, args.output_dir)
print(report["status"])
