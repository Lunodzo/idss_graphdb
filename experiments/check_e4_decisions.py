#!/usr/bin/env python3
"""Check E4 governance decisions against server/policy.default.yaml.

Usage: check_e4_decisions.py <e4-governance.csv>

Prints PASS/FAIL per row and exits non-zero if any default-policy row's
recorded decision does not match the expected decision, or if a "deny"
decision suppressed propagation (forwarded_delta should stay > 0 whenever
downstream peers exist, i.e. peer_count > 1).

This table mirrors server/policy.default.yaml; update both together.
"""
import csv
import sys

QUERY_KIND = {
    "q1_customers": "Customer",
    "q2_open_offers": "Offer",
    "q3_meter_readings_raw": "MeterReading",
    "q4_active_power_sum": "MeterReading",
}

# (kind, role) -> expected decision under server/policy.default.yaml
DEFAULT_POLICY_EXPECTATIONS = {
    ("Customer", "member"): "allow",
    ("Customer", "manager"): "allow",
    ("Customer", "observer"): "allow",
    ("Offer", "member"): "allow",
    ("Offer", "manager"): "allow",
    ("Offer", "observer"): "aggregate",
    ("MeterReading", "member"): "deny",
    ("MeterReading", "manager"): "aggregate",
    ("MeterReading", "observer"): "aggregate",
}


def expected_decision(policy: str, kind: str, role: str) -> str:
    if policy == "policy.permissive.yaml":
        return "allow"
    return DEFAULT_POLICY_EXPECTATIONS.get((kind, role), "deny")


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    failures = 0
    checked = 0
    with open(sys.argv[1], newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            kind = QUERY_KIND.get(row["query_label"])
            if kind is None:
                continue
            checked += 1
            expected = expected_decision(row["policy"], kind, row["role"])
            actual = row["decision"]
            ok = actual == expected
            if not ok:
                failures += 1
                print(f"FAIL policy={row['policy']} role={row['role']} query={row['query_label']} "
                      f"expected={expected} actual={actual}")

            if actual == "deny" and int(row.get("forwarded_delta", "0")) <= 0:
                failures += 1
                print(f"FAIL propagation-suppressed policy={row['policy']} role={row['role']} "
                      f"query={row['query_label']} forwarded_delta={row['forwarded_delta']}")

    print(f"Checked {checked} rows, {failures} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
