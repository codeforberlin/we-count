#!/usr/bin/env python3
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    openproject_issues.py
# @author  Michael Behrisch
# @date    2026-03-19

import argparse
import json
import os
import sys

import requests

PAGE_SIZE = 100


def fetch_custom_field_names(session, schema_url):
    """Return a mapping of customFieldN key to human-readable name from the schema."""
    r = session.get(schema_url)
    if not r.ok:
        print(f"Warning: could not fetch schema ({r.status_code}), custom field names will not be resolved.", file=sys.stderr)
        return {}
    schema = r.json()
    return {
        key: field["name"]
        for key, field in schema.items()
        if key.startswith("customField") and "name" in field
    }


def clean_issue(issue, custom_field_names):
    """Keep only necessary fields and rename customFieldN keys to human-readable names."""
    result = {}
    for key, value in issue.items():
        if key in ("id", "subject", "description") or key in custom_field_names:
            result[custom_field_names.get(key, key)] = value
    result["type"] = issue.get("_links", {}).get("type", {}).get("title")
    result["status"] = issue.get("_links", {}).get("status", {}).get("title")
    return result


def fetch_all_work_packages(base_url, token):
    session = requests.Session()
    session.auth = ("apikey", token)
    session.headers.update({"Content-Type": "application/json"})
    endpoint = f"{base_url}/api/v3/work_packages"
    custom_field_names = {}
    all_issues = []
    offset = 1

    while True:
        r = session.get(endpoint, params={"pageSize": PAGE_SIZE, "offset": offset})
        r.raise_for_status()
        data = r.json()
        elements = data.get("_embedded", {}).get("elements", [])
        if not custom_field_names and elements:
            schema_url = elements[0].get("_links", {}).get("schema", {}).get("href")
            if schema_url:
                custom_field_names = fetch_custom_field_names(session, base_url + schema_url)        
        all_issues.extend(clean_issue(e, custom_field_names) for e in elements)
        total = data.get("total", 0)
        print(f"Fetched {len(all_issues)}/{total} issues...", file=sys.stderr)
        if len(all_issues) >= total:
            break
        offset += 1

    return all_issues


def check_issues(issues, geojson_file):
    """Check that every issue's Seriennummer and Segment match a segment+instance in the GeoJSON."""
    with open(geojson_file, encoding="utf-8") as f:
        geojson = json.load(f)

    # Build lookup: segment_id (int) → set of MACs across all instances
    segments = {}
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        sid = props.get("segment_id")
        macs = {inst["mac"]: inst["status"] for inst in props.get("instance_ids", {}).values() if "mac" in inst}
        segments[sid] = macs

    errors = 0
    known_serials = {}
    for issue in issues:
        segment_field = issue.get("Segment", "")
        serial = issue.get("Seriennummer")
        if serial is None and issue['type'] not in ("Task", "Phase"):
            print(f"Issue {issue['id']}: No serial set for: {issue['subject']}")
            errors += 1
        if not segment_field or serial is None:
            continue
        segment_id = int(segment_field.split()[0].split("B")[0].split("A")[0])
        macs = segments.get(segment_id)
        if macs is None:
            print(f"Issue {issue['id']}: segment {segment_id} not found in GeoJSON")
            errors += 1
        elif serial not in macs:
            print(f"Issue {issue['id']}: MAC {serial} not found in segment {segment_id} (known MACs: {macs})")
            errors += 1
        elif serial in known_serials:
            print(f"Issue {issue['id']}: Duplicate serial set, already seen in: {known_serials[serial]}")
            errors += 1
        elif issue['status'] == "Counting" and macs[serial] != "active":
            print(f"Issue {issue['id']}: is counting but mac is {macs[serial]}")
            errors += 1
        elif issue['status'] != "Counting" and macs[serial] == "active":
            print(f"Issue {issue['id']}: is {issue['status']} but mac is active")
            errors += 1
        known_serials[serial] = issue['id']

    if errors == 0:
        print(f"All {len(issues)} issues passed the check.")
    else:
        print(f"{errors} issue(s) failed the check.")


def get_options(args=None):
    parser = argparse.ArgumentParser(description="Fetch all work packages from OpenProject as JSON.")
    parser.add_argument("-u", "--url", default="https://op.flotte-berlin.org/",
                        help="OpenProject instance URL (e.g. https://openproject.example.org)")
    parser.add_argument("-o", "--output",
                        help="Output file path (default: stdout)")
    parser.add_argument("-s", "--secrets-file", default="secrets.json",
                        metavar="FILE", help="JSON file containing OpenProject credentials")
    parser.add_argument("--check", metavar="GEOJSON",
                        help="Check issues against the given Telraam GeoJSON file")
    parser.add_argument("--issues",
                        help="Load issues from a local JSON file instead of fetching from API")
    return parser.parse_args(args=args)


def main():
    options = get_options()

    if options.issues:
        with open(options.issues, encoding="utf-8") as f:
            issues = json.load(f)
    else:
        secrets = {}
        if os.path.exists(options.secrets_file):
            with open(options.secrets_file, encoding="utf-8") as f:
                secrets = json.load(f)
        issues = fetch_all_work_packages(options.url, secrets.get("openproject_token"))

    if options.check:
        check_issues(issues, options.check)
    else:
        output = json.dumps(issues, indent=2, ensure_ascii=False)
        if options.output:
            with open(options.output, "w", encoding="utf-8") as f:
                f.write(output)
            print(f"Saved {len(issues)} issues to {options.output}", file=sys.stderr)
        else:
            print(output)


if __name__ == "__main__":
    main()
