#!/usr/bin/env python3
"""Scrape fuzzor endpoints and aggregate campaign data into a single JSON file.

Each endpoint is expected to serve a directory listing at its root, where
top-level entries are project names (e.g. ``bitcoin``).  Under each project
the scraper looks for ``harnesses/<name>/campaigns/<id>/`` containing
``stats.txt`` and optionally ``coverage-summary.json``.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.request import urlopen
from urllib.error import URLError

ONE_DAY_SECONDS = 86400


class LinkParser(HTMLParser):
    """Extract href values from directory listing HTML."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value != "../":
                    self.links.append(value.rstrip("/"))


def fetch(url):
    """Fetch URL and return response body as string."""
    with urlopen(url) as resp:
        return resp.read().decode("utf-8")


def list_dir(url):
    """Parse a directory listing page and return entry names."""
    html = fetch(url)
    parser = LinkParser()
    parser.feed(html)
    return parser.links


def parse_stats(text):
    """Parse stats.txt CSV into list of data points."""
    points = []
    for line in text.strip().splitlines():
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 4:
            continue
        timestamp = parts[0]
        stability_raw = parts[1]
        execs_per_sec = parts[2]
        corpus_count = parts[3]

        stability = None
        if stability_raw.startswith("Some(") and stability_raw.endswith(")"):
            try:
                stability = float(stability_raw[5:-1])
            except ValueError:
                pass

        pt = {
            "timestamp": timestamp,
            "execs_per_sec": int(float(execs_per_sec)),
            "corpus_count": int(float(corpus_count)),
        }
        if stability is not None:
            pt["stability"] = stability
        points.append(pt)
    return points


def parse_coverage(data):
    """Extract coverage totals and per-file data from coverage-summary.json."""
    entry = data["data"][0]
    totals = entry.get("totals", {})
    files = []
    for f in entry.get("files", []):
        if f["summary"]["lines"]["covered"] > 0:
            files.append({
                "filename": f["filename"],
                "lines": f["summary"]["lines"],
                "functions": f["summary"]["functions"],
                "branches": f["summary"]["branches"],
                "regions": f["summary"]["regions"],
            })
    files.sort(key=lambda f: f["lines"]["percent"], reverse=True)
    return {"totals": totals, "files": files}


def campaign_status(campaign_data):
    """Determine whether a campaign is completed, running, or stopped."""
    if campaign_data["coverage"] is not None:
        return "completed"
    if campaign_data["stats"]:
        last_ts = datetime.fromisoformat(
            campaign_data["stats"][-1]["timestamp"]
        )
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - last_ts).total_seconds()
        return "stopped" if age > ONE_DAY_SECONDS else "running"
    return "running"


def scrape_project(base_url, project):
    """Scrape all harnesses/campaigns for a single project."""
    harnesses_url = f"{base_url}/{project}/harnesses/"
    try:
        harness_names = list_dir(harnesses_url)
    except URLError:
        return {}

    result = {}
    for name in harness_names:
        harness_url = f"{harnesses_url}{name}/"
        try:
            entries = list_dir(harness_url)
        except URLError:
            result[name] = {"campaigns": []}
            continue

        if "campaigns" not in entries:
            result[name] = {"campaigns": []}
            continue

        campaigns_url = f"{harness_url}campaigns/"
        try:
            campaign_ids = list_dir(campaigns_url)
        except URLError:
            result[name] = {"campaigns": []}
            continue

        campaigns = []
        for cid in campaign_ids:
            campaign_url = f"{campaigns_url}{cid}/"
            cdata = {"id": cid, "stats": [], "coverage": None}

            try:
                cdata["stats"] = parse_stats(fetch(f"{campaign_url}stats.txt"))
            except URLError:
                pass

            try:
                cov_json = json.loads(fetch(f"{campaign_url}coverage-summary.json"))
                cdata["coverage"] = parse_coverage(cov_json)
            except (URLError, json.JSONDecodeError, KeyError, IndexError):
                pass

            cdata["status"] = campaign_status(cdata)
            campaigns.append(cdata)

        campaigns.sort(
            key=lambda c: c["stats"][0]["timestamp"] if c["stats"] else ""
        )

        result[name] = {"campaigns": campaigns}
        total_pts = sum(len(c["stats"]) for c in campaigns)
        print(f"    {name}: {len(campaigns)} campaign(s), "
              f"{total_pts} data points", file=sys.stderr)

    return result


def scrape_endpoint(endpoint, only_projects=None):
    """Discover projects at *endpoint* and scrape each one.

    If *only_projects* is given, only scrape projects whose names are in the set.
    """
    endpoint = endpoint.rstrip("/")
    projects = {}

    try:
        top_level = list_dir(endpoint)
    except URLError as exc:
        print(f"  Failed to list {endpoint}: {exc}", file=sys.stderr)
        return projects

    for entry in top_level:
        if only_projects is not None and entry not in only_projects:
            continue
        # Each top-level directory is a project
        print(f"  Project: {entry}", file=sys.stderr)
        harness_data = scrape_project(endpoint, entry)
        if entry in projects:
            projects[entry]["harnesses"].update(harness_data)
        else:
            projects[entry] = {"harnesses": harness_data}

    return projects


def main():
    parser = argparse.ArgumentParser(
        description="Scrape fuzzor endpoints and aggregate campaign data."
    )
    parser.add_argument(
        "endpoints", nargs="+",
        help="One or more base URLs to scrape (e.g. http://host:8888)",
    )
    parser.add_argument(
        "-o", "--output", default="data.json",
        help="Output JSON file (default: data.json)",
    )
    parser.add_argument(
        "-p", "--projects", nargs="+", metavar="NAME",
        help="Only scrape these projects (default: all discovered projects)",
    )
    args = parser.parse_args()

    all_projects = {}
    for endpoint in args.endpoints:
        print(f"Scraping {endpoint}", file=sys.stderr)
        only = set(args.projects) if args.projects else None
        projects = scrape_endpoint(endpoint, only_projects=only)
        for pname, pdata in projects.items():
            if pname in all_projects:
                all_projects[pname]["harnesses"].update(pdata["harnesses"])
            else:
                all_projects[pname] = pdata

    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "projects": all_projects,
    }

    with open(args.output, "w") as f:
        json.dump(output, f)

    total_harnesses = sum(
        len(p["harnesses"]) for p in all_projects.values()
    )
    print(f"Wrote {args.output} ({len(all_projects)} projects, "
          f"{total_harnesses} harnesses with campaigns)", file=sys.stderr)


if __name__ == "__main__":
    main()
