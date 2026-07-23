"""
================================================================================
trigger_collector.py — Helper Script to Trigger Timeseries Collector via GitHub API
================================================================================
Usage:
    python youtube_extractor/trigger_collector.py

Requires:
    - GITHUB_PAT (GitHub Personal Access Token) and GITHUB_REPO set in .env or system environment.
    - Standard Python library (no external dependencies).
================================================================================
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def load_env() -> None:
    """Load environment variables from a .env file in the current working directory."""
    if os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()


def main() -> None:
    load_env()

    pat = os.environ.get("GITHUB_PAT")
    repo = os.environ.get("GITHUB_REPO", "SakinduR/trendcast-githubactions")

    if not pat:
        print("Error: GITHUB_PAT not found in .env or system environment.")
        pat = input("Please enter your GitHub Personal Access Token (PAT): ").strip()
        if not pat:
            print("No PAT provided. Exiting.")
            sys.exit(1)

    print(f"Triggering Job 2 (Timeseries Collector) for repository '{repo}'...")

    url = f"https://api.github.com/repos/{repo}/actions/workflows/youtube_etl.yml/dispatches"

    headers = {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "Github-Workflow-Trigger-Script",
    }

    payload = {
        "ref": "main",
        "inputs": {
            "job": "job2_timeseries_collector",
        },
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as response:
            status = response.status
            if status == 204:
                print("Successfully triggered the Timeseries Collector job on GitHub Actions!")
            else:
                print(f"Workflow dispatch API returned status: {status}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason}")
        try:
            error_body = e.read().decode("utf-8")
            print("Response details:", error_body)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
