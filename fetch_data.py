import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

OWNER = "PostHog"
REPO = "posthog"
TOKEN = os.getenv("GITHUB_TOKEN")

if not TOKEN:
    raise SystemExit("❌ Missing GITHUB_TOKEN in .env (example: GITHUB_TOKEN=ghp_...)")

API_URL = "https://api.github.com/graphql"
HEADERS = {
    "Authorization": f"bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
}

# --- Config you can tweak ---
DAYS = 90
PAGE_SIZE = 50
MAX_FILES_PER_PR = 30        # keep lightweight + fast
MAX_REVIEWS_PER_PR = 30      # enough for impact analysis
REQUEST_TIMEOUT = 180        # seconds
MAX_RETRIES = 6
SLEEP_BETWEEN_PAGES = 0.12   # seconds

SINCE_DT = datetime.now(timezone.utc) - timedelta(days=DAYS)

QUERY = f"""
query($owner:String!, $name:String!, $cursor:String) {{
  repository(owner:$owner, name:$name) {{
    pullRequests(first: {PAGE_SIZE}, after: $cursor, states: MERGED, orderBy:{{field:UPDATED_AT, direction:DESC}}) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        number
        title
        url
        createdAt
        mergedAt
        updatedAt
        author {{ login }}

        changedFiles
        additions
        deletions

        labels(first: 25) {{ nodes {{ name }} }}
        files(first: {MAX_FILES_PER_PR}) {{ nodes {{ path }} }}
        comments {{ totalCount }}

        reviews(first: {MAX_REVIEWS_PER_PR}) {{
          nodes {{
            author {{ login }}
            state
            submittedAt
          }}
        }}
      }}
    }}
  }}
}}
"""

def post_gql(variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Robust GraphQL POST with retries + exponential backoff.
    Handles transient network issues + rate limits gracefully.
    """
    last_err = None
    backoff = 1.5

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(
                API_URL,
                json={"query": QUERY, "variables": variables},
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            # Rate limit / abuse detection / transient server errors
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")

            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")

            payload = r.json()

            if "errors" in payload:
                # Sometimes GitHub returns errors like rate limit messages here
                raise RuntimeError(f"GraphQL errors: {payload['errors']}")

            return payload["data"]

        except Exception as e:
            last_err = e
            wait = backoff ** attempt
            print(f"⚠️ Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            print(f"   Sleeping {wait:.1f}s then retrying...")
            time.sleep(wait)

    raise RuntimeError(f"❌ Failed after {MAX_RETRIES} retries. Last error: {last_err}")

def iso_to_dt(s: str) -> datetime:
    # GitHub ISO times often end with Z. Make them timezone-aware.
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def main():
    print(f"Fetching merged PRs for {OWNER}/{REPO} since {SINCE_DT.isoformat()} (last {DAYS} days)\n")

    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    stop_after_older_seen_pages = 1 # we’ll stop after seeing “old mergedAt” across N pages
    old_pages_seen = 0

    while True:
        data = post_gql({"owner": OWNER, "name": REPO, "cursor": cursor})
        pr_block = data["repository"]["pullRequests"]
        nodes = pr_block["nodes"] or []
        pages += 1

        kept_this_page = 0
        old_found_this_page = False

        for pr in nodes:
            merged_at = pr.get("mergedAt")
            if not merged_at:
                continue

            merged_dt = iso_to_dt(merged_at)

            # Filter strictly by mergedAt >= SINCE_DT
            if merged_dt < SINCE_DT:
                old_found_this_page = True
                continue

            author = (pr.get("author") or {}).get("login") or "unknown"
            labels = [x["name"] for x in (pr.get("labels") or {}).get("nodes", [])]
            files = [x["path"] for x in (pr.get("files") or {}).get("nodes", [])]
            comment_count = (pr.get("comments") or {}).get("totalCount", 0)

            reviews = pr.get("reviews", {}).get("nodes", []) or []
            review_flat = []
            for rv in reviews:
                review_flat.append({
                    "reviewer": (rv.get("author") or {}).get("login") or "unknown",
                    "state": rv.get("state"),
                    "submittedAt": rv.get("submittedAt"),
                })

            rows.append({
                "number": pr["number"],
                "title": pr["title"],
                "url": pr["url"],
                "author": author,
                "createdAt": pr["createdAt"],
                "mergedAt": pr["mergedAt"],
                "updatedAt": pr.get("updatedAt"),
                "changedFiles": pr.get("changedFiles", 0),
                "additions": pr.get("additions", 0),
                "deletions": pr.get("deletions", 0),
                "commentCount": comment_count,
                "labels": labels,
                "files": files,
                "reviews": review_flat,
            })
            kept_this_page += 1

        print(f"Page {pages}: kept {kept_this_page} PRs | total kept {len(rows)}")

        # Save progress every 10 pages
        if pages % 10 == 0:
            os.makedirs("data", exist_ok=True)
            temp_df = pd.DataFrame(rows)
            temp_df.to_parquet("data/prs_90d.parquet", index=False)
            print(f"Progress saved: {len(rows)} PRs")

        # Heuristic stopping:
        # Because sorting is UPDATED_AT (not mergedAt), old PRs can appear mixed.
        # We stop only after we’ve seen pages where most items are older than SINCE_DT a couple times.
        if kept_this_page == 0:
                print("Stopping: hit a page with 0 PRs in last 90 days.")
                break

        cursor = pr_block["pageInfo"]["endCursor"]
        time.sleep(SLEEP_BETWEEN_PAGES)

    if not rows:
        raise SystemExit("❌ No PRs collected. Possible causes: token issue, API blocked, or repo access problem.")

    df = pd.DataFrame(rows)

    # Final defensive filter:
    df["mergedAt_dt"] = pd.to_datetime(df["mergedAt"], utc=True, errors="coerce")
    df = df[df["mergedAt_dt"] >= pd.Timestamp(SINCE_DT)].drop(columns=["mergedAt_dt"]).reset_index(drop=True)

    os.makedirs("data", exist_ok=True)
    out_path = "data/prs_90d.parquet"
    df.to_parquet(out_path, index=False)

    print(f"\n✅ Saved {len(df)} merged PRs to: {out_path}")

if __name__ == "__main__":
    main()