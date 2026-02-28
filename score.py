import math
import re
from datetime import datetime, timezone

import pandas as pd


DATA_PATH = "data/prs_90d.parquet"

# --- Simple, explainable impact model weights ---
W_BASE_PR = 10
W_SIZE_M = 4
W_SIZE_L = 8
W_TESTS_DOCS = 2
W_BUGFIX = 3
W_INFRA_TOOLING = 3

W_REVIEW = 2
W_REVIEW_EARLY = 1  # review within 24h of PR creation

# Folder weights (signals "core" vs "peripheral"). Keep it simple + explainable.
CORE_MULTIPLIERS = [
    (re.compile(r"^(posthog|plugin-server|frontend|backend|hogvm|hogql|ee|api|src|apps)/"), 1.3),
    (re.compile(r"^(infrastructure|terraform|ops|docker|.github|helm|kubernetes)/"), 1.25),
]
DEFAULT_MULT = 1.0

BUGFIX_RE = re.compile(r"\b(fix|bug|regress|hotfix|incident|crash)\b", re.IGNORECASE)
TEST_DOC_RE = re.compile(r"(^|/)(test|tests|__tests__|docs|doc|documentation)(/|$)", re.IGNORECASE)
INFRA_RE = re.compile(r"(^|/)(\.github|ci|infra|infrastructure|terraform|docker|kubernetes|helm)(/|$)", re.IGNORECASE)


def pr_size_bucket(changed_files: int, additions: int, deletions: int) -> str:
    """
    Size is a proxy, not a goal. We avoid LOC directly by using broad buckets.
    """
    churn = additions + deletions
    if changed_files <= 5 and churn <= 200:
        return "S"
    if changed_files <= 20 and churn <= 800:
        return "M"
    return "L"


def core_multiplier(files: list[str]) -> float:
    for pattern, mult in CORE_MULTIPLIERS:
        if any(pattern.search(p) for p in files):
            return mult
    return DEFAULT_MULT


def has_tests_or_docs(files: list[str]) -> bool:
    return any(TEST_DOC_RE.search(p) for p in files)


def is_bugfix(title: str) -> bool:
    return bool(BUGFIX_RE.search(title or ""))


def is_infra_or_tooling(files: list[str]) -> bool:
    return any(INFRA_RE.search(p) for p in files)


def parse_dt(s: str):
    # GitHub timestamps are ISO
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main():
    df = pd.read_parquet(DATA_PATH)

    # Defensive cleanup
    df["author"] = df["author"].fillna("unknown")
    df["title"] = df["title"].fillna("")
    df["files"] = df["files"].apply(lambda x: x if isinstance(x, list) else [])
    df["labels"] = df["labels"].apply(lambda x: x if isinstance(x, list) else [])
    df["reviews"] = df["reviews"].apply(lambda x: x if isinstance(x, list) else [])

    # --- Build review table (reviewer contributions) ---
    review_rows = []
    for _, pr in df.iterrows():
        created = parse_dt(pr["createdAt"])
        for rv in pr["reviews"]:
            reviewer = rv.get("reviewer", "unknown")
            submitted = rv.get("submittedAt")
            if not submitted:
                continue
            submitted_dt = parse_dt(submitted)
            early = (submitted_dt - created).total_seconds() <= 24 * 3600
            review_rows.append(
                {
                    "reviewer": reviewer,
                    "pr_url": pr["url"],
                    "pr_title": pr["title"],
                    "submittedAt": submitted,
                    "early": early,
                }
            )
    reviews_df = pd.DataFrame(review_rows)

    # --- Score authored PRs ---
    author_scores = {}
    author_breakdown = {}
    author_top_prs = {}

    for _, pr in df.iterrows():
        author = pr["author"]
        files = pr["files"]
        title = pr["title"]

        size = pr_size_bucket(int(pr.get("changedFiles", 0)), int(pr.get("additions", 0)), int(pr.get("deletions", 0)))
        size_pts = 0
        if size == "M":
            size_pts = W_SIZE_M
        elif size == "L":
            size_pts = W_SIZE_L

        mult = core_multiplier(files)

        pts = W_BASE_PR + size_pts
        reasons = []

        reasons.append(f"Merged PR (+{W_BASE_PR})")
        if size_pts:
            reasons.append(f"Size {size} (+{size_pts})")

        if has_tests_or_docs(files):
            pts += W_TESTS_DOCS
            reasons.append(f"Tests/Docs (+{W_TESTS_DOCS})")

        if is_bugfix(title):
            pts += W_BUGFIX
            reasons.append(f"Bugfix/Regression (+{W_BUGFIX})")

        if is_infra_or_tooling(files):
            pts += W_INFRA_TOOLING
            reasons.append(f"Infra/Tooling (+{W_INFRA_TOOLING})")

        pts = pts * mult
        if mult != 1.0:
            reasons.append(f"Core area multiplier (x{mult:.2f})")

        author_scores[author] = author_scores.get(author, 0) + pts
        bd = author_breakdown.get(author, {"delivery": 0, "reviews": 0, "leadership": 0})
        # Treat infra/tooling as leadership component; rest as delivery
        if is_infra_or_tooling(files):
            bd["leadership"] += pts
        else:
            bd["delivery"] += pts
        author_breakdown[author] = bd

        # keep top PRs as evidence
        author_top_prs.setdefault(author, []).append(
            {
                "pts": pts,
                "title": title,
                "url": pr["url"],
                "why": ", ".join(reasons[:3]),
            }
        )

    # --- Score reviews ---
    reviewer_scores = {}
    reviewer_top_reviews = {}

    if not reviews_df.empty:
        for _, rv in reviews_df.iterrows():
            reviewer = rv["reviewer"]
            pts = W_REVIEW + (W_REVIEW_EARLY if rv["early"] else 0)

            reviewer_scores[reviewer] = reviewer_scores.get(reviewer, 0) + pts
            reviewer_top_reviews.setdefault(reviewer, []).append(
                {
                    "pts": pts,
                    "pr_title": rv["pr_title"],
                    "pr_url": rv["pr_url"],
                    "early": bool(rv["early"]),
                }
            )

    # --- Combine into final engineer table ---
    engineers = set(author_scores.keys()) | set(reviewer_scores.keys())
    rows = []
    for eng in engineers:
        delivery = author_breakdown.get(eng, {}).get("delivery", 0.0)
        leadership = author_breakdown.get(eng, {}).get("leadership", 0.0)
        reviews_pts = reviewer_scores.get(eng, 0.0)

        total = delivery + leadership + reviews_pts

        rows.append(
            {
                "engineer": eng,
                "impact_score": float(total),
                "delivery": float(delivery),
                "reviews": float(reviews_pts),
                "leadership": float(leadership),
            }
        )

    out = pd.DataFrame(rows).sort_values("impact_score", ascending=False).reset_index(drop=True)

    # Generate “why” bullets per engineer (simple and leader-friendly)
    whys = []
    for _, r in out.iterrows():
        eng = r["engineer"]
        bullets = []

        # Delivery evidence
        top_prs = sorted(author_top_prs.get(eng, []), key=lambda x: x["pts"], reverse=True)[:3]
        if top_prs:
            bullets.append(f"Shipped {len(author_top_prs.get(eng, []))} merged PRs (top examples linked).")

        # Review evidence
        if reviewer_scores.get(eng, 0) > 0:
            early_count = sum(1 for x in reviewer_top_reviews.get(eng, []) if x["early"])
            bullets.append(f"Unblocked via {len(reviewer_top_reviews.get(eng, []))} reviews ({early_count} early).")

        # Leadership evidence
        infra_count = 0
        for pr in (author_top_prs.get(eng, []) or []):
            if "Infra/Tooling" in pr["why"]:
                infra_count += 1
        if r["leadership"] > 0:
            bullets.append("Contributed leverage work (infra/tooling changes).")

        # Fallback if empty
        if not bullets:
            bullets = ["Contributed via merges and collaboration signals."]

        whys.append("\n".join([f"- {b}" for b in bullets[:3]]))

    out["why"] = whys

    # Save scored table + evidence objects (parquet keeps lists nicely)
    os_out = out.copy()
    os_out.to_parquet("data/engineer_scores.parquet", index=False)

    print("✅ Scored engineers and saved to data/engineer_scores.parquet")
    print("\nTop 5 by impact score:")
    print(out.head(5)[["engineer", "impact_score", "delivery", "reviews", "leadership"]].to_string(index=False))

if __name__ == "__main__":
    main()