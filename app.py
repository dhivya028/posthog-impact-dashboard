import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="PostHog Engineer Impact (90d)", layout="wide")

DATA_SCORES = "data/engineer_scores.parquet"
DATA_PRS = "data/prs_90d.parquet"


@st.cache_data(ttl=3600)
def load_data():
    scores = pd.read_parquet(DATA_SCORES)
    prs = pd.read_parquet(DATA_PRS)
    return scores, prs


def human(n: float) -> str:
    return f"{n:,.0f}"


def main():
    scores, prs = load_data()

    # Simple bot filter
    bot_like = {"posthog-bot", "github-actions", "dependabot[bot]", "dependabot"}
    exclude_bots = st.sidebar.checkbox("Exclude bots", value=True)

    if exclude_bots:
        scores = scores[~scores["engineer"].str.lower().isin({b.lower() for b in bot_like})].copy()
        prs = prs[~prs["author"].str.lower().isin({b.lower() for b in bot_like})].copy()

    # Header
    st.title("PostHog Engineer Impact Dashboard (Last 90 Days)")
    st.caption(
        "Impact is modeled as: **Shipped merged PRs (weighted by scope/area + quality signals)** + "
        "**Unblocking reviews (incl. early reviews)** + **Leverage work (infra/tooling)**. "
        "Each result is backed by PR links for quick validation."
    )

    # Top 5
    top5 = scores.sort_values("impact_score", ascending=False).head(5).reset_index(drop=True)

    # Layout: leaderboard + breakdown
    left, right = st.columns([1.35, 1])

    with left:
        st.subheader("Top 5 Most Impactful Engineers")
        for i, row in top5.iterrows():
            st.markdown(f"### {i+1}. `{row['engineer']}` — Impact Score **{human(row['impact_score'])}**")
            st.markdown(row["why"])
            st.divider()

    with right:
        st.subheader("Impact Breakdown (Top 5)")
        bdf = top5[["engineer", "delivery", "reviews", "leadership"]].copy()
        bdf = bdf.melt(id_vars=["engineer"], var_name="component", value_name="points")

        chart = (
            alt.Chart(bdf)
            .mark_bar()
            .encode(
                x=alt.X("engineer:N", title="Engineer", sort="-y"),
                y=alt.Y("points:Q", title="Points"),
                color=alt.Color("component:N", title="Component"),
                tooltip=["engineer", "component", alt.Tooltip("points:Q", format=",.0f")],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)

        st.subheader("Select Engineer (Drilldown)")
        selected = st.selectbox("Engineer", scores["engineer"].head(50).tolist(), index=0)

        # Drilldown: top PRs + review highlights
        st.markdown("**Top merged PRs (evidence)**")
        prs_sel = prs[prs["author"] == selected].copy()
        if prs_sel.empty:
            st.info("No authored PRs found in the current dataset for this engineer (or filtered).")
        else:
            # Rank PRs by simple heuristic: changedFiles + comments (not LOC)
            prs_sel["rank"] = prs_sel["changedFiles"].fillna(0) + prs_sel["commentCount"].fillna(0)
            prs_sel = prs_sel.sort_values("rank", ascending=False).head(8)

            for _, pr in prs_sel.iterrows():
                st.markdown(f"- [{pr['title']}]({pr['url']})  \n  "
                            f"  *changedFiles:* {int(pr.get('changedFiles',0))}, "
                            f"*comments:* {int(pr.get('commentCount',0))}")

        st.markdown("**Recent review activity (evidence)**")
        # Pull review evidence from PR-level reviews lists
        review_hits = []
        for _, pr in prs.iterrows():
            for rv in pr.get("reviews", []):
                if rv.get("reviewer") == selected:
                    review_hits.append({
                        "pr_title": pr["title"],
                        "pr_url": pr["url"],
                        "state": rv.get("state"),
                        "submittedAt": rv.get("submittedAt"),
                    })

        if not review_hits:
            st.info("No reviews captured for this engineer in the current dataset (or filtered).")
        else:
            rpdf = pd.DataFrame(review_hits)
            rpdf = rpdf.sort_values("submittedAt", ascending=False).head(10)
            for _, r in rpdf.iterrows():
                st.markdown(f"- [{r['pr_title']}]({r['pr_url']}) — **{r['state']}**  \n  "
                            f"  *submitted:* {r['submittedAt']}")

    # Footer small notes
    st.caption(
        "Notes: This dashboard avoids naive metrics like LOC/commit counts. "
        "It uses broad size buckets + area signals + review unblocking proxies. "
        "Use PR links to validate impact quickly."
    )


if __name__ == "__main__":
    main()