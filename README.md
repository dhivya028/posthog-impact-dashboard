PostHog Engineer Impact Dashboard

This project analyzes the PostHog GitHub repository to identify the most impactful engineers over the last 90 days. Instead of relying on simple metrics like commits or lines of code, the analysis focuses on meaningful engineering contributions such as shipped features, collaboration through reviews, and infrastructure work.

Pull request data is collected using the GitHub API, processed using Python and Pandas, and visualized in an interactive Streamlit dashboard. Each engineer receives an impact score based on three main factors: merged pull request contributions, code review activity, and infrastructure or tooling changes that benefit the broader team.

The dashboard presents the top 5 most impactful engineers, along with a breakdown of their contributions and links to the supporting pull requests. This allows engineering leaders to quickly understand who contributed the most and why.

Live Dashboard:
https://posthog-impact-dashboard-hm7kz5y6xr75juebaqngdw.streamlit.app/

Tech Stack:
Python, Pandas, GitHub API, Streamlit, Altair
