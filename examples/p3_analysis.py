"""P³ podcast analysis pipeline — demonstrates reactive partial re-execution.

This pipeline analyzes podcast episodes to find agent-related content.
It uses synthetic data so it runs without external dependencies.

To swap in real P³ DuckDB data, replace raw_episodes().func with:
    import duckdb
    con = duckdb.connect("~/Code/parakeet-podcast-processor/data/p3.duckdb")
    return con.execute("SELECT * FROM episodes").fetchdf().to_dict("records")

Demo:
    1. dag run examples/p3_analysis.py        — all 6 cells execute
    2. Change RECENT_DAYS from 14 to 7 below
    3. dag run examples/p3_analysis.py        — only 4/6 cells re-run
       raw_episodes and agent_episodes stay cached (unchanged)
"""

from collections import Counter
from datetime import datetime, timedelta, timezone

from dag.cell import cell

# --- Configuration ---
RECENT_DAYS = 14


# --- Synthetic data (replace with DuckDB for real P³) ---
def _synthetic_episodes():
    """Generate realistic podcast episode data."""
    now = datetime.now(timezone.utc)
    return [
        {
            "id": 1,
            "title": "Building AI Agents That Actually Work",
            "topics": ["agents", "LLMs", "production"],
            "published": now - timedelta(days=3),
        },
        {
            "id": 2,
            "title": "The State of DevOps in 2025",
            "topics": ["devops", "CI/CD", "platform-engineering"],
            "published": now - timedelta(days=5),
        },
        {
            "id": 3,
            "title": "Agent Frameworks Compared: LangGraph vs CrewAI",
            "topics": ["agents", "frameworks", "comparison"],
            "published": now - timedelta(days=10),
        },
        {
            "id": 4,
            "title": "Why Reactive Pipelines Beat Notebooks",
            "topics": ["data-engineering", "reactive", "notebooks"],
            "published": now - timedelta(days=12),
        },
        {
            "id": 5,
            "title": "Multi-Agent Systems in Production",
            "topics": ["agents", "production", "orchestration"],
            "published": now - timedelta(days=20),
        },
        {
            "id": 6,
            "title": "Kubernetes at Scale",
            "topics": ["kubernetes", "infrastructure", "scaling"],
            "published": now - timedelta(days=25),
        },
        {
            "id": 7,
            "title": "Autonomous Coding Agents",
            "topics": ["agents", "coding", "automation"],
            "published": now - timedelta(days=30),
        },
        {
            "id": 8,
            "title": "The Future of Databases",
            "topics": ["databases", "SQL", "distributed"],
            "published": now - timedelta(days=45),
        },
    ]


# --- Pipeline cells ---

@cell
def raw_episodes():
    """Load all podcast episodes."""
    return _synthetic_episodes()


@cell(depends_on=[raw_episodes])
def agent_episodes(raw_episodes):
    """Filter to episodes about agents."""
    return [
        ep for ep in raw_episodes
        if any("agent" in t.lower() for t in ep["topics"])
    ]


@cell(depends_on=[raw_episodes])
def recent_episodes(raw_episodes):
    """Filter to episodes from the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    return [ep for ep in raw_episodes if ep["published"] >= cutoff]


@cell(depends_on=[agent_episodes, recent_episodes])
def recent_agent_episodes(agent_episodes, recent_episodes):
    """Intersection: recent episodes that are about agents."""
    recent_ids = {ep["id"] for ep in recent_episodes}
    return [ep for ep in agent_episodes if ep["id"] in recent_ids]


@cell(depends_on=[recent_agent_episodes])
def topic_frequency(recent_agent_episodes):
    """Count topic occurrences across recent agent episodes."""
    counter: Counter[str] = Counter()
    for ep in recent_agent_episodes:
        counter.update(ep["topics"])
    return dict(counter.most_common())


@cell(depends_on=[topic_frequency, recent_agent_episodes, agent_episodes, raw_episodes])
def report(topic_frequency, recent_agent_episodes, agent_episodes, raw_episodes):
    """Format analysis as a summary report."""
    lines = [
        "# P³ Agent Episode Analysis",
        "",
        f"Total episodes: {len(raw_episodes)}",
        f"Agent-related: {len(agent_episodes)}",
        f"Recent (last {RECENT_DAYS} days): {len(recent_agent_episodes)}",
        "",
        "## Topic frequency (recent agent episodes)",
    ]
    for topic, count in topic_frequency.items():
        lines.append(f"  - {topic}: {count}")
    return "\n".join(lines)


CELLS = [raw_episodes, agent_episodes, recent_episodes,
         recent_agent_episodes, topic_frequency, report]
