"""FastAPI application for the data pipeline debugging environment."""

from __future__ import annotations

from textwrap import dedent

try:
    from openenv.core.env_server.http_server import create_app
except Exception as e:  # pragma: no cover
    raise ImportError(
        "openenv is required for the web interface. Install dependencies in this "
        "environment directory before running the server."
    ) from e

try:
    from models import DataPipelineDebugAction, DataPipelineDebugObservation
except ImportError:
    from models import DataPipelineDebugAction, DataPipelineDebugObservation
try:
    from .data_pipeline_debug_environment import DataPipelineDebugEnvironment
except ImportError:
    from data_pipeline_debug_environment import DataPipelineDebugEnvironment

from fastapi.responses import HTMLResponse

app = create_app(
    DataPipelineDebugEnvironment,
    DataPipelineDebugAction,
    DataPipelineDebugObservation,
    env_name="data_pipeline_debug_env",
    max_concurrent_envs=1,
)

LANDING_PAGE = dedent(
    """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Data Pipeline Debug Environment</title>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet" />
        <style>
          :root {
            --bg: #0e0f17;
            --bg-soft: #191b2a;
            --panel: rgba(18, 21, 35, 0.88);
            --panel-soft: rgba(255, 255, 255, 0.05);
            --text: #edf2ff;
            --muted: #aab5cc;
            --accent: #68e7c2;
            --accent-2: #ffd16b;
            --accent-3: #89a8ff;
            --line: rgba(255, 255, 255, 0.12);
            --shadow: 0 30px 90px rgba(0, 0, 0, 0.45);
          }

          * { box-sizing: border-box; }
          body {
            margin: 0;
            font-family: "IBM Plex Sans", system-ui, sans-serif;
            color: var(--text);
            background:
              radial-gradient(circle at 8% 10%, rgba(255, 209, 107, 0.18), transparent 24%),
              radial-gradient(circle at 92% 8%, rgba(137, 168, 255, 0.24), transparent 28%),
              radial-gradient(circle at 30% 85%, rgba(104, 231, 194, 0.16), transparent 35%),
              linear-gradient(180deg, var(--bg-soft) 0%, var(--bg) 75%);
            min-height: 100vh;
            overflow-x: hidden;
          }

          body::before {
            content: "";
            position: fixed;
            inset: -30vh -20vw auto -20vw;
            height: 50vh;
            background: linear-gradient(90deg, rgba(137, 168, 255, 0.14), rgba(104, 231, 194, 0.09), rgba(255, 209, 107, 0.11));
            filter: blur(48px);
            pointer-events: none;
            animation: drift 12s ease-in-out infinite alternate;
            z-index: 0;
          }

          @keyframes drift {
            from { transform: translateX(-3%); }
            to { transform: translateX(3%); }
          }

          @keyframes fadeUp {
            from { opacity: 0; transform: translateY(12px); }
            to { opacity: 1; transform: translateY(0); }
          }

          .shell {
            position: relative;
            z-index: 2;
            max-width: 1220px;
            margin: 26px auto;
            padding: 22px;
          }

          .hero {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 30px;
            box-shadow: var(--shadow);
            overflow: hidden;
            backdrop-filter: blur(9px);
          }

          .hero-top {
            padding: 32px 30px 20px;
            border-bottom: 1px solid var(--line);
            background:
              linear-gradient(120deg, rgba(137, 168, 255, 0.14), transparent 42%),
              linear-gradient(220deg, rgba(104, 231, 194, 0.14), transparent 36%),
              linear-gradient(340deg, rgba(255, 209, 107, 0.11), transparent 30%);
            animation: fadeUp 0.8s ease both;
          }

          .eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.07);
            color: var(--accent);
            font-size: 13px;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }

          h1 {
            margin: 18px 0 10px;
            font-family: "Space Grotesk", "IBM Plex Sans", system-ui, sans-serif;
            font-size: clamp(34px, 6vw, 64px);
            line-height: 0.92;
            letter-spacing: -0.03em;
            max-width: 900px;
          }

          .lead {
            max-width: 790px;
            color: var(--muted);
            font-size: 18px;
            line-height: 1.6;
            margin: 0;
          }

          .actions {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-top: 22px;
          }

          .button {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            padding: 12px 16px;
            border-radius: 14px;
            text-decoration: none;
            font-weight: 700;
            font-family: "Space Grotesk", "IBM Plex Sans", system-ui, sans-serif;
            border: 1px solid var(--line);
            color: var(--text);
            background: rgba(255, 255, 255, 0.03);
            transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
          }

          .button:hover {
            transform: translateY(-1px);
            border-color: rgba(255, 255, 255, 0.28);
            background: rgba(255, 255, 255, 0.08);
          }

          .button.primary {
            background: linear-gradient(135deg, var(--accent), var(--accent-3));
            color: #0b1220;
            border: none;
            box-shadow: 0 12px 30px rgba(104, 231, 194, 0.22);
          }

          .metrics {
            display: grid;
            grid-template-columns: repeat(3, minmax(120px, 1fr));
            gap: 10px;
            margin-top: 20px;
            max-width: 560px;
          }

          .metric {
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 12px;
            background: rgba(255, 255, 255, 0.04);
          }

          .metric strong {
            display: block;
            font-family: "Space Grotesk", sans-serif;
            font-size: 22px;
            line-height: 1;
            margin-bottom: 6px;
          }

          .metric span {
            color: var(--muted);
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
          }

          .grid {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 18px;
            padding: 24px 30px 30px;
          }

          .card {
            grid-column: span 4;
            background: var(--panel-soft);
            border: 1px solid var(--line);
            border-radius: 22px;
            padding: 20px;
            animation: fadeUp 0.65s ease both;
            transition: border-color 160ms ease, transform 160ms ease, background 160ms ease;
          }

          .card:hover {
            border-color: rgba(255, 255, 255, 0.26);
            transform: translateY(-2px);
            background: rgba(255, 255, 255, 0.08);
          }

          .card.wide { grid-column: span 6; }
          .card.full { grid-column: span 12; }

          .label {
            color: var(--accent-2);
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 10px;
          }

          h2, h3 {
            margin: 0 0 10px;
            line-height: 1.1;
          }

          p, li {
            color: var(--muted);
            line-height: 1.6;
          }

          code, pre {
            font-family: "IBM Plex Mono", "Cascadia Code", Consolas, monospace;
          }

          pre {
            margin: 0;
            padding: 16px;
            overflow: auto;
            border-radius: 16px;
            background: rgba(0, 0, 0, 0.24);
            border: 1px solid rgba(255, 255, 255, 0.08);
            color: #d8ffee;
            font-size: 14px;
          }

          .pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
          }

          .pill {
            display: inline-flex;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.06);
            border: 1px solid var(--line);
            color: var(--text);
            font-size: 13px;
            font-weight: 500;
          }

          @media (max-width: 900px) {
            .card, .card.wide, .card.full { grid-column: span 12; }
            .shell { padding: 14px; margin: 14px auto; }
            .hero-top, .grid { padding-left: 18px; padding-right: 18px; }
            .metrics { grid-template-columns: 1fr; max-width: none; }
          }
        </style>
      </head>
      <body>
        <main class="shell">
          <section class="hero">
            <div class="hero-top">
              <div class="eyebrow">OpenEnv | Data Pipeline Debugging</div>
              <h1>Debug ETL pipelines like a real data team.</h1>
              <p class="lead">
                This environment evaluates whether an agent can diagnose and repair broken
                data pipelines with deterministic grading across schema mismatches, null
                handling, type conversion bugs, aggregation drift, and dependency-chain failures.
              </p>
              <div class="actions">
                <a class="button primary" href="/docs">Open API Docs</a>
                <a class="button" href="/openapi.json">OpenAPI Schema</a>
              </div>
              <div class="metrics">
                <div class="metric"><strong>3</strong><span>Difficulty Levels</span></div>
                <div class="metric"><strong>1.00</strong><span>Max Step Reward</span></div>
                <div class="metric"><strong>Deterministic</strong><span>Grading</span></div>
              </div>
            </div>

            <div class="grid">
              <article class="card">
                <div class="label">Easy</div>
                <h3>CSV Null / Type Repair</h3>
                <p>Fix missing values, blank strings, and incorrect type coercion in a tabular cleanup stage.</p>
              </article>

              <article class="card">
                <div class="label">Medium</div>
                <h3>Schema Drift Recovery</h3>
                <p>Repair a multi-step customer payments pipeline where upstream field names and status values drift.</p>
              </article>

              <article class="card">
                <div class="label">Hard</div>
                <h3>Dependency Chain Debugging</h3>
                <p>Resolve a staged ETL break where fixing one transform can silently break downstream enrichment.</p>
              </article>

              <article class="card wide">
                <div class="label">What The Agent Sees</div>
                <div class="pill-row">
                  <span class="pill">broken pipeline code</span>
                  <span class="pill">expected schema contract</span>
                  <span class="pill">deterministic sample data</span>
                  <span class="pill">pass/fail grading feedback</span>
                </div>
              </article>

              <article class="card wide">
                <div class="label">Evaluation Signal</div>
                <p>
                  Rewards are dense and deterministic. Each step combines schema correctness, type fidelity,
                  and output-value accuracy, with explicit penalties for unsafe or broken code paths.
                </p>
                <div class="pill-row">
                  <span class="pill">schema score (35%)</span>
                  <span class="pill">type score (25%)</span>
                  <span class="pill">value score (40%)</span>
                  <span class="pill">bounded reward [0.0-1.0]</span>
                </div>
              </article>

              <article class="card full">
                <div class="label">Built For Teams</div>
                <pre>- Reproduces real ETL debugging workflows used in data teams
- Supports curriculum-style evaluation from easy to hard dependency chains
- Gives transparent grader signals for model iteration and benchmarking</pre>
              </article>
            </div>
          </section>
        </main>
      </body>
    </html>
    """
).strip()


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def landing_page() -> str:
    return LANDING_PAGE


@app.get("/web", response_class=HTMLResponse, include_in_schema=False)
async def landing_page_web() -> str:
    return LANDING_PAGE


def main():
    """Run the server locally."""

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
