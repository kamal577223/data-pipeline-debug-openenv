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
        <style>
          :root {
            --bg: #f3eed8;
            --panel: rgba(10, 16, 30, 0.92);
            --panel-soft: rgba(255, 255, 255, 0.08);
            --text: #f8f4e8;
            --muted: #b8c1d1;
            --accent: #76e3c4;
            --accent-2: #ffcf70;
            --line: rgba(255, 255, 255, 0.14);
            --shadow: 0 24px 80px rgba(0, 0, 0, 0.28);
          }

          * { box-sizing: border-box; }
          body {
            margin: 0;
            font-family: "Segoe UI", "IBM Plex Sans", system-ui, sans-serif;
            color: var(--text);
            background:
              radial-gradient(circle at top left, rgba(255, 207, 112, 0.35), transparent 28%),
              radial-gradient(circle at top right, rgba(118, 227, 196, 0.22), transparent 24%),
              linear-gradient(180deg, #d7d39b 0%, #e7e3be 30%, #f3eed8 100%);
            min-height: 100vh;
          }

          .shell {
            max-width: 1200px;
            margin: 32px auto;
            padding: 24px;
          }

          .hero {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 28px;
            box-shadow: var(--shadow);
            overflow: hidden;
          }

          .hero-top {
            padding: 28px 28px 18px;
            border-bottom: 1px solid var(--line);
            background:
              linear-gradient(135deg, rgba(118, 227, 196, 0.12), transparent 45%),
              linear-gradient(225deg, rgba(255, 207, 112, 0.12), transparent 35%);
          }

          .eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.06);
            color: var(--accent);
            font-size: 13px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }

          h1 {
            margin: 18px 0 10px;
            font-size: clamp(34px, 6vw, 62px);
            line-height: 0.95;
            letter-spacing: -0.03em;
          }

          .lead {
            max-width: 760px;
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
            font-weight: 600;
            border: 1px solid var(--line);
            color: var(--text);
            background: rgba(255, 255, 255, 0.04);
          }

          .button.primary {
            background: linear-gradient(135deg, var(--accent), #58b7f7);
            color: #08111e;
            border: none;
          }

          .grid {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 18px;
            padding: 22px 28px 28px;
          }

          .card {
            grid-column: span 4;
            background: var(--panel-soft);
            border: 1px solid var(--line);
            border-radius: 22px;
            padding: 20px;
          }

          .card.wide { grid-column: span 6; }
          .card.full { grid-column: span 12; }

          .label {
            color: var(--accent-2);
            font-size: 12px;
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

          ul {
            margin: 0;
            padding-left: 18px;
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
            color: #cde8dc;
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
            font-size: 14px;
          }

          @media (max-width: 900px) {
            .card, .card.wide, .card.full { grid-column: span 12; }
            .shell { padding: 14px; margin: 14px auto; }
            .hero-top, .grid { padding-left: 18px; padding-right: 18px; }
          }
        </style>
      </head>
      <body>
        <main class="shell">
          <section class="hero">
            <div class="hero-top">
              <div class="eyebrow">OpenEnv • Data Pipeline Debugging</div>
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
                <div class="label">Why You Saw Not Found</div>
                <p>
                  The backend originally exposed only API endpoints, so visiting the Space root showed FastAPI's
                  default <code>{"detail":"Not Found"}</code> response. This page is the custom landing layer on top
                  of the environment API.
                </p>
              </article>

              <article class="card full">
                <div class="label">Quick API Flow</div>
                <pre>1. Reset an episode to get a task prompt and broken pipeline
2. Submit repaired Python code in `candidate_pipeline`
3. Receive deterministic grading feedback and reward</pre>
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


def main(host: str = "0.0.0.0", port: int = 8000):
    """Run the server locally."""

    import uvicorn

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    main(port=args.port)
