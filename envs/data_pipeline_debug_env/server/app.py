"""FastAPI application for the data pipeline debugging environment."""

from __future__ import annotations

from textwrap import dedent

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

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

app = create_app(
    DataPipelineDebugEnvironment,
    DataPipelineDebugAction,
    DataPipelineDebugObservation,
    env_name="data_pipeline_debug_env",
    max_concurrent_envs=1,
)

_demo_env = DataPipelineDebugEnvironment()


def _dump_model(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    return value


def _demo_payload(observation):
    return {
        "observation": _dump_model(observation),
        "state": _dump_model(_demo_env.state),
    }


@app.post("/demo/reset", include_in_schema=False)
async def demo_reset(request: Request) -> JSONResponse:
    payload = await request.json()
    try:
        observation = _demo_env.reset(
            difficulty=payload.get("difficulty"),
            task_id=payload.get("task_id"),
            episode_id=payload.get("episode_id", "demo-web"),
        )
    except Exception as exc:  # pragma: no cover - demo safety
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(_demo_payload(observation))


@app.post("/demo/step", include_in_schema=False)
async def demo_step(request: Request) -> JSONResponse:
    payload = await request.json()
    try:
        action = DataPipelineDebugAction.model_validate(payload)
        observation = _demo_env.step(action)
    except Exception as exc:  # pragma: no cover - demo safety
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse(_demo_payload(observation))


LANDING_PAGE = dedent(
    """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Data Pipeline Debug Holo Deck</title>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;800&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet" />
        <style>
          :root {
            --bg: #070a12;
            --bg2: #090f1a;
            --surface: rgba(10, 18, 30, 0.74);
            --panel-2: rgba(10, 18, 30, 0.92);
            --text: #ecf5ff;
            --muted: #97aeca;
            --line: rgba(94, 227, 255, 0.22);
            --ok: #5ee3ff;
            --warn: #7df7b6;
            --info: #4b80ff;
            --danger: #ff6a85;
            --shadow: 0 10px 32px rgba(0, 0, 0, 0.55);
          }

          * { box-sizing: border-box; }

          body {
            margin: 0;
            font-family: "IBM Plex Sans", system-ui, sans-serif;
            color: var(--text);
            background:
              radial-gradient(1200px 700px at 10% -5%, rgba(94, 227, 255, 0.18), transparent 50%),
              radial-gradient(900px 600px at 90% -10%, rgba(75, 128, 255, 0.24), transparent 48%),
              linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);
            min-height: 100vh;
            overflow-x: hidden;
          }

          .back-grid {
            position: fixed;
            inset: 0;
            background-image:
              linear-gradient(rgba(94, 227, 255, 0.06) 1px, transparent 1px),
              linear-gradient(90deg, rgba(94, 227, 255, 0.06) 1px, transparent 1px);
            background-size: 42px 42px;
            mask-image: radial-gradient(circle at center, black 0%, transparent 85%);
            pointer-events: none;
            z-index: 0;
          }

          .shell {
            max-width: 1440px;
            margin: 0 auto;
            padding: 14px;
          }

          .topbar {
            position: sticky;
            top: 0;
            z-index: 20;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            margin: -14px -14px 14px;
            padding: 11px 24px;
            background: rgba(7, 10, 18, 0.84);
            border-bottom: 1px solid var(--line);
            backdrop-filter: blur(16px);
          }

          .brand {
            display: flex;
            gap: 10px;
            align-items: center;
            font-family: "Orbitron", sans-serif;
            font-weight: 700;
            letter-spacing: 0.02em;
          }

          .brand-mark {
            width: 32px;
            height: 32px;
            border-radius: 8px;
            display: grid;
            place-items: center;
            background: linear-gradient(140deg, var(--ok), var(--info));
            color: #011018;
            font-weight: 900;
            box-shadow: 0 0 24px rgba(94, 227, 255, 0.5);
          }

          .status {
            padding: 7px 12px;
            border-radius: 999px;
            border: 1px solid var(--line);
            color: var(--warn);
            background: rgba(125, 247, 182, 0.12);
            font-size: 13px;
            font-weight: 700;
          }

          .hero {
            background: linear-gradient(140deg, var(--panel-2), var(--surface));
            border: 1px solid var(--line);
            border-radius: 12px;
            box-shadow: var(--shadow);
            padding: 22px;
          }

          .eyebrow {
            display: inline-block;
            padding: 7px 12px;
            border-radius: 999px;
            background: rgba(94, 227, 255, 0.12);
            color: var(--ok);
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 14px;
            font-family: "Orbitron", sans-serif;
          }

          h1 {
            margin: 0 0 10px;
            font-family: "Orbitron", sans-serif;
            font-size: clamp(32px, 4.5vw, 54px);
            line-height: 0.95;
            letter-spacing: -0.02em;
          }

          .lead {
            max-width: 760px;
            color: var(--muted);
            font-size: 17px;
            line-height: 1.65;
            margin: 0;
          }

          .hero-grid,
          .demo-grid,
          .json-grid,
          .task-grid,
          .endpoint-grid {
            display: grid;
            gap: 16px;
            margin-top: 24px;
          }

          .hero-grid { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
          .demo-grid { grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr); align-items: start; }
          .json-grid { grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); }
          .task-grid { grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
          .endpoint-grid { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }

          .scene {
            position: relative;
            min-height: 340px;
            border-radius: 12px;
            border: 1px solid var(--line);
            overflow: hidden;
            background:
              radial-gradient(ellipse at center, rgba(94, 227, 255, 0.12), transparent 55%),
              linear-gradient(180deg, rgba(9, 18, 36, 0.95), rgba(8, 13, 26, 0.95));
            perspective: 1000px;
          }

          .track {
            position: absolute;
            left: 10%;
            right: 10%;
            bottom: 18%;
            height: 120px;
            transform: rotateX(65deg);
          }

          .track::before, .track::after {
            content: "";
            position: absolute;
            inset: 0;
            border: 1px solid rgba(94, 227, 255, 0.34);
            border-radius: 50%;
          }

          .track::after {
            inset: 16px;
            border-color: rgba(75, 128, 255, 0.36);
          }

          .runner {
            position: absolute;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            top: 50%;
            left: 50%;
            margin: -10px 0 0 -10px;
            background: var(--warn);
            box-shadow: 0 0 20px rgba(125, 247, 182, 0.8);
            transform: rotateZ(0deg) translateX(180px);
            animation: orbit 5s linear infinite;
          }

          .runner.r2 {
            animation-duration: 7s;
            background: var(--ok);
            box-shadow: 0 0 20px rgba(94, 227, 255, 0.8);
            transform: rotateZ(140deg) translateX(145px);
          }

          .runner.r3 {
            animation-duration: 4s;
            background: #9f99ff;
            box-shadow: 0 0 20px rgba(159, 153, 255, 0.8);
            transform: rotateZ(250deg) translateX(110px);
          }

          @keyframes orbit {
            from { transform: rotateZ(0deg) translateX(180px); }
            to { transform: rotateZ(360deg) translateX(180px); }
          }

          .cube {
            position: absolute;
            width: 74px;
            height: 74px;
            transform-style: preserve-3d;
            animation: spin 10s linear infinite;
          }

          .cube.c1 { top: 22%; left: 14%; }
          .cube.c2 { top: 14%; right: 14%; animation-direction: reverse; animation-duration: 12s; }

          .face {
            position: absolute;
            width: 74px;
            height: 74px;
            border: 1px solid rgba(94, 227, 255, 0.45);
            background: rgba(94, 227, 255, 0.08);
          }

          .f1 { transform: rotateY(0deg) translateZ(37px); }
          .f2 { transform: rotateY(90deg) translateZ(37px); }
          .f3 { transform: rotateY(180deg) translateZ(37px); }
          .f4 { transform: rotateY(-90deg) translateZ(37px); }
          .f5 { transform: rotateX(90deg) translateZ(37px); }
          .f6 { transform: rotateX(-90deg) translateZ(37px); }

          @keyframes spin {
            from { transform: rotateX(0deg) rotateY(0deg); }
            to { transform: rotateX(360deg) rotateY(360deg); }
          }

          .holo-ring {
            position: absolute;
            width: 280px;
            height: 280px;
            border: 2px solid rgba(94, 227, 255, 0.25);
            border-radius: 50%;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            animation: ringPulse 6s ease-in-out infinite;
          }

          .holo-ring.r2 { width: 340px; height: 340px; animation-delay: 1.5s; border-color: rgba(125, 247, 182, 0.2); }
          .holo-ring.r3 { width: 400px; height: 400px; animation-delay: 3s; border-color: rgba(75, 128, 255, 0.18); }

          @keyframes ringPulse {
            0%, 100% { opacity: 0.2; transform: translate(-50%, -50%) scale(0.94); }
            50% { opacity: 0.6; transform: translate(-50%, -50%) scale(1); }
          }

          .pipeline-label {
            position: absolute;
            padding: 6px 10px;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(10, 18, 30, 0.92);
            font-size: 11px;
            color: var(--ok);
            font-family: "Orbitron", sans-serif;
            letter-spacing: 0.08em;
          }

          .p1 { left: 6%; top: 12%; }
          .p2 { left: 40%; top: 7%; }
          .p3 { right: 8%; top: 14%; }
          .p4 { left: 10%; bottom: 12%; }
          .p5 { right: 10%; bottom: 18%; }

          .pipeline-band {
            margin-top: 14px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
          }

          .stage-card {
            position: relative;
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 12px;
            background: linear-gradient(160deg, rgba(255, 255, 255, 0.06), rgba(255, 255, 255, 0.01));
            overflow: hidden;
          }

          .stage-card::after {
            content: "";
            position: absolute;
            width: 180%;
            height: 140%;
            left: -40%;
            top: -20%;
            background: linear-gradient(90deg, transparent, rgba(94, 227, 255, 0.22), transparent);
            transform: rotate(12deg) translateX(-120%);
            animation: scan 4.8s linear infinite;
          }

          .stage-card:nth-child(2)::after { animation-delay: 0.6s; }
          .stage-card:nth-child(3)::after { animation-delay: 1.2s; }
          .stage-card:nth-child(4)::after { animation-delay: 1.8s; }
          .stage-card:nth-child(5)::after { animation-delay: 2.4s; }

          @keyframes scan {
            from { transform: rotate(12deg) translateX(-120%); }
            to { transform: rotate(12deg) translateX(120%); }
          }

          .stage-card strong {
            display: block;
            font-family: "Orbitron", sans-serif;
            font-size: 13px;
            letter-spacing: 0.08em;
            color: var(--ok);
          }

          .stage-card span {
            font-size: 12px;
            color: var(--muted);
          }

          .actions {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 16px;
          }

          .btn {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 11px 15px;
            border-radius: 8px;
            text-decoration: none;
            font-weight: 700;
            border: 1px solid var(--line);
            color: var(--text);
            background: rgba(0, 153, 0, 0.17);
          }

          .btn.secondary {
            background: rgba(168, 218, 220, 0.12);
            color: var(--info);
          }

          .stat, .panel, .card, .endpoint {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 10px;
            padding: 16px;
          }

          .stat strong {
            display: block;
            font-size: 28px;
            margin-bottom: 5px;
          }

          .stat span, .card p, .endpoint p, .footnote, .small {
            color: var(--muted);
          }

          .section {
            margin-top: 18px;
          }

          .section h2 {
            margin: 0 0 12px;
            font-size: 22px;
            letter-spacing: -0.03em;
          }

          .controls {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
            gap: 10px;
            margin-bottom: 12px;
          }

          label {
            display: grid;
            gap: 6px;
            color: var(--muted);
            font-size: 12px;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            font-weight: 700;
          }

          select, textarea, input {
            width: 100%;
            border: 1px solid var(--line);
            border-radius: 8px;
            background: #071107;
            color: var(--text);
            font: inherit;
            padding: 10px 12px;
          }

          textarea {
            min-height: 280px;
            resize: vertical;
            font-family: "IBM Plex Mono", "Cascadia Code", Consolas, monospace;
          }

          .button-row {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
          }

          button {
            border: 1px solid rgba(0, 204, 102, 0.30);
            background: rgba(0, 153, 0, 0.18);
            color: var(--text);
            padding: 10px 14px;
            border-radius: 8px;
            font-weight: 700;
            cursor: pointer;
          }

          button.secondary {
            background: rgba(168, 218, 220, 0.10);
            border-color: rgba(168, 218, 220, 0.28);
            color: var(--info);
          }

          .badge {
            display: inline-block;
            padding: 6px 10px;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: rgba(255, 255, 255, 0.04);
            color: var(--ok);
            font-size: 12px;
            font-weight: 700;
          }

          .kpis {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 10px;
            margin-top: 12px;
          }

          .kpi {
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 10px;
            background: rgba(255, 255, 255, 0.03);
          }

          .kpi span {
            display: block;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
          }

          .kpi strong {
            display: block;
            margin-top: 6px;
            font-size: 22px;
          }

          pre {
            font-family: "IBM Plex Mono", "Cascadia Code", Consolas, monospace;
            padding: 12px;
            overflow: auto;
            border-radius: 8px;
            background: #020502;
            border: 1px solid rgba(0, 153, 0, 0.16);
            color: #d8ffee;
            max-height: 260px;
            margin: 0;
            font-size: 12px;
            line-height: 1.5;
            white-space: pre-wrap;
          }

          .result {
            margin-top: 12px;
            padding: 12px;
            border: 1px solid rgba(0, 204, 102, 0.24);
            background: rgba(0, 204, 102, 0.10);
            border-radius: 8px;
          }

          .error {
            margin-top: 12px;
            padding: 12px;
            border: 1px solid rgba(230, 57, 70, 0.30);
            background: rgba(230, 57, 70, 0.12);
            color: #ffb8c0;
            border-radius: 8px;
          }

          .endpoint a {
            color: var(--ok);
            text-decoration: none;
            font-weight: 700;
          }

          .endpoint a:hover { text-decoration: underline; }

          @media (max-width: 980px) {
            .demo-grid { grid-template-columns: 1fr; }
            .json-grid { grid-template-columns: 1fr; }
          }
        </style>
      </head>
      <body>
        <div class="back-grid"></div>
        <main class="shell">
          <div class="topbar">
            <div class="brand">
              <span class="brand-mark">DP</span>
              Data Pipeline Holo Deck
            </div>
            <div class="status">Live Runtime</div>
          </div>

          <section class="hero">
            <div class="eyebrow">OpenEnv - Data Pipeline Debugging</div>
            <h1>Realistic ETL Incident Simulator with 3D live motion.</h1>
            <p class="lead">
              Diagnose and repair broken production-like pipelines across easy, medium, and hard tasks.
              This interface complements your existing content with a holographic 3D dataflow scene and live grading console.
            </p>
            <div class="actions">
              <a class="btn" href="/docs">Open API Docs</a>
              <a class="btn secondary" href="/openapi.json">OpenAPI Schema</a>
            </div>

            <div class="hero-grid">
              <div class="stat"><strong>3</strong><span>Difficulty Levels</span></div>
              <div class="stat"><strong>Hidden Eval</strong><span>Anti-overfit Grading</span></div>
              <div class="stat"><strong>Deterministic</strong><span>Grading</span></div>
              <div class="stat"><strong>Live Demo</strong><span>Reset + Step Console</span></div>
            </div>

            <div class="pipeline-band">
              <div class="stage-card"><strong>INGEST</strong><span>Raw source pulls and anomaly entry point.</span></div>
              <div class="stage-card"><strong>VALIDATE</strong><span>Null checks, type checks, schema contracts.</span></div>
              <div class="stage-card"><strong>TRANSFORM</strong><span>Business-logic fixes and drift correction.</span></div>
              <div class="stage-card"><strong>ENRICH</strong><span>Dependency-safe joins and derived fields.</span></div>
              <div class="stage-card"><strong>PUBLISH</strong><span>Deterministic grader reward and feedback.</span></div>
            </div>

            <div class="demo-grid">
              <article class="scene">
                <div class="holo-ring"></div>
                <div class="holo-ring r2"></div>
                <div class="holo-ring r3"></div>
                <div class="track">
                  <div class="runner"></div>
                  <div class="runner r2"></div>
                  <div class="runner r3"></div>
                </div>
                <div class="cube c1">
                  <div class="face f1"></div><div class="face f2"></div><div class="face f3"></div>
                  <div class="face f4"></div><div class="face f5"></div><div class="face f6"></div>
                </div>
                <div class="cube c2">
                  <div class="face f1"></div><div class="face f2"></div><div class="face f3"></div>
                  <div class="face f4"></div><div class="face f5"></div><div class="face f6"></div>
                </div>
                <div class="pipeline-label p1">INGEST</div>
                <div class="pipeline-label p2">VALIDATE</div>
                <div class="pipeline-label p3">TRANSFORM</div>
                <div class="pipeline-label p4">ENRICH</div>
                <div class="pipeline-label p5">PUBLISH</div>
              </article>

              <article class="panel">
                <h3 style="margin-top:0;">Live Debug Console</h3>
                <div class="controls">
                  <label>
                    Difficulty
                    <select id="difficulty">
                      <option value="easy">easy</option>
                      <option value="medium">medium</option>
                      <option value="hard">hard</option>
                    </select>
                  </label>
                  <label>
                    Task ID
                    <select id="taskId">
                      <option value="easy_csv_null_type">easy_csv_null_type</option>
                      <option value="medium_schema_drift">medium_schema_drift</option>
                      <option value="hard_dependency_chain">hard_dependency_chain</option>
                    </select>
                  </label>
                </div>
                <div class="button-row">
                  <button onclick="resetDemo()">Reset Episode</button>
                  <button class="secondary" onclick="submitStep()">Submit Candidate</button>
                </div>
                <p class="small">Tip: Reset loads the broken pipeline into the editor. Modify it and submit.</p>
                <label style="margin-top:10px;">
                  Candidate Pipeline
                  <textarea id="candidatePipeline"></textarea>
                </label>
                <div id="resultBox" class="result">Press Reset Episode to start.</div>
              </article>

            </div>

            <div class="json-grid">
              <article class="panel">
                <h3 style="margin-top:0;">Episode Snapshot</h3>
                <div class="kpis">
                  <div class="kpi"><span>Task</span><strong id="kTask">-</strong></div>
                  <div class="kpi"><span>Reward</span><strong id="kReward">-</strong></div>
                  <div class="kpi"><span>Score</span><strong id="kScore">-</strong></div>
                  <div class="kpi"><span>Attempts Left</span><strong id="kAttempts">-</strong></div>
                </div>
                <div style="margin-top:12px;">
                  <span class="badge" id="kPass">Awaiting run</span>
                </div>
                <h4 style="margin:14px 0 8px;">Prompt</h4>
                <pre id="promptBox">{}</pre>
                <h4 style="margin:14px 0 8px;">Feedback</h4>
                <pre id="feedbackBox">{}</pre>
              </article>
              <article class="panel">
                <h3 style="margin-top:0;">Observation JSON</h3>
                <pre id="obsBox">{}</pre>
              </article>
              <article class="panel">
                <h3 style="margin-top:0;">State JSON</h3>
                <pre id="stateBox">{}</pre>
              </article>
            </div>
          </section>

          <section class="section">
            <h2>Task Ladder</h2>
            <div class="task-grid">
              <article class="card">
                <h3>Easy - CSV Null / Type Repair</h3>
                <p>Repair missing values, type coercion bugs, and schema correctness in cleaned tabular output.</p>
                <span class="badge">Easy</span>
              </article>
              <article class="card">
                <h3>Medium - Schema Drift Recovery</h3>
                <p>Fix multi-step customer payment pipeline under mixed naming conventions and status drift.</p>
                <span class="badge">Medium</span>
              </article>
              <article class="card">
                <h3>Hard - Dependency Chain Debugging</h3>
                <p>Restore stage compatibility so upstream changes do not silently break downstream enrichment.</p>
                <span class="badge">Hard</span>
              </article>
            </div>
          </section>

          <section class="section">
            <h2>Endpoints</h2>
            <div class="endpoint-grid">
              <article class="endpoint">
                <h3><a href="/health">/health</a></h3>
                <p>Runtime liveness check.</p>
              </article>
              <article class="endpoint">
                <h3><a href="/metadata">/metadata</a></h3>
                <p>Environment metadata summary.</p>
              </article>
              <article class="endpoint">
                <h3><a href="/schema">/schema</a></h3>
                <p>Action, observation, and state schemas.</p>
              </article>
              <article class="endpoint">
                <h3><a href="/docs">/docs</a></h3>
                <p>Interactive OpenAPI docs.</p>
              </article>
            </div>
            <p class="footnote">This visual layer is for human demos. Official evaluation still uses standard OpenEnv endpoints.</p>
          </section>
        </main>

        <script>
          let currentObservation = null;

          function setTaskFromDifficulty() {
            const difficulty = document.getElementById("difficulty").value;
            const map = {
              easy: "easy_csv_null_type",
              medium: "medium_schema_drift",
              hard: "hard_dependency_chain",
            };
            document.getElementById("taskId").value = map[difficulty];
          }

          function setDifficultyFromTask() {
            const task = document.getElementById("taskId").value;
            const map = {
              easy_csv_null_type: "easy",
              medium_schema_drift: "medium",
              hard_dependency_chain: "hard",
            };
            document.getElementById("difficulty").value = map[task] || "easy";
          }

          async function postJson(url, payload) {
            const response = await fetch(url, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload),
            });
            return await response.json();
          }

          function setMessage(text, isError = false) {
            const box = document.getElementById("resultBox");
            box.className = isError ? "error" : "result";
            box.textContent = text;
          }

          function render(payload) {
            if (payload.error) {
              setMessage(payload.error, true);
              return;
            }
            const obs = payload.observation || {};
            const state = payload.state || {};
            currentObservation = obs;

            document.getElementById("kTask").textContent = obs.task_id || "-";
            document.getElementById("kReward").textContent =
              typeof obs.reward === "number" ? obs.reward.toFixed(2) : "-";
            document.getElementById("kScore").textContent =
              typeof obs.score === "number" ? obs.score.toFixed(2) : "-";
            document.getElementById("kAttempts").textContent = String(obs.attempts_remaining ?? "-");
            document.getElementById("kPass").textContent = obs.passed ? "Passed" : (obs.done ? "Done" : "In progress");

            document.getElementById("promptBox").textContent = obs.prompt || "{}";
            document.getElementById("feedbackBox").textContent = obs.feedback || "{}";
            document.getElementById("obsBox").textContent = JSON.stringify(obs, null, 2);
            document.getElementById("stateBox").textContent = JSON.stringify(state, null, 2);

            if (obs.broken_pipeline) {
              document.getElementById("candidatePipeline").value = obs.last_submission || obs.broken_pipeline;
            }

            const parts = [
              `done=${Boolean(obs.done)}`,
              `passed=${Boolean(obs.passed)}`,
              typeof obs.reward === "number" ? `reward=${obs.reward.toFixed(2)}` : "reward=-",
              typeof obs.score === "number" ? `score=${obs.score.toFixed(2)}` : "score=-",
            ];
            setMessage(parts.join(" | "), false);
          }

          async function resetDemo() {
            setTaskFromDifficulty();
            const difficulty = document.getElementById("difficulty").value;
            const taskId = document.getElementById("taskId").value;
            const data = await postJson("/demo/reset", {
              difficulty: difficulty,
              task_id: taskId,
              episode_id: "web-demo",
            });
            render(data);
          }

          async function submitStep() {
            if (!currentObservation) {
              setMessage("Reset episode first.", true);
              return;
            }
            const candidate = document.getElementById("candidatePipeline").value;
            const data = await postJson("/demo/step", {
              candidate_pipeline: candidate,
            });
            render(data);
          }

          document.getElementById("difficulty").addEventListener("change", setTaskFromDifficulty);
          document.getElementById("taskId").addEventListener("change", setDifficultyFromTask);
          setTaskFromDifficulty();
          resetDemo();
        </script>
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


@app.get("/metadata", include_in_schema=False)
async def metadata() -> dict[str, object]:
    """Hackathon-friendly metadata endpoint for quick environment introspection."""
    return {
        "name": "data_pipeline_debug_env",
        "domain": "Data Pipeline Debugging",
        "tasks": [
            {"difficulty": "easy", "task_id": "easy_csv_null_type"},
            {"difficulty": "medium", "task_id": "medium_schema_drift"},
            {"difficulty": "hard", "task_id": "hard_dependency_chain"},
        ],
        "reward_range": {"min_exclusive": 0.0, "max_exclusive": 1.0},
        "max_attempts_per_episode": 3,
    }


def main():
    """Run the server locally."""

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
