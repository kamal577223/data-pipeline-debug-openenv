"""FastAPI application for the data pipeline debugging environment."""

from __future__ import annotations

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
