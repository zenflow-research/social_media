"""Start the news scraper server in a clean environment (no CLAUDECODE).

This allows Claude CLI subprocess calls to work from within the server.
Runs on port 5000 to avoid conflicts.
"""
import os
import subprocess
import sys

env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

subprocess.run(
    [sys.executable, "-m", "uvicorn", "app.main:app",
     "--host", "0.0.0.0", "--port", "5000"],
    env=env,
    cwd=os.path.dirname(os.path.abspath(__file__)),
)
