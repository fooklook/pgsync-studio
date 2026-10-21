"""Verify a wheel installation from a temporary directory outside the checkout."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def main():
    with tempfile.TemporaryDirectory(prefix="pgsync-studio-install-") as temp:
        env = os.environ.copy()
        for key in list(env):
            if key.startswith(("PGSYNC_STUDIO_", "NACOS_")) or key == "PYTHONPATH":
                env.pop(key)
        env.update({
            "PG_USER": "studio", "ELASTICSEARCH": "true", "OPENSEARCH": "false",
            "PGSYNC_STUDIO_PROFILE": "dev", "NACOS_ENABLED": "false",
            "PGSYNC_STUDIO_HOME": str(Path(temp) / "runtime"),
            "PGSYNC_STUDIO_CONFIG": str(Path(temp) / "config.json"),
            "PGSYNC_STUDIO_HOST": "127.0.0.1",
        })
        probe = subprocess.run(
            [sys.executable, "-c", "import json,pgsync; from pgsync.web.app import static_dir; from pathlib import Path; print(json.dumps({'module':pgsync.__file__,'assets':all((Path(static_dir())/x).is_file() for x in ['index.html','app.js','styles.css','favicon.ico'])}))"],
            cwd=temp, env=env, check=True, capture_output=True, text=True,
        )
        info = json.loads(probe.stdout)
        assert "site-packages" in info["module"], "Install the wheel before this check; an editable checkout is not sufficient."
        assert info["assets"], "Installed wheel is missing UI assets."
        subprocess.run(
            [sys.executable, "-m", "pgsync.sync", "--help"],
            cwd=temp, env=env, check=True, capture_output=True, text=True,
        )
        runner_probe = (
            "import os,subprocess; from types import SimpleNamespace; "
            "from pgsync.web.sync_manager import NativeProcessRunner; "
            "r=NativeProcessRunner.__new__(NativeProcessRunner); "
            "r.manager=SimpleNamespace(repo_root=os.getcwd()); r.schema_path='demo.json'; "
            "subprocess.run(r._native_command(daemon=False,bootstrap=False)+['--help'],check=True,capture_output=True)"
        )
        subprocess.run(
            [sys.executable, "-c", runner_probe], cwd=temp, env=env,
            check=True, capture_output=True, text=True,
        )
        print("PASS installed native CLI and UI assets outside the checkout", flush=True)

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        env["PGSYNC_STUDIO_PORT"] = str(port)
        log_path = Path(temp) / "server.log"
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(
                [sys.executable, "-m", "pgsync.web"], cwd=temp, env=env,
                stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        raise AssertionError("Installed console exited during startup.")
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/bootstrap", timeout=2) as response:
                            payload = json.load(response)
                        break
                    except (OSError, urllib.error.URLError):
                        if time.monotonic() >= deadline:
                            raise AssertionError("Installed console did not become ready.")
                        time.sleep(0.2)
                assert payload["ok"] and payload["data"]["jobs"] == []
                for path in ["/", "/static/app.js", "/static/styles.css", "/favicon.ico", "/metrics"]:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
                        assert response.status == 200 and response.read()
                print("PASS installed console bootstrap, static assets, and metrics over HTTP", flush=True)
            finally:
                if os.name == "nt" and process.poll() is None:
                    # The Windows venv redirector can own a second Python
                    # process; stop the probe's entire tree before temp cleanup.
                    subprocess.run(
                        ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                        check=False, capture_output=True,
                    )
                else:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
