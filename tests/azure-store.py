#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["azure-storage-blob==12.30.1"]
# ///
"""Azurite or live Azure contract + Git push/clone/pull/cold-restart smoke.

Requires Docker (or WALGIT_TEST_CONTAINER_RUNTIME=podman), or a local
Azurite 3.37.0 executable selected with WALGIT_TEST_AZURITE=azurite-blob;
also cargo, git, and
`uv run --script tests/azure-store.py`. The Azure SDK is used only
to create the test container and mint a SAS from a synthetic emulator key.
Without arguments, no Azure account or developer credential is used.
For an existing Azure container, pass --account NAME --container NAME.
Live mode uses the Azure CLI login and requires account-scoped Blob Data
Contributor access and an unset AZURE_STORAGE_SAS_TOKEN. It leaves the
container and its uniquely prefixed Git repository in place.
"""
import argparse
import base64
import contextlib
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, generate_container_sas

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0"
ACCOUNT = "walgittest"
KEY = base64.b64encode(b"walgit-azurite-synthetic-test-key").decode()
CONTAINER = "walgit-test"


def run(args, *, env=None, cwd=ROOT, timeout=600, capture=False):
    return subprocess.run(args, cwd=cwd, env=env, timeout=timeout, check=True,
                          text=True, capture_output=capture)


def stop_process(process):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


@contextlib.contextmanager
def server(binary, config, env, url, log_path):
    with log_path.open("w") as log:
        process = subprocess.Popen([str(binary), "--config", str(config)], env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(150):
                if process.poll() is not None:
                    raise RuntimeError("walgit exited during startup:\n" + log_path.read_text())
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1) as reply:
                        if reply.status == 200:
                            break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("walgit startup timed out:\n" + log_path.read_text())
            yield
        finally:
            stop_process(process)


def git_smoke(env, endpoint):
    print("Azure Git smoke: push, clone, change, pull, and clone from a cold server", flush=True)
    account = env.get("WALGIT_TEST_AZURE_ACCOUNT", "")
    credential = "azure_cli" if account else "auto"
    prefix = "git-smoke/" + uuid4().hex + "/"
    print(f"Azure Git smoke prefix: {prefix}", flush=True)
    run(["cargo", "build", "--locked", "-p", "walgit-cli", "--bin", "walgit-server",
         "--features", "walgit-store/azure"], env=env, timeout=900)
    target = Path(env.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    binary = target / "debug" / "walgit-server"
    with tempfile.TemporaryDirectory(prefix="walgit-azure-git-") as temp:
        directory = Path(temp)
        env = dict(env, GIT_CONFIG_GLOBAL=str(directory / "gitconfig"),
                   GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0")
        env.pop("PORT", None)
        env = {key: value for key, value in env.items() if not key.startswith("WALGIT__")}
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        repo_url = url + "/test/azure.git"
        config = directory / "walgit.toml"
        def configure(cache):
            config.write_text(f'''
[server]
listen = "127.0.0.1:{port}"
public_url = "{url}"
auto_create_on_push = true
roles = ["serve"]
[server.auth]
mode = "none"
[store]
backend = "azure"
bucket = {json.dumps(env["WALGIT_TEST_AZURE_CONTAINER"])}
prefix = "{prefix}"
multipart_threshold = "1MiB"
multipart_part_size = "256KiB"
[store.azure]
endpoint = {json.dumps(endpoint)}
account = {json.dumps(account)}
credential = "{credential}"
[cache]
dir = "{cache}"
mode = "budget"
max_bytes = "128MiB"
''')
        def git(*args, cwd=directory):
            return run(["git", "-c", "user.name=Azure Test", "-c", "user.email=azure-test@example.invalid",
                        *args], env=env, cwd=cwd, timeout=90)
        source = directory / "source"
        git("init", "-b", "main", str(source))
        payload = os.urandom(2 * 1024 * 1024 + 17)
        (source / "payload.bin").write_bytes(payload)
        (source / "README.md").write_text("Azure storage smoke\n")
        git("add", ".", cwd=source)
        git("commit", "-m", "Seed repository", cwd=source)
        configure(directory / "warm-cache")
        with server(binary, config, env, url, directory / "warm.log"):
            git("push", repo_url, "main", cwd=source)
            checkout = directory / "checkout"
            git("clone", "-b", "main", repo_url, str(checkout))
            assert (checkout / "payload.bin").read_bytes() == payload
            (checkout / "README.md").write_text("Changed through Azure-backed Git\n")
            git("add", "README.md", cwd=checkout)
            git("commit", "-m", "Update recipe", cwd=checkout)
            git("push", "origin", "main", cwd=checkout)
            git("pull", "--ff-only", repo_url, "main", cwd=source)
            assert (source / "README.md").read_text() == "Changed through Azure-backed Git\n"
        # Different, empty cache: the bucket must be sufficient to recover refs and packs.
        configure(directory / "cold-cache")
        with server(binary, config, env, url, directory / "cold.log"):
            cold = directory / "cold-clone"
            git("clone", "-b", "main", repo_url, str(cold))
            assert (cold / "payload.bin").read_bytes() == payload
            assert (cold / "README.md").read_text() == "Changed through Azure-backed Git\n"
    print("Azure Git smoke passed", flush=True)


@contextlib.contextmanager
def azurite():
    executable = os.environ.get("WALGIT_TEST_AZURITE")
    if executable:
        binary = shutil.which(executable)
        if binary is None:
            raise RuntimeError(f"Azurite executable not found: {executable}")
        with tempfile.TemporaryDirectory(prefix="walgit-azurite-") as directory:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            process = subprocess.Popen([
                binary, "--blobHost", "127.0.0.1", "--blobPort", str(port),
                "--location", directory, "--disableProductStyleUrl",
                "--disableTelemetry", "--silent",
            ], env=dict(os.environ, AZURITE_ACCOUNTS=f"{ACCOUNT}:{KEY}"))
            try:
                yield f"http://127.0.0.1:{port}/{ACCOUNT}"
            finally:
                stop_process(process)
        return
    runtime = os.environ.get("WALGIT_TEST_CONTAINER_RUNTIME", "docker")
    if shutil.which(runtime) is None:
        raise RuntimeError(f"{runtime} is required for the isolated Azurite test")
    name = "walgit-azure-test-" + uuid4().hex[:12]
    try:
        run([runtime, "run", "--detach", "--rm", "--name", name,
             "--publish", "127.0.0.1::10000", "--env", f"AZURITE_ACCOUNTS={ACCOUNT}:{KEY}",
             IMAGE, "azurite-blob", "--blobHost", "0.0.0.0", "--disableProductStyleUrl"], timeout=180)
        address = run([runtime, "port", name, "10000/tcp"], capture=True).stdout.strip().splitlines()[0]
        yield f"http://{address}/{ACCOUNT}"
    finally:
        subprocess.run([runtime, "rm", "--force", name], capture_output=True, timeout=30, check=False)


def check_backend(env):
    run(["cargo", "test", "--locked", "-p", "walgit-store", "--features", "azure", "--lib"], env=env)
    run(["cargo", "test", "--locked", "-p", "walgit-store", "--features", "azure",
         "--test", "contract", "azure_contract", "--", "--nocapture"], env=env)
    git_smoke(env, env["WALGIT_TEST_AZURE_ENDPOINT"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", help="Existing live Azure storage account (Azure CLI identity)")
    parser.add_argument("--container", help="Existing container in that account")
    args = parser.parse_args()
    if (args.account is None) != (args.container is None):
        parser.error("--account and --container must be supplied together")
    if args.account is not None:
        if not re.fullmatch(r"[a-z0-9]{3,24}", args.account):
            parser.error("--account must be a valid Azure storage account name")
        if (not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", args.container)
                or "--" in args.container):
            parser.error("--container must be a valid Azure container name")
        if "AZURE_STORAGE_SAS_TOKEN" in os.environ:
            parser.error("unset AZURE_STORAGE_SAS_TOKEN to test Azure CLI identity")
        env = dict(os.environ, WALGIT_TEST_AZURE_ACCOUNT=args.account,
                   WALGIT_TEST_AZURE_CONTAINER=args.container,
                   WALGIT_TEST_AZURE_ENDPOINT=f"https://{args.account}.blob.core.windows.net")
        check_backend(env)
        return
    with azurite() as endpoint:
        client = BlobServiceClient(endpoint, credential=KEY, retry_total=0, connection_timeout=1)
        # A cold container runtime can take well over ten seconds to start Node.
        for _ in range(600):
            try:
                client.create_container(CONTAINER)
                break
            except (ServiceRequestError, ServiceResponseError):
                time.sleep(0.1)
        else:
            raise RuntimeError("Azurite startup timed out")
        sas = generate_container_sas(ACCOUNT, CONTAINER, account_key=KEY,
                                    permission=ContainerSasPermissions(read=True, write=True, delete=True, list=True, create=True),
                                    expiry=datetime.now(timezone.utc) + timedelta(hours=2))
        env = dict(os.environ, WALGIT_TEST_AZURE_ENDPOINT=endpoint,
                   WALGIT_TEST_AZURE_ACCOUNT="",
                   WALGIT_TEST_AZURE_CONTAINER=CONTAINER, AZURE_STORAGE_SAS_TOKEN=sas)
        check_backend(env)


if __name__ == "__main__":
    main()
