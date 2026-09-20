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
For real Event Grid delivery, also pass --event-grid-queue NAME;
see docs/EVENTS.md for the required subscriptions and queue permissions.
"""
import argparse
import base64
import contextlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, HTTPServer
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


def build_server(env):
    run(["cargo", "build", "--locked", "-p", "walgit-cli", "--bin", "walgit-server",
         "--features", "walgit-store/azure"], env=env, timeout=900)
    target = Path(env.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    return target / "debug" / "walgit-server"


def git_smoke(env, endpoint):
    print("Azure Git smoke: push, clone, change, pull, and clone from a cold server", flush=True)
    account = env.get("WALGIT_TEST_AZURE_ACCOUNT", "")
    credential = "azure_cli" if account else "auto"
    prefix = "git-smoke/" + uuid4().hex + "/"
    print(f"Azure Git smoke prefix: {prefix}", flush=True)
    binary = build_server(env)
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


def event_grid_smoke(env, queue):
    binary = build_server(env)
    account = env["WALGIT_TEST_AZURE_ACCOUNT"]
    container = env["WALGIT_TEST_AZURE_CONTAINER"]
    endpoint = env["WALGIT_TEST_AZURE_ENDPOINT"]
    captured = []

    class Sink(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()

        def do_POST(self):
            captured.extend(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

    with tempfile.TemporaryDirectory(prefix="walgit-event-grid-") as temp:
        directory = Path(temp)
        env = {key: value for key, value in env.items()
               if not key.startswith(("WALGIT__", "AZURE_STORAGE_")) and key != "PORT"}
        token = secrets.token_urlsafe(32)
        env.update(GIT_CONFIG_GLOBAL=str(directory / "gitconfig"), GIT_CONFIG_NOSYSTEM="1",
                   GIT_TERMINAL_PROMPT="0", GIT_CONFIG_COUNT="1",
                   GIT_CONFIG_KEY_0="http.extraHeader",
                   GIT_CONFIG_VALUE_0=f"Authorization: Bearer {token}",
                   WALGIT_EVENT_GRID_SMOKE_TOKEN=token, AZURE_EXTENSION_USE_DYNAMIC_INSTALL="no")

        def az(*args):
            try:
                return run(["az", *args, "--only-show-errors", "--output", "json"],
                           env=env, timeout=90, capture=True).stdout
            except subprocess.CalledProcessError as error:
                raise RuntimeError(f"Azure CLI failed:\n{error.stderr}") from None

        queue_args = ["--account-name", account, "--queue-name", queue, "--auth-mode", "login"]

        def acknowledge(message):
            az("storage", "message", "delete", *queue_args,
               "--id", message["id"], "--pop-receipt", message["popReceipt"])

        with HTTPServer(("127.0.0.1", 0), Sink) as sink:
            worker = threading.Thread(target=sink.serve_forever, daemon=True)
            worker.start()
            sink_url = f"http://127.0.0.1:{sink.server_address[1]}"
            try:
                with urllib.request.urlopen(sink_url, timeout=2) as reply:
                    assert reply.status == 200
                for schema in ("native", "cloud"):
                    work = directory / schema
                    source = work / "source"
                    source.mkdir(parents=True)
                    prefix = f"event-grid-smoke/{schema}/{uuid4().hex}/"
                    print(f"Event Grid {schema} prefix: {prefix}", flush=True)
                    with socket.socket() as listener:
                        listener.bind(("127.0.0.1", 0))
                        port = listener.getsockname()[1]
                    url = f"http://127.0.0.1:{port}"
                    config = work / "walgit.toml"
                    config.write_text(f'''
[server]
listen = "127.0.0.1:{port}"
public_url = "{url}"
auto_create_on_push = true
roles = ["serve", "events"]
[server.auth]
mode = "token"
anonymous_read = false
[[server.auth.tokens]]
principal = "event-grid-test"
token_env = "WALGIT_EVENT_GRID_SMOKE_TOKEN"
write = true
[store]
backend = "azure"
bucket = {json.dumps(container)}
prefix = "{prefix}"
[store.azure]
endpoint = {json.dumps(endpoint)}
account = {json.dumps(account)}
credential = "azure_cli"
[cache]
dir = {json.dumps(str(work / "cache"))}
mode = "budget"
max_bytes = "128MiB"
[events]
webhook_url = "{sink_url}"
sweep_interval = "0s"
''')

                    def git(*args, capture=False):
                        return run(["git", "-c", "user.name=Azure Test",
                                    "-c", "user.email=azure-test@example.invalid", *args],
                                   env=env, cwd=source, timeout=90, capture=capture)

                    git("init", "-b", "main")
                    manifest = prefix + "repos/test/azure/manifest.pb"
                    subject = f"/blobServices/default/containers/{container}/blobs/{manifest}"
                    notify = url + "/_events/notify"
                    with server(binary, config, env, url, work / "server.log"):
                        try:
                            urllib.request.urlopen(urllib.request.Request(notify, data=b"{}"), timeout=5)
                        except urllib.error.HTTPError as error:
                            assert error.code == 401
                        else:
                            raise AssertionError("notify accepted an unauthenticated request")
                        for seq in (1, 2):
                            before = len(captured)
                            git("commit", "--allow-empty", "-m", f"{schema} push {seq}")
                            oid = git("rev-parse", "HEAD", capture=True).stdout.strip()
                            git("push", url + "/test/azure.git", "main")
                            etag = json.loads(az("storage", "blob", "show", "--account-name", account,
                                "--container-name", container, "--name", manifest,
                                "--auth-mode", "login", "--query", "properties.etag")).strip('"')
                            assert len(captured) == before, "events arrived without a notification"
                            deadline = time.monotonic() + 600
                            notice_at = 0
                            matched = None
                            while time.monotonic() < deadline and matched is None:
                                messages = json.loads(az("storage", "message", "get", *queue_args,
                                    "--num-messages", "1", "--visibility-timeout", "600"))
                                for message in messages:
                                    payload = json.loads(base64.b64decode(message["content"], validate=True))
                                    events = payload if isinstance(payload, list) else [payload]
                                    assert len(events) == 1, "expected one Event Grid event per queue message"
                                    event = events[0]
                                    if event.get("subject") != subject:
                                        continue
                                    kind = "eventType" if schema == "native" else "type"
                                    assert event[kind] == "Microsoft.Storage.BlobCreated"
                                    if schema == "cloud":
                                        assert event["specversion"] == "1.0"
                                    if event["data"]["eTag"].strip('"') != etag:
                                        acknowledge(message)
                                        continue
                                    print(f"{schema}: received {type(payload).__name__} JSON, "
                                          f"{event['data']['api']}, ETag {etag}", flush=True)
                                    matched = message
                                if matched is None:
                                    if time.monotonic() >= notice_at:
                                        print(f"{schema}: waiting for manifest ETag {etag}", flush=True)
                                        notice_at = time.monotonic() + 30
                                    time.sleep(2)
                            assert matched is not None, f"no {schema} notification for {subject}, ETag {etag}"
                            for emitted in (1, 0):
                                request = urllib.request.Request(notify, data=base64.b64decode(matched["content"], validate=True),
                                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
                                with urllib.request.urlopen(request, timeout=90) as reply:
                                    reports = json.load(reply)
                                assert len(reports) == 1
                                assert reports[0]["head_seq"] == seq
                                assert reports[0]["emitted"] == emitted
                                assert len(captured) == before + 1
                                event = captured[-1]
                                assert event["repo"] == "test/azure"
                                assert event["ref_name"] == "refs/heads/main"
                                assert event["new"] == oid
                                assert event["_walgit"]["seq"] == str(seq)
                                cursor_url = f"{endpoint}/{container}/{prefix}repos/test/azure/events/cursor.json"
                                cursor = json.loads(az("rest", "--method", "get", "--url", cursor_url,
                                    "--resource", "https://storage.azure.com/", "--headers",
                                    "x-ms-version=2023-11-03", f"x-ms-date={formatdate(usegmt=True)}"))
                                assert cursor["published_seq"] == seq
                            acknowledge(matched)
                            print(f"{schema}: seq {seq}, OID {oid}, durable cursor and duplicate verified", flush=True)
            finally:
                sink.shutdown()
                worker.join()
    print("Azure Event Grid queue smoke passed for both schemas", flush=True)


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
    parser.add_argument("--event-grid-queue", help="Existing test queue; run live Event Grid checks instead")
    args = parser.parse_args()
    if (args.account is None) != (args.container is None):
        parser.error("--account and --container must be supplied together")
    if args.event_grid_queue is not None and args.account is None:
        parser.error("--event-grid-queue requires live --account and --container")
    if args.account is not None:
        if not re.fullmatch(r"[a-z0-9]{3,24}", args.account):
            parser.error("--account must be a valid Azure storage account name")
        if (not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", args.container)
                or "--" in args.container):
            parser.error("--container must be a valid Azure container name")
        if args.event_grid_queue is not None and (
                not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", args.event_grid_queue)
                or "--" in args.event_grid_queue):
            parser.error("--event-grid-queue must be a valid Azure queue name")
        if "AZURE_STORAGE_SAS_TOKEN" in os.environ:
            parser.error("unset AZURE_STORAGE_SAS_TOKEN to test Azure CLI identity")
        env = dict(os.environ, WALGIT_TEST_AZURE_ACCOUNT=args.account,
                   WALGIT_TEST_AZURE_CONTAINER=args.container,
                   WALGIT_TEST_AZURE_ENDPOINT=f"https://{args.account}.blob.core.windows.net")
        if args.event_grid_queue is not None:
            event_grid_smoke(env, args.event_grid_queue)
        else:
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
