import hashlib
import json
import os
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def timestamp():
    return datetime.now(UTC).isoformat()


class Run:
    def __init__(self, path, task, *, prior=None):
        self.path = Path(path).resolve()
        self.task = task
        self.prior = prior

    def __enter__(self):
        if self.path.parent.name != "runs":
            raise ValueError("Specify a new runs/<run_id> directory")
        self.path.mkdir(parents=True)
        root = Path(__file__).resolve().parents[2]
        self.info = {
            "id": self.path.name,
            "task": self.task,
            "prior_run": self.prior,
            "started_utc": timestamp(),
            "command": [sys.executable, "-m", "honda", *sys.argv[1:]],
            "cwd": str(Path.cwd()),
            "python": sys.version,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "code_sha256": {
                str(path.relative_to(root)): sha256(path)
                for path in sorted((root / "src").rglob("*.py"))
            },
            "status": "RUNNING",
        }
        self.save()
        return self

    def save(self):
        temporary = self.path / "run_manifest.tmp"
        write_json(temporary, self.info)
        temporary.replace(self.path / "run_manifest.json")

    def __exit__(self, exc_type, exc, traceback):
        self.info["artifacts_sha256"] = {
            str(path.relative_to(self.path)): sha256(path)
            for path in sorted(self.path.rglob("*"))
            if path.is_file() and path.name != "run_manifest.json"
            and not path.name.startswith(".") and "scratch" not in path.relative_to(self.path).parts
        }
        self.info["ended_utc"] = timestamp()
        self.info["status"] = "FAILED" if exc_type else "OK"
        if exc_type:
            self.info["error"] = f"{exc_type.__name__}: {exc}"
        self.save()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def completed_input(run, path, expected_task, names):
    path = Path(path).resolve()
    manifest_path = path / "run_manifest.json"
    manifest = read_json(manifest_path)
    if manifest["task"] != expected_task or manifest["status"] != "OK":
        raise ValueError(f"Invalid upstream run: {path}")
    verified = {}
    for name in names:
        target = path / name
        files = sorted(p for p in target.rglob("*") if p.is_file() and not p.name.startswith(".")) if target.is_dir() else [target]
        keys = [str(p.relative_to(path)) for p in files]
        registered = manifest["artifacts_sha256"]
        if target.is_dir():
            expected_keys = {key for key in registered if key.startswith(name + "/")}
        else:
            expected_keys = {name}
        if set(keys) != expected_keys:
            raise ValueError(f"Upstream artifact inventory differs: {target}")
        for file_path, key in zip(files, keys):
            digest = sha256(file_path)
            if registered.get(key) != digest:
                raise ValueError(f"Upstream artifact checksum mismatch: {file_path}")
            verified[key] = digest
    run.info.setdefault("inputs", {})[str(path)] = {
        "manifest_sha256": sha256(manifest_path), "artifacts_sha256": verified,
    }
    return path
