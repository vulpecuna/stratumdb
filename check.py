#!/usr/bin/env python3
"""Structural check of the repository: only the expected files, in the expected shapes.

Run by CI on every push and before every automated commit. Exits 1 on any problem.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))

TOP = {"README.md", "sources.json", "collect.py", "check.py", ".gitignore", "state/refused.json",
       "state/quarantine.json",
       ".github/workflows/collect.yml", ".github/workflows/check.yml"}
HEX64 = re.compile(r"^[0-9a-f]{64}$")
VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+_-]*$")
TAG = re.compile(r"^[0-9A-Za-z@][0-9A-Za-z.+_@/-]*$")  # a listed version; never used as a path
LIST_KEYS = {"source", "tags", "versions"}


def tracked() -> list[str]:
    try:
        out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT,
                             capture_output=True, text=True, check=True).stdout
        return sorted(set(out.split("\n")) - {""})
    except (OSError, subprocess.CalledProcessError):
        return sorted(str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts)


def section_of(path: str) -> tuple[str, str, str] | None:
    """(kind, name, rest) for a data path, or None when it is outside every declared source."""
    parts = path.split("/")
    if parts[1] == "releases" and len(parts) == 3 and parts[2].endswith(".json"):
        name = parts[2][:-5]
        return ("releases", name, "") if name in SOURCES["releases"] else None
    if parts[1] == "wordpress":
        return ("wordpress", "wordpress", "/".join(parts[2:]))
    if parts[1] == "npm":
        for pkg in SOURCES["npm"]:
            if path.startswith(f"data/npm/{pkg}/"):
                return ("npm", pkg, path[len(f"data/npm/{pkg}/"):])
    if parts[1] == "cdnjs":
        for lib in SOURCES["cdnjs"]:
            if path.startswith(f"data/cdnjs/{lib}/"):
                return ("cdnjs", lib, path[len(f"data/cdnjs/{lib}/"):])
    for kind in ("git", "wp-plugin", "wp-theme", "pypi", "rubygems", "nuget", "docker"):
        if parts[1] != kind:
            continue
        for name in SOURCES.get(kind, []):
            d = "/".join(name.split("://", 1)[1].split("/")[:3]) if kind == "git" else name
            if path == f"data/{kind}/{d}/versions.json":
                return (kind, name, "versions.json")
        return None
    if parts[1] == "maven":
        for coord in SOURCES["maven"]:
            prefix = "data/maven/" + coord.replace(":", "/") + "/"
            if path.startswith(prefix):
                return ("maven", coord, path[len(prefix):])
    return None


def check_list(doc: object) -> str | None:
    if not isinstance(doc, dict) or not set(doc) <= LIST_KEYS or "versions" not in doc:
        return "not a version list"
    if not all(isinstance(v, str) and TAG.match(v) for v in doc["versions"]):
        return "a version is not a plain version string"
    if not isinstance(doc.get("tags", {}), dict):
        return "tags is not an object"
    return None


def check_digests(doc: object) -> str | None:
    if not isinstance(doc, dict):
        return "not an object"
    for k, v in doc.items():
        if not isinstance(k, str) or not k or k.startswith("/") or ".." in k.split("/"):
            return f"bad path {k!r}"
        if not isinstance(v, str) or not HEX64.match(v):
            return f"{k}: value is not a sha256 hex digest"
    return None


def main() -> int:
    problems: list[str] = []
    files = tracked()
    for f in files:
        if f in TOP:
            continue
        if not (f.startswith("data/") and f.endswith(".json")):
            problems.append(f"{f}: not an allowed file")
            continue
        sec = section_of(f)
        if sec is None:
            problems.append(f"{f}: not under a source declared in sources.json")
            continue
        kind, _name, rest = sec
        try:
            doc = json.loads((ROOT / f).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            problems.append(f"{f}: unreadable ({exc})")
            continue
        if kind == "releases" or rest == "versions.json":
            err = check_list(doc)
        elif rest.startswith("v/") and rest.count("/") == 1 and VERSION.match(rest[2:-5]):
            err = check_digests(doc)
        else:
            err = "unexpected file inside a source"
        if err:
            problems.append(f"{f}: {err}")
    for p in problems:
        print(p)
    print(f"checked {len(files)} files, {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
