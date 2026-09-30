#!/usr/bin/env python3
"""Refresh data/ from the upstream sources listed in sources.json.

Standard library only. Every section is incremental: a published release is
immutable, so a version already written under data/ is never fetched again,
and a run that finds nothing new changes nothing.

    python collect.py all [--deadline-minutes N] [--jobs N]
    python collect.py releases|npm|cdnjs|maven|wordpress [--only NAME ...]

A transport failure is never written down: the version is simply retried on the
next run. A version that genuinely publishes no matching file is written as an
empty object, because that is a fact about the release.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
SOURCES = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))

UA = "stratumdb-collector/1.0"
# Pre-releases and snapshot builds (0.0.0-<stamp>) are listed in versions.json but
# their files are not digested.
PRERELEASE = re.compile(r"(?i)(alpha|beta|[-.]rc|[-.]pre|dev|nightly|canary|next|experimental|insiders|\+|^0\.0\.0-)")
# Files a browser is served from a package: scripts, stylesheets and their source maps.
WEB_FILE = re.compile(r"\.(js|mjs|cjs|css|map)$")

DEADLINE = float("inf")
_lock = threading.Lock()
STATS = {"requests": 0, "bytes": 0, "written": 0, "failed": 0}


class Transport(RuntimeError):
    pass


class Refused(Transport):
    """A 4xx other than 404/429: the publisher refuses this item, and asking again changes nothing."""


def out_of_time() -> bool:
    return time.monotonic() > DEADLINE


def fetch(url: str, *, allow_404: bool = False, tries: int = 3) -> bytes | None:
    for attempt in range(tries):
        if out_of_time():
            raise Transport("deadline reached")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as r:
                body = r.read()
            with _lock:
                STATS["requests"] += 1
                STATS["bytes"] += len(body)
            return body
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and allow_404:
                return None
            if exc.code < 500 and exc.code != 429:
                raise Refused(f"HTTP {exc.code} {url}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            pass
        time.sleep(2 ** attempt * 2)
    raise Transport(f"gave up on {url}")


def fetch_json(url: str, **kw) -> object:
    body = fetch(url, **kw)
    return None if body is None else json.loads(body)


# --------------------------------------------------------------------------- output

def vkey(v: str) -> tuple:
    parts = re.split(r"[.\-+_]", v)
    return tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in parts) + ((2, 0, v),)


def dump(doc: object) -> str:
    if isinstance(doc, dict) and doc and all(isinstance(v, str) for v in doc.values()):
        # one entry per line: small diffs, greppable
        rows = [f"{json.dumps(k)}:{json.dumps(doc[k])}" for k in sorted(doc)]
        return "{\n" + ",\n".join(rows) + "\n}\n"
    return json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def write(path: Path, doc: object) -> bool:
    text = dump(doc)
    if path.is_file() and path.read_text(encoding="utf-8") == text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    with _lock:
        STATS["written"] += 1
    return True


def safe(v: str) -> str:
    if not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.+_-]*", v) or ".." in v:
        raise ValueError(f"refusing version string {v!r}")
    return v


# Items a publisher refused with a 4xx, so later runs do not ask again. Delete an
# entry (or the file) to retry it.
REFUSED_FILE = ROOT / "state" / "refused.json"
REFUSED: dict[str, str] = json.loads(REFUSED_FILE.read_text()) if REFUSED_FILE.is_file() else {}


def refuse(key: str, why: str) -> None:
    with _lock:
        REFUSED[key] = why
        REFUSED_FILE.parent.mkdir(exist_ok=True)
        REFUSED_FILE.write_text(json.dumps(dict(sorted(REFUSED.items())), indent=1) + "\n")


def per_version(section: Path, versions: list[str], one, jobs: int, label: str) -> None:
    """Write section/v/<version>.json for every version not already written."""
    todo = [v for v in versions
            if not (section / "v" / f"{safe(v)}.json").is_file() and f"{label} {v}" not in REFUSED]
    if not todo:
        return
    print(f"  {label}: {len(todo)} version(s) to digest", flush=True)
    done = [0]

    def run(v: str) -> None:
        if out_of_time():
            return
        try:
            doc = one(v)
        except Transport as exc:
            with _lock:
                STATS["failed"] += 1
            if isinstance(exc, Refused):
                refuse(f"{label} {v}", str(exc))
            if not out_of_time():
                print(f"    {label} {v}: {exc}", flush=True)
            return
        if doc is not None:
            write(section / "v" / f"{v}.json", doc)
        with _lock:
            done[0] += 1
            if done[0] % 10 == 0:
                print(f"    {label}: {done[0]}/{len(todo)}", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        list(pool.map(run, todo))


# --------------------------------------------------------------------------- release lists

def _walk(doc: object, path: list[str]) -> list[object]:
    items = [doc]
    for key in path:
        nxt: list[object] = []
        for it in items:
            if key == "*":
                nxt.extend(it.values() if isinstance(it, dict) else it or [])
            elif isinstance(it, dict) and key in it:
                nxt.append(it[key])
        items = nxt
    return items


def release_list(spec: dict) -> list[str]:
    kind = spec["kind"]
    raw: list[str] = []
    if kind == "html":
        for url in spec["url"]:
            raw += re.findall(spec["pattern"], fetch(url).decode("utf-8", "replace"))
        return raw
    if kind == "html-tree":
        branches = set(re.findall(spec["branch"], fetch(spec["url"]).decode("utf-8", "replace")))
        for b in sorted(branches):
            body = fetch(spec["sub"].format(b=b), allow_404=True) or b""
            raw += re.findall(spec["pattern"], body.decode("utf-8", "replace"))
        return raw
    if kind in ("json", "json-keys"):
        doc = fetch_json(spec["url"])
        if kind == "json-keys":
            raw = [str(k) for k in doc]
        else:
            for it in _walk(doc, spec.get("path", [])):
                if "where" in spec and not all(_walk(it, spec["where"])):
                    continue
                val = it.get(spec["field"]) if "field" in spec else it
                if val is not None:
                    raw.append(str(val))
    elif kind == "gitea-api":
        page = 1
        while True:
            rows = fetch_json(f"{spec['url']}?limit=50&page={page}")
            if not rows:
                break
            raw += [r["tag_name"] for r in rows]
            page += 1
    elif kind == "hashicorp":
        after = ""
        while True:
            q = f"?limit=20&after={urllib.parse.quote(after)}" if after else "?limit=20"
            rows = fetch_json(spec["url"] + q)
            if not rows:
                break
            raw += [r["version"] for r in rows]
            after = rows[-1]["timestamp_created"]
    else:
        raise ValueError(f"unknown kind {kind}")
    if "pattern" in spec:
        pat = re.compile(spec["pattern"])
        raw = [m.group(1) for m in map(pat.search, raw) if m]
    return raw


def cmd_releases(names: list[str] | None, jobs: int) -> None:
    specs = SOURCES["releases"]
    for name in names or sorted(specs):
        if out_of_time():
            return
        try:
            versions = sorted(set(release_list(specs[name])), key=vkey)
        except Transport as exc:
            STATS["failed"] += 1
            print(f"  releases {name}: {exc}")
            continue
        if not versions:
            STATS["failed"] += 1
            print(f"  releases {name}: source answered but listed nothing; left unchanged")
            continue
        url = specs[name]["url"]
        changed = write(DATA / "releases" / f"{name}.json",
                        {"source": url if isinstance(url, str) else url[0] if len(url) == 1 else url,
                         "versions": versions})
        print(f"  releases {name}: {len(versions)}{' (updated)' if changed else ''}")


# --------------------------------------------------------------------------- npm (via jsDelivr)

JSD_PKG = "https://data.jsdelivr.com/v1/packages/npm/{p}"
JSD_VER = "https://data.jsdelivr.com/v1/packages/npm/{p}@{v}?structure=flat"


def cmd_npm(names: list[str] | None, jobs: int) -> None:
    pkgs = SOURCES["npm"]
    for pkg in names or sorted(pkgs):
        if out_of_time():
            return
        section = DATA / "npm" / pkg
        try:
            doc = fetch_json(JSD_PKG.format(p=pkg))
        except Transport as exc:
            STATS["failed"] += 1
            print(f"  npm {pkg}: {exc}")
            continue
        versions = sorted({v["version"] for v in doc.get("versions", []) if v.get("version")}, key=vkey)
        write(section / "versions.json", {"source": "npm", "tags": doc.get("tags", {}), "versions": versions})
        if pkgs[pkg].get("files", True) is False:
            continue

        def one(v: str, pkg=pkg) -> dict:
            listing = fetch_json(JSD_VER.format(p=pkg, v=v), allow_404=True) or {}
            return {f["name"].lstrip("/"): base64.b64decode(f["hash"]).hex()
                    for f in listing.get("files", []) if WEB_FILE.search(f["name"]) and f.get("hash")}

        per_version(section, [v for v in versions if not PRERELEASE.search(v)], one, jobs, f"npm {pkg}")


# --------------------------------------------------------------------------- cdnjs

CDNJS_LIB = "https://api.cdnjs.com/libraries/{l}?fields=versions"
CDNJS_VER = "https://api.cdnjs.com/libraries/{l}/{v}?fields=files,sri"
CDNJS_FILE = "https://cdnjs.cloudflare.com/ajax/libs/{l}/{v}/{f}"


def cmd_cdnjs(names: list[str] | None, jobs: int) -> None:
    libs = SOURCES["cdnjs"]
    for lib in names or sorted(libs):
        if out_of_time():
            return
        section = DATA / "cdnjs" / lib
        try:
            doc = fetch_json(CDNJS_LIB.format(l=lib))
        except Transport as exc:
            STATS["failed"] += 1
            print(f"  cdnjs {lib}: {exc}")
            continue
        versions = sorted(set(doc.get("versions") or []), key=vkey)
        write(section / "versions.json", {"source": "cdnjs", "versions": versions})
        if libs[lib].get("files", True) is False:
            continue

        def one(v: str, lib=lib) -> dict:
            listing = fetch_json(CDNJS_VER.format(l=lib, v=v), allow_404=True) or {}
            sri = listing.get("sri") or {}
            out = {}
            for f in listing.get("files") or []:
                if not WEB_FILE.search(f):
                    continue
                url = CDNJS_FILE.format(l=lib, v=v, f=urllib.parse.quote(f))
                body = fetch(url)
                if f in sri and sri[f] != "sha512-" + base64.b64encode(hashlib.sha512(body).digest()).decode():
                    # cdnjs's own integrity value is stale for a few files. A second download
                    # that returns the same bytes shows they are what is served; a different
                    # one means a transfer went wrong, and the version is retried next run.
                    if fetch(url) != body:
                        raise Transport(f"{f}: two downloads returned different bytes")
                out[f] = hashlib.sha256(body).hexdigest()
            return out

        per_version(section, [v for v in versions if not PRERELEASE.search(v)], one, jobs, f"cdnjs {lib}")


# --------------------------------------------------------------------------- maven

MAVEN = "https://repo1.maven.org/maven2/{g}/{a}"


def jar_digests(body: bytes) -> dict:
    out = {}
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        for info in z.infolist():
            n = info.filename
            if info.is_dir() or n.endswith(".class") or n.startswith("META-INF/"):
                continue
            out[n] = hashlib.sha256(z.read(info)).hexdigest()
    return out


def cmd_maven(names: list[str] | None, jobs: int) -> None:
    arts = SOURCES["maven"]
    for coord in names or sorted(arts):
        if out_of_time():
            return
        g, a = coord.split(":")
        base = MAVEN.format(g=g.replace(".", "/"), a=a)
        section = DATA / "maven" / g / a
        try:
            meta = fetch(f"{base}/maven-metadata.xml").decode()
        except Transport as exc:
            STATS["failed"] += 1
            print(f"  maven {coord}: {exc}")
            continue
        versions = sorted(set(re.findall(r"<version>([^<]+)</version>", meta)), key=vkey)
        write(section / "versions.json", {"source": "maven-central", "versions": versions})
        opts = arts[coord]
        if opts.get("files", True) is False:
            continue
        floor = vkey(opts.get("min", "0"))
        # Every published version: on Maven Central a pre-release is a release like any other.
        want = [v for v in versions if vkey(v) >= floor]

        def one(v: str, base=base, a=a) -> dict:
            body = fetch(f"{base}/{v}/{a}-{v}.jar", allow_404=True)
            return {} if body is None else jar_digests(body)

        per_version(section, want, one, max(1, jobs // 2), f"maven {coord}")


# --------------------------------------------------------------------------- wordpress core

WP_ZIP = "https://downloads.wordpress.org/release/wordpress-{v}.zip"


def cmd_wordpress(names: list[str] | None, jobs: int) -> None:
    section = DATA / "wordpress"
    listing = DATA / "releases" / "wordpress.json"
    if not listing.is_file():
        cmd_releases(["wordpress"], jobs)
    versions = json.loads(listing.read_text())["versions"]
    floor = vkey(SOURCES["wordpress"].get("min", "0"))
    want = [v for v in versions if vkey(v) >= floor and not PRERELEASE.search(v)]
    if names:
        want = [v for v in want if v in names]

    def one(v: str) -> dict:
        body = fetch(WP_ZIP.format(v=v), allow_404=True)
        if body is None:
            return {}
        out = {}
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            for info in z.infolist():
                n = info.filename.removeprefix("wordpress/")
                if info.is_dir() or n.endswith(".php"):
                    continue
                out[n] = hashlib.sha256(z.read(info)).hexdigest()
        return out

    per_version(section, want, one, max(1, jobs // 2), "wordpress")


# --------------------------------------------------------------------------- version lists only

def _git_tags(url: str) -> list[str]:
    try:
        r = subprocess.run(["git", "ls-remote", "--tags", "--refs", url], capture_output=True, text=True,
                           timeout=120, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})
    except subprocess.TimeoutExpired as exc:
        raise Transport(f"timeout {url}") from exc
    if r.returncode:
        raise Transport(f"git ls-remote failed on {url}")
    return [line.split("refs/tags/", 1)[1] for line in r.stdout.splitlines() if "refs/tags/" in line]


def _paged(url: str, key: str, field: str, pages: int = 10) -> list[str]:
    """Docker Hub answers 403 past page 10 to an anonymous client; its default order is
    newest first, so ten pages of 100 are the newest thousand tags."""
    out: list[str] = []
    while url and pages:
        doc = fetch_json(url)
        out += [str(r[field]) for r in doc.get(key, [])]
        url, pages = doc.get("next"), pages - 1
    return out


LISTERS = {
    "git": _git_tags,
    "wp-plugin": lambda n: [k for k in (fetch_json(
        "https://api.wordpress.org/plugins/info/1.2/?action=plugin_information"
        f"&request%5Bslug%5D={n}&request%5Bfields%5D%5Bversions%5D=1") or {}).get("versions", {}) if k != "trunk"],
    "wp-theme": lambda n: list((fetch_json(
        "https://api.wordpress.org/themes/info/1.2/?action=theme_information"
        f"&request%5Bslug%5D={n}&request%5Bfields%5D%5Bversions%5D=1") or {}).get("versions", {})),
    "pypi": lambda n: list((fetch_json(f"https://pypi.org/pypi/{n}/json") or {}).get("releases", {})),
    "rubygems": lambda n: [r["number"] for r in fetch_json(f"https://rubygems.org/api/v1/versions/{n}.json") or []],
    "nuget": lambda n: (fetch_json(f"https://api.nuget.org/v3-flatcontainer/{n.lower()}/index.json") or {}).get("versions", []),
    "docker": lambda n: _paged(f"https://hub.docker.com/v2/repositories/{n}/tags?page_size=100", "results", "name"),
}
# A tag may carry a path or a scope (release/METEOR@3.1); it is data, never a file name.
LIST_VERSION = re.compile(r"^[0-9A-Za-z@][0-9A-Za-z.+_@/-]*$")


def list_dir(kind: str, name: str) -> Path:
    if kind == "git":
        u = urllib.parse.urlparse(name)
        return DATA / "git" / u.hostname / u.path.strip("/")
    return DATA / kind / name


def cmd_lists(names: list[str] | None, jobs: int) -> None:
    jobs_list = [(k, n) for k in LISTERS for n in SOURCES.get(k, []) if not names or n in names]

    def run(item: tuple[str, str]) -> None:
        kind, name = item
        if out_of_time() or f"{kind} {name}" in REFUSED:
            return
        try:
            versions = sorted({v for v in LISTERS[kind](name) if LIST_VERSION.match(v) and any(c.isdigit() for c in v)},
                              key=vkey)
        except Transport as exc:
            STATS["failed"] += 1
            if isinstance(exc, Refused):
                refuse(f"{kind} {name}", str(exc))
            print(f"  {kind} {name}: {exc}", flush=True)
            return
        if versions:
            write(list_dir(kind, name) / "versions.json", {"source": kind, "versions": versions})

    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        list(pool.map(run, jobs_list))
    print(f"  {len(jobs_list)} list(s) read", flush=True)


# --------------------------------------------------------------------------- main

SECTIONS = {"releases": cmd_releases, "lists": cmd_lists, "npm": cmd_npm, "cdnjs": cmd_cdnjs, "maven": cmd_maven,
            "wordpress": cmd_wordpress}


def main() -> int:
    global DEADLINE
    p = argparse.ArgumentParser()
    p.add_argument("section", choices=[*SECTIONS, "all"])
    p.add_argument("--only", nargs="*")
    p.add_argument("--jobs", type=int, default=8)
    p.add_argument("--deadline-minutes", type=float, default=0)
    a = p.parse_args()
    if a.deadline_minutes:
        DEADLINE = time.monotonic() + a.deadline_minutes * 60
    for name in (SECTIONS if a.section == "all" else [a.section]):
        print(f"[{name}]", flush=True)
        SECTIONS[name](a.only if a.section != "all" else None, a.jobs)
    print(f"requests {STATS['requests']}, bytes {STATS['bytes']}, files written {STATS['written']}, "
          f"failures {STATS['failed']}{', deadline reached' if out_of_time() else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
