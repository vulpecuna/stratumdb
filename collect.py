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
import re
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


def out_of_time() -> bool:
    return time.monotonic() > DEADLINE


def fetch(url: str, *, allow_404: bool = False, tries: int = 3) -> bytes | None:
    for attempt in range(tries):
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
                raise Transport(f"HTTP {exc.code} {url}") from exc
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


def per_version(section: Path, versions: list[str], one, jobs: int, label: str) -> None:
    """Write section/v/<version>.json for every version not already written."""
    todo = [v for v in versions if not (section / "v" / f"{safe(v)}.json").is_file()]
    if not todo:
        return
    print(f"  {label}: {len(todo)} version(s) to digest", flush=True)

    def run(v: str) -> None:
        if out_of_time():
            return
        try:
            doc = one(v)
        except Transport as exc:
            with _lock:
                STATS["failed"] += 1
            print(f"    {label} {v}: {exc}", flush=True)
            return
        if doc is not None:
            write(section / "v" / f"{v}.json", doc)

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
    for lib in names or SOURCES["cdnjs"]:
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

        def one(v: str, lib=lib) -> dict:
            listing = fetch_json(CDNJS_VER.format(l=lib, v=v), allow_404=True) or {}
            sri = listing.get("sri") or {}
            out = {}
            for f in listing.get("files") or []:
                if not WEB_FILE.search(f):
                    continue
                body = fetch(CDNJS_FILE.format(l=lib, v=v, f=urllib.parse.quote(f)))
                s512 = "sha512-" + base64.b64encode(hashlib.sha512(body).digest()).decode()
                if f in sri and sri[f] != s512:
                    raise Transport(f"{f}: served bytes disagree with the published integrity value")
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


# --------------------------------------------------------------------------- main

SECTIONS = {"releases": cmd_releases, "npm": cmd_npm, "cdnjs": cmd_cdnjs, "maven": cmd_maven,
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
