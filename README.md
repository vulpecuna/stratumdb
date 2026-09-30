# stratumdb

A machine-maintained index of published software releases and of the digests
of the files those releases ship. Refreshed automatically every six hours from
the publishers' own distribution points. Nothing in `data/` is written by hand.

## Layout

```
sources.json                         what is tracked
data/releases/<product>.json         {"source": ..., "versions": [...]}
data/npm/<package>/versions.json     every published version, with dist-tags
data/npm/<package>/v/<version>.json  {"<path>": "<sha256>"} for every .js/.mjs/.cjs/.css/.map file in the package
data/cdnjs/<library>/versions.json
data/cdnjs/<library>/v/<version>.json   same, for the files cdnjs serves
data/maven/<group>/<artifact>/versions.json
data/maven/<group>/<artifact>/v/<version>.json   every non-class resource in the jar
data/wordpress/v/<version>.json      every non-PHP file in the release zip
data/git/<host>/<owner>/<repo>/versions.json   every tag (git ls-remote)
data/wp-plugin/<slug>/versions.json  data/wp-theme/<slug>/versions.json
data/pypi/<name>/versions.json       data/rubygems/<name>/versions.json
data/docker/<image>/versions.json    the newest thousand tags
state/refused.json                   items a publisher refused (4xx); not asked again until removed
```

Digests are lowercase hexadecimal SHA-256 of the exact bytes served. Versions
are sorted oldest first. Pre-releases appear in `versions.json` but are not
digested. A version file containing `{}` means the release was fetched and ships
no file of the kinds recorded.

## Where the numbers come from

| section | list | bytes |
| --- | --- | --- |
| npm | `data.jsdelivr.com/v1/packages/npm/<pkg>` | the per-file SHA-256 jsDelivr publishes for the package tarball |
| cdnjs | `api.cdnjs.com/libraries/<lib>` | downloaded from `cdnjs.cloudflare.com`; each file's SHA-512 is checked against the `sri` value cdnjs publishes before its SHA-256 is recorded |
| maven | `repo1.maven.org/.../maven-metadata.xml` | the jar, read member by member |
| wordpress | `api.wordpress.org/core/stable-check/1.0/` | `downloads.wordpress.org/release/wordpress-<v>.zip` |
| releases | the URL in each file's `source` | — |
| git | `git ls-remote --tags` on the repository | — |
| wp-plugin, wp-theme | `api.wordpress.org/{plugins,themes}/info/1.2/` | — |
| pypi, rubygems, docker | the registry's own API | — |

## Running it

```sh
python collect.py all --deadline-minutes 45   # incremental: only what is not in data/ yet
python check.py                               # structure and shape of every file
```

Standard library only; Python 3.11 or newer.
