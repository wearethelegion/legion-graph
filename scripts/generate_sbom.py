#!/usr/bin/env python3
"""Regenerate sbom.json and SBOM.md from running container images.

Usage:
    python3 scripts/generate_sbom.py

Prerequisite:
    The legion-graph stack must be built (Docker images present locally).
    The stack does not need to be running; this only inspects the images.

Output:
    sbom.json  — machine-readable, full data
    SBOM.md    — human-readable, organised by license family + per-package table
"""
from __future__ import annotations

import datetime
import json
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
IMAGES = [
    "kgrag-auth",
    "kgrag-rest-api",
    "kgrag-cognee",
    "kgrag-search",
    "kgrag-ingestion",
]
COPYLEFT_KEYWORDS = ("GPL", "AGPL", "LGPL", "SSPL", "MPL", "EPL", "CDDL")
PERMISSIVE_KEYWORDS = ("MIT", "BSD", "APACHE", "ISC", "PSF", "UNLICENSE", "PUBLIC DOMAIN", "ZLIB")

EXTRACT_SCRIPT = r"""
import importlib.metadata as m, json, sys
out = []
for dist in m.distributions():
    meta = dist.metadata
    lic = meta.get('License') or ''
    if not lic or lic.upper() == 'UNKNOWN':
        for c in (meta.get_all('Classifier') or []):
            if c.startswith('License :: '):
                lic = c.replace('License :: OSI Approved :: ', '').replace('License :: ', '')
                break
    out.append({
        'name': meta.get('Name') or dist.name,
        'version': meta.get('Version') or dist.version,
        'license': (lic or 'UNKNOWN').strip().split('\n')[0][:120],
        'summary': (meta.get('Summary') or '').strip()[:200],
        'url': meta.get('Home-page') or '',
    })
seen, deduped = set(), []
for pkg in sorted(out, key=lambda p: p['name'].lower()):
    k = pkg['name'].lower()
    if k in seen:
        continue
    seen.add(k)
    deduped.append(pkg)
json.dump(deduped, sys.stdout)
"""


def inventory_image(image: str, extract_path: Path) -> list[dict]:
    """Run the extract script inside a container and return its package list."""
    full_image = f"{image}:latest"
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{extract_path}:/tmp/extract.py:ro",
        "--entrypoint", "python3",
        full_image,
        "/tmp/extract.py",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        sys.stderr.write(f"  WARN: {image} extraction returned {result.returncode}\n")
        sys.stderr.write(result.stderr[:500] + "\n")
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"  ERROR: {image} produced invalid JSON: {exc}\n")
        return []


def main() -> int:
    if not shutil.which("docker"):
        sys.stderr.write("ERROR: docker is required but not on PATH\n")
        return 1

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as fp:
        fp.write(EXTRACT_SCRIPT)
        extract_path = Path(fp.name)

    try:
        by_image = {}
        for img in IMAGES:
            print(f"Inventorying {img}...", flush=True)
            pkgs = inventory_image(img, extract_path)
            by_image[img] = pkgs
            print(f"  → {len(pkgs)} packages")
    finally:
        extract_path.unlink(missing_ok=True)

    # Build union
    union: dict[str, dict] = {}
    for img, pkgs in by_image.items():
        for p in pkgs:
            key = p["name"].lower()
            if key not in union:
                union[key] = {**p, "in_images": []}
            if img not in union[key]["in_images"]:
                union[key]["in_images"].append(img)

    license_counts: dict[str, int] = defaultdict(int)
    for pkg in union.values():
        license_counts[pkg["license"]] += 1

    copyleft = []
    permissive_count = 0
    unknown_count = 0
    for pkg in union.values():
        lic_upper = pkg["license"].upper()
        if lic_upper == "UNKNOWN":
            unknown_count += 1
            continue
        is_copyleft = any(kw in lic_upper for kw in COPYLEFT_KEYWORDS)
        is_permissive_only = any(kw in lic_upper for kw in PERMISSIVE_KEYWORDS) and not is_copyleft
        if is_copyleft and not is_permissive_only:
            copyleft.append(pkg)
        else:
            permissive_count += 1

    sbom = {
        "metadata": {
            "project": "legion-graph",
            "generated_at": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
            "generator": "scripts/generate_sbom.py (importlib.metadata inside each container)",
            "image_count": len(IMAGES),
            "images": IMAGES,
            "unique_python_packages": len(union),
            "permissive_count_approx": permissive_count,
            "unknown_count": unknown_count,
            "copyleft_family_count": len(copyleft),
        },
        "license_distribution": dict(sorted(license_counts.items(), key=lambda x: -x[1])),
        "copyleft_dependencies_for_review": copyleft,
        "packages": sorted(union.values(), key=lambda p: p["name"].lower()),
    }
    (REPO_ROOT / "sbom.json").write_text(json.dumps(sbom, indent=2) + "\n")

    print(f"\nWrote sbom.json — {len(union)} unique packages")
    print(f"  Permissive: ~{permissive_count}")
    print(f"  Unknown:    {unknown_count}")
    print(f"  Copyleft:   {len(copyleft)}")
    print("\nNote: SBOM.md is hand-curated (with reviewer commentary on UNKNOWN")
    print("and weak-copyleft packages) and is NOT auto-regenerated by this script.")
    print("After bumping dependencies, regenerate sbom.json (this script) then")
    print("manually refresh the curated commentary in SBOM.md against the new data.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
