#!/usr/bin/env python3
"""Validate OWL ontology files using rdflib — same parser Cognee uses.

Loads each .owl file, lists all owl:Class and owl:ObjectProperty entries,
and checks fuzzy-match distinctness at 80% threshold (Cognee's cutoff).
"""

import sys
from pathlib import Path
from difflib import SequenceMatcher

try:
    from rdflib import Graph, Namespace, RDF, RDFS, OWL
except ImportError:
    print("ERROR: rdflib not installed. Run: pip install rdflib")
    sys.exit(1)


def validate_owl(file_path: str) -> bool:
    """Load and validate a single OWL file."""
    print(f"\n{'=' * 60}")
    print(f"Validating: {file_path}")
    print(f"{'=' * 60}")

    g = Graph()
    try:
        g.parse(file_path, format="xml")
        print(f"✅ Parsed successfully ({len(g)} triples)")
    except Exception as e:
        print(f"❌ Parse FAILED: {e}")
        return False

    # Extract classes
    classes = []
    for cls in g.subjects(RDF.type, OWL.Class):
        name = str(cls).split("#")[-1] if "#" in str(cls) else str(cls).split("/")[-1]
        classes.append(name)
    classes.sort()

    print(f"\n📦 Classes ({len(classes)}):")
    for c in classes:
        print(f"   - {c}")

    # Extract object properties
    props = []
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        name = str(prop).split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]
        props.append(name)
    props.sort()

    print(f"\n🔗 ObjectProperties ({len(props)}):")
    for p in props:
        print(f"   - {p}")

    # Check fuzzy-match collisions at 80% threshold
    print(f"\n🔍 Fuzzy-match collision check (80% threshold):")
    normalized = {c: c.lower().replace(" ", "_").strip() for c in classes}
    collisions = []
    names = list(normalized.values())
    for i, n1 in enumerate(names):
        for n2 in names[i + 1 :]:
            ratio = SequenceMatcher(None, n1, n2).ratio()
            if ratio >= 0.8:
                c1 = [k for k, v in normalized.items() if v == n1][0]
                c2 = [k for k, v in normalized.items() if v == n2][0]
                collisions.append((c1, c2, ratio))

    if collisions:
        print("   ⚠️  Potential collisions:")
        for c1, c2, ratio in collisions:
            print(f"      {c1} ↔ {c2} (ratio={ratio:.2f})")
    else:
        print("   ✅ No collisions — all class names are distinct")

    return True


if __name__ == "__main__":
    owl_dir = Path(__file__).parent
    owl_files = sorted(owl_dir.glob("*.owl"))

    if not owl_files:
        print("No .owl files found in", owl_dir)
        sys.exit(1)

    all_ok = True
    for f in owl_files:
        if not validate_owl(str(f)):
            all_ok = False

    print(f"\n{'=' * 60}")
    if all_ok:
        print("✅ All OWL files validated successfully")
    else:
        print("❌ Some files failed validation")
    sys.exit(0 if all_ok else 1)
