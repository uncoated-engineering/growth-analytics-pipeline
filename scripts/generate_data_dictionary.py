"""Render docs/data_dictionary.md from the YAML contracts in contracts/.

The contracts are the single source of truth; this script only formats them.
Run via `make docs` (or directly) after changing any contract.
"""

from semantic.catalog import LAYERS, PROJECT_ROOT, Catalog

OUTPUT = PROJECT_ROOT / "docs" / "data_dictionary.md"

LAYER_BLURBS = {
    "bronze": "Raw sources landed as-is in Delta, plus an `ingestion_timestamp`.",
    "silver": "Cleaned, conformed, analysis-ready entities.",
    "gold": "Business-level marts consumed by analysts, dashboards, and the chatbot.",
}


def render(catalog: Catalog) -> str:
    lines = [
        "# Data Dictionary",
        "",
        "> Generated from the YAML data contracts in `contracts/` by",
        "> `scripts/generate_data_dictionary.py`. Do not edit by hand —",
        "> edit the contract and regenerate (`make docs`).",
        "",
    ]

    for layer in LAYERS:
        tables = catalog.by_layer(layer)
        lines += [f"## {layer.capitalize()} layer", "", LAYER_BLURBS[layer], ""]
        for t in tables:
            lines += [f"### `{t.table}`", ""]
            lines += [t.description, ""]
            lines += [f"- **Grain**: {t.grain}"]
            lines += [f"- **Path**: `{t.path}`"]
            lines += [f"- **Owner**: {t.owner}"]
            if t.source:
                lines += [f"- **Source**: {t.source}"]
            if t.partitioned_by:
                lines += [f"- **Partitioned by**: {', '.join(t.partitioned_by)}"]
            if t.upstreams:
                upstream_links = ", ".join(f"`{u}`" for u in t.upstreams)
                lines += [f"- **Upstreams**: {upstream_links}"]
            lines += ["", "| Column | Type | Description |", "|---|---|---|"]
            for c in t.columns:
                lines += [f"| `{c.name}` | {c.type} | {c.description} |"]
            lines += [""]

    return "\n".join(lines)


def main() -> None:
    catalog = Catalog()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(render(catalog))
    print(f"Wrote {OUTPUT.relative_to(PROJECT_ROOT)} ({len(catalog.tables)} tables)")


if __name__ == "__main__":
    main()
