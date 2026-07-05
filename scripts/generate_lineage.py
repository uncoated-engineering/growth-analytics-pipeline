"""Render the lineage graph from the YAML contracts in contracts/.

Outputs:
- docs/lineage.md    - Mermaid flowchart of raw files -> bronze -> silver -> gold
- docs/lineage.json  - OpenLineage-style dataset/job documents (static
                       declaration; the same shape a runtime emitter would
                       produce, minus run facets)

The contracts' `upstreams` fields are the single source of truth. A test
asserts they stay consistent with the Airflow Dataset wiring.
"""

import json

from semantic.catalog import PROJECT_ROOT, Catalog

OUTPUT_MD = PROJECT_ROOT / "docs" / "lineage.md"
OUTPUT_JSON = PROJECT_ROOT / "docs" / "lineage.json"

NAMESPACE = "growth-analytics-pipeline"


def render_mermaid(catalog: Catalog) -> str:
    lines = [
        "# Data Lineage",
        "",
        "> Generated from the YAML data contracts in `contracts/` by",
        "> `scripts/generate_lineage.py`. Do not edit by hand.",
        "",
        "```mermaid",
        "flowchart LR",
    ]

    for layer, title in (("bronze", "Bronze"), ("silver", "Silver"), ("gold", "Gold")):
        lines.append(f'  subgraph {layer}["{title}"]')
        for t in catalog.by_layer(layer):
            lines.append(f"    {t.table}[{t.table}]")
        lines.append("  end")

    # Raw file sources feed bronze
    lines.append('  subgraph raw["Raw files"]')
    for t in catalog.by_layer("bronze"):
        if t.source:
            source_id = f"src_{t.table}"
            source_label = t.source.split(" ")[0].split("/")[-1]
            lines.append(f"    {source_id}[({source_label})]")
    lines.append("  end")
    for t in catalog.by_layer("bronze"):
        if t.source:
            lines.append(f"  src_{t.table} --> {t.table}")

    for upstream, downstream in catalog.lineage_edges():
        lines.append(f"  {upstream} --> {downstream}")

    lines += ["```", ""]
    return "\n".join(lines)


def render_openlineage(catalog: Catalog) -> dict:
    """Static OpenLineage-style export: datasets + the jobs that link them."""
    datasets = []
    jobs = []
    for t in catalog.tables.values():
        datasets.append(
            {
                "namespace": NAMESPACE,
                "name": f"{t.layer}.{t.table}",
                "facets": {
                    "documentation": {"description": t.description},
                    "schema": {
                        "fields": [
                            {"name": c.name, "type": c.type, "description": c.description}
                            for c in t.columns
                        ]
                    },
                },
            }
        )
        if t.layer == "bronze" and t.source:
            inputs = [{"namespace": NAMESPACE, "name": f"raw.{t.source.split(' ')[0]}"}]
        else:
            inputs = [
                {
                    "namespace": NAMESPACE,
                    "name": f"{catalog.tables[u].layer}.{u}",
                }
                for u in t.upstreams
            ]
        jobs.append(
            {
                "namespace": NAMESPACE,
                "name": f"{t.layer}_{t.table}" if not t.table.startswith(t.layer) else t.table,
                "inputs": inputs,
                "outputs": [{"namespace": NAMESPACE, "name": f"{t.layer}.{t.table}"}],
            }
        )
    return {"datasets": datasets, "jobs": jobs}


def main() -> None:
    catalog = Catalog()
    OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_MD.write_text(render_mermaid(catalog))
    OUTPUT_JSON.write_text(json.dumps(render_openlineage(catalog), indent=2) + "\n")
    print(
        f"Wrote {OUTPUT_MD.relative_to(PROJECT_ROOT)} and {OUTPUT_JSON.relative_to(PROJECT_ROOT)}"
    )


if __name__ == "__main__":
    main()
