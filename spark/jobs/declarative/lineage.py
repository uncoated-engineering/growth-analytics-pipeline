"""
Pipeline lineage and impact analysis.

Generates data lineage graphs and impact analysis reports from the
pipeline manifest. Useful for understanding data flow, planning changes,
and debugging pipeline issues.

Features:
    - Dependency graph visualization (ASCII and DOT format)
    - Impact analysis: "If I change table X, what's affected?"
    - Upstream trace: "Where does this data come from?"
    - Pipeline health dashboard

Usage:
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest()
    print(lineage.ascii_graph())
    print(lineage.impact_analysis("bronze.feature_releases"))
"""

from __future__ import annotations

from spark.jobs.declarative.manifest import PipelineManifest, TableDefinition, load_manifest


class PipelineLineage:
    """Pipeline lineage graph and impact analysis."""

    def __init__(self, manifest: PipelineManifest) -> None:
        self._manifest = manifest

    @classmethod
    def from_manifest(cls, path: str | None = None) -> PipelineLineage:
        return cls(load_manifest(path))

    def ascii_graph(self) -> str:
        """Generate an ASCII representation of the pipeline dependency graph."""
        lines = [
            "Pipeline Lineage Graph",
            "=" * 60,
            "",
        ]

        for layer in ["bronze", "silver", "gold"]:
            tables = self._manifest.get_layer(layer)
            if not tables:
                continue

            lines.append(f"  [{layer.upper()}]")
            for table in tables:
                downstream = self._manifest.get_downstream(table.qualified_name)
                marker = "+" if downstream else "-"
                lines.append(f"    {marker} {table.name}")

                # Show upstream connections
                for dep in table.depends_on:
                    lines.append(f"      <- {dep}")

                # Show downstream connections
                for ds in downstream:
                    lines.append(f"      -> {ds.qualified_name}")

            lines.append("")

        return "\n".join(lines)

    def dot_graph(self) -> str:
        """Generate a DOT (Graphviz) representation of the lineage graph."""
        lines = [
            "digraph pipeline {",
            "  rankdir=LR;",
            "  node [shape=box, style=filled];",
            "",
        ]

        # Color nodes by layer
        layer_colors = {
            "bronze": "#CD7F32",
            "silver": "#C0C0C0",
            "gold": "#FFD700",
        }

        # Subgraphs for each layer
        for layer in ["bronze", "silver", "gold"]:
            tables = self._manifest.get_layer(layer)
            if not tables:
                continue
            color = layer_colors.get(layer, "#FFFFFF")
            lines.append(f"  subgraph cluster_{layer} {{")
            lines.append(f'    label="{layer.upper()}";')
            lines.append("    style=filled;")
            lines.append("    color=lightgrey;")
            for table in tables:
                node_id = table.qualified_name.replace(".", "_")
                lines.append(f'    {node_id} [label="{table.name}", fillcolor="{color}"];')
            lines.append("  }")
            lines.append("")

        # Edges
        for table in self._manifest.tables.values():
            target_id = table.qualified_name.replace(".", "_")
            for dep in table.depends_on:
                source_id = dep.replace(".", "_")
                lines.append(f"  {source_id} -> {target_id};")

        lines.append("}")
        return "\n".join(lines)

    def impact_analysis(self, qualified_name: str) -> str:
        """Analyze the downstream impact of changing a table.

        Args:
            qualified_name: e.g., "bronze.feature_releases"

        Returns:
            Human-readable impact analysis report.
        """
        affected = self._get_all_downstream(qualified_name)

        lines = [
            f"Impact Analysis: {qualified_name}",
            "=" * 60,
            "",
        ]

        if not affected:
            lines.append("  No downstream dependencies. Change is isolated.")
            return "\n".join(lines)

        lines.append(f"  Direct downstream: {len(self._manifest.get_downstream(qualified_name))}")
        lines.append(f"  Total affected: {len(affected)}")
        lines.append("")

        # Group by layer
        for layer in ["bronze", "silver", "gold"]:
            layer_affected = [t for t in affected if t.layer == layer]
            if layer_affected:
                lines.append(f"  [{layer.upper()}] ({len(layer_affected)} affected)")
                for table in layer_affected:
                    depth = self._depth_from(qualified_name, table.qualified_name)
                    indent = "    " + "  " * depth
                    lines.append(f"{indent}{table.name}: {table.description[:50]}")

        return "\n".join(lines)

    def upstream_trace(self, qualified_name: str) -> str:
        """Trace all upstream dependencies of a table.

        Args:
            qualified_name: e.g., "gold.feature_conversion_impact"

        Returns:
            Human-readable upstream trace.
        """
        ancestors = self._get_all_upstream(qualified_name)

        lines = [
            f"Upstream Trace: {qualified_name}",
            "=" * 60,
            "",
        ]

        if not ancestors:
            lines.append("  No upstream dependencies. This is a source table.")
            return "\n".join(lines)

        for layer in ["bronze", "silver", "gold"]:
            layer_ancestors = [t for t in ancestors if t.layer == layer]
            if layer_ancestors:
                lines.append(f"  [{layer.upper()}]")
                for table in layer_ancestors:
                    lines.append(f"    - {table.name}: {table.description[:50]}")

        return "\n".join(lines)

    def pipeline_health(self) -> str:
        """Generate a pipeline health summary showing quality rules and freshness."""
        lines = [
            "Pipeline Health Dashboard",
            "=" * 60,
            "",
        ]

        for layer in ["bronze", "silver", "gold"]:
            tables = self._manifest.get_layer(layer)
            if not tables:
                continue

            lines.append(f"  [{layer.upper()}]")
            for table in tables:
                n_input_rules = len(table.input_quality_rules)
                n_output_rules = len(table.output_quality_rules)
                freshness = f"{table.freshness.max_age_hours}h SLA" if table.freshness else "no SLA"
                n_metrics = len(table.metrics)
                parts = [
                    f"quality: {n_input_rules}in/{n_output_rules}out rules",
                    f"freshness: {freshness}",
                ]
                if n_metrics:
                    parts.append(f"metrics: {n_metrics}")

                lines.append(f"    {table.name}: {', '.join(parts)}")
            lines.append("")

        return "\n".join(lines)

    def _get_all_downstream(self, qualified_name: str) -> list[TableDefinition]:
        """Recursively collect all downstream dependencies."""
        visited: set[str] = set()
        result: list[TableDefinition] = []

        def collect(name: str) -> None:
            for table in self._manifest.get_downstream(name):
                if table.qualified_name not in visited:
                    visited.add(table.qualified_name)
                    result.append(table)
                    collect(table.qualified_name)

        collect(qualified_name)
        return result

    def _get_all_upstream(self, qualified_name: str) -> list[TableDefinition]:
        """Recursively collect all upstream dependencies."""
        visited: set[str] = set()
        result: list[TableDefinition] = []

        def collect(name: str) -> None:
            table = self._manifest.tables.get(name)
            if not table:
                return
            for dep in table.depends_on:
                if dep not in visited and dep in self._manifest.tables:
                    visited.add(dep)
                    result.append(self._manifest.tables[dep])
                    collect(dep)

        collect(qualified_name)
        return result

    def _depth_from(self, source: str, target: str, current_depth: int = 0) -> int:
        """Calculate the shortest path depth from source to target."""
        if source == target:
            return current_depth

        downstream = self._manifest.get_downstream(source)
        for table in downstream:
            if table.qualified_name == target:
                return current_depth + 1
            depth = self._depth_from(table.qualified_name, target, current_depth + 1)
            if depth > 0:
                return depth
        return current_depth
