"""Archivist Agent - Living context maintenance."""

from pathlib import Path
from typing import Dict, List, Any
import json
import networkx as nx
from datetime import datetime

from ..models.nodes import ModuleNode
from ..graph.knowledge_graph import KnowledgeGraphManager


class ArchivistAgent:
    """Agent responsible for maintaining living documentation."""
    
    def __init__(self, repo_path: str, output_dir: str = ".cartography"):
        self.repo_path = Path(repo_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.trace_log: List[Dict[str, Any]] = []
    
    def archive(self, knowledge_graph: KnowledgeGraphManager, lineage_graph: nx.DiGraph,
               day_one_answers: Dict[str, str]):
        """Generate all living documentation artifacts."""
        print("📚 Archivist: Generating living documentation")
        
        # Generate CODEBASE.md
        codebase_md = self._generate_codebase_md(knowledge_graph, lineage_graph)
        self._write_file("CODEBASE.md", codebase_md)
        
        # Generate onboarding brief
        onboarding_brief = self._generate_onboarding_brief(
            knowledge_graph, day_one_answers
        )
        self._write_file("onboarding_brief.md", onboarding_brief)
        
        # Export graphs
        self._export_graphs(knowledge_graph, lineage_graph)
        
        # Write trace log
        self._write_trace_log()
        
        print(f"✅ Archivist: Artifacts saved to {self.output_dir}")
    
    def _generate_codebase_md(self, kg: KnowledgeGraphManager, 
                              lineage_graph: nx.DiGraph) -> str:
        """Generate the living context file."""
        sections = []
        
        # Header
        sections.append("# CODEBASE.md - Living Architecture Context\n")
        sections.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        
        # Architecture Overview
        sections.append("## 🏗️ Architecture Overview\n")
        sections.append(self._get_architecture_overview(kg))
        
        # Critical Path
        sections.append("\n## 🎯 Critical Path (Top Modules by PageRank)\n")
        sections.append(self._get_critical_path(kg))
        
        # Data Sources & Sinks
        sections.append("\n## 💾 Data Sources & Sinks\n")
        sections.append(self._get_data_sources_sinks(lineage_graph))
        
        # Known Debt
        sections.append("\n## ⚠️ Known Technical Debt\n")
        sections.append(self._get_known_debt(kg))
        
        # High-Velocity Files
        sections.append("\n## 📈 High-Velocity Files (Last 30 days)\n")
        sections.append(self._get_high_velocity_files(kg))
        
        # Module Purpose Index
        sections.append("\n## 📋 Module Purpose Index\n")
        sections.append(self._get_module_purpose_index(kg))
        
        return '\n'.join(sections)
    
    def _get_architecture_overview(self, kg: KnowledgeGraphManager) -> str:
        """Generate architecture overview section."""
        lines = []
        
        # Count languages
        languages = {}
        for module in kg.modules.values():
            lang = module.language
            languages[lang] = languages.get(lang, 0) + 1
        
        lines.append(f"**Total Modules:** {len(kg.modules)}")
        lines.append(f"**Languages:** {', '.join([f'{k} ({v})' for k, v in languages.items()])}")
        lines.append(f"**Datasets:** {len(kg.datasets)}")
        lines.append(f"**Transformations:** {len(kg.transformations)}")
        
        # Domain distribution
        domains = {}
        for module in kg.modules.values():
            if module.domain_cluster:
                domains[module.domain_cluster] = domains.get(module.domain_cluster, 0) + 1
        
        if domains:
            lines.append("\n**Domain Distribution:**")
            for domain, count in domains.items():
                lines.append(f"- {domain}: {count} modules")
        
        return '\n'.join(lines)
    
    def _get_critical_path(self, kg: KnowledgeGraphManager) -> str:
        """Get critical path modules."""
        # Modules with domain_cluster = "critical_path" from PageRank
        critical_modules = [
            m for m in kg.modules.values() 
            if m.domain_cluster == "critical_path"
        ]
        
        if not critical_modules:
            return "No critical path modules identified yet."
        
        lines = []
        for module in critical_modules[:5]:
            lines.append(f"- **{module.path}**")
            if module.purpose_statement:
                lines.append(f"  - *{module.purpose_statement}*")
        
        return '\n'.join(lines)
    
    def _get_data_sources_sinks(self, lineage_graph: nx.DiGraph) -> str:
        """Get data sources and sinks."""
        lines = []
        
        sources = []
        sinks = []
        
        for node in lineage_graph.nodes():
            if 'dataset:' in node:
                if lineage_graph.in_degree(node) == 0:
                    sources.append(node.replace('dataset:', ''))
                if lineage_graph.out_degree(node) == 0:
                    sinks.append(node.replace('dataset:', ''))
        
        lines.append("**Source Datasets (Entry Points):**")
        for source in sources[:5]:
            lines.append(f"- {source}")
        
        lines.append("\n**Sink Datasets (Exit Points):**")
        for sink in sinks[:5]:
            lines.append(f"- {sink}")
        
        return '\n'.join(lines)
    
    def _get_known_debt(self, kg: KnowledgeGraphManager) -> str:
        """Get known technical debt indicators."""
        lines = []
        
        # Dead code candidates
        dead_code = [m.path for m in kg.modules.values() 
                    if m.is_dead_code_candidate]
        
        if dead_code:
            lines.append("**Dead Code Candidates:**")
            for path in dead_code[:5]:
                lines.append(f"- {path}")
        else:
            lines.append("No dead code candidates identified.")
        
        # Circular dependencies would go here
        lines.append("\n**Documentation Drift:**")
        # This would come from Semanticist
        
        return '\n'.join(lines)
    
    def _get_high_velocity_files(self, kg: KnowledgeGraphManager) -> str:
        """Get high-velocity files."""
        high_velocity = sorted(
            [(m.path, m.change_velocity_30d) for m in kg.modules.values()],
            key=lambda x: x[1], reverse=True
        )[:10]
        
        lines = []
        for path, velocity in high_velocity:
            if velocity > 0:
                lines.append(f"- **{path}**: {velocity} changes")
        
        if not lines:
            lines.append("No change data available or no changes in last 30 days.")
        
        return '\n'.join(lines)
    
    def _get_module_purpose_index(self, kg: KnowledgeGraphManager) -> str:
        """Get module purpose index."""
        lines = []
        
        for path, module in list(kg.modules.items())[:20]:
            if module.purpose_statement:
                lines.append(f"- **{path}**: {module.purpose_statement}")
        
        if len(kg.modules) > 20:
            lines.append(f"\n... and {len(kg.modules) - 20} more modules")
        
        return '\n'.join(lines)
    
    def _generate_onboarding_brief(self, kg: KnowledgeGraphManager,
                                   day_one_answers: Dict[str, str]) -> str:
        """Generate onboarding brief for new FDEs."""
        sections = []
        
        sections.append("# 🚀 FDE Day-One Onboarding Brief\n")
        sections.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        
        sections.append("## 📋 The Five Critical Questions\n")
        
        for q_num, answer in day_one_answers.items():
            sections.append(f"\n### Question {q_num}")
            sections.append(answer)
        
        sections.append("\n## 🔍 Quick Start Commands\n")
        sections.append("```bash")
        sections.append("# Analyze the codebase")
        sections.append("python -m src.cli analyze /path/to/repo")
        sections.append("")
        sections.append("# Query the knowledge graph")
        sections.append("python -m src.cli query")
        sections.append("```")
        
        sections.append("\n## 🗺️ Key Resources")
        sections.append("- [CODEBASE.md](./CODEBASE.md) - Living architecture context")
        sections.append("- [module_graph.json](./module_graph.json) - Module dependency graph")
        sections.append("- [lineage_graph.json](./lineage_graph.json) - Data lineage graph")
        sections.append("- [cartography_trace.jsonl](./cartography_trace.jsonl) - Analysis audit log")
        
        return '\n'.join(sections)
    
    def _export_graphs(self, kg: KnowledgeGraphManager, lineage_graph: nx.DiGraph):
        """Export graphs to JSON."""
        # Export module graph
        module_graph_data = {
            "nodes": [
                {"id": path, **module.dict()}
                for path, module in kg.modules.items()
            ],
            "edges": [
                {"source": e.source_module, "target": e.target_module}
                for e in kg.imports
            ]
        }
        
        self._write_json("module_graph.json", module_graph_data)
        
        # Export lineage graph
        lineage_data = nx.node_link_data(lineage_graph)
        self._write_json("lineage_graph.json", lineage_data)
    
    def _write_trace_log(self):
        """Write the trace log."""
        log_path = self.output_dir / "cartography_trace.jsonl"
        with open(log_path, 'w') as f:
            for entry in self.trace_log:
                f.write(json.dumps(entry) + '\n')
    
    def _write_file(self, filename: str, content: str):
        """Write a text file to the output directory."""
        path = self.output_dir / filename
        with open(path, 'w',encoding='utf-8') as f:
            f.write(content)
    
    def _write_json(self, filename: str, data: Any):
        """Write a JSON file to the output directory."""
        path = self.output_dir / filename
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def log_action(self, action: str, agent: str, details: Dict[str, Any]):
        """Log an action to the trace."""
        self.trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": agent,
            "action": action,
            "details": details
        })