#!/usr/bin/env python3
"""Interactive CLI for the Brownfield Cartographer."""

import sys
import os
from pathlib import Path
import click
from typing import Optional, Dict, Any
import tempfile
import subprocess
import time
from datetime import datetime
import json

# Rich imports for beautiful terminal UI
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.syntax import Syntax
from rich.markdown import Markdown
from rich.tree import Tree
from rich import print as rprint
from rich.prompt import Prompt, Confirm
from rich.layout import Layout
from rich.live import Live
from rich.text import Text
from rich.columns import Columns
from rich import box

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.orchestrator import Orchestrator

# Initialize rich console
console = Console()


class InteractiveShell:
    """Interactive shell for the Brownfield Cartographer."""
    
    def __init__(self, orchestrator: Orchestrator):
        self.orch = orchestrator
        self.history = []
        self.commands = {
            "help": self.show_help,
            "h": self.show_help,
            "?": self.show_help,
            "exit": self.exit_shell,
            "quit": self.exit_shell,
            "q": self.exit_shell,
            "clear": self.clear_screen,
            "cls": self.clear_screen,
            "status": self.show_status,
            "stats": self.show_stats,
            "explain": self.explain_module,
            "find": self.find_implementation,
            "trace": self.trace_lineage,
            "lineage": self.trace_lineage,
            "blast": self.blast_radius,
            "radius": self.blast_radius,
            "graph": self.show_graph,
            "viz": self.visualize,
            "sources": self.show_sources,
            "sinks": self.show_sinks,
            "critical": self.show_critical,
            "domains": self.show_domains,
            "search": self.semantic_search,
            "save": self.save_session,
            "load": self.load_session,
            "export": self.export_graph,
            "reload": self.reload_analysis,
        }
        
        # Command history for persistence
        self.history_file = Path.home() / ".cartographer_history"
        self.load_history()
    
    def show_banner(self):
        """Display welcome banner."""
        banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║     ██████╗ ██████╗  ██████╗ ██╗    ██╗███╗   ██╗           ║
║     ██╔══██╗██╔══██╗██╔═══██╗██║    ██║████╗  ██║           ║
║     ██████╔╝██████╔╝██║   ██║██║ █╗ ██║██╔██╗ ██║           ║
║     ██╔══██╗██╔══██╗██║   ██║██║███╗██║██║╚██╗██║           ║
║     ██████╔╝██║  ██║╚██████╔╝╚███╔███╔╝██║ ╚████║           ║
║     ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═══╝           ║
║                                                              ║
║     ██████╗ █████╗ ██████╗ ████████╗ ██████╗  ██████╗       ║
║     ██╔════╝██╔══██╗██╔══██╗╚══██╔══╝██╔═══██╗██╔════╝       ║
║     ██║     ███████║██████╔╝   ██║   ██║   ██║██║            ║
║     ██║     ██╔══██║██╔══██╗   ██║   ██║   ██║██║            ║
║     ╚██████╗██║  ██║██║  ██║   ██║   ╚██████╔╝╚██████╗       ║
║      ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝  ╚═════╝       ║
║                                                              ║
║     Codebase Intelligence System v1.0                        ║
║     Type 'help' for commands, 'exit' to quit                 ║
╚══════════════════════════════════════════════════════════════╝
        """
        console.print(banner, style="bold cyan")
        
        # Show quick stats if available
        if self.orch.kg_manager and self.orch.kg_manager.model.modules:
            self.show_status()
    
    def show_help(self, args=None):
        """Display help information."""
        help_table = Table(title="📚 Available Commands", box=box.ROUNDED, show_header=True)
        help_table.add_column("Command", style="cyan", no_wrap=True)
        help_table.add_column("Description", style="white")
        help_table.add_column("Example", style="dim")
        
        commands = [
            ("help, h, ?", "Show this help message", "help"),
            ("exit, quit, q", "Exit the shell", "exit"),
            ("clear, cls", "Clear the screen", "clear"),
            ("status", "Show current analysis status", "status"),
            ("stats", "Show detailed statistics", "stats"),
            ("explain <path>", "Explain what a module does", "explain src/main.py"),
            ("find <concept>", "Find implementation of a concept", "find revenue calculation"),
            ("trace <dataset>", "Trace data lineage", "trace daily_active_users"),
            ("blast <module>", "Calculate blast radius", "blast src/models/user.py"),
            ("sources", "Show source datasets", "sources"),
            ("sinks", "Show sink datasets", "sinks"),
            ("critical", "Show critical modules", "critical"),
            ("domains", "Show domain clusters", "domains"),
            ("search <query>", "Semantic search", "search data ingestion"),
            ("graph", "Show graph statistics", "graph"),
            ("viz", "Visualize knowledge graph", "viz"),
            ("save <name>", "Save session", "save my_analysis"),
            ("load <name>", "Load session", "load my_analysis"),
            ("export <format>", "Export graph (json/gexf/graphml)", "export json"),
            ("reload", "Reload analysis", "reload"),
        ]
        
        for cmd, desc, example in commands:
            help_table.add_row(cmd, desc, example)
        
        console.print(help_table)
        
        # Show query patterns
        console.print("\n[bold yellow]🔍 Query Patterns:[/bold yellow]")
        patterns = Table(box=box.SIMPLE, show_header=False)
        patterns.add_column("Pattern", style="green")
        patterns.add_column("Example", style="dim")
        patterns.add_rows([
            ("Where is X?", "where is the revenue calculation"),
            ("What does X do?", "what does kafka_consumer.py do"),
            ("Trace lineage of X", "trace lineage of users table"),
            ("What breaks if X changes?", "what breaks if I change models/user.py"),
            ("Find all X", "find all database connections"),
        ])
        console.print(patterns)
    
    def show_status(self, args=None):
        """Show current analysis status."""
        if not self.orch.kg_manager or not self.orch.kg_manager.model.modules:
            console.print("[yellow]⚠️  No analysis loaded. Run 'analyze' first.[/yellow]")
            return
        
        kg = self.orch.kg_manager.model
        
        # Create status panel
        status_text = Text()
        status_text.append("📊 Analysis Status\n", style="bold blue")
        status_text.append(f"📁 Repository: ", style="white")
        status_text.append(f"{self.orch.repo_path}\n", style="cyan")
        status_text.append(f"⏱️  Analyzed: ", style="white")
        status_text.append(f"{kg.metadata.get('analyzed_at', 'Unknown')}\n", style="cyan")
        status_text.append(f"📦 Modules: ", style="white")
        status_text.append(f"{len(kg.modules)}\n", style="green")
        status_text.append(f"💾 Datasets: ", style="white")
        status_text.append(f"{len(kg.datasets)}\n", style="green")
        status_text.append(f"🔄 Transformations: ", style="white")
        status_text.append(f"{len(kg.transformations)}\n", style="green")
        
        console.print(Panel(status_text, title="System Status", border_style="blue"))
    
    def show_stats(self, args=None):
        """Show detailed statistics."""
        if not self.orch.kg_manager:
            console.print("[yellow]⚠️  No analysis loaded.[/yellow]")
            return
        
        kg = self.orch.kg_manager.model
        
        # Language distribution
        lang_table = Table(title="🌐 Language Distribution", box=box.ROUNDED)
        lang_table.add_column("Language", style="cyan")
        lang_table.add_column("Count", justify="right", style="green")
        lang_table.add_column("Percentage", justify="right", style="yellow")
        
        languages = {}
        total = len(kg.modules)
        for module in kg.modules.values():
            lang = module.language or "unknown"
            languages[lang] = languages.get(lang, 0) + 1
        
        for lang, count in sorted(languages.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total) * 100
            lang_table.add_row(lang.capitalize(), str(count), f"{percentage:.1f}%")
        
        console.print(lang_table)
        
        # Domain distribution
        if any(m.domain_cluster for m in kg.modules.values()):
            domain_table = Table(title="🏷️ Domain Distribution", box=box.ROUNDED)
            domain_table.add_column("Domain", style="cyan")
            domain_table.add_column("Count", justify="right", style="green")
            
            domains = {}
            for module in kg.modules.values():
                if module.domain_cluster:
                    domains[module.domain_cluster] = domains.get(module.domain_cluster, 0) + 1
            
            for domain, count in sorted(domains.items(), key=lambda x: x[1], reverse=True):
                domain_table.add_row(domain, str(count))
            
            console.print(domain_table)
        
        # Complexity metrics
        metrics_table = Table(title="📈 Complexity Metrics", box=box.ROUNDED)
        metrics_table.add_column("Metric", style="cyan")
        metrics_table.add_column("Value", justify="right", style="green")
        
        total_loc = sum(m.loc for m in kg.modules.values())
        avg_complexity = sum(m.complexity_score for m in kg.modules.values()) / max(len(kg.modules), 1)
        high_velocity = sum(1 for m in kg.modules.values() if m.change_velocity_30d > 10)
        dead_code = sum(1 for m in kg.modules.values() if m.is_dead_code_candidate)
        
        metrics_table.add_row("Total Lines of Code", f"{total_loc:,}")
        metrics_table.add_row("Average Complexity", f"{avg_complexity:.2f}")
        metrics_table.add_row("High Velocity Files", str(high_velocity))
        metrics_table.add_row("Dead Code Candidates", str(dead_code))
        metrics_table.add_row("Total Datasets", str(len(kg.datasets)))
        metrics_table.add_row("Total Transformations", str(len(kg.transformations)))
        
        console.print(metrics_table)
    
    def explain_module(self, args):
        """Explain what a module does."""
        if not args:
            console.print("[red]❌ Please specify a module path[/red]")
            return
        
        module_path = args[0]
        
        with console.status(f"[bold green]Analyzing {module_path}..."):
            result = self.orch.query(f"explain {module_path}")
        
        if result.get('error'):
            console.print(f"[red]❌ {result['error']}[/red]")
            return
        
        console.print()
        for res in result['results']:
            if isinstance(res, str):
                # Check if it's markdown
                if '##' in res or '**' in res:
                    console.print(Markdown(res))
                else:
                    console.print(Panel(res, title=f"📄 {module_path}", border_style="green"))
        
        if result['evidence']:
            evidence_tree = Tree("📋 Evidence")
            for evidence in result['evidence'][:3]:
                if 'file' in evidence:
                    branch = evidence_tree.add(f"[cyan]{evidence['file']}[/cyan]")
                    if evidence.get('line'):
                        branch.add(f"Line: {evidence['line']}")
                    if evidence.get('analysis_method'):
                        branch.add(f"Method: {evidence['analysis_method']}")
            console.print(evidence_tree)
    
    def find_implementation(self, args):
        """Find implementation of a concept."""
        if not args:
            console.print("[red]❌ Please specify what to find[/red]")
            return
        
        query = ' '.join(args)
        
        with console.status(f"[bold green]Searching for '{query}'..."):
            result = self.orch.query(f"find {query}")
        
        if result.get('error'):
            console.print(f"[red]❌ {result['error']}[/red]")
            return
        
        if not result['results']:
            console.print(f"[yellow]No results found for '{query}'[/yellow]")
            return
        
        # Create results table
        table = Table(title=f"🔍 Results for: '{query}'", box=box.ROUNDED)
        table.add_column("Type", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Location", style="yellow")
        table.add_column("Details", style="white")
        
        for res in result['results']:
            if '📁 Module:' in res:
                parts = res.split('\n')
                name = parts[0].replace('📁 Module:', '').strip()
                purpose = parts[1].replace('Purpose:', '').strip() if len(parts) > 1 else ''
                table.add_row("Module", name, "", purpose[:50])
            elif '🔧 Function:' in res:
                lines = res.split('\n')
                name = lines[0].replace('🔧 Function:', '').strip()
                location = lines[1].replace('In:', '').strip() if len(lines) > 1 else ''
                signature = lines[2].replace('Signature:', '').strip() if len(lines) > 2 else ''
                table.add_row("Function", name, location, signature[:50])
        
        console.print(table)
        
        if result['evidence']:
            console.print("\n[dim]Evidence locations:[/dim]")
            for evidence in result['evidence'][:3]:
                console.print(f"  • [cyan]{evidence.get('file')}[/cyan]:{evidence.get('line', '')}")
    
    def trace_lineage(self, args):
        """Trace data lineage."""
        if not args:
            console.print("[red]❌ Please specify a dataset to trace[/red]")
            return
        
        dataset = args[0]
        direction = 'upstream'
        
        if len(args) > 1 and args[1] in ['upstream', 'downstream']:
            direction = args[1]
        
        with console.status(f"[bold green]Tracing {direction} lineage for '{dataset}'..."):
            result = self.orch.query(f"trace {direction} lineage of {dataset}")
        
        if result.get('error'):
            console.print(f"[red]❌ {result['error']}[/red]")
            return
        
        # Build lineage tree
        tree = Tree(f"[bold cyan]📊 {direction.capitalize()} Lineage for: {dataset}[/bold cyan]")
        
        if result['results']:
            for res in result['results']:
                if '💾 Dataset:' in str(res):
                    parts = str(res).split('\n')
                    name = parts[0].replace('💾 Dataset:', '').strip()
                    rel = parts[1].replace('Relationship:', '').strip() if len(parts) > 1 else ''
                    
                    if direction == 'upstream':
                        tree.add(f"[green]{name}[/green] [dim]({rel})[/dim]")
                    else:
                        tree.add(f"[yellow]{name}[/yellow] [dim]({rel})[/dim]")
        
        console.print(tree)
        
        if result['evidence']:
            console.print("\n[dim]Found in:[/dim]")
            for evidence in result['evidence'][:5]:
                console.print(f"  • [cyan]{evidence.get('file')}[/cyan] (line {evidence.get('line', 'N/A')})")
    
    def blast_radius(self, args):
        """Calculate blast radius for a module."""
        if not args:
            console.print("[red]❌ Please specify a module[/red]")
            return
        
        module = args[0]
        
        with console.status(f"[bold green]Calculating blast radius for '{module}'..."):
            result = self.orch.query(f"blast radius of {module}")
        
        if result.get('error'):
            console.print(f"[red]❌ {result['error']}[/red]")
            return
        
        if result['results']:
            console.print(f"[bold red]💥 Blast Radius: {len(result['results'])} affected modules[/bold red]\n")
            
            # Create impact table
            table = Table(box=box.ROUNDED)
            table.add_column("Module", style="cyan")
            table.add_column("Impact", style="yellow")
            
            for res in result['results'][:20]:
                if '📦 Module:' in str(res):
                    parts = str(res).split('\n')
                    name = parts[0].replace('📦 Module:', '').strip()
                    impact = parts[1].replace('Impact:', '').strip() if len(parts) > 1 else 'Direct dependency'
                    table.add_row(name, impact)
            
            console.print(table)
            
            if len(result['results']) > 20:
                console.print(f"\n[dim]... and {len(result['results']) - 20} more modules[/dim]")
        else:
            console.print("[green]✅ No downstream dependencies found[/green]")
    
    def show_sources(self, args=None):
        """Show source datasets."""
        if not self.orch.hydrologist:
            console.print("[yellow]⚠️  No lineage analysis available[/yellow]")
            return
        
        sources = self.orch.hydrologist.find_sources()
        
        if sources:
            table = Table(title="🔝 Source Datasets (Entry Points)", box=box.ROUNDED)
            table.add_column("Dataset", style="green")
            table.add_column("Type", style="cyan")
            
            for source in sources[:20]:
                dataset = self.orch.kg_manager.get_dataset(source)
                storage_type = dataset.storage_type if dataset else "unknown"
                table.add_row(source, storage_type)
            
            console.print(table)
            
            if len(sources) > 20:
                console.print(f"\n[dim]... and {len(sources) - 20} more sources[/dim]")
        else:
            console.print("[yellow]No source datasets found[/yellow]")
    
    def show_sinks(self, args=None):
        """Show sink datasets."""
        if not self.orch.hydrologist:
            console.print("[yellow]⚠️  No lineage analysis available[/yellow]")
            return
        
        sinks = self.orch.hydrologist.find_sinks()
        
        if sinks:
            table = Table(title="🎯 Sink Datasets (Exit Points)", box=box.ROUNDED)
            table.add_column("Dataset", style="yellow")
            table.add_column("Type", style="cyan")
            
            for sink in sinks[:20]:
                dataset = self.orch.kg_manager.get_dataset(sink)
                storage_type = dataset.storage_type if dataset else "unknown"
                table.add_row(sink, storage_type)
            
            console.print(table)
            
            if len(sinks) > 20:
                console.print(f"\n[dim]... and {len(sinks) - 20} more sinks[/dim]")
        else:
            console.print("[yellow]No sink datasets found[/yellow]")
    
    def show_critical(self, args=None):
        """Show critical modules."""
        if not self.orch.kg_manager:
            console.print("[yellow]⚠️  No analysis available[/yellow]")
            return
        
        critical = [
            m for m in self.orch.kg_manager.model.modules.values()
            if m.domain_cluster == "critical_path"
        ]
        
        if critical:
            table = Table(title="⭐ Critical Modules", box=box.ROUNDED)
            table.add_column("Module", style="cyan")
            table.add_column("Purpose", style="white")
            table.add_column("Complexity", justify="right", style="yellow")
            
            for module in sorted(critical, key=lambda x: x.complexity_score, reverse=True)[:10]:
                table.add_row(
                    module.path,
                    (module.purpose_statement or "N/A")[:50],
                    f"{module.complexity_score:.2f}"
                )
            
            console.print(table)
        else:
            console.print("[yellow]No critical modules identified[/yellow]")
    
    def show_domains(self, args=None):
        """Show domain clusters."""
        if not self.orch.kg_manager:
            console.print("[yellow]⚠️  No analysis available[/yellow]")
            return
        
        domains = {}
        for module in self.orch.kg_manager.model.modules.values():
            if module.domain_cluster:
                if module.domain_cluster not in domains:
                    domains[module.domain_cluster] = []
                domains[module.domain_cluster].append(module.path)
        
        if domains:
            for domain, modules in domains.items():
                panel = Panel(
                    "\n".join([f"  • {m}" for m in modules[:5]]),
                    title=f"[bold cyan]{domain}[/bold cyan] ({len(modules)} modules)",
                    border_style="blue"
                )
                console.print(panel)
                
                if len(modules) > 5:
                    console.print(f"  [dim]... and {len(modules) - 5} more[/dim]")
                console.print()
        else:
            console.print("[yellow]No domain clusters identified[/yellow]")
    
    def semantic_search(self, args):
        """Semantic search across the codebase."""
        if not args:
            console.print("[red]❌ Please specify a search query[/red]")
            return
        
        query = ' '.join(args)
        
        with console.status(f"[bold green]Searching semantically for '{query}'..."):
            # This would use embeddings for semantic search
            # For now, use the regular find
            result = self.orch.query(f"find {query}")
        
        if result.get('error'):
            console.print(f"[red]❌ {result['error']}[/red]")
            return
        
        if result['results']:
            console.print(f"[green]Found {len(result['results'])} results:[/green]\n")
            for res in result['results']:
                console.print(res)
                console.print()
    
    def show_graph(self, args=None):
        """Show graph statistics."""
        if not self.orch.kg_manager:
            console.print("[yellow]⚠️  No graph available[/yellow]")
            return
        
        graph = self.orch.kg_manager.graph
        
        stats = Table(title="📊 Graph Statistics", box=box.ROUNDED)
        stats.add_column("Metric", style="cyan")
        stats.add_column("Value", justify="right", style="green")
        
        stats.add_row("Total Nodes", str(graph.number_of_nodes()))
        stats.add_row("Total Edges", str(graph.number_of_edges()))
        stats.add_row("Density", f"{nx.density(graph):.4f}")
        
        # Find cycles
        try:
            cycles = list(nx.simple_cycles(graph))
            stats.add_row("Circular Dependencies", str(len(cycles)))
        except:
            stats.add_row("Circular Dependencies", "N/A")
        
        # Calculate average clustering
        try:
            clustering = nx.average_clustering(graph)
            stats.add_row("Avg Clustering", f"{clustering:.4f}")
        except:
            stats.add_row("Avg Clustering", "N/A")
        
        console.print(stats)
    
    def visualize(self, args=None):
        """Visualize the knowledge graph."""
        console.print("[yellow]🎨 Generating visualization...[/yellow]")
        
        try:
            from pyvis.network import Network
            
            # Create pyvis network
            net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")
            
            # Add nodes
            for node, data in self.orch.kg_manager.graph.nodes(data=True):
                color = {
                    'module': '#00ff00',
                    'dataset': '#ff9900',
                    'transformation': '#0066ff'
                }.get(data.get('type', 'module'), '#ffffff')
                
                net.add_node(node, label=node.split(':')[-1], color=color, title=str(data))
            
            # Add edges
            for u, v, data in self.orch.kg_manager.graph.edges(data=True):
                color = {
                    'imports': '#00ff00',
                    'produces': '#ff9900',
                    'consumes': '#0066ff'
                }.get(data.get('type', ''), '#ffffff')
                
                net.add_edge(u, v, color=color, title=data.get('type', ''))
            
            # Save and open
            output_path = Path(".cartography") / "graph_viz.html"
            net.show(str(output_path))
            
            console.print(f"[green]✅ Visualization saved to {output_path}[/green]")
            
            # Try to open in browser
            import webbrowser
            webbrowser.open(f"file://{output_path.absolute()}")
            
        except Exception as e:
            console.print(f"[red]❌ Error generating visualization: {e}[/red]")
    
    def save_session(self, args):
        """Save current session."""
        if not args:
            name = Prompt.ask("Enter session name", default=f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        else:
            name = args[0]
        
        session_dir = Path.home() / ".cartographer_sessions"
        session_dir.mkdir(exist_ok=True)
        
        session_path = session_dir / f"{name}.json"
        
        session_data = {
            "repo_path": str(self.orch.repo_path),
            "timestamp": datetime.now().isoformat(),
            "history": self.history[-50:],  # Save last 50 commands
            "stats": {
                "modules": len(self.orch.kg_manager.model.modules) if self.orch.kg_manager else 0,
                "datasets": len(self.orch.kg_manager.model.datasets) if self.orch.kg_manager else 0
            }
        }
        
        with open(session_path, 'w') as f:
            json.dump(session_data, f, indent=2)
        
        console.print(f"[green]✅ Session saved to {session_path}[/green]")
    
    def load_session(self, args):
        """Load a saved session."""
        session_dir = Path.home() / ".cartographer_sessions"
        
        if not session_dir.exists():
            console.print("[yellow]No saved sessions found[/yellow]")
            return
        
        sessions = list(session_dir.glob("*.json"))
        
        if not sessions:
            console.print("[yellow]No saved sessions found[/yellow]")
            return
        
        if not args:
            # Show session list
            table = Table(title="Saved Sessions", box=box.ROUNDED)
            table.add_column("Name", style="cyan")
            table.add_column("Repository", style="green")
            table.add_column("Date", style="yellow")
            
            for session_path in sessions:
                try:
                    with open(session_path, 'r') as f:
                        data = json.load(f)
                    table.add_row(
                        session_path.stem,
                        Path(data.get('repo_path', 'unknown')).name,
                        data.get('timestamp', 'unknown')[:10]
                    )
                except:
                    pass
            
            console.print(table)
            
            name = Prompt.ask("Enter session name to load")
        else:
            name = args[0]
        
        session_path = session_dir / f"{name}.json"
        
        if not session_path.exists():
            console.print(f"[red]❌ Session '{name}' not found[/red]")
            return
        
        with open(session_path, 'r') as f:
            session_data = json.load(f)
        
        console.print(f"[green]✅ Loaded session from {session_path}[/green]")
        console.print(f"Repository: {session_data.get('repo_path')}")
        console.print(f"Date: {session_data.get('timestamp')}")
    
    def export_graph(self, args):
        """Export graph in various formats."""
        if not args:
            fmt = Prompt.ask("Export format", choices=["json", "gexf", "graphml"], default="json")
        else:
            fmt = args[0]
        
        if not self.orch.kg_manager:
            console.print("[red]❌ No graph to export[/red]")
            return
        
        output_dir = Path(".cartography") / "exports"
        output_dir.mkdir(exist_ok=True)
        
        if fmt == "json":
            path = output_dir / "graph_export.json"
            self.orch.kg_manager.serialize(str(path))
        elif fmt == "gexf":
            path = output_dir / "graph_export.gexf"
            import networkx as nx
            nx.write_gexf(self.orch.kg_manager.graph, str(path))
        elif fmt == "graphml":
            path = output_dir / "graph_export.graphml"
            import networkx as nx
            nx.write_graphml(self.orch.kg_manager.graph, str(path))
        
        console.print(f"[green]✅ Graph exported to {path}[/green]")
    
    def reload_analysis(self, args=None):
        """Reload the analysis."""
        if Confirm.ask("Reload analysis? This will re-analyze the codebase"):
            with console.status("[bold green]Reloading analysis..."):
                self.orch.run_analysis(incremental=True)
            console.print("[green]✅ Analysis reloaded[/green]")
    
    def clear_screen(self, args=None):
        """Clear the terminal screen."""
        console.clear()
        self.show_banner()
    
    def exit_shell(self, args=None):
        """Exit the shell."""
        self.save_history()
        console.print("[yellow]👋 Goodbye![/yellow]")
        sys.exit(0)
    
    def load_history(self):
        """Load command history from file."""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    self.history = [line.strip() for line in f.readlines()]
            except:
                self.history = []
    
    def save_history(self):
        """Save command history to file."""
        try:
            with open(self.history_file, 'w') as f:
                for cmd in self.history[-100:]:  # Keep last 100 commands
                    f.write(cmd + '\n')
        except:
            pass
    
    def run(self):
        """Run the interactive shell."""
        self.show_banner()
        
        while True:
            try:
                # Get user input with rich prompt
                command_line = Prompt.ask(
                    "\n[bold cyan]🔍[/bold cyan]",
                    default=""
                )
                
                if not command_line.strip():
                    continue
                
                # Add to history
                self.history.append(command_line)
                
                # Parse command
                parts = command_line.strip().split()
                cmd = parts[0].lower()
                args = parts[1:] if len(parts) > 1 else []
                
                # Execute command
                if cmd in self.commands:
                    self.commands[cmd](args)
                else:
                    # Try as a natural language query
                    with console.status("[bold green]Processing query..."):
                        result = self.orch.query(command_line)
                    
                    if result.get('error'):
                        console.print(f"[red]❌ {result['error']}[/red]")
                        console.print("[dim]Type 'help' for available commands[/dim]")
                    else:
                        for res in result['results']:
                            console.print(Panel(res, border_style="green"))
            
            except KeyboardInterrupt:
                console.print("\n[yellow]Use 'exit' to quit[/yellow]")
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")


@click.group()
def cli():
    """Brownfield Cartographer - Codebase Intelligence System"""
    pass


@cli.command()
@click.argument('repo_path', type=click.Path(exists=True))
@click.option('--output-dir', '-o', default='.cartography', help='Output directory for artifacts')
@click.option('--incremental', '-i', is_flag=True, help='Run in incremental mode')
@click.option('--interactive', '-it', is_flag=True, help='Start interactive shell after analysis')
def analyze(repo_path: str, output_dir: str, incremental: bool, interactive: bool):
    """Analyze a codebase and generate intelligence artifacts."""
    
    repo_path = os.path.abspath(repo_path)
    
    console.rule("[bold blue]Brownfield Cartographer Analysis[/bold blue]")
    console.print(f"📂 Repository: [cyan]{repo_path}[/cyan]")
    console.print(f"📁 Output: [cyan]{output_dir}[/cyan]")
    console.print(f"🔄 Mode: [cyan]{'Incremental' if incremental else 'Full'}[/cyan]")
    
    # Initialize orchestrator with progress tracking
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console
    ) as progress:
        
        task = progress.add_task("[cyan]Initializing...", total=4)
        
        orch = Orchestrator(repo_path, output_dir)
        progress.update(task, advance=1, description="[cyan]Running Surveyor Agent...")
        
        # Run analysis
        success = orch.run_analysis(incremental=incremental)
        
        if success:
            progress.update(task, advance=3, description="[green]Analysis complete!")
            console.print("\n[green]✅ Analysis complete![/green]")
            
            if interactive:
                console.print("\n[bold]Starting interactive shell...[/bold]")
                shell = InteractiveShell(orch)
                shell.run()
        else:
            console.print("\n[red]❌ Analysis failed. Check the error messages above.[/red]")
            sys.exit(1)


@cli.command()
@click.option('--repo-path', '-r', type=click.Path(exists=True), help='Path to repository')
@click.option('--query', '-q', help='Query to run (if not provided, starts interactive mode)')
def query(repo_path: Optional[str], query: Optional[str]):
    """Query the knowledge graph."""
    
    # Find repository path
    if not repo_path:
        current = Path.cwd()
        while current != current.parent:
            if (current / '.cartography').exists():
                repo_path = str(current)
                break
            current = current.parent
        
        if not repo_path:
            console.print("[red]❌ No .cartography directory found. Run analyze first or specify --repo-path[/red]")
            sys.exit(1)
    
    output_dir = Path(repo_path) / '.cartography'
    
    if not output_dir.exists():
        console.print(f"[red]❌ No .cartography directory found in {repo_path}[/red]")
        sys.exit(1)
    
    # Initialize orchestrator
    with console.status("[bold green]Loading knowledge graph..."):
        orch = Orchestrator(repo_path, str(output_dir))
    
    if query:
        # Single query mode
        with console.status("[bold green]Processing query..."):
            result = orch.query(query)
        
        console.print()
        for res in result['results']:
            console.print(Panel(res, border_style="green"))
        
        if result['evidence']:
            console.print("\n[dim]Evidence:[/dim]")
            for evidence in result['evidence']:
                console.print(f"  • [cyan]{evidence.get('file')}[/cyan]:{evidence.get('line', '')}")
    else:
        # Interactive mode
        shell = InteractiveShell(orch)
        shell.run()


@cli.command()
@click.argument('github_url')
@click.option('--branch', '-b', default='main', help='Branch to clone')
@click.option('--output-dir', '-o', default='.cartography')
@click.option('--interactive', '-it', is_flag=True, help='Start interactive shell after analysis')
def clone_and_analyze(github_url: str, branch: str, output_dir: str, interactive: bool):
    """Clone a GitHub repository and analyze it."""
    
    console.rule("[bold blue]Clone & Analyze[/bold blue]")
    console.print(f"📦 Cloning: [cyan]{github_url}[/cyan]")
    console.print(f"🌿 Branch: [cyan]{branch}[/cyan]")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            task = progress.add_task("[yellow]Cloning repository...", total=None)
            
            try:
                subprocess.run(
                    ['git', 'clone', '--branch', branch, '--depth', '1', github_url, tmpdir],
                    check=True,
                    capture_output=True
                )
            except subprocess.CalledProcessError as e:
                progress.stop()
                console.print(f"[red]❌ Failed to clone: {e.stderr.decode()}[/red]")
                sys.exit(1)
            
            progress.update(task, description="[green]✅ Clone complete")
        
        console.print()
        
        # Run analysis
        ctx = click.get_current_context()
        ctx.invoke(analyze, repo_path=tmpdir, output_dir=output_dir, 
                  incremental=False, interactive=interactive)


if __name__ == '__main__':
    cli()