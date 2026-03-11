"""Orchestrator for running the full analysis pipeline."""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import json

from dotenv import load_dotenv

from .agents.surveyor import SurveyorAgent
from .agents.hydrologist import HydrologistAgent
from .agents.semanticist import SemanticistAgent
from .agents.archivist import ArchivistAgent
from .agents.navigator import NavigatorAgent
from .graph.knowledge_graph import KnowledgeGraphManager
from .analyzers.git_analyzer import GitAnalyzer


class Orchestrator:
    """Orchestrates the multi-agent analysis pipeline."""
    
    def __init__(self, repo_path: str, output_dir: str = ".cartography"):
        load_dotenv()
        
        self.repo_path = Path(repo_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize components
        self.git_analyzer = GitAnalyzer(str(repo_path))
        self.kg_manager = KnowledgeGraphManager()
        
        # Initialize agents
        self.surveyor = None
        self.hydrologist = None
        self.semanticist = None
        self.archivist = None
        self.navigator = None
        
        self.analysis_time = None
    
    def run_analysis(self, incremental: bool = False):
        """Run the full analysis pipeline."""
        print("\n" + "="*60)
        print("🚀 Brownfield Cartographer - Codebase Intelligence System")
        print("="*60 + "\n")
        
        self.analysis_time = datetime.now()
        
        # Check for incremental mode
        if incremental and self._load_previous_state():
            print("📦 Running in incremental mode (analyzing changed files only)")
        
        # Phase 1: Surveyor - Static Structure
        print("\n" + "-"*40)
        print("PHASE 1: Surveyor Agent - Static Structure Analysis")
        print("-"*40)
        
        self.surveyor = SurveyorAgent(str(self.repo_path))
        modules, import_graph = self.surveyor.analyze()
        
        # Add to knowledge graph
        for module in modules.values():
            self.kg_manager.add_module(module)
        
        # Phase 2: Hydrologist - Data Lineage
        print("\n" + "-"*40)
        print("PHASE 2: Hydrologist Agent - Data Lineage Analysis")
        print("-"*40)
        
        self.hydrologist = HydrologistAgent(str(self.repo_path))
        lineage_graph = self.hydrologist.analyze()
        
        # Add datasets and transformations to knowledge graph
        for dataset in self.hydrologist.datasets.values():
            self.kg_manager.add_dataset(dataset)
        
        for trans in self.hydrologist.transformations.values():
            self.kg_manager.add_transformation(trans)
        
        # Phase 3: Semanticist - LLM Analysis
        print("\n" + "-"*40)
        print("PHASE 3: Semanticist Agent - LLM Semantic Analysis")
        print("-"*40)
        
        groq_key = os.getenv('GROQ_API_KEY')
        self.semanticist = SemanticistAgent(str(self.repo_path), groq_key)
        
        # Only run if we have API key
        if groq_key:
            modules = self.semanticist.analyze(modules)
            
            # Generate day-one answers
            day_one_answers = self.semanticist.answer_day_one_questions(
                modules, lineage_graph
            )
        else:
            print("⚠️  No GROQ_API_KEY found. Skipping semantic analysis.")
            day_one_answers = {}
        
        # Phase 4: Archivist - Living Documentation
        print("\n" + "-"*40)
        print("PHASE 4: Archivist Agent - Living Documentation")
        print("-"*40)
        
        self.archivist = ArchivistAgent(str(self.repo_path), str(self.output_dir))
        
        # Archive results
        self.archivist.archive(
            self.kg_manager.model,
            lineage_graph,
            day_one_answers
        )
        
        # Initialize Navigator
        self.navigator = NavigatorAgent(
            self.kg_manager.model,
            lineage_graph,
            self.surveyor,
            self.hydrologist,
            self.semanticist
        )
        
        # Save state for incremental updates
        self._save_state()
        
        # Print summary
        self._print_summary()
        
        return True
    
    def query(self, query_text: str) -> Dict[str, Any]:
        """Query the knowledge graph."""
        if not self.navigator:
            return {"error": "No analysis has been run yet. Run analyze first."}
        
        return self.navigator.query(query_text)
    
    def _save_state(self):
        """Save state for incremental updates."""
        state_path = self.output_dir / "orchestrator_state.json"
        
        state = {
            "analysis_time": self.analysis_time.isoformat(),
            "last_commit": self.git_analyzer.get_current_commit(),
            "repo_path": str(self.repo_path)
        }
        
        with open(state_path, 'w') as f:
            json.dump(state, f, indent=2)
        
        # Save knowledge graph
        kg_path = self.output_dir / "knowledge_graph.json"
        self.kg_manager.serialize(str(kg_path))
    
    def _load_previous_state(self) -> bool:
        """Load previous state for incremental updates."""
        state_path = self.output_dir / "orchestrator_state.json"
        kg_path = self.output_dir / "knowledge_graph.json"
        
        if not state_path.exists() or not kg_path.exists():
            return False
        
        try:
            with open(state_path, 'r') as f:
                state = json.load(f)
            
            # Check if repo has changed
            current_commit = self.git_analyzer.get_current_commit()
            if state.get("last_commit") == current_commit:
                print("📦 No changes detected since last analysis")
                return True
            
            # Load previous knowledge graph
            self.kg_manager.deserialize(str(kg_path))
            return True
            
        except Exception as e:
            print(f"⚠️  Error loading previous state: {e}")
            return False
    
    def _print_summary(self):
        """Print analysis summary."""
        print("\n" + "="*60)
        print("✅ ANALYSIS COMPLETE")
        print("="*60)
        
        print(f"\n📊 Summary:")
        print(f"  • Modules analyzed: {len(self.kg_manager.model.modules)}")
        print(f"  • Datasets found: {len(self.kg_manager.model.datasets)}")
        print(f"  • Transformations: {len(self.kg_manager.model.transformations)}")
        
        if self.semanticist and self.semanticist.purpose_cache:
            print(f"  • Purpose statements generated: {len(self.semanticist.purpose_cache)}")
        
        if self.semanticist and self.semanticist.doc_drift_flags:
            print(f"  • Documentation drift flags: {len(self.semanticist.doc_drift_flags)}")
        
        print(f"\n📁 Artifacts saved to: {self.output_dir}/")
        print(f"  • CODEBASE.md - Living architecture context")
        print(f"  • onboarding_brief.md - Day-One onboarding brief")
        print(f"  • module_graph.json - Module dependency graph")
        print(f"  • lineage_graph.json - Data lineage graph")
        print(f"  • cartography_trace.jsonl - Analysis audit log")
        
        print("\n💡 Next steps:")
        print("  • Run 'python -m src.cli query' to interact with the knowledge graph")
        print("  • Or use: python -m src.cli query \"trace lineage of my_table\"")
        print("="*60 + "\n")