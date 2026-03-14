"""Surveyor Agent - Static structure analysis with fixed query API."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import networkx as nx
from datetime import datetime

from ..models.nodes import ModuleNode
from ..utils.language_router import get_language_router
from ..analyzers.git_analyzer import GitAnalyzer


class SurveyorAgent:
    """Agent responsible for static code structure analysis."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.language_router = get_language_router()
        self.git_analyzer = GitAnalyzer(repo_path)
        self.import_graph = nx.DiGraph()
        self.modules: Dict[str, ModuleNode] = {}
        
        # Track statistics
        self.analyzed_count = 0
        self.error_count = 0
    
    def analyze(self) -> Tuple[Dict[str, ModuleNode], nx.DiGraph]:
        """Run full analysis on the repository."""
        print(f"🔍 Surveyor: Analyzing {self.repo_path}")
        
        # Get all files
        all_files = self._get_code_files()
        print(f"  Found {len(all_files)} files to analyze")
        
        # Analyze each file
        for file_path in all_files:
            self._analyze_file(file_path)
        
        # Build import graph
        self._build_import_graph()
        
        # Add git velocity
        self._add_git_velocity()
        
        # Detect dead code candidates
        self._detect_dead_code()
        
        # Calculate PageRank for critical modules
        self._calculate_pagerank()
        
        print(f"✅ Surveyor: Analyzed {self.analyzed_count} modules, {self.error_count} errors")
        return self.modules, self.import_graph
    
    def _get_code_files(self) -> List[Path]:
        """Get all code files in the repository."""
        code_files = []
        extensions = ['.py', '.sql', '.yml', '.yaml', '.js', '.ts', '.ipynb', '.json', '.md']
        
        for ext in extensions:
            code_files.extend(self.repo_path.rglob(f"*{ext}"))
        
        return code_files
    
    def _analyze_file(self, file_path: Path) -> bool:
        """Analyze a single file."""
        try:
            # Always try to read the file first
            with open(file_path, 'rb') as f:
                content = f.read()
            
            # Get language
            language = self.language_router.get_language_name(str(file_path))
            
            # Create module node with language or 'unknown'
            module = ModuleNode(
                path=str(file_path.relative_to(self.repo_path)),
                language=language or "unknown",
                loc=len(content.splitlines()),
                last_modified=datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                )
            )
            
            # Extract Python-specific info using the fixed query method
            if language == 'python':
                self._extract_python_info(module, file_path)
            
            self.modules[module.path] = module
            self.analyzed_count += 1
            return True
            
        except Exception as e:
            self.error_count += 1
            # Don't print every error - too noisy
            if self.error_count % 100 == 0:
                print(f"⚠️  Error analyzing {file_path.name}: {e}")
            return False
    
    def _extract_python_info(self, module: ModuleNode, file_path: Path):
        """Extract Python-specific information using the language router."""
        try:
            # Use the router's query_file method which has the correct QueryCursor API
            imports = self.language_router.query_file(str(file_path), 'imports')
            functions = self.language_router.query_file(str(file_path), 'functions')
            
            # Process imports
            for imp in imports:
                if imp['text'] and imp['text'] not in module.imports:
                    # Clean up import text
                    import_text = imp['text'].strip()
                    if import_text:
                        module.imports.append(import_text)
            
            # Process functions
            for func in functions:
                if func['text']:
                    # Extract function name from text
                    func_text = func['text']
                    func_name = func_text.replace('def', '').strip()
                    if '(' in func_name:
                        func_name = func_name.split('(')[0].strip()
                    
                    if func_name and not func_name.startswith('_'):
                        module.public_functions.append({
                            'name': func_name,
                            'signature': func_text,
                            'line_start': func['start_line']
                        })
            
        except Exception as e:
            # Don't fail the whole analysis for query errors
            # Only log at debug level
            pass
    
    def _build_import_graph(self):
        """Build the import graph using NetworkX."""
        for module_path, module in self.modules.items():
            self.import_graph.add_node(module_path, **module.dict())
            
            # Simple import resolution - just add edges for now
            for imported in module.imports[:5]:  # Limit to first 5 imports
                # In production, you'd resolve actual paths
                pass
    
    def _add_git_velocity(self):
        """Add git change velocity to modules."""
        try:
            velocity_data = self.git_analyzer.get_change_velocity(days=30)
            
            for module_path, changes in velocity_data.items():
                if module_path in self.modules:
                    self.modules[module_path].change_velocity_30d = changes
        except Exception as e:
            print(f"Warning: Could not get git velocity: {e}")
    
    def _detect_dead_code(self):
        """Detect potential dead code candidates."""
        for module_path, module in self.modules.items():
            # Check if module has no imports and isn't a test file
            if not module.imports and 'test' not in module_path.lower():
                module.is_dead_code_candidate = True
    
    def _calculate_pagerank(self):
        """Calculate PageRank to identify critical modules."""
        if len(self.import_graph.nodes()) > 0:
            try:
                pagerank = nx.pagerank(self.import_graph)
                
                # Tag top modules as critical
                if pagerank:
                    threshold = sorted(pagerank.values(), reverse=True)[:5][-1] if len(pagerank) >= 5 else 0
                    for node, score in pagerank.items():
                        if node in self.modules and score >= threshold:
                            self.modules[node].domain_cluster = "critical_path"
            except Exception as e:
                print(f"Warning: Could not calculate PageRank: {e}")
    
    def blast_radius(self, module_path: str) -> List[str]:
        """Calculate blast radius for a module change."""
        if module_path not in self.import_graph:
            return []
        
        try:
            # Get all downstream nodes
            descendants = list(nx.descendants(self.import_graph, module_path))
            return descendants
        except:
            return []