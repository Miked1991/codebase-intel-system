"""Surveyor Agent - Static structure analysis."""

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import networkx as nx
from datetime import datetime, timedelta

from ..models.nodes import ModuleNode, FunctionNode
from ..models.edges import ImportEdge
from ..utils.language_router import LanguageRouter
from ..analyzers.git_analyzer import GitAnalyzer


class SurveyorAgent:
    """Agent responsible for static code structure analysis."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.language_router = LanguageRouter()
        self.git_analyzer = GitAnalyzer(repo_path)
        self.import_graph = nx.DiGraph()
        self.modules: Dict[str, ModuleNode] = {}
        self.functions: Dict[str, FunctionNode] = {}
        
        # Tree-sitter query patterns
        self.python_queries = {
            "import": """
                (import_statement
                    name: (dotted_name) @import_name)
                (import_from_statement
                    module_name: (dotted_name) @from_module
                    name: (dotted_name) @import_name)
            """,
            "function": """
                (function_definition
                    name: (identifier) @func_name
                    parameters: (parameters) @params
                    body: (block) @body) @func_def
            """,
            "class": """
                (class_definition
                    name: (identifier) @class_name
                    body: (block) @body) @class_def
            """,
            "decorator": """
                (decorator
                    (identifier) @decorator_name)
            """
        }
    
    def analyze(self) -> Tuple[Dict[str, ModuleNode], nx.DiGraph]:
        """Run full analysis on the repository."""
        print(f"🔍 Surveyor: Analyzing {self.repo_path}")
        
        # Get all files
        all_files = self._get_code_files()
        
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
        
        print(f"✅ Surveyor: Analyzed {len(self.modules)} modules")
        return self.modules, self.import_graph
    
    def _get_code_files(self) -> List[Path]:
        """Get all code files in the repository."""
        code_files = []
        extensions = ['.py', '.sql', '.yml', '.yaml', '.js', '.ts', '.ipynb']
        
        for ext in extensions:
            code_files.extend(self.repo_path.rglob(f"*{ext}"))
        
        return code_files
    
    def _analyze_file(self, file_path: Path):
        """Analyze a single file."""
        if not self.language_router.is_supported(str(file_path)):
            return
        
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            
            tree = self.language_router.parse_file(str(file_path), content)
            if not tree:
                return
            
            language = self.language_router.get_language(str(file_path))
            
            # Create module node
            module = ModuleNode(
                path=str(file_path.relative_to(self.repo_path)),
                language=language,
                loc=len(content.splitlines()),
                last_modified=datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                )
            )
            
            # Extract based on language
            if language == 'python':
                self._extract_python_info(module, tree, content)
            elif language == 'sql':
                self._extract_sql_info(module, tree, content)
            
            self.modules[module.path] = module
            
        except Exception as e:
            print(f"⚠️  Error analyzing {file_path}: {e}")
    
    def _extract_python_info(self, module: ModuleNode, tree, content: bytes):
        """Extract Python-specific information."""
        root_node = tree.root_node
        
        # Extract imports
        query = self.language_router.parsers['.py'].language.query(
            self.python_queries["import"]
        )
        captures = query.captures(root_node)
        
        for node, tag in captures:
            if tag == "import_name" or tag == "from_module":
                import_text = node.text.decode('utf8') if node.text else ""
                if import_text and import_text not in module.imports:
                    module.imports.append(import_text)
        
        # Extract functions
        query = self.language_router.parsers['.py'].language.query(
            self.python_queries["function"]
        )
        captures = query.captures(root_node)
        
        functions = []
        for node, tag in captures:
            if tag == "func_def":
                # Get function name
                name_node = node.child_by_field_name('name')
                if name_node and name_node.text:
                    func_name = name_node.text.decode('utf8')
                    
                    # Check if public (no leading underscore)
                    is_public = not func_name.startswith('_')
                    
                    # Get parameters
                    params_node = node.child_by_field_name('parameters')
                    params = params_node.text.decode('utf8') if params_node else "()"
                    
                    function = {
                        "name": func_name,
                        "signature": f"{func_name}{params}",
                        "is_public": is_public,
                        "line_start": node.start_point[0] + 1,
                        "line_end": node.end_point[0] + 1
                    }
                    functions.append(function)
                    
                    if is_public:
                        module.public_functions.append(function)
        
        # Update module
        if functions:
            module.complexity_score = len(functions) / max(module.loc, 1)
    
    def _extract_sql_info(self, module: ModuleNode, tree, content: bytes):
        """Extract SQL-specific information."""
        # SQL extraction logic will be handled by Hydrologist
        pass
    
    def _build_import_graph(self):
        """Build the import graph using NetworkX."""
        for module_path, module in self.modules.items():
            self.import_graph.add_node(module_path, **module.dict())
            
            for imported in module.imports:
                # Try to resolve the import to a file path
                resolved = self._resolve_import(module_path, imported)
                if resolved and resolved in self.modules:
                    self.import_graph.add_edge(module_path, resolved)
    
    def _resolve_import(self, module_path: str, import_name: str) -> Optional[str]:
        """Resolve an import name to a file path."""
        # Simple resolution for now
        module_dir = Path(module_path).parent
        
        # Check for direct file
        potential_path = module_dir / f"{import_name.replace('.', '/')}.py"
        if potential_path.exists():
            return str(potential_path)
        
        # Check for __init__.py
        init_path = module_dir / import_name / "__init__.py"
        if init_path.exists():
            return str(init_path)
        
        return None
    
    def _add_git_velocity(self):
        """Add git change velocity to modules."""
        velocity_data = self.git_analyzer.get_change_velocity(days=30)
        
        for module_path, changes in velocity_data.items():
            if module_path in self.modules:
                self.modules[module_path].change_velocity_30d = changes
    
    def _detect_dead_code(self):
        """Detect potential dead code candidates."""
        for module_path, module in self.modules.items():
            # Check if module has no incoming imports and isn't an entry point
            if module_path not in self.import_graph or \
               self.import_graph.in_degree(module_path) == 0:
                
                # Check if it's not a common entry point
                if not any(name in module_path for name in 
                          ['main', 'cli', '__init__', '__main__']):
                    module.is_dead_code_candidate = True
    
    def _calculate_pagerank(self):
        """Calculate PageRank to identify critical modules."""
        if len(self.import_graph.nodes()) > 0:
            pagerank = nx.pagerank(self.import_graph)
            
            # Tag top modules as critical
            threshold = sorted(pagerank.values(), reverse=True)[:5][-1]
            for node, score in pagerank.items():
                if node in self.modules and score >= threshold:
                    self.modules[node].domain_cluster = "critical_path"
    
    def get_circular_dependencies(self) -> List[List[str]]:
        """Find circular dependencies in the import graph."""
        cycles = list(nx.simple_cycles(self.import_graph))
        return cycles[:10]  # Return top 10 cycles
    
    def blast_radius(self, module_path: str) -> List[str]:
        """Calculate blast radius for a module change."""
        if module_path not in self.import_graph:
            return []
        
        # Get all downstream nodes
        descendants = list(nx.descendants(self.import_graph, module_path))
        return descendants