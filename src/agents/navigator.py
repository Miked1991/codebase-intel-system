"""Navigator Agent - Query interface for the knowledge graph."""

from typing import Dict, List, Optional, Any
import networkx as nx
from langgraph.graph import StateGraph, END
from dataclasses import dataclass, field

from ..models.nodes import ModuleNode
from ..graph.knowledge_graph import KnowledgeGraphManager


@dataclass
class NavigatorState:
    """State for the Navigator agent."""
    
    query: str = ""
    query_type: str = ""
    results: List[Dict[str, Any]] = field(default_factory=list)
    evidence: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


class NavigatorAgent:
    """Agent for querying the knowledge graph."""
    
    def __init__(self, kg: KnowledgeGraphManager, lineage_graph: nx.DiGraph,
                 surveyor, hydrologist, semanticist):
        self.kg = kg
        self.lineage_graph = lineage_graph
        self.surveyor = surveyor
        self.hydrologist = hydrologist
        self.semanticist = semanticist
        
        # Initialize LangGraph
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph for query processing."""
        workflow = StateGraph(NavigatorState)
        
        # Add nodes
        workflow.add_node("classify_query", self._classify_query)
        workflow.add_node("find_implementation", self._find_implementation)
        workflow.add_node("trace_lineage", self._trace_lineage)
        workflow.add_node("blast_radius", self._blast_radius)
        workflow.add_node("explain_module", self._explain_module)
        workflow.add_node("format_results", self._format_results)
        
        # Add edges
        workflow.set_entry_point("classify_query")
        workflow.add_conditional_edges(
            "classify_query",
            self._route_query,
            {
                "implementation": "find_implementation",
                "lineage": "trace_lineage",
                "blast": "blast_radius",
                "explain": "explain_module"
            }
        )
        
        workflow.add_edge("find_implementation", "format_results")
        workflow.add_edge("trace_lineage", "format_results")
        workflow.add_edge("blast_radius", "format_results")
        workflow.add_edge("explain_module", "format_results")
        workflow.add_edge("format_results", END)
        
        return workflow.compile()
    
    def _classify_query(self, state: NavigatorState) -> NavigatorState:
        """Classify the query type."""
        query = state.query.lower()
        
        if "where is" in query or "find" in query or "implementation" in query:
            state.query_type = "implementation"
        elif "trace" in query or "lineage" in query or "upstream" in query or "downstream" in query:
            state.query_type = "lineage"
        elif "blast" in query or "radius" in query or "break" in query:
            state.query_type = "blast"
        elif "explain" in query or "what does" in query or "purpose" in query:
            state.query_type = "explain"
        else:
            state.query_type = "implementation"  # default
        
        return state
    
    def _route_query(self, state: NavigatorState) -> str:
        """Route to appropriate handler."""
        return state.query_type
    
    def _find_implementation(self, state: NavigatorState) -> NavigatorState:
        """Find implementation of a concept."""
        query = state.query.lower()
        
        # Extract concept (simplified)
        words = query.split()
        concept = None
        
        for word in words:
            if word not in ['where', 'is', 'the', 'find', 'implementation', 'of', 'code', 'for']:
                concept = word
                break
        
        if not concept:
            state.error = "Could not identify concept to find"
            return state
        
        # Search in modules
        results = []
        for path, module in self.kg.modules.items():
            if concept in path.lower():
                results.append({
                    "type": "module",
                    "name": path,
                    "purpose": module.purpose_statement,
                    "evidence": [{"file": path, "line": 1}]
                })
            
            # Search in functions
            for func in module.public_functions:
                if concept in func['name'].lower():
                    results.append({
                        "type": "function",
                        "name": func['name'],
                        "module": path,
                        "signature": func['signature'],
                        "evidence": [{"file": path, "line": func['line_start']}]
                    })
        
        state.results = results[:5]  # Limit to 5 results
        
        # Add evidence
        for result in results[:5]:
            state.evidence.extend(result.get('evidence', []))
        
        return state
    
    def _trace_lineage(self, state: NavigatorState) -> NavigatorState:
        """Trace data lineage."""
        query = state.query.lower()
        
        # Extract dataset name
        words = query.split()
        dataset = None
        
        for i, word in enumerate(words):
            if word in ['trace', 'lineage', 'upstream', 'downstream']:
                if i + 1 < len(words):
                    dataset = words[i + 1]
                    break
        
        if not dataset:
            # Look for table names in query
            for word in words:
                if '_' in word or '.' in word:  # Likely a dataset name
                    dataset = word
                    break
        
        if not dataset:
            state.error = "Could not identify dataset to trace"
            return state
        
        # Determine direction
        direction = 'upstream' if 'upstream' in query else 'downstream'
        
        # Trace lineage
        lineage = self.hydrologist.trace_lineage(dataset, direction)
        
        results = []
        for item in lineage[:10]:
            results.append({
                "dataset": item,
                "direction": direction,
                "relationship": f"{direction} dependency"
            })
        
        state.results = results
        
        # Add evidence from transformations
        for node in self.lineage_graph.nodes():
            if 'dataset:' + dataset in node:
                for neighbor in self.lineage_graph.predecessors(node):
                    if 'trans:' in neighbor:
                        trans_data = self.lineage_graph.nodes[neighbor]
                        if 'source_file' in trans_data:
                            state.evidence.append({
                                "file": trans_data['source_file'],
                                "line": trans_data.get('line_range', [0, 0])[0]
                            })
        
        return state
    
    def _blast_radius(self, state: NavigatorState) -> NavigatorState:
        """Calculate blast radius for a module."""
        query = state.query.lower()
        
        # Extract module path
        words = query.split()
        module_path = None
        
        for word in words:
            if '/' in word or '.py' in word or '\\' in word:
                module_path = word
                break
        
        if not module_path:
            state.error = "Could not identify module for blast radius"
            return state
        
        # Calculate blast radius
        affected = self.surveyor.blast_radius(module_path)
        
        results = []
        for module in affected[:20]:
            results.append({
                "module": module,
                "impact": "directly or indirectly depends on this module"
            })
        
        state.results = results
        
        # Add evidence
        for module in affected[:5]:
            if module in self.kg.modules:
                state.evidence.append({
                    "file": module,
                    "line": 1,
                    "purpose": self.kg.modules[module].purpose_statement
                })
        
        return state
    
    def _explain_module(self, state: NavigatorState) -> NavigatorState:
        """Explain what a module does."""
        query = state.query.lower()
        
        # Extract module path
        words = query.split()
        module_path = None
        
        for word in words:
            if '/' in word or '.py' in word or '\\' in word:
                module_path = word
                break
        
        if not module_path:
            # Try to find by name
            for path in self.kg.modules.keys():
                path_parts = path.split('/')
                if any(part in query for part in path_parts[-2:]):
                    module_path = path
                    break
        
        if not module_path:
            state.error = "Could not identify module to explain"
            return state
        
        module = self.kg.modules.get(module_path)
        if not module:
            state.error = f"Module {module_path} not found"
            return state
        
        # Build explanation
        explanation = [f"## {module_path}"]
        explanation.append(f"**Language:** {module.language}")
        explanation.append(f"**Lines of Code:** {module.loc}")
        
        if module.purpose_statement:
            explanation.append(f"\n**Purpose:** {module.purpose_statement}")
        
        if module.domain_cluster:
            explanation.append(f"**Domain:** {module.domain_cluster}")
        
        if module.public_functions:
            explanation.append(f"\n**Public Functions ({len(module.public_functions)}):**")
            for func in module.public_functions[:5]:
                explanation.append(f"- `{func['signature']}`")
        
        if module.imports:
            explanation.append(f"\n**Imports ({len(module.imports)}):**")
            for imp in module.imports[:10]:
                explanation.append(f"- {imp}")
        
        if module.change_velocity_30d > 0:
            explanation.append(f"\n**Change Velocity:** {module.change_velocity_30d} changes in last 30 days")
        
        state.results = [{"explanation": '\n'.join(explanation)}]
        
        # Add evidence
        state.evidence.append({
            "file": module_path,
            "line": 1,
            "analysis_method": "static_analysis"
        })
        
        return state
    
    def _format_results(self, state: NavigatorState) -> NavigatorState:
        """Format results for output."""
        if state.error:
            return state
        
        formatted = []
        for result in state.results:
            if isinstance(result, dict):
                if 'explanation' in result:
                    formatted.append(result['explanation'])
                elif 'type' in result and result['type'] == 'module':
                    formatted.append(f"📁 Module: {result['name']}")
                    if result.get('purpose'):
                        formatted.append(f"   Purpose: {result['purpose']}")
                elif 'type' in result and result['type'] == 'function':
                    formatted.append(f"🔧 Function: {result['name']}")
                    formatted.append(f"   In: {result['module']}")
                    formatted.append(f"   Signature: {result['signature']}")
                elif 'dataset' in result:
                    formatted.append(f"💾 Dataset: {result['dataset']}")
                    formatted.append(f"   Relationship: {result['relationship']}")
                elif 'module' in result:
                    formatted.append(f"📦 Module: {result['module']}")
                    formatted.append(f"   Impact: {result['impact']}")
        
        state.results = formatted
        
        return state
    
    def query(self, query_text: str) -> Dict[str, Any]:
        """Execute a query against the knowledge graph."""
        initial_state = NavigatorState(query=query_text)
        
        try:
            final_state = self.graph.invoke(initial_state)
            
            return {
                "query": query_text,
                "results": final_state.results if final_state.results else ["No results found"],
                "evidence": final_state.evidence,
                "error": final_state.error
            }
        except Exception as e:
            return {
                "query": query_text,
                "error": str(e),
                "results": [],
                "evidence": []
            }