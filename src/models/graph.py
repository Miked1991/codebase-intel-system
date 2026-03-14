"""Combined knowledge graph model."""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel
from .nodes import ModuleNode, DatasetNode, FunctionNode, TransformationNode
from .edges import ImportEdge, ProducesEdge, ConsumesEdge, CallsEdge, ConfiguresEdge


class KnowledgeGraph(BaseModel):
    """Container for the complete knowledge graph."""
    
    modules: Dict[str, ModuleNode] = {}
    datasets: Dict[str, DatasetNode] = {}
    functions: Dict[str, FunctionNode] = {}
    transformations: Dict[str, TransformationNode] = {}
    
    imports: List[ImportEdge] = []
    produces: List[ProducesEdge] = []
    consumes: List[ConsumesEdge] = []
    calls: List[CallsEdge] = []
    configures: List[ConfiguresEdge] = []
    
    metadata: Dict[str, Any] = {
        "analyzed_at": None,
        "repo_url": None,
        "total_files": 0,
        "total_lines": 0
    }