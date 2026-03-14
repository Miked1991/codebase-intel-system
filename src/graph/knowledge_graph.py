"""Knowledge graph management with NetworkX."""

import networkx as nx
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

from ..models.nodes import ModuleNode, DatasetNode, FunctionNode, TransformationNode
from ..models.edges import ImportEdge, ProducesEdge, ConsumesEdge, CallsEdge, ConfiguresEdge
from ..models.graph import KnowledgeGraph as KGModel


class KnowledgeGraphManager:
    """Manages the knowledge graph using NetworkX."""
    
    def __init__(self):
        self.graph = nx.MultiDiGraph()
        self.model = KGModel()
    
    def add_module(self, module: ModuleNode):
        """Add a module node to the graph."""
        node_id = f"module:{module.path}"
        self.graph.add_node(
            node_id,
            type="module",
            **module.dict(exclude={'imports', 'public_functions', 'public_classes'})
        )
        self.model.modules[module.path] = module
    
    def add_dataset(self, dataset: DatasetNode):
        """Add a dataset node to the graph."""
        node_id = f"dataset:{dataset.name}"
        self.graph.add_node(
            node_id,
            type="dataset",
            **dataset.dict()
        )
        self.model.datasets[dataset.name] = dataset
    
    def add_transformation(self, transformation: TransformationNode):
        """Add a transformation node to the graph."""
        node_id = f"trans:{transformation.source_file}::{len(self.model.transformations)}"
        self.graph.add_node(
            node_id,
            type="transformation",
            **transformation.dict()
        )
        self.model.transformations[node_id] = transformation
    
    def add_import_edge(self, edge: ImportEdge):
        """Add an import edge to the graph."""
        source_id = f"module:{edge.source_module}"
        target_id = f"module:{edge.target_module}"
        
        if source_id in self.graph and target_id in self.graph:
            self.graph.add_edge(
                source_id,
                target_id,
                type="imports",
                **edge.dict(exclude={'source_module', 'target_module'})
            )
            self.model.imports.append(edge)
    
    def add_produces_edge(self, edge: ProducesEdge):
        """Add a produces edge to the graph."""
        trans_id = f"trans:{edge.transformation}"
        dataset_id = f"dataset:{edge.dataset}"
        
        if trans_id in self.graph and dataset_id in self.graph:
            self.graph.add_edge(
                trans_id,
                dataset_id,
                type="produces",
                **edge.dict(exclude={'transformation', 'dataset'})
            )
            self.model.produces.append(edge)
    
    def add_consumes_edge(self, edge: ConsumesEdge):
        """Add a consumes edge to the graph."""
        dataset_id = f"dataset:{edge.dataset}"
        trans_id = f"trans:{edge.transformation}"
        
        if dataset_id in self.graph and trans_id in self.graph:
            self.graph.add_edge(
                dataset_id,
                trans_id,
                type="consumes",
                **edge.dict(exclude={'dataset', 'transformation'})
            )
            self.model.consumes.append(edge)
    
    def get_module(self, path: str) -> Optional[ModuleNode]:
        """Get a module by path."""
        return self.model.modules.get(path)
    
    def get_dataset(self, name: str) -> Optional[DatasetNode]:
        """Get a dataset by name."""
        return self.model.datasets.get(name)
    
    def find_by_purpose(self, keyword: str) -> List[ModuleNode]:
        """Find modules by purpose statement keyword."""
        results = []
        for module in self.model.modules.values():
            if module.purpose_statement and keyword.lower() in module.purpose_statement.lower():
                results.append(module)
        return results
    
    def find_by_domain(self, domain: str) -> List[ModuleNode]:
        """Find modules by domain cluster."""
        return [m for m in self.model.modules.values() if m.domain_cluster == domain]
    
    def get_critical_nodes(self, algorithm: str = 'pagerank', top_k: int = 10) -> List[str]:
        """Get critical nodes using graph algorithms."""
        if algorithm == 'pagerank':
            scores = nx.pagerank(self.graph)
            sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            return [node for node, _ in sorted_nodes[:top_k]]
        
        elif algorithm == 'betweenness':
            scores = nx.betweenness_centrality(self.graph)
            sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            return [node for node, _ in sorted_nodes[:top_k]]
        
        return []
    
    def detect_cycles(self) -> List[List[str]]:
        """Detect cycles in the graph."""
        try:
            cycles = list(nx.simple_cycles(self.graph))
            return cycles
        except:
            return []
    
    def get_downstream(self, node_id: str) -> List[str]:
        """Get all downstream nodes."""
        if node_id not in self.graph:
            return []
        
        try:
            return list(nx.descendants(self.graph, node_id))
        except:
            return []
    
    def get_upstream(self, node_id: str) -> List[str]:
        """Get all upstream nodes."""
        if node_id not in self.graph:
            return []
        
        try:
            return list(nx.ancestors(self.graph, node_id))
        except:
            return []
    
    def serialize(self, path: str):
        """Serialize the graph to a file."""
        data = {
            "graph": nx.node_link_data(self.graph),
            "model": self.model.dict()
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def deserialize(self, path: str):
        """Deserialize the graph from a file."""
        with open(path, 'r') as f:
            data = json.load(f)
        
        self.graph = nx.node_link_graph(data["graph"])
        self.model = KGModel(**data["model"])