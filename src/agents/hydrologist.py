"""Hydrologist Agent - Data lineage analysis."""

from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import networkx as nx
import sqlglot
from sqlglot import parse_one, exp
import json

from ..models.nodes import DatasetNode, TransformationNode
from ..models.edges import ProducesEdge, ConsumesEdge
from ..utils.language_router import LanguageRouter


class HydrologistAgent:
    """Agent responsible for data lineage analysis."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.language_router = LanguageRouter()
        self.lineage_graph = nx.DiGraph()
        self.datasets: Dict[str, DatasetNode] = {}
        self.transformations: Dict[str, TransformationNode] = {}
        
        # Patterns for data operations
        self.data_patterns = {
            'pandas_read': ['read_csv', 'read_sql', 'read_parquet', 'read_json'],
            'pandas_write': ['to_csv', 'to_sql', 'to_parquet', 'to_json'],
            'spark_read': ['spark.read', 'spark.read.format', 'spark.sql'],
            'spark_write': ['write.csv', 'write.parquet', 'write.json', 'write.save'],
            'sqlalchemy': ['create_engine', 'execute', 'session.query']
        }
    
    def analyze(self) -> nx.DiGraph:
        """Run full data lineage analysis."""
        print("💧 Hydrologist: Analyzing data lineage")
        
        # Find all relevant files
        sql_files = self.repo_path.rglob("*.sql")
        python_files = self.repo_path.rglob("*.py")
        yaml_files =list(self.repo_path.rglob("*.yml"))
        yaml_files.extend(self.repo_path.rglob("*.yaml"))
        notebook_files = self.repo_path.rglob("*.ipynb")
        
        # Analyze each type
        for sql_file in sql_files:
            self._analyze_sql_file(sql_file)
        
        for py_file in python_files:
            self._analyze_python_file(py_file)
        
        for yaml_file in yaml_files:
            self._analyze_yaml_file(yaml_file)
        
        for nb_file in notebook_files:
            self._analyze_notebook(nb_file)
        
        # Build lineage graph
        self._build_lineage_graph()
        
        print(f"✅ Hydrologist: Found {len(self.datasets)} datasets and "
              f"{len(self.transformations)} transformations")
        
        return self.lineage_graph
    
    def _analyze_sql_file(self, file_path: Path):
        """Analyze SQL file for table dependencies."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Parse SQL
            statements = sqlglot.parse_all(content, dialect='duckdb')
            
            for i, statement in enumerate(statements):
                if not statement:
                    continue
                
                # Extract tables
                tables = self._extract_tables_from_sql(statement)
                
                if tables:
                    # Create transformation
                    trans_id = f"{file_path}::stmt_{i}"
                    transformation = TransformationNode(
                        source_datasets=list(tables['sources']),
                        target_datasets=list(tables['targets']),
                        transformation_type='sql_script',
                        source_file=str(file_path),
                        line_range=(i*10, (i+1)*10),  # Approximate
                        sql_query=content[:200] + "..."
                    )
                    self.transformations[trans_id] = transformation
                    
                    # Create dataset nodes
                    for table in tables['sources']:
                        if table not in self.datasets:
                            self.datasets[table] = DatasetNode(
                                name=table,
                                storage_type='table',
                                file_paths=[str(file_path)]
                            )
                    
                    for table in tables['targets']:
                        if table not in self.datasets:
                            self.datasets[table] = DatasetNode(
                                name=table,
                                storage_type='table',
                                file_paths=[str(file_path)]
                            )
        
        except Exception as e:
            print(f"⚠️  Error analyzing SQL {file_path}: {e}")
    
    def _extract_tables_from_sql(self, statement) -> Dict[str, Set[str]]:
        """Extract source and target tables from SQL statement."""
        sources = set()
        targets = set()
        
        # Find all table references
        for table in statement.find_all(exp.Table):
            table_name = table.name
            
            # Check if it's in a FROM clause (source)
            if table.find_ancestor(exp.From):
                sources.add(table_name)
            
            # Check if it's in an INSERT/UPDATE target
            if table.find_ancestor(exp.Insert, exp.Update):
                targets.add(table_name)
        
        # If no explicit targets found and it's a SELECT, assume CTE or temp
        if not targets and statement.find(exp.Select):
            # This is likely a CTE definition or temp view
            pass
        
        return {'sources': sources, 'targets': targets}
    
    def _analyze_python_file(self, file_path: Path):
        """Analyze Python file for data operations."""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Simple pattern matching for data operations
            lines = content.split('\n')
            
            for i, line in enumerate(lines):
                # Check for read operations
                for pattern in self.data_patterns['pandas_read']:
                    if pattern in line:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'read')
                
                for pattern in self.data_patterns['pandas_write']:
                    if pattern in line:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'write')
                
                # Check for SQLAlchemy
                if 'execute' in line and ('SELECT' in line.upper() or 
                                          'INSERT' in line.upper()):
                    self._extract_sql_from_line(line, i, file_path)
        
        except Exception as e:
            print(f"⚠️  Error analyzing Python {file_path}: {e}")
    
    def _extract_dataset_from_line(self, line: str, line_num: int, 
                                   file_path: Path, op_type: str):
        """Extract dataset name from a line of code."""
        # Very simple extraction - in production, use AST parsing
        import re
        
        # Look for strings that might be file paths or table names
        matches = re.findall(r'[\'"]([^\'"]+\.(csv|parquet|json|db))[\'"]', line)
        matches.extend(re.findall(r'[\'"]([^\'"]+)[\'"]\s*,\s*', line))
        
        for match in matches:
            dataset_name = match[0] if isinstance(match, tuple) else match
            
            # Create dataset node
            if dataset_name not in self.datasets:
                self.datasets[dataset_name] = DatasetNode(
                    name=dataset_name,
                    storage_type='file' if '.' in dataset_name else 'table',
                    file_paths=[str(file_path)]
                )
            
            # Create transformation
            trans_id = f"{file_path}::line_{line_num}"
            
            if op_type == 'read':
                transformation = TransformationNode(
                    source_datasets=[dataset_name],
                    target_datasets=[],
                    transformation_type='python_read',
                    source_file=str(file_path),
                    line_range=(line_num, line_num)
                )
                self.transformations[trans_id] = transformation
            
            elif op_type == 'write':
                transformation = TransformationNode(
                    source_datasets=[],
                    target_datasets=[dataset_name],
                    transformation_type='python_write',
                    source_file=str(file_path),
                    line_range=(line_num, line_num)
                )
                self.transformations[trans_id] = transformation
    
    def _extract_sql_from_line(self, line: str, line_num: int, file_path: Path):
        """Extract SQL query from a line."""
        # Look for SQL in strings
        import re
        
        sql_match = re.search(r'["\'](SELECT.*?FROM.*?)["\']', line, re.IGNORECASE)
        if sql_match:
            sql = sql_match.group(1)
            
            try:
                # Parse the SQL
                statement = parse_one(sql)
                tables = self._extract_tables_from_sql(statement)
                
                if tables:
                    trans_id = f"{file_path}::inline_sql_{line_num}"
                    transformation = TransformationNode(
                        source_datasets=list(tables['sources']),
                        target_datasets=list(tables['targets']),
                        transformation_type='inline_sql',
                        source_file=str(file_path),
                        line_range=(line_num, line_num),
                        sql_query=sql
                    )
                    self.transformations[trans_id] = transformation
            except:
                pass
    
    def _analyze_yaml_file(self, file_path: Path):
        """Analyze YAML configuration files (dbt, Airflow, etc.)."""
        try:
            import yaml
            
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            
            # Check if it's a dbt project
            if 'models' in data:
                self._analyze_dbt_config(file_path, data)
            
            # Check for Airflow DAG
            if 'dag' in data or 'tasks' in data:
                self._analyze_airflow_config(file_path, data)
        
        except Exception as e:
            print(f"⚠️  Error analyzing YAML {file_path}: {e}")
    
    def _analyze_dbt_config(self, file_path: Path, config: dict):
        """Analyze dbt configuration."""
        if 'models' in config:
            for model in config['models']:
                if isinstance(model, dict):
                    model_name = model.get('name', '')
                    if model_name:
                        trans_id = f"{file_path}::{model_name}"
                        transformation = TransformationNode(
                            source_datasets=[],
                            target_datasets=[model_name],
                            transformation_type='dbt_model',
                            source_file=str(file_path),
                            line_range=(0, 0),
                            description=model.get('description', '')
                        )
                        self.transformations[trans_id] = transformation
                        
                        if model_name not in self.datasets:
                            self.datasets[model_name] = DatasetNode(
                                name=model_name,
                                storage_type='table',
                                file_paths=[str(file_path)],
                                schema_snapshot=model.get('columns', {})
                            )
    
    def _analyze_airflow_config(self, file_path: Path, config: dict):
        """Analyze Airflow DAG configuration."""
        if 'tasks' in config:
            for task in config['tasks']:
                if isinstance(task, dict):
                    task_id = task.get('task_id', '')
                    if task_id:
                        trans_id = f"{file_path}::{task_id}"
                        transformation = TransformationNode(
                            source_datasets=[],
                            target_datasets=[],
                            transformation_type='airflow_task',
                            source_file=str(file_path),
                            line_range=(0, 0),
                            description=task.get('description', '')
                        )
                        self.transformations[trans_id] = transformation
    
    def _analyze_notebook(self, file_path: Path):
        """Analyze Jupyter notebook."""
        try:
            import nbformat
            
            with open(file_path, 'r') as f:
                nb = nbformat.read(f, as_version=4)
            
            for cell_idx, cell in enumerate(nb.cells):
                if cell.cell_type == 'code':
                    # Treat code cells like Python files
                    lines = cell.source.split('\n')
                    
                    for line_idx, line in enumerate(lines):
                        # Check for data operations
                        for pattern in self.data_patterns['pandas_read']:
                            if pattern in line:
                                self._extract_dataset_from_line(
                                    line, cell_idx * 100 + line_idx, 
                                    file_path, 'read')
        
        except Exception as e:
            print(f"⚠️  Error analyzing notebook {file_path}: {e}")
    
    def _build_lineage_graph(self):
        """Build the lineage graph using NetworkX."""
        # Add dataset nodes
        for name, dataset in self.datasets.items():
            self.lineage_graph.add_node(f"dataset:{name}", 
                                        type='dataset', 
                                        **dataset.dict())
        
        # Add transformation nodes
        for tid, trans in self.transformations.items():
            self.lineage_graph.add_node(f"trans:{tid}", 
                                        type='transformation',
                                        **trans.dict())
            
            # Add edges
            for source in trans.source_datasets:
                self.lineage_graph.add_edge(
                    f"dataset:{source}",
                    f"trans:{tid}",
                    type='consumes'
                )
            
            for target in trans.target_datasets:
                self.lineage_graph.add_edge(
                    f"trans:{tid}",
                    f"dataset:{target}",
                    type='produces'
                )
    
    def blast_radius(self, node_name: str) -> List[str]:
        """Calculate blast radius for a dataset or transformation."""
        if f"dataset:{node_name}" in self.lineage_graph:
            node = f"dataset:{node_name}"
        elif f"trans:{node_name}" in self.lineage_graph:
            node = f"trans:{node_name}"
        else:
            return []
        
        # Get all downstream nodes
        descendants = list(nx.descendants(self.lineage_graph, node))
        return [d.replace('dataset:', '').replace('trans:', '') 
                for d in descendants]
    
    def find_sources(self) -> List[str]:
        """Find source datasets (in-degree 0)."""
        sources = []
        for node in self.lineage_graph.nodes():
            if node.startswith('dataset:') and \
               self.lineage_graph.in_degree(node) == 0:
                sources.append(node.replace('dataset:', ''))
        return sources
    
    def find_sinks(self) -> List[str]:
        """Find sink datasets (out-degree 0)."""
        sinks = []
        for node in self.lineage_graph.nodes():
            if node.startswith('dataset:') and \
               self.lineage_graph.out_degree(node) == 0:
                sinks.append(node.replace('dataset:', ''))
        return sinks
    
    def trace_lineage(self, dataset: str, direction: str = 'upstream') -> List[str]:
        """Trace lineage of a dataset upstream or downstream."""
        node = f"dataset:{dataset}"
        if node not in self.lineage_graph:
            return []
        
        if direction == 'upstream':
            # Get all ancestors
            ancestors = list(nx.ancestors(self.lineage_graph, node))
            return [a.replace('dataset:', '').replace('trans:', '') 
                    for a in ancestors if a.startswith('dataset:')]
        else:
            # Get all descendants
            descendants = list(nx.descendants(self.lineage_graph, node))
            return [d.replace('dataset:', '').replace('trans:', '') 
                    for d in descendants if d.startswith('dataset:')]