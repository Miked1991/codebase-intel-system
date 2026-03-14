"""Hydrologist Agent - Data lineage analysis."""

from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import networkx as nx
import sqlglot
from sqlglot import parse_one, exp
import json

from ..models.nodes import DatasetNode, TransformationNode
from ..models.edges import ProducesEdge, ConsumesEdge
from ..utils.language_router import get_language_router
from ..analyzers.sql_lineage import SQLLineageAnalyzer


class HydrologistAgent:
    """Agent responsible for data lineage analysis."""
    
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.language_router = get_language_router()
        self.sql_analyzer = SQLLineageAnalyzer(dialect="duckdb")
        self.lineage_graph = nx.DiGraph()
        self.datasets: Dict[str, DatasetNode] = {}
        self.transformations: Dict[str, TransformationNode] = {}
        
        # Patterns for data operations
        self.data_patterns = {
            'pandas_read': ['read_csv', 'read_sql', 'read_parquet', 'read_json', 'read_excel'],
            'pandas_write': ['to_csv', 'to_sql', 'to_parquet', 'to_json', 'to_excel'],
            'spark_read': ['spark.read', 'spark.read.format', 'spark.sql', 'spark.table'],
            'spark_write': ['write.csv', 'write.parquet', 'write.json', 'write.save', 'write.table'],
            'sqlalchemy': ['create_engine', 'execute', 'session.query', 'pd.read_sql'],
            'dask_read': ['dd.read_csv', 'dd.read_parquet', 'dd.read_json'],
            'dask_write': ['dd.to_csv', 'dd.to_parquet', 'dd.to_json'],
            'polars_read': ['pl.read_csv', 'pl.read_parquet', 'pl.read_json'],
            'polars_write': ['pl.write_csv', 'pl.write_parquet', 'pl.write_json']
        }
    
    def analyze(self) -> nx.DiGraph:
        """Run full data lineage analysis."""
        print("💧 Hydrologist: Analyzing data lineage")
        
        # Find all relevant files
        sql_files = list(self.repo_path.rglob("*.sql"))
        python_files = list(self.repo_path.rglob("*.py"))
        yaml_files = list(self.repo_path.rglob("*.yml")) + list(self.repo_path.rglob("*.yaml"))
        notebook_files = list(self.repo_path.rglob("*.ipynb"))
        
        print(f"  Found {len(sql_files)} SQL files, {len(python_files)} Python files, "
              f"{len(yaml_files)} YAML files, {len(notebook_files)} notebooks")
        
        # Analyze each type
        sql_count = 0
        for sql_file in sql_files:
            if self._analyze_sql_file(sql_file):
                sql_count += 1
        
        py_count = 0
        for py_file in python_files:
            if self._analyze_python_file(py_file):
                py_count += 1
        
        yaml_count = 0
        for yaml_file in yaml_files:
            if self._analyze_yaml_file(yaml_file):
                yaml_count += 1
        
        nb_count = 0
        for nb_file in notebook_files:
            if self._analyze_notebook(nb_file):
                nb_count += 1
        
        # Build lineage graph
        self._build_lineage_graph()
        
        print(f"✅ Hydrologist: Successfully analyzed {sql_count} SQL, {py_count} Python, "
              f"{yaml_count} YAML, {nb_count} notebooks")
        print(f"  Found {len(self.datasets)} datasets and {len(self.transformations)} transformations")
        
        return self.lineage_graph
    
    def _analyze_sql_file(self, file_path: Path) -> bool:
        """Analyze SQL file for table dependencies."""
        try:
            # Use sqlglot analyzer
            result = self.sql_analyzer.analyze_file(str(file_path))
            
            if 'error' in result:
                print(f"⚠️  Error in SQL {file_path.name}: {result['error']}")
                return False
            
            # Process tables
            for table_type, tables in result['tables'].items():
                for table in tables:
                    if table and table not in self.datasets:
                        self.datasets[table] = DatasetNode(
                            name=table,
                            storage_type='table',
                            file_paths=[str(file_path)]
                        )
            
            # Process transformations with line_range
            for trans in result['transformations']:
                trans_id = f"{file_path}::stmt_{trans['statement_idx']}"
                
                # Calculate approximate line range
                line_start = trans.get('statement_idx', 0) * 10
                line_end = line_start + 10
                
                transformation = TransformationNode(
                    source_datasets=trans.get('tables', []) + trans.get('sources', []),
                    target_datasets=[trans['target']] if trans.get('target') else [],
                    transformation_type=f"sql_{trans['type']}",
                    source_file=str(file_path),
                    line_range=(line_start, line_end),
                    sql_query=f"Statement {trans['statement_idx']}"
                )
                self.transformations[trans_id] = transformation
            
            return True
            
        except Exception as e:
            print(f"⚠️  Error analyzing SQL {file_path.name}: {e}")
            return False
    
    def _analyze_python_file(self, file_path: Path) -> bool:
        """Analyze Python file for data operations."""
        try:
            # Handle encoding errors - try utf-8, fallback to latin-1
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except UnicodeDecodeError:
                # Fallback to latin-1 which never fails
                with open(file_path, 'r', encoding='latin-1') as f:
                    lines = f.readlines()
            
            found_ops = False
            
            for i, line in enumerate(lines):
                line_lower = line.lower()
                
                # Check for read operations
                for pattern in self.data_patterns['pandas_read']:
                    if pattern in line_lower:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'read', 'pandas'
                        )
                        found_ops = True
                
                for pattern in self.data_patterns['pandas_write']:
                    if pattern in line_lower:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'write', 'pandas'
                        )
                        found_ops = True
                
                # Check for Spark operations
                for pattern in self.data_patterns['spark_read']:
                    if pattern in line_lower:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'read', 'spark'
                        )
                        found_ops = True
                
                for pattern in self.data_patterns['spark_write']:
                    if pattern in line_lower:
                        self._extract_dataset_from_line(
                            line, i, file_path, 'write', 'spark'
                        )
                        found_ops = True
                
                # Check for SQLAlchemy
                if 'execute' in line_lower and ('select' in line_lower or 'insert' in line_lower or 'update' in line_lower or 'delete' in line_lower):
                    self._extract_sql_from_line(line, i, file_path)
                    found_ops = True
                
                # Check for pd.read_sql
                if 'pd.read_sql' in line_lower or 'read_sql' in line_lower:
                    self._extract_dataset_from_line(
                        line, i, file_path, 'read', 'sqlalchemy'
                    )
                    found_ops = True
            
            return found_ops
            
        except Exception as e:
            print(f"⚠️  Error analyzing Python {file_path.name}: {e}")
            return False
    
    def _extract_dataset_from_line(self, line: str, line_num: int, 
                                   file_path: Path, op_type: str, engine: str):
        """Extract dataset name from a line of code."""
        import re
        
        # Look for strings that might be file paths or table names
        # Pattern for quoted strings
        string_pattern = r'[\'"]([^\'"]+\.(csv|parquet|json|db|xlsx|sqlite))[\'"]'
        matches = re.findall(string_pattern, line, re.IGNORECASE)
        
        # Also look for table names in SQL-like contexts
        if not matches:
            table_pattern = r'[\'"]([a-zA-Z_][a-zA-Z0-9_]*)[\'"]\s*[,)]'
            matches = re.findall(table_pattern, line)
            matches = [(m, 'table') for m in matches]
        
        # Look for variable assignments
        if not matches:
            var_pattern = r'=\s*[\'"]([^\'"]+)[\'"]'
            matches = re.findall(var_pattern, line)
            matches = [(m, 'variable') for m in matches]
        
        for match in matches:
            dataset_name = match[0] if isinstance(match, tuple) else match
            
            # Clean up the name
            dataset_name = dataset_name.strip('\'"')
            
            # Skip if too long or looks like a query
            if len(dataset_name) > 200 or 'select' in dataset_name.lower():
                continue
            
            # Create dataset node if it doesn't exist
            if dataset_name and dataset_name not in self.datasets:
                storage_type = 'file' if '.' in dataset_name else 'table'
                self.datasets[dataset_name] = DatasetNode(
                    name=dataset_name,
                    storage_type=storage_type,
                    file_paths=[str(file_path)]
                )
            
            # Create transformation with line_range
            if dataset_name:
                trans_id = f"{file_path}::line_{line_num}_{engine}_{op_type}"
                
                if op_type == 'read':
                    transformation = TransformationNode(
                        source_datasets=[dataset_name],
                        target_datasets=[],
                        transformation_type=f'python_{engine}_read',
                        source_file=str(file_path),
                        line_range=(line_num, line_num)
                    )
                else:  # write
                    transformation = TransformationNode(
                        source_datasets=[],
                        target_datasets=[dataset_name],
                        transformation_type=f'python_{engine}_write',
                        source_file=str(file_path),
                        line_range=(line_num, line_num)
                    )
                
                self.transformations[trans_id] = transformation
    
    def _extract_sql_from_line(self, line: str, line_num: int, file_path: Path):
        """Extract SQL query from a line."""
        import re
        
        # Look for SQL in strings
        sql_match = re.search(r'["\'](SELECT.*?FROM.*?)["\']', line, re.IGNORECASE)
        if sql_match:
            sql = sql_match.group(1)
            
            try:
                # Use sqlglot to parse the SQL
                result = self.sql_analyzer.analyze_sql(sql, str(file_path))
                
                if not result.get('errors'):
                    trans_id = f"{file_path}::inline_sql_{line_num}"
                    
                    # Collect all datasets
                    all_datasets = []
                    for table_type in ['read', 'write']:
                        all_datasets.extend(result['tables'].get(table_type, []))
                    
                    transformation = TransformationNode(
                        source_datasets=result['tables'].get('read', []),
                        target_datasets=result['tables'].get('write', []),
                        transformation_type='inline_sql',
                        source_file=str(file_path),
                        line_range=(line_num, line_num),
                        sql_query=sql[:200] + "..." if len(sql) > 200 else sql
                    )
                    self.transformations[trans_id] = transformation
                    
                    # Create dataset nodes
                    for dataset in all_datasets:
                        if dataset and dataset not in self.datasets:
                            self.datasets[dataset] = DatasetNode(
                                name=dataset,
                                storage_type='table',
                                file_paths=[str(file_path)]
                            )
            except Exception:
                # Silently fail for unparseable SQL
                pass
    
    def _analyze_yaml_file(self, file_path: Path) -> bool:
        """Analyze YAML configuration files (dbt, Airflow, etc.)."""
        try:
            import yaml
            
            # Handle encoding errors
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
            except UnicodeDecodeError:
                with open(file_path, 'r', encoding='latin-1') as f:
                    data = yaml.safe_load(f)
            
            if not data:
                return False
            
            found = False
            
            # Check if it's a dbt project
            if 'models' in data or 'sources' in data:
                self._analyze_dbt_config(file_path, data)
                found = True
            
            # Check for Airflow DAG
            if 'dag' in data or 'tasks' in data:
                self._analyze_airflow_config(file_path, data)
                found = True
            
            return found
            
        except Exception as e:
            print(f"⚠️  Error analyzing YAML {file_path.name}: {e}")
            return False
    
    def _analyze_dbt_config(self, file_path: Path, config: dict):
        """Analyze dbt configuration."""
        # Parse models
        if 'models' in config:
            models = config['models']
            if isinstance(models, list):
                for model in models:
                    if isinstance(model, dict):
                        self._process_dbt_model(model, file_path)
            elif isinstance(models, dict):
                for model_name, model_config in models.items():
                    if isinstance(model_config, dict):
                        model = {'name': model_name, **model_config}
                        self._process_dbt_model(model, file_path)
        
        # Parse sources
        if 'sources' in config:
            sources = config['sources']
            if isinstance(sources, list):
                for source in sources:
                    if isinstance(source, dict):
                        self._process_dbt_source(source, file_path)
    
    def _process_dbt_model(self, model: dict, file_path: Path):
        """Process a dbt model definition."""
        model_name = model.get('name')
        if not model_name:
            return
        
        # Create transformation with line_range
        trans_id = f"{file_path}::{model_name}"
        transformation = TransformationNode(
            source_datasets=[],  # Will be filled by SQL analysis
            target_datasets=[model_name],
            transformation_type='dbt_model',
            source_file=str(file_path),
            line_range=(0, 0),  # Default value
            description=model.get('description', '')
        )
        self.transformations[trans_id] = transformation
        
        # Create dataset
        if model_name not in self.datasets:
            # Extract schema from columns if available
            schema = {}
            if 'columns' in model:
                for col in model.get('columns', []):
                    if isinstance(col, dict) and 'name' in col:
                        col_name = col['name']
                        col_type = col.get('data_type', 'unknown')
                        schema[col_name] = col_type
            
            self.datasets[model_name] = DatasetNode(
                name=model_name,
                storage_type='table',
                file_paths=[str(file_path)],
                schema_snapshot=schema
            )
    
    def _process_dbt_source(self, source: dict, file_path: Path):
        """Process a dbt source definition."""
        source_name = source.get('name')
        if not source_name:
            return
        
        # Process tables in source
        if 'tables' in source:
            for table in source['tables']:
                if isinstance(table, dict):
                    table_name = table.get('name')
                    if table_name:
                        full_name = f"{source_name}.{table_name}"
                        
                        # Create dataset
                        if full_name not in self.datasets:
                            self.datasets[full_name] = DatasetNode(
                                name=full_name,
                                storage_type='source',
                                file_paths=[str(file_path)],
                                is_source_of_truth=True
                            )
    
    def _analyze_airflow_config(self, file_path: Path, config: dict):
        """Analyze Airflow DAG configuration."""
        if 'dag_id' in config:
            dag_id = config.get('dag_id')
            if dag_id:
                # Create transformation for the DAG
                trans_id = f"{file_path}::{dag_id}"
                transformation = TransformationNode(
                    source_datasets=[],
                    target_datasets=[],
                    transformation_type='airflow_dag',
                    source_file=str(file_path),
                    line_range=(0, 0),
                    description=config.get('description', '')
                )
                self.transformations[trans_id] = transformation
        
        if 'tasks' in config:
            for task in config.get('tasks', []):
                if isinstance(task, dict):
                    task_id = task.get('task_id')
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
    
    def _analyze_notebook(self, file_path: Path) -> bool:
        """Analyze Jupyter notebook."""
        try:
            import nbformat
            
            # Handle encoding errors
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    nb = nbformat.read(f, as_version=4)
            except UnicodeDecodeError:
                with open(file_path, 'r', encoding='latin-1') as f:
                    nb = nbformat.read(f, as_version=4)
            
            found_ops = False
            
            for cell_idx, cell in enumerate(nb.cells):
                if cell.cell_type == 'code':
                    # Treat code cells like Python files
                    lines = cell.source.split('\n')
                    
                    for line_idx, line in enumerate(lines):
                        line_lower = line.lower()
                        
                        # Check for data operations
                        for pattern in self.data_patterns['pandas_read']:
                            if pattern in line_lower:
                                self._extract_dataset_from_line(
                                    line, cell_idx * 100 + line_idx, 
                                    file_path, 'read', 'pandas'
                                )
                                found_ops = True
                        
                        for pattern in self.data_patterns['pandas_write']:
                            if pattern in line_lower:
                                self._extract_dataset_from_line(
                                    line, cell_idx * 100 + line_idx, 
                                    file_path, 'write', 'pandas'
                                )
                                found_ops = True
            
            return found_ops
            
        except Exception as e:
            print(f"⚠️  Error analyzing notebook {file_path.name}: {e}")
            return False
    
    def _build_lineage_graph(self):
        """Build the lineage graph using NetworkX."""
        # Add dataset nodes
        for name, dataset in self.datasets.items():
            node_id = f"dataset:{name}"
            self.lineage_graph.add_node(
                node_id, 
                type='dataset', 
                **dataset.dict()
            )
        
        # Add transformation nodes
        for tid, trans in self.transformations.items():
            node_id = f"trans:{tid}"
            self.lineage_graph.add_node(
                node_id, 
                type='transformation',
                **trans.dict()
            )
            
            # Add edges
            for source in trans.source_datasets:
                source_node = f"dataset:{source}"
                if source_node in self.lineage_graph:
                    self.lineage_graph.add_edge(
                        source_node,
                        node_id,
                        type='consumes',
                        file=trans.source_file
                    )
            
            for target in trans.target_datasets:
                target_node = f"dataset:{target}"
                if target_node in self.lineage_graph:
                    self.lineage_graph.add_edge(
                        node_id,
                        target_node,
                        type='produces',
                        file=trans.source_file
                    )
    
    def blast_radius(self, node_name: str) -> List[str]:
        """
        Calculate blast radius for a dataset or transformation.
        
        Args:
            node_name: Name of the dataset or transformation
            
        Returns:
            List of affected node names
        """
        # Check if it's a dataset
        if f"dataset:{node_name}" in self.lineage_graph:
            node = f"dataset:{node_name}"
        elif f"trans:{node_name}" in self.lineage_graph:
            node = f"trans:{node_name}"
        else:
            # Try partial match
            for n in self.lineage_graph.nodes():
                if node_name in n:
                    node = n
                    break
            else:
                return []
        
        try:
            # Get all downstream nodes
            descendants = list(nx.descendants(self.lineage_graph, node))
            # Filter to just dataset nodes and clean names
            result = []
            for d in descendants:
                if d.startswith('dataset:'):
                    result.append(d.replace('dataset:', ''))
                elif d.startswith('trans:'):
                    result.append(d.replace('trans:', ''))
            return result
        except Exception:
            return []
    
    def find_sources(self) -> List[str]:
        """Find source datasets (in-degree 0)."""
        sources = []
        for node in self.lineage_graph.nodes():
            if node.startswith('dataset:') and \
               self.lineage_graph.in_degree(node) == 0:
                sources.append(node.replace('dataset:', ''))
        return sorted(sources)
    
    def find_sinks(self) -> List[str]:
        """Find sink datasets (out-degree 0)."""
        sinks = []
        for node in self.lineage_graph.nodes():
            if node.startswith('dataset:') and \
               self.lineage_graph.out_degree(node) == 0:
                sinks.append(node.replace('dataset:', ''))
        return sorted(sinks)
    
    def trace_lineage(self, dataset: str, direction: str = 'upstream') -> List[str]:
        """
        Trace lineage of a dataset upstream or downstream.
        
        Args:
            dataset: Dataset name
            direction: 'upstream' or 'downstream'
            
        Returns:
            List of dataset names in the lineage path
        """
        node = f"dataset:{dataset}"
        if node not in self.lineage_graph:
            return []
        
        try:
            if direction == 'upstream':
                # Get all ancestors
                ancestors = list(nx.ancestors(self.lineage_graph, node))
                return [a.replace('dataset:', '') for a in ancestors if a.startswith('dataset:')]
            else:
                # Get all descendants
                descendants = list(nx.descendants(self.lineage_graph, node))
                return [d.replace('dataset:', '') for d in descendants if d.startswith('dataset:')]
        except Exception:
            return []