"""Parser for DAG configuration files (Airflow, dbt, etc.)."""

from pathlib import Path
from typing import Dict, List, Optional, Any, Set
import yaml
import json
import re


class DAGConfigParser:
    """Parse configuration files for data pipeline DAGs."""
    
    def __init__(self):
        self.supported_formats = ['yaml', 'yml', 'json', 'py']
    
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse a configuration file and extract DAG structure."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {"error": f"File not found: {file_path}"}
        
        extension = file_path.suffix.lower().lstrip('.')
        
        if extension in ['yaml', 'yml']:
            return self._parse_yaml(file_path)
        elif extension == 'json':
            return self._parse_json(file_path)
        elif extension == 'py':
            return self._parse_python_dag(file_path)
        else:
            return {"error": f"Unsupported file format: {extension}"}
    
    def _parse_yaml(self, file_path: Path) -> Dict[str, Any]:
        """Parse YAML configuration (dbt, Airflow, etc.)."""
        result = {
            "file": str(file_path),
            "format": "yaml",
            "type": "unknown",
            "dags": [],
            "tasks": [],
            "models": [],
            "sources": []
        }
        
        try:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            
            if not data:
                return result
            
            # Detect configuration type
            config_type = self._detect_config_type(data)
            result['type'] = config_type
            
            if config_type == 'dbt_project':
                self._parse_dbt_project(data, result)
            elif config_type == 'dbt_model':
                self._parse_dbt_model(data, result, file_path)
            elif config_type == 'airflow_dag':
                # Airflow DAGs are typically Python files, but can have YAML configs
                self._parse_airflow_config(data, result)
            elif config_type == 'dagster':
                self._parse_dagster_config(data, result)
            elif config_type == 'prefect':
                self._parse_prefect_config(data, result)
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _parse_json(self, file_path: Path) -> Dict[str, Any]:
        """Parse JSON configuration."""
        result = {
            "file": str(file_path),
            "format": "json",
            "type": "unknown",
            "dags": [],
            "tasks": []
        }
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Check for common patterns
            if 'nodes' in data and 'edges' in data:
                # Could be a DAG definition
                result['type'] = 'dag_definition'
                result['nodes'] = data.get('nodes', [])
                result['edges'] = data.get('edges', [])
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _parse_python_dag(self, file_path: Path) -> Dict[str, Any]:
        """Parse Python DAG definition files (Airflow, etc.)."""
        result = {
            "file": str(file_path),
            "format": "python",
            "type": "unknown",
            "dags": [],
            "tasks": [],
            "operators": []
        }
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Simple regex-based parsing (in production, use AST)
            
            # Look for DAG definitions
            dag_pattern = r'DAG\s*\(\s*[\'"]([^\'"]+)[\'"]'
            dags = re.findall(dag_pattern, content)
            result['dags'] = dags
            
            # Look for task definitions (Airflow operators)
            task_pattern = r'(\w+)\s*=\s*(\w+Operator)\s*\('
            tasks = re.findall(task_pattern, content)
            for task_id, operator in tasks:
                result['tasks'].append({
                    "id": task_id,
                    "operator": operator
                })
            
            # Look for dependencies
            dep_pattern = r'(\w+)\s*>>\s*(\w+)'
            deps = re.findall(dep_pattern, content)
            
            # Look for PythonOperator callables
            python_callable_pattern = r'python_callable\s*=\s*(\w+)'
            callables = re.findall(python_callable_pattern, content)
            
            result['dependencies'] = deps
            result['python_callables'] = callables
            
            # Detect framework
            if 'airflow' in content.lower():
                result['type'] = 'airflow'
            elif 'dagster' in content.lower():
                result['type'] = 'dagster'
            elif 'prefect' in content.lower():
                result['type'] = 'prefect'
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _detect_config_type(self, data: Dict) -> str:
        """Detect the type of configuration."""
        if isinstance(data, dict):
            # dbt project
            if 'models' in data and 'vars' in data:
                return 'dbt_project'
            
            # dbt model config
            if 'model' in data and 'config' in data:
                return 'dbt_model'
            
            # Airflow DAG config
            if 'dag_id' in data and 'schedule_interval' in data:
                return 'airflow_dag'
            
            # Dagster config
            if 'schedules' in data and 'jobs' in data:
                return 'dagster'
            
            # Prefect config
            if 'flow_name' in data and 'tasks' in data:
                return 'prefect'
        
        return 'unknown'
    
    def _parse_dbt_project(self, data: Dict, result: Dict):
        """Parse dbt project configuration."""
        result['project_name'] = data.get('name')
        result['profile'] = data.get('profile')
        result['model_paths'] = data.get('model-paths', ['models'])
        result['seed_paths'] = data.get('seed-paths', ['seeds'])
        
        # Extract model configurations
        if 'models' in data:
            models_config = data['models']
            if isinstance(models_config, dict):
                for model_name, model_config in models_config.items():
                    if isinstance(model_config, dict):
                        result['models'].append({
                            "name": model_name,
                            "materialized": model_config.get('materialized'),
                            "schema": model_config.get('schema')
                        })
    
    def _parse_dbt_model(self, data: Dict, result: Dict, file_path: Path):
        """Parse dbt model configuration (schema.yml)."""
        if 'models' in data:
            for model in data['models']:
                if isinstance(model, dict):
                    model_info = {
                        "name": model.get('name'),
                        "description": model.get('description'),
                        "columns": []
                    }
                    
                    # Extract columns
                    if 'columns' in model:
                        for col in model['columns']:
                            if isinstance(col, dict):
                                model_info['columns'].append({
                                    "name": col.get('name'),
                                    "description": col.get('description'),
                                    "data_type": col.get('data_type')
                                })
                    
                    # Extract tests
                    if 'tests' in model:
                        model_info['tests'] = model['tests']
                    
                    result['models'].append(model_info)
        
        # Extract sources
        if 'sources' in data:
            for source in data['sources']:
                if isinstance(source, dict):
                    source_info = {
                        "name": source.get('name'),
                        "schema": source.get('schema'),
                        "tables": []
                    }
                    
                    if 'tables' in source:
                        for table in source['tables']:
                            if isinstance(table, dict):
                                source_info['tables'].append({
                                    "name": table.get('name'),
                                    "description": table.get('description')
                                })
                    
                    result['sources'].append(source_info)
    
    def _parse_airflow_config(self, data: Dict, result: Dict):
        """Parse Airflow configuration."""
        if 'dag_id' in data:
            result['dags'].append({
                "id": data['dag_id'],
                "schedule": data.get('schedule_interval'),
                "description": data.get('description')
            })
        
        if 'tasks' in data:
            for task in data['tasks']:
                if isinstance(task, dict):
                    result['tasks'].append({
                        "id": task.get('task_id'),
                        "operator": task.get('operator'),
                        "dependencies": task.get('dependencies', [])
                    })
    
    def _parse_dagster_config(self, data: Dict, result: Dict):
        """Parse Dagster configuration."""
        if 'jobs' in data:
            for job in data['jobs']:
                if isinstance(job, dict):
                    result['dags'].append({
                        "name": job.get('name'),
                        "description": job.get('description')
                    })
    
    def _parse_prefect_config(self, data: Dict, result: Dict):
        """Parse Prefect configuration."""
        if 'flow_name' in data:
            result['dags'].append({
                "name": data['flow_name'],
                "description": data.get('description')
            })
        
        if 'tasks' in data:
            for task in data['tasks']:
                if isinstance(task, dict):
                    result['tasks'].append({
                        "name": task.get('name'),
                        "type": task.get('type')
                    })
    
    def build_dag_graph(self, config_files: List[str]) -> Dict[str, Any]:
        """Build a DAG graph from multiple configuration files."""
        graph = {
            "nodes": {},
            "edges": []
        }
        
        for file_path in config_files:
            config = self.parse_file(file_path)
            
            if 'error' in config:
                continue
            
            file_name = Path(file_path).name
            
            # Add DAG nodes
            for dag in config.get('dags', []):
                dag_id = dag.get('id') or dag.get('name')
                if dag_id:
                    node_id = f"dag:{dag_id}"
                    if node_id not in graph['nodes']:
                        graph['nodes'][node_id] = {
                            "type": "dag",
                            "name": dag_id,
                            "file": file_name,
                            "config": dag
                        }
            
            # Add task nodes
            for task in config.get('tasks', []):
                task_id = task.get('id') or task.get('name')
                if task_id:
                    node_id = f"task:{task_id}"
                    graph['nodes'][node_id] = {
                        "type": "task",
                        "name": task_id,
                        "operator": task.get('operator'),
                        "file": file_name
                    }
            
            # Add dependencies as edges
            for dep in config.get('dependencies', []):
                if len(dep) == 2:
                    graph['edges'].append({
                        "from": f"task:{dep[0]}",
                        "to": f"task:{dep[1]}"
                    })
        
        return graph