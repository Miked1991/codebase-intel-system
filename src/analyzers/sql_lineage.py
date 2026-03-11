"""SQL lineage analysis using sqlglot."""

from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.optimizer import optimize
import networkx as nx


class SQLLineageAnalyzer:
    """Extract data lineage from SQL files using sqlglot."""
    
    def __init__(self, dialect: str = "duckdb"):
        self.dialect = dialect
        self.supported_dialects = [
            "duckdb", "postgres", "mysql", "sqlite", "bigquery", 
            "snowflake", "redshift", "spark", "presto", "trino"
        ]
        
        if dialect not in self.supported_dialects:
            print(f"⚠️  Warning: {dialect} may not be fully supported. Using duckdb as fallback.")
            self.dialect = "duckdb"
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a SQL file and extract lineage information."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {"error": f"File not found: {file_path}"}
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            return self.analyze_sql(content, str(file_path))
            
        except Exception as e:
            return {"error": f"Error analyzing {file_path}: {e}"}
    
    def analyze_sql(self, sql: str, source: str = "unknown") -> Dict[str, Any]:
        """Parse SQL and extract lineage information."""
        result = {
            "source": source,
            "tables": {
                "read": [],
                "write": [],
                "intermediate": []
            },
            "columns": {},
            "transformations": [],
            "errors": []
        }
        
        try:
            # Parse SQL into AST
            statements = sqlglot.parse_all(sql, dialect=self.dialect)
            
            for i, statement in enumerate(statements):
                if not statement:
                    continue
                
                # Analyze each statement
                stmt_result = self._analyze_statement(statement, i)
                
                # Merge results
                for table_type in ['read', 'write', 'intermediate']:
                    result['tables'][table_type].extend(
                        stmt_result['tables'].get(table_type, [])
                    )
                
                result['columns'].update(stmt_result['columns'])
                result['transformations'].extend(stmt_result['transformations'])
            
            # Deduplicate tables
            for table_type in ['read', 'write', 'intermediate']:
                result['tables'][table_type] = list(set(result['tables'][table_type]))
            
        except Exception as e:
            result['errors'].append(str(e))
        
        return result
    
    def _analyze_statement(self, statement: exp.Expression, idx: int) -> Dict[str, Any]:
        """Analyze a single SQL statement."""
        result = {
            "tables": {
                "read": [],
                "write": [],
                "intermediate": []
            },
            "columns": {},
            "transformations": []
        }
        
        # Determine statement type
        if isinstance(statement, exp.Select):
            # SELECT statement (read)
            tables = self._extract_tables(statement)
            result['tables']['read'].extend(tables)
            
            # Extract CTEs (intermediate)
            ctes = self._extract_ctes(statement)
            result['tables']['intermediate'].extend(ctes)
            
            # Extract column lineage
            columns = self._extract_columns(statement)
            result['columns'].update(columns)
            
            # Add transformation
            result['transformations'].append({
                "type": "select",
                "statement_idx": idx,
                "tables": tables,
                "ctes": ctes
            })
            
        elif isinstance(statement, exp.Insert):
            # INSERT statement (write)
            target = self._extract_target_table(statement)
            if target:
                result['tables']['write'].append(target)
            
            # Extract source tables from the SELECT part
            if isinstance(statement.this, exp.Select):
                source_tables = self._extract_tables(statement.this)
                result['tables']['read'].extend(source_tables)
            
            result['transformations'].append({
                "type": "insert",
                "statement_idx": idx,
                "target": target,
                "sources": source_tables if 'source_tables' in locals() else []
            })
            
        elif isinstance(statement, exp.Update):
            # UPDATE statement (write)
            target = self._extract_target_table(statement)
            if target:
                result['tables']['write'].append(target)
            
            result['transformations'].append({
                "type": "update",
                "statement_idx": idx,
                "target": target
            })
            
        elif isinstance(statement, exp.Create):
            # CREATE statement (write)
            target = self._extract_target_table(statement)
            if target:
                result['tables']['write'].append(target)
            
            # If it's CREATE TABLE AS, extract sources
            if isinstance(statement.expression, exp.Select):
                source_tables = self._extract_tables(statement.expression)
                result['tables']['read'].extend(source_tables)
            
            result['transformations'].append({
                "type": "create",
                "statement_idx": idx,
                "target": target
            })
        
        return result
    
    def _extract_tables(self, expression: exp.Expression) -> List[str]:
        """Extract all table names from an expression."""
        tables = []
        
        for table in expression.find_all(exp.Table):
            table_name = table.name
            if table_name and table_name not in tables:
                tables.append(table_name)
        
        # Extract from FROM clause
        from_clause = expression.args.get('from')
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                table_name = table.name
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        # Extract from JOINs
        for join in expression.find_all(exp.Join):
            for table in join.find_all(exp.Table):
                table_name = table.name
                if table_name and table_name not in tables:
                    tables.append(table_name)
        
        return tables
    
    def _extract_target_table(self, statement: exp.Expression) -> Optional[str]:
        """Extract target table from INSERT/UPDATE/CREATE statements."""
        if isinstance(statement, exp.Insert):
            # Get the table being inserted into
            into = statement.args.get('into')
            if into:
                for table in into.find_all(exp.Table):
                    return table.name
        
        elif isinstance(statement, exp.Update):
            # Get the table being updated
            for table in statement.find_all(exp.Table):
                return table.name
        
        elif isinstance(statement, exp.Create):
            # Get the table being created
            return statement.name
        
        return None
    
    def _extract_ctes(self, expression: exp.Expression) -> List[str]:
        """Extract CTE names from a WITH clause."""
        ctes = []
        
        with_clause = expression.args.get('with')
        if with_clause:
            for cte in with_clause.find_all(exp.CTE):
                if cte.alias:
                    ctes.append(cte.alias)
        
        return ctes
    
    def _extract_columns(self, expression: exp.Expression) -> Dict[str, List[str]]:
        """Extract column-level lineage (simplified)."""
        columns = {}
        
        # Find all column references
        for column in expression.find_all(exp.Column):
            table = column.table
            name = column.name
            
            if table:
                if table not in columns:
                    columns[table] = []
                if name not in columns[table]:
                    columns[table].append(name)
        
        return columns
    
    def build_lineage_graph(self, sql_files: List[str]) -> nx.DiGraph:
        """Build a lineage graph from multiple SQL files."""
        graph = nx.DiGraph()
        
        for file_path in sql_files:
            result = self.analyze_file(file_path)
            
            if 'error' in result:
                continue
            
            file_name = Path(file_path).name
            
            # Add table nodes
            for table_type, tables in result['tables'].items():
                for table in tables:
                    node_id = f"table:{table}"
                    if node_id not in graph:
                        graph.add_node(node_id, type='table', file=file_name)
            
            # Add edges for transformations
            for trans in result['transformations']:
                trans_id = f"trans:{file_name}:{trans['statement_idx']}"
                graph.add_node(trans_id, type='transformation', **trans)
                
                # Connect sources to transformation
                for source in trans.get('tables', []):
                    graph.add_edge(f"table:{source}", trans_id, type='consumes')
                
                # Connect transformation to targets
                target = trans.get('target')
                if target:
                    graph.add_edge(trans_id, f"table:{target}", type='produces')
        
        return graph
    
    def trace_lineage(self, graph: nx.DiGraph, table: str, 
                     direction: str = 'upstream') -> List[str]:
        """Trace lineage of a table upstream or downstream."""
        node = f"table:{table}"
        
        if node not in graph:
            return []
        
        if direction == 'upstream':
            # Get all ancestors
            ancestors = nx.ancestors(graph, node)
            return [n.replace('table:', '') for n in ancestors if n.startswith('table:')]
        else:
            # Get all descendants
            descendants = nx.descendants(graph, node)
            return [n.replace('table:', '') for n in descendants if n.startswith('table:')]
    
    def get_dbt_lineage(self, manifest_path: str) -> Dict[str, Any]:
        """Extract lineage from a dbt manifest file."""
        try:
            import json
            
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            
            lineage = {
                "nodes": {},
                "edges": []
            }
            
            # Extract nodes
            for node_name, node_data in manifest.get('nodes', {}).items():
                if node_data.get('resource_type') == 'model':
                    lineage['nodes'][node_name] = {
                        "name": node_data.get('name'),
                        "schema": node_data.get('schema'),
                        "database": node_data.get('database'),
                        "depends_on": node_data.get('depends_on', {}).get('nodes', [])
                    }
            
            # Build edges
            for node_name, node_data in lineage['nodes'].items():
                for dep in node_data['depends_on']:
                    if dep in lineage['nodes']:
                        lineage['edges'].append({
                            "from": dep,
                            "to": node_name
                        })
            
            return lineage
            
        except Exception as e:
            return {"error": f"Error parsing dbt manifest: {e}"}