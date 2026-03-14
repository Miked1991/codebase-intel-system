"""Pydantic models for knowledge graph nodes."""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from pydantic import BaseModel, Field, validator


class ModuleNode(BaseModel):
    """Represents a code module/file in the knowledge graph."""
    
    path: str
    language: Optional[str] = "unknown"  # Make language optional with default
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: float = 0.0
    change_velocity_30d: int = 0
    is_dead_code_candidate: bool = False
    last_modified: Optional[datetime] = None
    imports: List[str] = Field(default_factory=list)
    public_functions: List[Dict[str, Any]] = Field(default_factory=list)
    public_classes: List[Dict[str, Any]] = Field(default_factory=list)
    loc: int = 0  # Lines of code
    comment_ratio: float = 0.0
    docstring_quality: float = 0.0
    
    @validator('language', pre=True, always=True)
    def validate_language(cls, v):
        """Ensure language is a string or None."""
        if v is None:
            return "unknown"
        return str(v)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class DatasetNode(BaseModel):
    """Represents a dataset (table, file, stream) in the knowledge graph."""
    
    name: str
    storage_type: str = "table"  # table|file|stream|api
    # Allow both dict and list, but convert to dict internally
    schema_snapshot: Optional[Union[Dict[str, str], List[Dict[str, Any]], Dict[str, Any]]] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    file_paths: List[str] = Field(default_factory=list)
    format: Optional[str] = None  # parquet, csv, json, etc.
    partition_columns: List[str] = Field(default_factory=list)
    
    @validator('schema_snapshot', pre=True, always=True)
    def validate_schema_snapshot(cls, v):
        """Convert list of column definitions to dict if needed."""
        if v is None:
            return {}
        
        # If it's already a dict, return as is
        if isinstance(v, dict):
            return v
        
        # If it's a list of column dicts, convert to dict mapping name -> type
        if isinstance(v, list):
            schema_dict = {}
            for col in v:
                if isinstance(col, dict):
                    col_name = col.get('name')
                    # Try to get data_type, fallback to description or empty string
                    col_type = col.get('data_type') or col.get('type') or col.get('description', 'string')
                    if col_name:
                        schema_dict[col_name] = str(col_type)
                elif isinstance(col, str):
                    # Simple string column name
                    schema_dict[col] = 'string'
            return schema_dict
        
        # Fallback - convert to empty dict
        return {}
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class FunctionNode(BaseModel):
    """Represents a function/method in the knowledge graph."""
    
    qualified_name: str
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False
    line_start: int
    line_end: int
    complexity: int = 1  # Cyclomatic complexity
    docstring: Optional[str] = None
    decorators: List[str] = Field(default_factory=list)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class TransformationNode(BaseModel):
    """Represents a data transformation operation."""
    
    source_datasets: List[str] = Field(default_factory=list)
    target_datasets: List[str] = Field(default_factory=list)
    transformation_type: str  # sql_script, python_transform, etc.
    source_file: str
    line_range: Optional[Tuple[int, int]] = (0, 0)  # Make optional with default
    sql_query: Optional[str] = None
    transformation_logic: Optional[str] = None
    description: Optional[str] = None
    
    @validator('line_range', pre=True, always=True)
    def validate_line_range(cls, v):
        """Ensure line_range is a tuple or provide default."""
        if v is None:
            return (0, 0)
        if isinstance(v, (list, tuple)) and len(v) == 2:
            try:
                return (int(v[0]), int(v[1]))
            except (ValueError, TypeError):
                return (0, 0)
        return (0, 0)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }