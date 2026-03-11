"""Pydantic models for knowledge graph nodes."""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class ModuleNode(BaseModel):
    """Represents a code module/file in the knowledge graph."""
    
    path: str
    language: str
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


class DatasetNode(BaseModel):
    """Represents a dataset (table, file, stream) in the knowledge graph."""
    
    name: str
    storage_type: str = "table"  # table|file|stream|api
    schema_snapshot: Optional[Dict[str, str]] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    file_paths: List[str] = Field(default_factory=list)
    format: Optional[str] = None  # parquet, csv, json, etc.
    partition_columns: List[str] = Field(default_factory=list)


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


class TransformationNode(BaseModel):
    """Represents a data transformation operation."""
    
    source_datasets: List[str]
    target_datasets: List[str]
    transformation_type: str  # sql_script, python_transform, etc.
    source_file: str
    line_range: tuple[int, int]
    sql_query: Optional[str] = None
    transformation_logic: Optional[str] = None
    description: Optional[str] = None