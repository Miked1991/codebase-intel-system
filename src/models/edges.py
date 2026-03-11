"""Pydantic models for knowledge graph edges."""

from typing import Optional
from pydantic import BaseModel


class ImportEdge(BaseModel):
    """Represents an import relationship between modules."""
    
    source_module: str
    target_module: str
    import_count: int = 1
    import_type: str = "explicit"  # explicit, wildcard, relative
    line_number: Optional[int] = None


class ProducesEdge(BaseModel):
    """Represents a transformation producing a dataset."""
    
    transformation: str
    dataset: str
    confidence: float = 1.0
    source_file: str
    line_range: tuple[int, int]


class ConsumesEdge(BaseModel):
    """Represents a transformation consuming a dataset."""
    
    transformation: str
    dataset: str
    confidence: float = 1.0
    source_file: str
    line_range: tuple[int, int]


class CallsEdge(BaseModel):
    """Represents a function call relationship."""
    
    caller: str
    callee: str
    call_count: int = 1
    line_numbers: list[int] = None


class ConfiguresEdge(BaseModel):
    """Represents a configuration relationship."""
    
    config_file: str
    target: str  # module or pipeline name
    config_type: str  # yaml, env, json