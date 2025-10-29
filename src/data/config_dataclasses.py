from dataclasses import dataclass
from typing import List, Dict, Optional, Any

@dataclass

class GraphConfig:
    fields_to_keep: Dict[str, List[str]]
    fields_to_exclude: List[str]
    measurements_table: Optional[Dict] = None

@dataclass

class ComparisonMetrics:
    query_id: str
    missing_llm: Dict[str, List[Any]]
    extra_llm: Dict[str, List[Any]]
    nodes_gt: int
    edges_gt: int
    nodes_llm: int
    edges_llm: int
