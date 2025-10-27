from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass

class GraphConfig:
    fields_to_keep: Dict[str, List[str]]
    fields_to_exclude: List[str]
    measurements_table: Optional[Dict] = None
