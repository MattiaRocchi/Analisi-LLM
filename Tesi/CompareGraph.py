from typing import Dict, List, Set, Tuple, Any
import json
from config_dataclasses import ComparisonMetrics

class CompareGraph:

    @staticmethod
    def normalize_node(node: Dict) -> Tuple[str, str]:
        properties = node.get('properties', {})
        node_id = properties.get('id', '')
        label = node.get('label', '')
        return (str(node_id), str(label))
    
    @staticmethod
    def normalize_edge(edge: Dict) -> Tuple[str, str, str, str]:
        start = edge.get('start_id', '')
        end = edge.get('end_id', '')
        edge_type = edge.get('label', edge.get('type', ''))
        edge_id = edge.get('id', '')
        return (str(start), str(end), str(edge_type), str(edge_id))
    
    @staticmethod
    def parse_path(value: str) -> list:
        # Rimuove ::path finale e parentesi quadre esterne
        inner = value.strip()
        if '::path' in inner:
            inner = inner[:inner.rfind('::path')].strip()
        if inner.startswith('['):
            inner = inner[1:]
        if inner.endswith(']'):
            inner = inner[:-1]
        
        elements = []
        depth = 0
        current = []

        for char in inner:
            if char == '{':
                depth += 1
                current.append(char)
            elif char == '}':
                depth -= 1
                current.append(char)
                if depth == 0:
                    # Oggetto JSON completo — estrailo
                    json_str = ''.join(current).strip()
                    try:
                        elements.append(json.loads(json_str))
                    except json.JSONDecodeError:
                        pass
                    current = []
            elif depth > 0:
                # Siamo dentro un oggetto, accumula tutto
                current.append(char)
            # Se depth == 0 e non siamo in un oggetto,
            # siamo su virgole o suffissi ::vertex/::edge — li ignoriamo
        
        return elements

    @staticmethod
    def parse_agtype_value(value: Any) -> Any:
        if value is None:
            return None
        
        if isinstance(value, str):
            stripped = value.strip()
            
            # Caso path AGE: [..., {...}::vertex, {...}::edge, ...]::path
            if stripped.endswith('::path'):
                return CompareGraph.parse_path(stripped)
            
            # Caso normale: {…}::vertex o {…}::edge
            clean_value = stripped.split('::')[0].strip()
            try:
                return json.loads(clean_value)
            except (json.JSONDecodeError, ValueError):
                return value
        
        return value
    
    @staticmethod
    def is_node(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        
        properties = item.get('properties', {})
        has_properties_id = isinstance(properties, dict) and 'id' in properties
        has_label = 'label' in item
        
        is_not_edge = 'start_id' not in item and 'end_id' not in item
        
        return has_properties_id and has_label and is_not_edge
    
    @staticmethod
    def is_edge(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        
        has_start = 'start_id' in item
        has_end = 'end_id' in item
        has_label = 'label' in item
        
        return has_start and has_end and has_label
    
    @staticmethod
    def extract_from_item(item: Any, nodes: Set, edges: Set, node_id_map: Dict):
        if item is None:
            return
        
        item = CompareGraph.parse_agtype_value(item)
        
        if isinstance(item, dict):
            if CompareGraph.is_edge(item):
                edge_tuple = CompareGraph.normalize_edge(item)
                edges.add(edge_tuple)
            elif CompareGraph.is_node(item):
                node_tuple = CompareGraph.normalize_node(item)
                nodes.add(node_tuple)
                internal_id = str(item.get('id', ''))
                semantic_id = str(item.get('properties', {}).get('id', ''))
                if internal_id and semantic_id:
                    node_id_map[internal_id] = semantic_id
            else:
                for value in item.values():
                    CompareGraph.extract_from_item(value, nodes, edges, node_id_map)
                
        elif isinstance(item, list):
            for element in item:
                CompareGraph.extract_from_item(element, nodes, edges, node_id_map)

    @staticmethod
    def resolve_edges(edges: Set, node_id_map: Dict) -> Set:
        resolved_edges = set()
        
        for start_internal, end_internal, edge_type, edge_id in edges:

            start_semantic = node_id_map.get(start_internal, start_internal)
            end_semantic = node_id_map.get(end_internal, end_internal)
            
            resolved_edges.add((start_semantic, end_semantic, edge_type))
        
        return resolved_edges

    @staticmethod
    def extract_graph_elements(results: List[Dict]) -> Tuple[Set, Set]:
        nodes = set()
        edges = set()
        node_id_map = {}
        
        for row in results:
            for value in row.values():
                CompareGraph.extract_from_item(value, nodes, edges, node_id_map)
        
        resolved_edges = CompareGraph.resolve_edges(edges, node_id_map)
        
        return nodes, resolved_edges
    
    @staticmethod
    def compare(results_gt: List[Dict], results_llm: List[Dict], id: str) -> ComparisonMetrics:

        gt_nodes, gt_edges = CompareGraph.extract_graph_elements(results_gt)
        llm_nodes, llm_edges = CompareGraph.extract_graph_elements(results_llm)

        missllm_nodes = gt_nodes - llm_nodes
        extrallm_nodes = llm_nodes - gt_nodes

        missllm_edges = gt_edges - llm_edges
        extrallm_edges = llm_edges - gt_edges

        return ComparisonMetrics(
            query_id=id,
            missing_llm={
                'nodes': list(missllm_nodes),
                'edges': list(missllm_edges)
            },
            extra_llm={
                'nodes': list(extrallm_nodes),
                'edges': list(extrallm_edges)
            },
            nodes_gt=len(gt_nodes),
            nodes_llm=len(llm_nodes),
            edges_gt=len(gt_edges),
            edges_llm=len(llm_edges)
        )