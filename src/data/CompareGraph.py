from typing import Dict, List, Any, Set, Tuple
from config_dataclasses import ComparisonMetrics

class CompareGraph:

    @staticmethod
    def normalize_node(node: Dict) -> Tuple[str, str]:

        node_id = node.get('id', '')
        label = node.get('label', '')
        return (str(node_id), str(label))
    
    @staticmethod
    def normalize_edge(edge: Dict) -> Tuple[str, str, str]:

        start = edge.get('start_id', '')
        end = edge.get('end_id', '')
        edge_type = edge.get('type', '')
        return (str(start), str(end), str(edge_type))
    

    @staticmethod
    def extract_graph_elements(results: List[Dict]) -> Tuple[Set, Set]:

        nodes = set()
        edges = set()
        
        for row in results:
            for x, value in row.items():
                if isinstance(value, dict):
                    if 'id' in value and 'label' in value:
                        nodes.add(CompareGraph.normalize_node(value))
                    elif 'start_id' in value and 'end_id' in value:
                        edges.add(CompareGraph.normalize_edge(value))
        
        return nodes, edges
    
    @staticmethod
    def compare(results_gt: List[Dict], results_llm: List[Dict], id: str) -> ComparisonMetrics:
        gt_nodes , gt_edges = CompareGraph.extract_graph_elements(results_gt)
        llm_nodes, llm_edges = CompareGraph.extract_graph_elements(results_llm)

        missllm_nodes = gt_nodes - llm_nodes
        extrallm_nodes = llm_nodes - gt_nodes

        missllm_edges = gt_edges - llm_edges
        extrallm_edges = llm_edges - gt_edges

        return ComparisonMetrics(
            query_id = id,
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

