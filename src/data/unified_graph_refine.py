import json
import sys
from typing import Dict, List, Tuple, Any
from config_dataclasses import GraphConfig

# Import configurations
import config_v0
import config_v1_v2


def _build_node_mappings(graph_data: Dict) -> Dict[str, str]:
    """
    Build mapping from node internal ID to URN identifier.
   
    Args:
        graph_data: Original graph data
       
    Returns:
        Dictionary mapping internal node IDs to URNs
    """
    node_id_map = {}
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
    return node_id_map


def _build_has_device_map(graph_data: Dict, node_id_map: Dict[str, str]) -> Dict[str, List[str]]:
    """
    Build hasDevice relationship mapping from edges.
   
    Args:
        graph_data: Original graph data
        node_id_map: Mapping from internal IDs to URNs
       
    Returns:
        Dictionary mapping parent URNs to list of child URNs
    """
    has_device_map = {}
   
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
       
        if not start_urn or not end_urn:
            continue
           
        # Handle both belongsTo and hasDevice edge types
        if edge['type'] == 'belongsTo':
            parent_urn = end_urn
            child_urn = start_urn
        elif edge['type'] == 'hasDevice':
            parent_urn = start_urn
            child_urn = end_urn
        else:
            continue
           
        if parent_urn not in has_device_map:
            has_device_map[parent_urn] = []
        if child_urn not in has_device_map[parent_urn]:
            has_device_map[parent_urn].append(child_urn)
   
    return has_device_map


def _clean_graph_base(graph_data: Dict, config: GraphConfig,
                     include_label: bool = False,
                     include_edges: bool = True,
                     skip_measurements: bool = True) -> Tuple[List[Dict], List[Dict]]:
    """
    Base function for graph cleaning that can be customized for different versions.
   
    Args:
        graph_data: Original graph data
        config: Configuration specifying fields to keep/exclude
        include_label: Whether to include node labels in output
        include_edges: Whether to include edges in output
        skip_measurements: Whether to skip Measurement nodes
       
    Returns:
        Tuple of (cleaned_nodes, cleaned_edges)
    """
    # Build mappings
    node_id_map = _build_node_mappings(graph_data)
    has_device_map = _build_has_device_map(graph_data, node_id_map)
   
    clean_nodes = []
   
    # Process nodes
    for node in graph_data['nodes']:
        node_type = node['label']
       
        # Skip Measurement nodes if configured
        if skip_measurements and node_type == 'Measurement':
            continue
       
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
       
        # Create clean node structure
        clean_node = {'properties': {}}
        if include_label:
            clean_node['label'] = node_type
       
        # Determine allowed fields for this node type
        common_fields = config.fields_to_keep['common']
        type_fields = config.fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
       
        # Filter properties
        for key, value in node['properties'].items():
            if key in config.fields_to_exclude:
                continue
            if key in allowed_fields:
                clean_node['properties'][key] = value
       
        # Add hasDevice relationships
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
       
        clean_nodes.append(clean_node)
   
    # Build edges if requested
    clean_edges = []
    if include_edges:
        edges_dict = {}
        for parent_urn, children_urns in has_device_map.items():
            for child_urn in children_urns:
                edge_key = (parent_urn, child_urn)
                edges_dict[edge_key] = {
                    'type': 'hasDevice',
                    'start_id': parent_urn,
                    'end_id': child_urn,
                    'properties': {}
                }
        clean_edges = list(edges_dict.values())
   
    return clean_nodes, clean_edges


def clean_graph_v0(graph_data: Dict) -> Dict:
    """
    Generate V0 graph using configuration from config_v0.py
    V0 is a minimal structure with basic schema information.
    """
    config = config_v0.config
    clean_nodes, clean_edges = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=False,
        include_edges=True,
        skip_measurements=True
    )
   
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v1(graph_data: Dict) -> Dict:
    """
    Generate V1 graph using configuration from config_v1_v2.py
    V1 includes detailed properties for each node type.
    """
    config = config_v1_v2.config
    clean_nodes, clean_edges = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=True,
        include_edges=True,
        skip_measurements=True
    )
   
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v2(graph_data: Dict) -> Dict:
    """
    Generate V2 graph using configuration from config_v1_v2.py
    V2 includes nodes with properties + measurements table reference.
    """
    config = config_v1_v2.config
    clean_nodes, _ = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=True,
        include_edges=False,
        skip_measurements=True
    )
   
    # Add measurements table node from configuration
    if config.measurements_table:
        clean_nodes.append(config.measurements_table)
   
    return {
        'nodes': clean_nodes
    }


def process_all_graphs(input_file: str = 'Agri_graph.json',
                      output_v0: str = 'graph_v0.json',
                      output_v1: str = 'graph_v1.json',
                      output_v2: str = 'graph_v2.json') -> None:
    """
    Process input graph and generate all three versions using their respective configurations.
    """
   
    print(f"Reading input file: {input_file}")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            graph_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return
   
    print(f"Input graph has {len(graph_data['nodes'])} nodes and {len(graph_data['edges'])} edges\n")
   
    # Generate V0
    print("Generating V0 (minimal structure)...")
    print("  Using configuration from: config_v0.py")
    try:
        clean_v0 = clean_graph_v0(graph_data)
        with open(output_v0, 'w', encoding='utf-8') as f:
            json.dump(clean_v0, f, indent=2, ensure_ascii=False)
        print(f"  Saved to '{output_v0}'")
        print(f"    Nodes: {len(clean_v0['nodes'])}, Edges: {len(clean_v0['edges'])}")
    except Exception as e:
        print(f"  Error generating V0: {e}")
   
    # Generate V1
    print("\nGenerating V1 (with properties)...")
    print("  Using configuration from: config_v1_v2.py")
    try:
        clean_v1 = clean_graph_v1(graph_data)
        with open(output_v1, 'w', encoding='utf-8') as f:
            json.dump(clean_v1, f, indent=2, ensure_ascii=False)
        print(f"  Saved to '{output_v1}'")
        print(f"    Nodes: {len(clean_v1['nodes'])}, Edges: {len(clean_v1['edges'])}")
    except Exception as e:
        print(f"  Error generating V1: {e}")
   
    # Generate V2
    print("\nGenerating V2 (with measurements table)...")
    print("  Using configuration from: config_v1_v2.py")
    try:
        clean_v2 = clean_graph_v2(graph_data)
        with open(output_v2, 'w', encoding='utf-8') as f:
            json.dump(clean_v2, f, indent=2, ensure_ascii=False)
        print(f"  Saved to '{output_v2}'")
        print(f"    Nodes: {len(clean_v2['nodes'])}")
    except Exception as e:
        print(f"  Error generating V2: {e}")
   
    print("\n" + "="*60)
    print("Graph generation completed!")
    print("="*60)


if __name__ == "__main__":
    # Default file names
    input_file = '../../data/raw/Graph/Agri_graph.json'
    output_v0 = '../../data/raw/Graph/graph_v0.json'
    output_v1 = '../../data/raw/Graph/graph_v1.json'
    output_v2 = '../../data/raw/Graph/graph_v2.json'
   
    # Allow custom file names from command line
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_v0 = sys.argv[2]
    if len(sys.argv) > 3:
        output_v1 = sys.argv[3]
    if len(sys.argv) > 4:
        output_v2 = sys.argv[4]
   
    try:
        process_all_graphs(input_file, output_v0, output_v1, output_v2)
    except ImportError as e:
        print(f"Error importing configuration: {e}")
        print("Make sure config_v0.py, config_v1_v2.py, and config_dataclasses.py are in the same directory.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()