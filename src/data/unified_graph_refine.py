import json
import sys
from typing import Dict, List, Tuple, Any, Set
from config_dataclasses import GraphConfig

# Import configurations
import config_v0
import config_v1_v2


def _build_node_mappings(graph_data: Dict) -> Tuple[Dict[str, str], Dict[str, str]]:

    node_id_map = {}
    node_type_map = {}
    
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
            node_type_map[urn_id] = node['label']
    
    return node_id_map, node_type_map


def _build_relationship_map(graph_data: Dict, node_id_map: Dict[str, str], 
                           node_type_map: Dict[str, str], skip_measurements: bool = True) -> Dict[str, List[Tuple[str, str]]]:

    relationship_map = {}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        start_type = node_type_map.get(start_urn)
        end_type = node_type_map.get(end_urn)
        
        # belongsTo: child (start) -> parent (end)
        if edge['type'] == 'belongsTo':
            source_urn = start_urn
            target_urn = end_urn
            edge_type = 'belongsTo'
            
        # hasDevice: parent (start) -> child (end)
        elif edge['type'] == 'hasDevice':
            source_urn = start_urn
            target_urn = end_urn
            # Only keep hasDevice for Device->Device relationships
            if start_type == 'Device' and end_type == 'Device':
                edge_type = 'hasDevice'
            else:
                # Skip or convert non-Device->Device hasDevice edges
                continue
        else:
            continue
        
        if source_urn not in relationship_map:
            relationship_map[source_urn] = []
        
        # Avoid duplicates
        if (target_urn, edge_type) not in relationship_map[source_urn]:
            relationship_map[source_urn].append((target_urn, edge_type))
    
    return relationship_map


def _clean_graph_base(graph_data: Dict, config: GraphConfig,
                     include_label: bool = False,
                     include_edges: bool = True) -> Tuple[List[Dict], List[Dict]]:
    
    # Build mappings
    node_id_map, node_type_map = _build_node_mappings(graph_data)
    relationship_map = _build_relationship_map(graph_data, node_id_map, node_type_map)
    
    clean_nodes = []
    
    # Process nodes
    for node in graph_data['nodes']:
        node_type = node['label']
        
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
        
        # Add outgoing relationships (edges starting from this node)
        if urn_id in relationship_map:
            # Separate by edge type
            belongs_to = [target for target, etype in relationship_map[urn_id] if etype == 'belongsTo']
            has_device = [target for target, etype in relationship_map[urn_id] if etype == 'hasDevice']
            
            if belongs_to:
                clean_node['properties']['belongsTo'] = belongs_to
            if has_device:
                clean_node['properties']['hasDevice'] = has_device
        
        clean_nodes.append(clean_node)
    
    # Build edges if requested
    clean_edges = []
    if include_edges:
        edges_set = set()  # Use set to avoid duplicates
        
        for source_urn, targets_list in relationship_map.items():
            for target_urn, edge_type in targets_list:
                edge_key = (source_urn, target_urn, edge_type)
                if edge_key not in edges_set:
                    edges_set.add(edge_key)
                    clean_edges.append({
                        'type': edge_type,
                        'start_id': source_urn,
                        'end_id': target_urn,
                        'properties': {}
                    })
    
    return clean_nodes, clean_edges


def clean_graph_v0(graph_data: Dict) -> Dict:
    #Generate V0 graph with minimal structure
    config = config_v0.config
    clean_nodes, clean_edges = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=True,
        include_edges=True,
    )
    
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v1(graph_data: Dict) -> Dict:
    #Generate V1 graph with full properties
    config = config_v1_v2.config
    clean_nodes, clean_edges = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=True,
        include_edges=True,
    )
    
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v2(graph_data: Dict) -> Dict:
    #Generate V2 graph with measurements table
    config = config_v1_v2.config
    clean_nodes, clean_edges = _clean_graph_base(
        graph_data=graph_data,
        config=config,
        include_label=True,
        include_edges=True,
    )
    
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def process_all_graphs(input_file: str = 'Agri_graph.json',
                      output_v0: str = 'graph_v0.json',
                      output_v1: str = 'graph_v1.json',
                      output_v2: str = 'graph_v2.json') -> None:

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
    print("Using configuration from: config_v0.py")
    try:
        clean_v0 = clean_graph_v0(graph_data)
        with open(output_v0, 'w', encoding='utf-8') as f:
            json.dump(clean_v0, f, indent=2, ensure_ascii=False)
        print(f"Saved to '{output_v0}'")
        print(f"Nodes: {len(clean_v0['nodes'])}, Edges: {len(clean_v0['edges'])}")
    except Exception as e:
        print(f"Error generating V0: {e}")
    
    # Generate V1
    print("\nGenerating V1 (with properties)...")
    print("Using configuration from: config_v1_v2.py")
    try:
        clean_v1 = clean_graph_v1(graph_data)
        with open(output_v1, 'w', encoding='utf-8') as f:
            json.dump(clean_v1, f, indent=2, ensure_ascii=False)
        print(f"Saved to '{output_v1}'")
        print(f"Nodes: {len(clean_v1['nodes'])}, Edges: {len(clean_v1['edges'])}")
    except Exception as e:
        print(f"Error generating V1: {e}")
    
    # Generate V2
    print("\nGenerating V2 (with measurements table)...")
    print("Using configuration from: config_v1_v2.py")
    try:
        clean_v2 = clean_graph_v2(graph_data)
        with open(output_v2, 'w', encoding='utf-8') as f:
            json.dump(clean_v2, f, indent=2, ensure_ascii=False)
        print(f"Saved to '{output_v2}'")
        print(f"Nodes: {len(clean_v2['nodes'])}, Edges: {len(clean_v2['edges'])}")
    except Exception as e:
        print(f"Error generating V2: {e}")
    
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