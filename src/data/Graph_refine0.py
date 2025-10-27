import json

def clean_graph(input_file='grafo_agricolo.json', output_file='grafo_pulito_v0.json'):
    """
    Version 0: Clean graph schema
    - Only URN ids
    - Remove temporal data and metadata
    - Only hasDevice relationships (belongsTo converted to hasDevice in properties)
    """
    
    with open(input_file, 'r', encoding='utf-8') as f:
        graph_data = json.load(f)
    
    # Fields to keep for each node type
    fields_to_keep = {
        'common': ['id', 'name', 'type'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location'],
        'Device': ['location']
    }
    
    # Map from numeric id to URN
    node_id_map = {}
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
    
    # Build a hasDevice map for each node
    has_device_map = {}  # {parent_urn: [child_urn1, child_urn2, ...]}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        # If it's belongsTo (A->B), add hasDevice (B->A)
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        # If it's already hasDevice, keep it
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    # Clean nodes and add hasDevice to properties
    clean_nodes = []
    
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
        
        node_type = node['label']
        
        # Build clean node
        clean_node = {
            'id': urn_id,
            'label': node_type,
            'properties': {}
        }
        
        # Add only relevant fields
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            # Exclude temporal fields, domain, namespace, belongsTo, original hasDevice
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'description',
                      'irrigationSystemType', 'value',  'x', 'y', 'z','controlledProperty',
                      'deviceCategory', 'colture']:

                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        # Add hasDevice from relationships
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
    # Create only hasDevice edges (no duplicates)
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
    
    # Create clean graph
    clean_graph_data = {
        'nodes': clean_nodes,
        'edges': clean_edges
    }
    
    # Save file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clean_graph_data, f, indent=2, ensure_ascii=False)
    
    print(f"Clean graph saved to '{output_file}'")
    print(f"  Nodes: {len(clean_nodes)}")
    print(f"  Edges: {len(clean_edges)} (all 'hasDevice')")
    
    # Statistics
    node_types = {}
    nodes_with_devices = 0
    for node in clean_nodes:
        label = node['label']
        node_types[label] = node_types.get(label, 0) + 1
        if 'hasDevice' in node['properties']:
            nodes_with_devices += 1
    
    print(f"\n  Node types:")
    for node_type, count in node_types.items():
        print(f"    - {node_type}: {count}")
    print(f"\n  Nodes with hasDevice: {nodes_with_devices}")
    
    return clean_graph_data

if __name__ == "__main__":
    import sys
    
    # You can pass file names as arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'grafo_agricolo.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'graph0.json'
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}\n")
    
    try:
        clean_graph(input_file, output_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        print("Make sure the file is in the same folder as the script.")
    except Exception as e:
        print(f"Error: {e}")