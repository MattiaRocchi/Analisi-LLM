import json

def clean_graph(input_file='grafo_agricolo.json', output_file='grafo_pulito_v0.json'):
    
    with open(input_file, 'r', encoding='utf-8') as f:
        graph_data = json.load(f)
    
    # Fields to keep for each node type
    fields_to_keep = {
        'common': ['id', 'name', 'type'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location', 'colture', 'irrigationSystemType'],
        'Device': ['location', 'value', 'controlledProperty', 'deviceCategory', 'x', 'y', 'z'],
        'Measurement': ['device_id', 'timestamp', 'controlled_property', 'location', 'value', 'raw_value']
    }
    
    # Map from numeric id to URN
    node_id_map = {}
    for node in graph_data['nodes']:
        if node['label'] == 'Measurement':
            # For Measurements use numeric id as key
            node_id_map[node['id']] = node['properties'].get('id')
        else:
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
        
        # Handle belongsTo (A->B becomes hasDevice B->A)
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        # Handle existing hasDevice
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    # Clean nodes and add hasDevice/hasMeasurement to properties
    clean_nodes = []
    
    for node in graph_data['nodes']:
        node_type = node['label']
        
        # For Measurement, use id from properties
        if node_type == 'Measurement':
            urn_id = node['properties'].get('id')
        else:
            urn_id = node['properties'].get('id')
        
        if not urn_id:
            continue
        
        # Build clean node
        clean_node = {
            'label': node_type,
            'properties': {}
        }
        
        # Add only relevant fields
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            # For Measurement, keep all specified fields
            if node_type == 'Measurement':
                if key in allowed_fields:
                    clean_node['properties'][key] = value
            else:
                # For other nodes, exclude temporal fields, domain, namespace, etc.
                if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                          'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                          'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel']:
                    continue
                
                if key in allowed_fields or key in common_fields:
                    clean_node['properties'][key] = value
        
        # Add hasDevice from relationships
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
    
    # Create clean graph
    clean_graph_data = {
        'nodes': clean_nodes,
    }
    
    # Save file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clean_graph_data, f, indent=2, ensure_ascii=False)
    
    print(f"Clean graph saved to '{output_file}'")
    print(f"  Nodes: {len(clean_nodes)}")
    
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
        print(f"  - {node_type}: {count}")
    
    print(f"\n  Nodes with hasDevice: {nodes_with_devices}")
    
    
    return clean_graph_data

if __name__ == "__main__":
    import sys
    
    # You can pass file names as arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'Agri_graph.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'graphV2.json'
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}\n")
    
    try:
        clean_graph(input_file, output_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        print("Make sure the file is in the same folder as the script.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()