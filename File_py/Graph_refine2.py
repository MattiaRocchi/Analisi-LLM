import json

def clean_graph(input_file='grafo_agricolo.json', output_file='grafo_pulito_v1.json'):
    
    with open(input_file, 'r', encoding='utf-8') as f:
        graph_data = json.load(f)
    
    # Fields to keep for each node type
    fields_to_keep = {
        'common': ['id', 'name', 'type'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location', 'colture', 'irrigationSystemType'],
        'Device': ['location', 'value', 'controlledProperty', 'deviceCategory', 'x', 'y', 'z']
    }
    
    # Map from numeric id to URN
    node_id_map = {}
    devices_with_measurements = set()
    
    for node in graph_data['nodes']:
        if node['label'] != 'Measurement':
            urn_id = node['properties'].get('id')
            if urn_id:
                node_id_map[node['id']] = urn_id
    
    # Check which devices have measurements
    for edge in graph_data['edges']:
        if edge['type'] == 'hasMeasurement':
            device_urn = node_id_map.get(edge['start_id'])
            if device_urn:
                devices_with_measurements.add(device_urn)
    
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
    
    # Clean nodes and add hasDevice to properties
    clean_nodes = []
    
    for node in graph_data['nodes']:
        node_type = node['label']
        
        # Skip Measurement nodes - we'll add a single table node later
        if node_type == 'Measurement':
            continue
        
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
            # Exclude temporal fields, domain, namespace, etc.
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'hasMeasurement']:
                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        # Add hasDevice from relationships
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        # Add hasMeasurements flag for devices that have measurements
        if node_type == 'Device' and urn_id in devices_with_measurements:
            clean_node['properties']['hasMeasurements'] = True
        
        clean_nodes.append(clean_node)
    
    # Add a single node representing the measurements table
    measurements_table_node = {
        'id': 'table:public.measurements',
        'name': 'public.measurements',
        'columns': [
            'id',
            'device_id',
            'timestamp',
            'controlled_property',
            'value',
            'raw_value',
            'location'
        ]
    }

    clean_nodes.append(measurements_table_node)
    
    # Create clean graph
    clean_graph_data = {
        'nodes': clean_nodes,
    }
    
    # Save file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clean_graph_data, f, indent=2, ensure_ascii=False)
    
    print(f"Clean graph saved to '{output_file}'")
    print(f"  Nodes: {len(clean_nodes)}")
    
    return clean_graph_data

if __name__ == "__main__":
    import sys
    
    # You can pass file names as arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else '../Graph/Agri_graph.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'graphV2p.json'
    
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