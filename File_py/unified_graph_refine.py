import json

def clean_graph_v0(graph_data):
    fields_to_keep = {
        'common': ['id', 'name'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location'],
        'Device': ['location']
    }
    
    node_id_map = {}
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
    
    has_device_map = {}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    clean_nodes = []
    
    for node in graph_data['nodes']:
        node_type = node['label']
        
        # Skip Measurement nodes in V0
        if node_type == 'Measurement':
            continue
        
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
        
        clean_node = {
            'id': urn_id,
            'label': node_type,
            'properties': {}
        }
        
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'description',
                      'irrigationSystemType', 'type', 'value', 'x', 'y', 'z', 'controlledProperty',
                      'deviceCategory', 'colture']:
                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
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
    
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v1(graph_data):

    fields_to_keep = {
        'common': ['id', 'name', 'type'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location', 'colture', 'irrigationSystemType'],
        'Device': ['location', 'value', 'controlledProperty', 'deviceCategory', 'x', 'y', 'z']
    }
    
    node_id_map = {}
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
    
    has_device_map = {}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    clean_nodes = []
    
    for node in graph_data['nodes']:
        node_type = node['label']
        
        # Skip Measurement nodes in V1
        if node_type == 'Measurement':
            continue
        
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
        
        clean_node = {
            'id': urn_id,
            'label': node_type,
            'properties': {}
        }
        
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'hasMeasurement']:
                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
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
    
    return {
        'nodes': clean_nodes,
        'edges': clean_edges
    }


def clean_graph_v2(graph_data):
    fields_to_keep = {
        'common': ['id', 'name', 'type'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location', 'colture', 'irrigationSystemType'],
        'Device': ['location', 'value', 'controlledProperty', 'deviceCategory', 'x', 'y', 'z']
    }
    
    node_id_map = {}
    
    for node in graph_data['nodes']:
        if node['label'] != 'Measurement':
            urn_id = node['properties'].get('id')
            if urn_id:
                node_id_map[node['id']] = urn_id
    
    has_device_map = {}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    clean_nodes = []
    
    for node in graph_data['nodes']:
        node_type = node['label']
        
        if node_type == 'Measurement':
            continue
        
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
        
        clean_node = {
            'label': node_type,
            'properties': {}
        }
        
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'hasMeasurement']:
                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
    # Add measurements table node
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
    
    return {
        'nodes': clean_nodes
    }


def process_all_graphs(input_file='Agri_graph.json', 
                       output_v0='graph_v0.json',
                       output_v1='graph_v1.json', 
                       output_v2='graph_v2.json'):
    """
    Process input graph and generate all three versions
    """
    
    print(f"Reading input file: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        graph_data = json.load(f)
    
    print(f"Input graph has {len(graph_data['nodes'])} nodes and {len(graph_data['edges'])} edges\n")
    
    # Generate V0
    print("Generating V0 (minimal structure)...")
    clean_v0 = clean_graph_v0(graph_data)
    with open(output_v0, 'w', encoding='utf-8') as f:
        json.dump(clean_v0, f, indent=2, ensure_ascii=False)
    print(f" Saved to '{output_v0}'")
    print(f"    Nodes: {len(clean_v0['nodes'])}, Edges: {len(clean_v0['edges'])}")
    
    # Generate V1
    print("\nGenerating V1 (with properties)...")
    clean_v1 = clean_graph_v1(graph_data)
    with open(output_v1, 'w', encoding='utf-8') as f:
        json.dump(clean_v1, f, indent=2, ensure_ascii=False)
    print(f" Saved to '{output_v1}'")
    print(f"    Nodes: {len(clean_v1['nodes'])}, Edges: {len(clean_v1['edges'])}")
    
    # Generate V2
    print("\nGenerating V2 (with measurements table)...")
    clean_v2 = clean_graph_v2(graph_data)
    with open(output_v2, 'w', encoding='utf-8') as f:
        json.dump(clean_v2, f, indent=2, ensure_ascii=False)
    print(f" Saved to '{output_v2}'")
    print(f"    Nodes: {len(clean_v2['nodes'])}")
    
    print("\n✓ All graphs generated successfully!")
    

if __name__ == "__main__":
    import sys
    
    # Default file names
    input_file = '../Graph/Agri_graph.json'
    output_v0 = 'graph_v0.json'
    output_v1 = 'graph_v1.json'
    output_v2 = 'graph_v2.json'
    
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
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        print("Make sure the file path is correct.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()