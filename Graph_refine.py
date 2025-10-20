import json

def clean_graph(input_file='grafo_agricolo.json', output_file='grafo_pulito_v0.json'):
    """
    Versione 0: Schema pulito del grafo
    - Solo id URN
    - Rimozione dati temporali e metadati
    - Solo relazioni hasDevice (belongsTo convertiti in hasDevice nelle properties)
    """
    
    with open(input_file, 'r', encoding='utf-8') as f:
        graph_data = json.load(f)
    
    # Campi da mantenere per ogni tipo di nodo
    fields_to_keep = {
        'common': ['id', 'name'],
        'AgriFarm': ['location'],
        'AgriParcel': ['location'],
        'Device': ['location']
    }
    
    # Mappa da id numerico a URN
    node_id_map = {}
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if urn_id:
            node_id_map[node['id']] = urn_id
    
    # Costruisci una mappa di hasDevice per ogni nodo
    has_device_map = {}  # {parent_urn: [child_urn1, child_urn2, ...]}
    
    for edge in graph_data['edges']:
        start_urn = node_id_map.get(edge['start_id'])
        end_urn = node_id_map.get(edge['end_id'])
        
        if not start_urn or not end_urn:
            continue
        
        # Se è belongsTo (A->B), aggiungi hasDevice (B->A)
        if edge['type'] == 'belongsTo':
            if end_urn not in has_device_map:
                has_device_map[end_urn] = []
            if start_urn not in has_device_map[end_urn]:
                has_device_map[end_urn].append(start_urn)
        
        # Se è già hasDevice, mantienilo
        elif edge['type'] == 'hasDevice':
            if start_urn not in has_device_map:
                has_device_map[start_urn] = []
            if end_urn not in has_device_map[start_urn]:
                has_device_map[start_urn].append(end_urn)
    
    # Pulisci i nodi e aggiungi hasDevice nelle properties
    clean_nodes = []
    
    for node in graph_data['nodes']:
        urn_id = node['properties'].get('id')
        if not urn_id:
            continue
        
        node_type = node['label']
        
        # Costruisci il nodo pulito
        clean_node = {
            'id': urn_id,
            'label': node_type,
            'properties': {}
        }
        
        # Aggiungi solo i campi rilevanti
        common_fields = fields_to_keep['common']
        type_fields = fields_to_keep.get(node_type, [])
        allowed_fields = set(common_fields + type_fields)
        
        for key, value in node['properties'].items():
            # Escludi campi temporali, domain, namespace, belongsTo, hasDevice originali
            if key in ['dateCreated', 'dateModified', 'dateObserved', 'timestamp_kafka', 
                      'unixtimestampCreated', 'unixtimestampModified', 'timestamp_subscription',
                      'domain', 'namespace', 'belongsTo', 'hasDevice', 'hasAgriParcel', 'description',
                      'irrigationSystemType', 'type', 'value',  'x', 'y', 'z','controlledProperty',
                      'deviceCategory', 'colture']:

                continue
            
            if key in allowed_fields or key in common_fields:
                clean_node['properties'][key] = value
        
        # Aggiungi hasDevice dalle relazioni
        if urn_id in has_device_map:
            clean_node['properties']['hasDevice'] = has_device_map[urn_id]
        
        clean_nodes.append(clean_node)
    
    # Crea gli edge solo hasDevice (no duplicati)
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
    
    # Crea il grafo pulito
    clean_graph_data = {
        'nodes': clean_nodes,
        'edges': clean_edges
    }
    
    # Salva il file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(clean_graph_data, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Grafo pulito salvato in '{output_file}'")
    print(f"  Nodi: {len(clean_nodes)}")
    print(f"  Edge: {len(clean_edges)} (tutti 'hasDevice')")
    
    # Statistiche
    node_types = {}
    nodes_with_devices = 0
    for node in clean_nodes:
        label = node['label']
        node_types[label] = node_types.get(label, 0) + 1
        if 'hasDevice' in node['properties']:
            nodes_with_devices += 1
    
    print(f"\n  Tipi di nodi:")
    for node_type, count in node_types.items():
        print(f"    - {node_type}: {count}")
    print(f"\n  Nodi con hasDevice: {nodes_with_devices}")
    
    return clean_graph_data

if __name__ == "__main__":
    import sys
    
    # Puoi passare i nomi dei file come argomenti
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'grafo_agricolo.json'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'graph0.json'
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}\n")
    
    try:
        clean_graph(input_file, output_file)
    except FileNotFoundError:
        print(f"❌ Errore: File '{input_file}' non trovato!")
        print("Assicurati che il file sia nella stessa cartella dello script.")
    except Exception as e:
        print(f"❌ Errore: {e}")