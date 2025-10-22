import psycopg2
import json
import sys
from datetime import datetime

def extract_graph_to_json_optimized():
    try:
        print(f"Starting extraction: {datetime.now()}")
        
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            host="137.204.70.156",
            port=45432,
            database="test_postgres_graphs",
            user="postgres",
            password="psw"
        )
        
        conn.autocommit = True
        cur = conn.cursor()
        
        # **CORRECT QUERY - removed internal comments**
        print("Executing Cypher query...")
        query = """
        CREATE EXTENSION IF NOT EXISTS age;
        LOAD 'age';
        SET search_path = ag_catalog, "$user", public;
        SELECT 
        m::json AS m_json, 
        r::json AS r_json, 
        n::json AS n_json
        FROM cypher('agri_graph', $$
        MATCH (m:AgriFarm {name: 'ZESPRI AZ. AGR. DALLE FABBRICHE ANDREA'}) - [r*1..3] - (n)
        RETURN m, r, n 
        $$) AS (m agtype, r agtype, n agtype);
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        print(f"Query completed. Found {len(results)} rows")
        
        # Structure for JSON
        graph_data = {
            "nodes": [],
            "edges": []
        }
        
        node_ids = set()
        edge_ids = set()
        
        # Process results
        print("Processing nodes and relationships...")
        for i, row in enumerate(results):
            if i % 1000 == 0:  # Show progress every 1000 rows
                print(f"  Processed {i}/{len(results)} rows...")
                
            m_raw, r_raw, n_raw = row
    
            # Safe parsing
            m_dict = m_raw if isinstance(m_raw, dict) else json.loads(m_raw)
            n_dict = n_raw if isinstance(n_raw, dict) else json.loads(n_raw)

            # Add node m
            if m_dict['id'] not in node_ids:
                graph_data["nodes"].append({
                    "id": m_dict['id'],
                    "label": m_dict['label'],
                    "properties": m_dict['properties']
                })
                node_ids.add(m_dict['id'])

            # Add node n
            if n_dict['id'] not in node_ids:
                graph_data["nodes"].append({
                    "id": n_dict['id'],
                    "label": n_dict['label'],
                    "properties": n_dict['properties']
                })
                node_ids.add(n_dict['id'])

            # Parse relationships
            try:
                if isinstance(r_raw, str):
                    rels = json.loads(r_raw)
                else:
                    rels = r_raw
                if not isinstance(rels, list):
                    rels = [rels]
            except json.JSONDecodeError as err:
                print(f"Error parsing relationships: {err}")
                continue

            for rel in rels:
                rel_dict = rel if isinstance(rel, dict) else json.loads(rel)
                # Avoid duplicates with edge_ids
                if rel_dict['id'] not in edge_ids:
                    graph_data["edges"].append({
                        "id": rel_dict['id'],
                        "type": rel_dict['label'],
                        "start_id": rel_dict['start_id'],
                        "end_id": rel_dict['end_id'],
                        "properties": rel_dict['properties']
                    })
                    edge_ids.add(rel_dict['id'])
        
        print(f"Basic processing completed: {len(graph_data['nodes'])} nodes, {len(graph_data['edges'])} edges")
        
        # Retrieve measurements efficiently
        device_ids = []
        device_map = {}
        
        for node in graph_data["nodes"]:
            if node["label"] == "Device":
                device_id = node["properties"].get("id")
                if device_id:
                    device_ids.append(device_id)
                    device_map[device_id] = node["id"]
        
        print(f"Found {len(device_ids)} devices, retrieving measurements...")
        
        if device_ids:
            # Split into batches and limit results
            batch_size = 100
            all_measurements = []
            
            for i in range(0, len(device_ids), batch_size):
                batch = device_ids[i:i + batch_size]
                measurements_query = """
                SELECT device_id, timestamp, controlled_property, location, value, raw_value
                FROM public.measurements
                WHERE device_id = ANY(%s)
                ORDER BY device_id, timestamp
                LIMIT 5000
                """
                
                cur.execute(measurements_query, (batch,))
                batch_results = cur.fetchall()
                all_measurements.extend(batch_results)
                print(f"  Batch {i//batch_size + 1}: {len(batch_results)} measurements")
                
                # Exit if we're getting too large
                if len(all_measurements) > 20000:
                    print("  Reached maximum limit of 20k measurements")
                    break
            
            print(f"Total measurements to process: {len(all_measurements)}")
            
            # Calculate IDs safely
            max_node_id = max([node["id"] for node in graph_data["nodes"]]) if graph_data["nodes"] else 0
            max_edge_id = max([edge["id"] for edge in graph_data["edges"]]) if graph_data["edges"] else 0
            
            measurement_id_counter = max_node_id + 1
            edge_id_counter = max_edge_id + 1
            
            # Process measurements with progress
            print("Creating measurement nodes...")
            processed_count = 0
            
            for meas in all_measurements:
                device_id, timestamp, controlled_property, location, value, raw_value = meas
                
                if device_id in device_map:
                    # Create Measurement node
                    measurement_node = {
                        "id": measurement_id_counter,
                        "label": "Measurement",
                        "properties": {
                            "id": f"urn:ngsi-ld:Measurement:{device_id}:{timestamp.isoformat()}",
                            "device_id": device_id,
                            "timestamp": timestamp.isoformat(),
                            "controlled_property": controlled_property,
                            "location": location,
                            "value": value,
                            "raw_value": raw_value
                        }
                    }
                    
                    graph_data["nodes"].append(measurement_node)
                    
                    # Create edge Device -> Measurement
                    measurement_edge = {
                        "id": edge_id_counter,
                        "type": "hasMeasurement",
                        "start_id": device_map[device_id],
                        "end_id": measurement_id_counter,
                        "properties": {}
                    }
                    
                    graph_data["edges"].append(measurement_edge)
                    
                    edge_id_counter += 1
                    measurement_id_counter += 1
                    processed_count += 1
                    
                    if processed_count % 1000 == 0:
                        print(f"  Created {processed_count} measurements...")
        
        # Save to JSON file
        print("Saving JSON file...")
        with open('./../Graph/Agri_graph.json', 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nExport completed at {datetime.now()}")
        print(f"  Total nodes: {len(graph_data['nodes'])}")
        print(f"  Total relationships: {len(graph_data['edges'])}")
        
        # Statistics
        node_counts = {}
        for node in graph_data["nodes"]:
            label = node["label"]
            node_counts[label] = node_counts.get(label, 0) + 1
        
        print(f"\n  Nodes by type:")
        for label, count in node_counts.items():
            print(f"    - {label}: {count}")
        
        edge_counts = {}
        for edge in graph_data["edges"]:
            edge_type = edge["type"]
            edge_counts[edge_type] = edge_counts.get(edge_type, 0) + 1
        
        print(f"\n  Relationships by type:")
        for edge_type, count in edge_counts.items():
            print(f"    - {edge_type}: {count}")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    extract_graph_to_json_optimized()