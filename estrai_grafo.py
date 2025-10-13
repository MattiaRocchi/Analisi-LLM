import psycopg2
import json
import sys

def extract_graph_to_json():
    try:
        # Connessione al database PostgreSQL
        conn = psycopg2.connect(
            host="137.204.70.156",  # modifica se necessario
            port=45432,
            database="test_postgres_graphs",  # nome del database
            user="postgres",     # il tuo username
            password="psw"  # la tua password
        )
        
        conn.autocommit = True
        cur = conn.cursor()
        
        # Esegui la query per estrarre il grafo
        query = """
        CREATE EXTENSION IF NOT EXISTS age;
        LOAD 'age';
        SET search_path = ag_catalog, "$user", public;
        SELECT 
        m::json AS m_json, 
        r::json AS r_json, 
        n::json AS n_json
        FROM cypher('agri_graph', $$
        MATCH (m:AgriFarm {name: 'ZESPRI AZ. AGR. DALLE FABBRICHE ANDREA'}) - [r*] - (n) 
        RETURN m, r, n 
        $$) AS (m agtype, r agtype, n agtype);
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Struttura per il JSON
        graph_data = {
            "nodes": [],
            "edges": []
        }
        
        node_ids = set()  # Per evitare duplicati
        
        for row in results:
            m_raw, r_raw, n_raw = row
    
            try:
                m_dict = m_raw
                n_dict = n_raw
                rels = r_raw
            except json.JSONDecodeError as err:
                print(f"Errore nel parsing di nodo m o n: {err}")

            # Aggiungi nodo m
            if m_dict['id'] not in node_ids:
                graph_data["nodes"].append({
                    "id": m_dict['id'],
                    "label": m_dict['label'],
                    "properties": m_dict['properties']
                })
                node_ids.add(m_dict['id'])

            # Aggiungi nodo n
            if n_dict['id'] not in node_ids:
                graph_data["nodes"].append({
                    "id": n_dict['id'],
                    "label": n_dict['label'],
                    "properties": n_dict['properties']
                })
                node_ids.add(n_dict['id'])

            # Parse delle relazioni
            try:
                if isinstance(r_raw, str):
                    rels = json.loads(r_raw)
                else:
                    rels = r_raw
                if not isinstance(rels, list):
                    rels = [rels]
            except json.JSONDecodeError as err:
                print(f"Errore nel parsing delle relazioni: {err}")

            for rel in rels:
                graph_data["edges"].append({
                    "id": rel['id'],
                    "type": rel['label'],
                    "start_id": rel['start_id'],
                    "end_id": rel['end_id'],
                    "properties": rel['properties']
                })
        
        # Salva in file JSON
        with open('grafo_agricolo.json', 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, ensure_ascii=False)
        
        print("Grafo esportato con successo in 'grafo_agricolo.json'")
        print(f"Nodi trovati: {len(graph_data['nodes'])}")
        print(f"Relazioni trovate: {len(graph_data['edges'])}")
        
        # Chiudi la connessione
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Errore: {e}")
        sys.exit(1)

if __name__ == "__main__":
    extract_graph_to_json()