import json
import yaml

class SchemaExtractor:
    @staticmethod
    def extract_schema(json_path: str) -> str:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        schema_summary = {
            "graph_model": {"nodes": {}, "relationships": []},
            "relational_model": {
                "measurements_table": {
                    "table_name": "public.measurements",
                    "columns": [
                        "timestamp (TIMESTAMPTZ, Hypertable time dimension)",
                        "device_id (TEXT, Foreign Key logica verso l'id URN di Device)",
                        "value (DOUBLE PRECISION)"
                    ]
                }
            }
        }

        # Dizionario di supporto per mappare l'ID del nodo alla sua Label
        id_to_label = {}

        # Estrazione nodi e proprietà
        for node in data.get('nodes', []):
            label = node.get('label') or node.get('type')
            node_id = node.get('properties', {}).get('id')
            
            if not label or label == 'Measurement':
                continue
                
            if node_id:
                id_to_label[node_id] = label

            props_keys = [k for k in node.get('properties', {}).keys() if k != 'id']
            if label not in schema_summary["graph_model"]["nodes"]:
                schema_summary["graph_model"]["nodes"][label] = set()
            schema_summary["graph_model"]["nodes"][label].update(props_keys)

        for label in schema_summary["graph_model"]["nodes"]:
            schema_summary["graph_model"]["nodes"][label] = sorted(list(schema_summary["graph_model"]["nodes"][label]))

        # Estrazione tipi di relazione con DIREZIONALITA'
        edge_signatures = set()
        for edge in data.get('edges', []):
            rel_type = edge.get('type') or edge.get('label')
            if rel_type and rel_type != 'hasMeasurement':
                start_label = id_to_label.get(edge.get('start_id'), "UnknownNode")
                end_label = id_to_label.get(edge.get('end_id'), "UnknownNode")
                
                # Crea la firma: (NodoPartenza)-[:TIPO_RELAZIONE]->(NodoArrivo)
                signature = f"({start_label})-[:{rel_type}]->({end_label})"
                edge_signatures.add(signature)
                
        schema_summary["graph_model"]["relationships"] = sorted(list(edge_signatures))
        return yaml.dump(schema_summary, sort_keys=False, allow_unicode=True)

    @staticmethod
    def get_full_prompt_context(json_path: str) -> str:
        schema_yaml = SchemaExtractor.extract_schema(json_path)
        header = (
            "Architettura target: sistema multistore PostgreSQL unificato.\n"
            "Motore Grafo: Apache AGE. Motore Time-Series: TimescaleDB.\n"
            "Schema strutturale estratto dinamicamente:\n\n"
        )
        return header + schema_yaml