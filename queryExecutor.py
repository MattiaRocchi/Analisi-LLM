#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yaml
import json
import psycopg2
import psycopg2.extensions
from psycopg2.extensions import cursor as PgCursor, connection as PgConnection
from typing import Dict, List, Any, Optional
import argparse
from datetime import datetime


class QueryExecutor:
    """
    Esegue query Apache AGE su PostgreSQL e salva il risultato in JSON
    Gestisce file YAML con struttura query_set
    """

    def __init__(self, db_config: Dict[str, str]):
        """
        Inizializza la connessione al database
        
        Args:
            db_config: Dizionario con host, port, database, user, password
        """
        self.db_config = db_config
        self.connection: Optional[PgConnection] = None
        self.cursor: Optional[PgCursor] = None
        
    def connect(self):
        """Stabilisce la connessione al database PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config.get('port', 5432),
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()
            
            # Carica l'estensione AGE
            self.cursor.execute("LOAD 'age';")
            self.cursor.execute("SET search_path = ag_catalog, '$user', public;")
            
            print("Connessione al database stabilita")
        except Exception as e:
            print(f"Errore nella connessione al database: {e}")
            raise
    
    def disconnect(self):
        """Chiude la connessione al database"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connessione chiusa")
    
    def execute_query(self, query: str, query_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Esegue una query (Cypher o SQL ibrido) su Apache AGE
        
        Args:
            query: Query da eseguire
            query_id: ID della query per il tracking
            
        Returns:
            Dizionario con risultati e metadati
        """
        try:
            query_clean = query.strip()
            
            if query_id:
                print(f"\n Esecuzione query {query_id}...")
            else:
                print(f"\n Esecuzione query...")
            
            print(f"Query:\n{query_clean[:200]}{'...' if len(query_clean) > 200 else ''}\n")
            
            # Verifica che il cursor sia disponibile
            if self.cursor is None:
                raise RuntimeError("Cursor non disponibile. Chiamare connect() prima di eseguire query.")
            
            # Esegui la query
            start_time = datetime.now()
            
            self.cursor.execute(query_clean)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Recupera i risultati
            try:
                if self.cursor.description is None:
                    # Nessun risultato da fetchare
                    results = []
                    column_names: List[str] = []
                else:
                    results = self.cursor.fetchall()
                    column_names = [desc[0] for desc in self.cursor.description]
            except (psycopg2.ProgrammingError, AttributeError) as e:
                # La query non restituisce risultati o c'è un errore nel fetch
                print(f" Nessun risultato da fetchare: {e}")
                results = []
                column_names = []
            
            # Converti i risultati in formato JSON-serializzabile
            parsed_results = []
            for row in results:
                row_dict = {}
                for i, value in enumerate(row):
                    col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                    row_dict[col_name] = self._parse_agtype(value)
                parsed_results.append(row_dict)
            
            result_data = {
                "query_id": query_id,
                "status": "success",
                "execution_time_seconds": execution_time,
                "row_count": len(parsed_results),
                "columns": column_names,
                "results": parsed_results
            }
            
            print(f" Query eseguita con successo")
            print(f" Righe restituite: {len(parsed_results)}")
            print(f" Tempo di esecuzione: {execution_time:.3f}s")
            
            return result_data
            
        except Exception as e:
            error_msg = str(e)
            print(f"Errore nell'esecuzione della query: {error_msg}")
            
            return {
                "query_id": query_id,
                "status": "error",
                "error": error_msg,
                "results": []
            }
            
    def _parse_agtype(self, agtype_value) -> Any:
        """
        Converte il tipo agtype di Apache AGE in oggetti Python
        """
        if agtype_value is None:
            return None
        
        # AGE restituisce stringhe JSON per i tipi agtype
        if isinstance(agtype_value, str):
            # Prova a parsare come JSON
            try:
                return json.loads(agtype_value)
            except (json.JSONDecodeError, TypeError):
                return agtype_value
        
        # Per altri tipi (int, float, bool), restituisci direttamente
        return agtype_value
    
    def load_yaml_file(self, yaml_path: str) -> Dict:
        """
        Carica il file YAML con le risposte dell'LLM
        
        Args:
            yaml_path: Percorso del file YAML
            
        Returns:
            Dizionario con il contenuto del YAML
        """
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            print(f"File YAML caricato: {yaml_path}")
            return data
        except Exception as e:
            print(f"Errore nel caricamento del file YAML: {e}")
            raise
    
    def save_results_to_json(self, results: List[Dict], output_path: str, metadata: Optional[Dict] = None):
        """
        Salva i risultati in un file JSON
        
        Args:
            results: Lista dei risultati da salvare
            output_path: Percorso del file JSON di output
            metadata: Metadati aggiuntivi da includere
        """
        try:
            output_data = {
                "metadata": metadata if metadata else {},
                "queries": results
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"\n Risultati salvati in: {output_path}")
            print(f"Query totali: {len(results)}")
            successful = sum(1 for r in results if r.get('status') == 'success')
            print(f"Query riuscite: {successful}/{len(results)}")
        except Exception as e:
            print(f"Errore nel salvataggio del file JSON: {e}")
            raise
    
    def process_yaml_and_execute(self, yaml_path: str, output_path: str, 
                                query_id_filter: Optional[str] = None):
        """
        Processo completo: carica YAML, esegue query, salva JSON
        """
        # Carica il YAML
        yaml_data = self.load_yaml_file(yaml_path)
        
        # ============================================================
        # FIX MIGLIORATO: Gestisci diverse strutture YAML
        # ============================================================
        
        query_set = []
        
        # Caso 1: Il YAML è già una lista
        if isinstance(yaml_data, list):
            print("Struttura YAML rilevata: Lista diretta")
            
            # CONTROLLO SPECIALE per struttura ChatGPT annidata
            first_item = yaml_data[0] if yaml_data else {}
            if (isinstance(first_item, dict) and 
                len(first_item) == 1 and 
                list(first_item.keys())[0].startswith('Q')):
                
                print("Rilevata struttura ChatGPT annidata - conversione in corso")
                # Converti struttura annidata in struttura piatta
                for item in yaml_data:
                    for key, value in item.items():
                        if isinstance(value, dict):
                            # Crea un nuovo item con id e tutti i campi
                            new_item = value.copy()
                            new_item['id'] = key
                            query_set.append(new_item)
            else:
                # Struttura lista normale
                query_set = yaml_data
        
        # Caso 2: Il YAML è un dizionario con varie chiavi possibili
        elif isinstance(yaml_data, dict):
            # Prova varie chiavi comuni
            for key in ['query_set', 'queries', 'query_list', 'results', 'responses']:
                if key in yaml_data:
                    query_set = yaml_data[key]
                    print(f"Struttura YAML rilevata: Dizionario con chiave '{key}'")
                    break
            
            # Se ancora vuoto, usa tutto il dizionario come unico elemento
            if not query_set:
                query_set = [yaml_data]
                print("Struttura YAML rilevata: Dizionario singolo")
        
        else:
            raise ValueError(f"Formato YAML non supportato: tipo {type(yaml_data)}")
        
        # ============================================================
        
        if not query_set:
            raise ValueError("Nessuna query trovata nel file YAML")
        
        print(f"\n Trovate {len(query_set)} query nel file YAML")
        
        # DEBUG: Stampa la struttura delle prime 2 query per verifica
        print("\n Struttura prime query:")
        for i, q in enumerate(query_set[:2]):
            print(f"  Query {i}: {list(q.keys()) if isinstance(q, dict) else type(q)}")
        
        # Resto del codice rimane invariato...
        # Filtra per ID se richiesto
        if query_id_filter:
            query_set = [q for q in query_set if q.get('id') == query_id_filter]
            if not query_set:
                raise ValueError(f"Query con ID '{query_id_filter}' non trovata")
            print(f"Filtro applicato: esecuzione solo query {query_id_filter}")
        
        # Connetti al database
        self.connect()
        
        all_results = []
        
        try:
            # Esegui ogni query nel set
            for query_item in query_set:
                query_id = query_item.get('id', 'unknown')
                description = query_item.get('description', '')
                
                # Cerca la query nelle possibili strutture
                query = None
                reasoning = None
                
                # PRIMA cerca direttamente nel item
                if 'query' in query_item:
                    query = query_item['query']
                    reasoning = query_item.get('reasoning', '')
                # POI cerca in strutture annidate
                elif 'response_structure' in query_item:
                    response_struct = query_item['response_structure']
                    query = response_struct.get('query', '')
                    reasoning = response_struct.get('reasoning', '')
                
                if not query or not query.strip():
                    print(f"\n Query {query_id}: nessuna query trovata, skip")
                    all_results.append({
                        "query_id": query_id,
                        "description": description,
                        "status": "skipped",
                        "error": "Query vuota o non trovata",
                        "results": []
                    })
                    continue
                
                # Esegui la query
                result = self.execute_query(query, query_id)
                
                # Aggiungi informazioni aggiuntive al risultato
                result['description'] = description
                result['reasoning'] = reasoning
                result['query_text'] = query
                
                all_results.append(result)
            
            # Prepara i metadati
            metadata = {
                "source_file": yaml_path,
                "execution_timestamp": datetime.now().isoformat(),
                "database": self.db_config['database'],
                "host": self.db_config['host'],
                "total_queries": len(all_results)
            }
            
            # Salva i risultati
            self.save_results_to_json(all_results, output_path, metadata)
            
        finally:
            # Disconnetti sempre
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(
        description='Esegue query Apache AGE (SQL/Cypher ibride) da file YAML e salva risultati in JSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Esegue tutte le query sul server FIWARE
  python %(prog)s response_Gemini.yaml risultati.json
  
  # Esegue solo la query Q1
  python %(prog)s response_Gemini.yaml risultati_q1.json --query-id Q1
  
  # Usa parametri di connessione personalizzati
  python %(prog)s queries.yaml results.json --host 192.168.1.100 --port 5432 --database mydb
        """
    )
    parser.add_argument('yaml_file', help='File YAML con le query')
    parser.add_argument('output_file', help='File JSON di output')
    parser.add_argument('--host', default='137.204.70.156', 
                        help='Host PostgreSQL (default: 137.204.70.156)')
    parser.add_argument('--port', type=int, default=45432, 
                        help='Porta PostgreSQL (default: 45432)')
    parser.add_argument('--database', default='test_postgres_graphs', 
                        help='Nome del database (default: test_postgres_graphs)')
    parser.add_argument('--user', default='postgres', 
                        help='Username PostgreSQL (default: postgres)')
    parser.add_argument('--password', default='psw', 
                        help='Password PostgreSQL (default: psw)')
    parser.add_argument('--query-id', 
                        help='Esegue solo la query con questo ID (es. Q1, Q2, ...)')
    
    args = parser.parse_args()
    
    # Configurazione database
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    print("=" * 70)
    print("AGE Query Executor per FIWARE")
    print("=" * 70)
    print(f" Server: {args.host}:{args.port}")
    print(f" Database: {args.database}")
    print(f" Input: {args.yaml_file}")
    print(f" Output: {args.output_file}")
    if args.query_id:
        print(f"Filtro: Query {args.query_id} solamente")
    print("=" * 70)
    
    # Esegui il processo
    try:
        executor = QueryExecutor(db_config)
        executor.process_yaml_and_execute(
            args.yaml_file, 
            args.output_file,
            args.query_id
        )
        
        print("\n" + "=" * 70)
        print(" Processo completato con successo!")
        print("=" * 70)
    except Exception as e:
        print("\n" + "=" * 70)
        print(f" Errore durante l'esecuzione: {e}")
        print("=" * 70)
        raise


if __name__ == "__main__":
    main()