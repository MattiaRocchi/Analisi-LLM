import yaml
import json
import argparse
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from data.db_conn import db_conn as db_conn_module
from data.CompareGraph import CompareGraph

load_dotenv()

class QueryExecutor:
    
    def __init__(self, db_config: Dict[str, Optional[str]]):
       # Convert None
        clean_config = {k: (v or "") for k, v in db_config.items()}

        for key, value in clean_config.items():
            if not value:
                raise ValueError(f"Missing DB config value for '{key}'")

        self.db = db_conn_module(clean_config)
    
    def execute_query(self, query: str, query_id: str) -> Dict:
        try:
            start = datetime.now()
            rows, columns = self.db.execute_raw(query)
            exec_time = (datetime.now() - start).total_seconds()
            
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    col = columns[i] if i < len(columns) else f"col_{i}"
                    row_dict[col] = self.db.parse_agtype(value)
                results.append(row_dict)
            
            print(f"{query_id}: {len(results)} rows, {exec_time:.3f}s")
            return {"query_id": query_id, "results": results}
            
        except Exception as e:
            print(f"Errore in {query_id}: {e}")
            return {"query_id": query_id, "results": [], "error": str(e)}
    
    def run(self, gt_file: str, llm_file: str, output_file: str):
        
        # Carica YAML
        with open(gt_file, 'r') as f:
            gt_data = yaml.safe_load(f)
        with open(llm_file, 'r') as f:
            llm_data = yaml.safe_load(f)
        
        # Crea mapping
        gt_map = {q['id']: q for q in gt_data.get('responses_results', [])}
        llm_map = {q['id']: q for q in llm_data.get('responses', [])}
        
        # Query comuni
        query_ids = set(gt_map.keys()) & set(llm_map.keys())
        
        print(f"\n{'='*70}")
        print(f"Comparing {len(query_ids)} queries")
        print(f"{'='*70}\n")
        
        # Connetti DB
        self.db.connect()
        
        comparisons = []
        
        try:
            for q_id in sorted(query_ids):
                print(f"\nQuery {q_id}:")
                
                gt_query = gt_map[q_id]['query']
                llm_query = llm_map[q_id]['query']
                
                gt_result = self.execute_query(gt_query, f"{q_id}_GT")
                llm_result = self.execute_query(llm_query, f"{q_id}_LLM")
                
                # Comparison
                if 'error' not in gt_result and 'error' not in llm_result:
                    metrics = CompareGraph.compare(
                        gt_result['results'],
                        llm_result['results'],
                        q_id
                    )
                    
                    comparisons.append({
                        'query_id': q_id,
                        'nodes_gt': metrics.nodes_gt,
                        'nodes_llm': metrics.nodes_llm,
                        'edges_gt': metrics.edges_gt,
                        'edges_llm': metrics.edges_llm,
                        'missing_llm': metrics.missing_llm,
                        'extra_llm': metrics.extra_llm
                    })
                    
                    print(f"  Nodes: GT={metrics.nodes_gt}, LLM={metrics.nodes_llm}")
                    print(f"  Edges: GT={metrics.edges_gt}, LLM={metrics.edges_llm}")
                    
                else:
                    comparisons.append({
                        'query_id': q_id,
                        'error': {
                            'gt': gt_result.get('error'),
                            'llm': llm_result.get('error')
                        }
                    })
        
        finally:
            self.db.disconnect()
        
        # Save results
        output = {
            'gt_file': gt_file,
            'llm_file': llm_file,
            'comparisons': comparisons
        }
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\nResults saved to: {output_file}\n")


# Config database
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}


def main():
    parser = argparse.ArgumentParser(description='Compare AGE queries (GT vs LLM)')
    parser.add_argument('gt_file', help='Ground truth YAML file')
    parser.add_argument('llm_file', help='LLM generated YAML file')
    parser.add_argument('output_file', help='Output JSON file')
    
    args = parser.parse_args()
    
    try:
        executor = QueryExecutor(DB_CONFIG)
        executor.run(args.gt_file, args.llm_file, args.output_file)
        print("Done!")
    except Exception as e:
        print(f"\nError: {e}")
        raise


if __name__ == "__main__":
    main()