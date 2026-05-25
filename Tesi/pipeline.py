import os
import yaml
from typing import Dict, List
from dotenv import load_dotenv

from LLMClient import LLMClient
from SchemaExtractor import SchemaExtractor
from FewShotSelector import FewShotSelector

load_dotenv()

class AgriQueryPipeline:
    def __init__(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.client = LLMClient(model_name=self.config.get('model_name', 'openai/gpt-4o'))

    def load_dataset(self) -> List[Dict]:
        """Unisce i file YAML per creare il database degli esempi."""
        with open(self.config['ground_truth_path'], 'r', encoding='utf-8') as f:
            gt_data = yaml.safe_load(f)['responses_results']
        with open(self.config['template_query_path'], 'r', encoding='utf-8') as f:
            tp_data = yaml.safe_load(f)['query_descriptions']
        
        queries = {str(item['id']): item['query'] for item in gt_data}
        dataset = []
        for item in tp_data:
            q_id = str(item['id'])
            if q_id in queries:
                dataset.append({
                    "id": q_id, 
                    "question": item['nl_query'], 
                    "query": queries[q_id]
                })
        return dataset

    def start(self):
        print("Inizializzazione sistema in corso...")
        dataset = self.load_dataset()
        schema = SchemaExtractor.get_full_prompt_context(self.config['refined_graph_path'])
        
        # Il selettore viene creato UNA volta sola qui
        selector = FewShotSelector(ground_truth_examples=dataset)
        
        print("\n--- SISTEMA AGRI-QUERY PRONTO ---")
        while True:
            user_q = input("\nCosa vuoi sapere dal database agricolo? (o 'exit' per uscire): ")
            if user_q.lower() in ['exit', 'quit', 'esci']:
                break
                
            print("Analisi domanda e selezione esempi simili...")
            few_shot = selector.select_top_k(user_q, k=2)

            # --- BLOCCO HACK PER IL DEBUG ---
            # 2. Cerca manualmente la Q10 nel database e forzala nella lista
            q10_example = next((ex for ex in dataset if ex['id'] == 'Q10'), None)
            if q10_example and q10_example not in few_shot:
                few_shot.append(q10_example)

            print(f"\n[DEBUG FEW-SHOT] Esempi selezionati ({len(few_shot)}):")
            for i, ex in enumerate(few_shot, 1):
                # Usiamo .get() per sicurezza sulle chiavi del dizionario
                ex_id = ex.get('id', 'N/A')
                ex_q = ex.get('question', 'N/A')
                print(f"  {i}) ID: {ex_id} -> {ex_q}")
            print("-" * 50)
            
            print("Generazione query ibrida...")
            generated = self.client.generate_query(
                instructions_path=self.config['instructions_path'], 
                schema_context=schema,
                few_shot_examples=few_shot,
                user_question=user_q
            )
            
            print(f"\n[QUERY GENERATA]:\n{generated}")

if __name__ == "__main__":
    # Verifica che il percorso al config sia esatto
    app = AgriQueryPipeline("Tesi/config/pipeline_conf.yaml")
    app.start()