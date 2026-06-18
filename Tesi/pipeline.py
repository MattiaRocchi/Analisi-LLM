import os
import yaml
from typing import Dict, List
from dotenv import load_dotenv

from LLMClient import LLMClient
from SchemaExtractor import SchemaExtractor
from FewShotSelector import FewShotSelector

from queryExecutor import QueryExecutor

# Insegna a yaml a usare la pipe "|" per le stringhe multilinea così da mantenere la leggibilità del codice
def str_presenter(dumper, data):
    if len(data.splitlines()) > 1:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(str, str_presenter)
yaml.representer.SafeRepresenter.add_representer(str, str_presenter)

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
        print("Lettura system_instructions...")
        dataset = self.load_dataset()
        schema = SchemaExtractor.get_full_prompt_context(self.config['refined_graph_path'])
        
        # Il selettore viene creato
        selector = FewShotSelector(ground_truth_examples=dataset)
        
        print("\n--- SISTEMA AGRI-QUERY PRONTO ---")
        while True:
            user_q = input("\nCosa vuoi sapere dal database agricolo? (o 'exit' per uscire): ")
            if user_q.lower() in ['exit', 'quit', 'esci']:
                break
                
            print("Analisi domanda e selezione esempi simili...")
            few_shot = selector.select_top_k(user_q, k=5)

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
    
    def run_test(self, test_file_path: str, output_yaml_path: str,gt_file_path:str, output_results_path:str):
        """
        Modalità test: legge il file di test, genera le query con l'LLM 
        e le salva in un file YAML pronto per il QueryExecutor.
        """
        print("Inizializzazione sistema in corso...")
        dataset = self.load_dataset()
        schema = SchemaExtractor.get_full_prompt_context(self.config['refined_graph_path'])
        
        # Inizializza il selettore (Ibrido)
        selector = FewShotSelector(ground_truth_examples=dataset)
        
        # 1. Carica le domande di test (es. le Q21-Q30)
        print(f"Lettura domande di test da: {test_file_path}")
        with open(test_file_path, 'r', encoding='utf-8') as f:
            test_queries = yaml.safe_load(f).get('questions_test', [])

        results_to_save = []
        total = len(test_queries)
        print(f"\nInizio elaborazione Batch per {total} query...\n" + "="*50)

        # 2. Loop di inferenza
        for i, test in enumerate(test_queries, 1):
            q_id = test['id']
            nl_query = test['nl_query']
            print(f"\n[{i}/{total}] Processando {q_id}: {nl_query}")

            # Estrazione Few-Shot
            few_shot = selector.select_top_k(nl_query, k=5)
            
            # Generazione (il tuo LLMClient pulisce già i markdown)
            generated_query = self.client.generate_query(
                instructions_path=self.config['instructions_path'], 
                schema_context=schema,
                few_shot_examples=few_shot,
                user_question=nl_query
            )
            
            # Prepariamo l'oggetto per il salvataggio
            results_to_save.append({
                "id": q_id,
                "nl_query": nl_query,
                "query": generated_query
            })

        # 3. Salvataggio nel file YAML finale con la radice 'responses'
        with open(output_yaml_path, 'w', encoding='utf-8') as f:
            # allow_unicode=True mantiene gli accenti, sort_keys=False mantiene l'ordine Q21 -> Q30
            yaml.dump({"responses": results_to_save}, f, allow_unicode=True, sort_keys=False)
            
        print("\n" + "="*50)
        print(f"Test completato! Query generate salvate in: {output_yaml_path}")
        print("="*50)

        executor = QueryExecutor(DB_CONFIG)
        executor.run(gt_file=gt_file_path , llm_file=output_yaml_path, output_file=output_results_path)


# Config database
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

if __name__ == "__main__":
    # Verifica che il percorso al config sia esatto
    app = AgriQueryPipeline("Tesi/config/pipeline_conf.yaml")
    print("\n" + "="*40)
    print(" BENVENUTO NEL SISTEMA AGRI-QUERY ")
    print("="*40)
    print("Scegli la modalità di esecuzione:")
    print("1) Modalità Interattiva (Chat con LLM)")
    print("2) Modalità Batch (Esegui 10 query di test)")
    print("3) Esci")
    
    scelta = input("\nInserisci il numero (1, 2 o 3): ")
    
    if scelta == '1':
        app.start()
    elif scelta == '2':
        # Metti qui i nomi reali dei tuoi file
        app.run_test(
            test_file_path="Tesi/Query_test/QueryTest.yaml", 
            output_yaml_path="Tesi/output/llm_generated_q21_q30.yaml",
            gt_file_path="Tesi/Query_test/groundTruth.yaml",
            output_results_path="Tesi/output/analysis_results.yaml"
        )
    else:
        print("Uscita dal sistema.")