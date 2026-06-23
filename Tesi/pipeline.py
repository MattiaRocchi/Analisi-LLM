import os
import yaml
import re
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
    def __init__(self, config_path: str, selected_model: str = None, token: str = None):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Usa il modello scelto dinamicamente, o il default del config
        model_name = selected_model or self.config.get('model_name', 'gpt-4o')
        # Passiamo sia il nome del modello che il token dedicato al Client
        self.client = LLMClient(model_name=model_name, token=token)

    def load_dataset(self) -> List[Dict]:
        #Uniamo i file per creare gli esempi per il few_shot
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

    def extract_cypher(self, text: str) -> str:
        match = re.search(r'[`]{3}(?:cypher|sql)?\n(.*?)\n[`]{3}', text, re.DOTALL | re.IGNORECASE)
        return match.group(1).strip() if match else text.strip()

    def start(self):
        print("Lettura system_instructions")
        dataset = self.load_dataset()
        schema = SchemaExtractor.get_full_prompt_context(self.config['refined_graph_path'])
        
        # Il selettore viene creato
        selector = FewShotSelector(ground_truth_examples=dataset,token=token)
        
        print(" (Scrivi 'back', 'exit' o 'esci' per tornare al menu principale)")
        
        while True:
            user_q = input("\nCosa vuoi sapere dal database agricolo?: ")
            
            # Torna al menu principale invece di chiudere lo script
            if user_q.lower() in ['exit', 'quit', 'esci', 'back', 'indietro']:
                print("\n Ritorno al menu principale")
                break
                
            print("Analisi domanda e selezione esempi simili")
            few_shot = selector.select_top_k(user_q, k=3)

            #Parte usata per analisi sulla selezione dei k esempi migliori dal few_shot
            print(f"\n[DEBUG FEW-SHOT] Esempi selezionati ({len(few_shot)}):")
            for i, ex in enumerate(few_shot, 1):
                ex_id = ex.get('id', 'N/A')
                ex_q = ex.get('question', 'N/A')
                print(f"  {i}) ID: {ex_id} -> {ex_q}")
            print(" ")
            
            print("Generazione query in corso")
            generated = self.client.generate_query(
                instructions_path=self.config['instructions_path'], 
                schema_context=schema,
                few_shot_examples=few_shot,
                user_question=user_q
            )
            
            clean_query = self.extract_cypher(generated)
            print(f"\n[QUERY GENERATA]:\n{clean_query}")
    
    def run_test(self, test_file_path: str, output_yaml_path: str, gt_file_path: str, output_results_path: str):

        print("Inizializzazione sistema in corso per il Test")
        dataset = self.load_dataset()
        schema = SchemaExtractor.get_full_prompt_context(self.config['refined_graph_path'])
        
        selector = FewShotSelector(ground_truth_examples=dataset,token=token)
        
        print(f"Lettura domande di test da: {test_file_path}")
        with open(test_file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            test_queries = data.get('questions_test', data.get('responses_results', []))

        results_to_save = []
        total = len(test_queries)
        print(f"\nInizio elaborazione Test per {total} query\n" + "="*50)

        for i, test in enumerate(test_queries, 1):
            q_id = test['id']
            nl_query = test['nl_query']
            print(f"\n[{i}/{total}] Processando {q_id}: {nl_query}")

            few_shot = selector.select_top_k(nl_query, k=3)
            
            generated_query = self.client.generate_query(
                instructions_path=self.config['instructions_path'], 
                schema_context=schema,
                few_shot_examples=few_shot,
                user_question=nl_query
            )
            
            clean_query = self.extract_cypher(generated_query)
            
            results_to_save.append({
                "id": q_id,
                "nl_query": nl_query,
                "query": clean_query
            })

        # Salvataggio nel file YAML
        with open(output_yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump({"responses": results_to_save}, f, allow_unicode=True, sort_keys=False)
            
        print("\n")
        print(f"Test LLM completato! Query generate salvate in: {output_yaml_path}")
        print("Avvio della validazione su Database (QueryExecutor)")
        print("")

        # Esecuzione validazione sul db
        executor = QueryExecutor(DB_CONFIG)
        executor.run(gt_file=gt_file_path, llm_file=output_yaml_path, output_file=output_results_path)


# Configurazione Database per QueryExecutor
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def menu_scelta_modello():
    modelli = {
        "1": ("DeepSeek V3 (Consigliato per codice)", "DeepSeek-V3-0324", "DEEPV3_TOKEN"),
        "2": ("Codeestral", "Codestral-2501", "CODESTRAL_TOKEN"),
        "3": ("OpenAI GPT-4o", "gpt-4o", "OPENAI_API_KEY"),
        "4": ("LLaMA 3.3 70B Instruct", "Llama-3.3-70B-Instruct", "LLAMA_TOKEN"),
    }
    
    while True:
        print("\n")
        print("SELEZIONA IL MODELLO LLM \n")
        print("")
        for key, (nome_bello, _, _) in modelli.items():
            print(f" {key}) {nome_bello}")
            
        scelta = input("\nInserisci il numero del modello (o 'q' per uscire): ")
        
        if scelta.lower() in ['q', 'quit', 'exit', 'esci']:
            exit()
        if scelta in modelli:
            _, model, token_key = modelli[scelta]
            return model, token_key
            
        print("Scelta non valida, riprova.")

if __name__ == "__main__":
    CONFIG_FILE = "Tesi/config/pipeline_conf.yaml"
    
    #Gestisce il cambio di modello LLM
    while True:
        modello_selezionato, token_env_name = menu_scelta_modello()
        
        # Recupera il token dinamico in base al modello scelto
        token = os.getenv(token_env_name)
        if not token:
            print(f"\nERRORE: Token non trovato! Assicurati di avere '{token_env_name}' nel file .env.")
            input("Premi INVIO per tornare alla selezione del modello")
            continue
        
        try:
            #passiamo anche il token all'inizializzazione della pipeline
            app = AgriQueryPipeline(CONFIG_FILE, selected_model=modello_selezionato, token=token)
        except Exception as e:
            print(f"\nErrore durante l'inizializzazione: {e}")
            input("Premi INVIO per riprovare")
            continue
            
        #gestisce le azioni per il modello selezionato
        while True:
            nome_modello_display = modello_selezionato.split('/')[-1]
            print("\n")
            print(f" MENU PRINCIPALE (Modello: {nome_modello_display})\n")
            print("")
            print(" 1) Modalità Interattiva (Chat con LLM)")
            print(" 2) Modalità Test (Esegui test DB Q21-Q30)")
            print(" 3) Cambia Modello LLM")
            print(" 4) Esci")
            
            scelta = input("\nCosa vuoi fare? (1-4): ")
            
            if scelta == '1':
                app.start() 
            elif scelta == '2':
                out_yaml = f"Tesi/output/llm_generated_q21_q30_{nome_modello_display}.yaml"
                out_results = f"Tesi/output/analysis_results_{nome_modello_display}.yaml"
                
                app.run_test(
                    test_file_path="Tesi/Query_test/QueryTest.yaml", 
                    output_yaml_path=out_yaml,
                    gt_file_path="Tesi/Query_test/groundTruth.yaml",
                    output_results_path=out_results
                )
                input("\nPremi INVIO per tornare al menu principale")
            elif scelta == '3':
                print("\nRitorno alla selezione del modello")
                break # Esce dal loop interno per cambiare modello
            elif scelta == '4':
                print("\n Uscita dal sistema")
                exit()
            else:
                print("Scelta non valida.")