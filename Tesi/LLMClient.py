import os
from typing import List, Dict
from dotenv import load_dotenv
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage, ChatRequestMessage
from azure.core.credentials import AzureKeyCredential

load_dotenv()

class LLMClient:
    def __init__(self, model_name: str, token: str):
        self.model_name = model_name
        self.token = token # Usa il token passato dalla pipeline
        
        if not self.token:
            raise ValueError(f"Token mancante per il modello {model_name}")
            
        self.client = ChatCompletionsClient(
            endpoint="https://models.inference.ai.azure.com",
            credential=AzureKeyCredential(self.token),
            connection_timeout=10,
            read_timeout=30
        )

    def generate_query(self, 
                       instructions_path: str, 
                       schema_context: str,
                       few_shot_examples: List[Dict[str, str]], 
                       user_question: str) -> str:
        
        # Lettura istruzioni come testo puro
        with open(instructions_path, 'r', encoding='utf-8') as f:
            system_instructions = f.read().strip()

        # Creazione del testo degli esempi
        examples_text = ""
        if few_shot_examples:
            examples_text = "\n\nESEMPI DI QUERY (FEW-SHOT)\n"
            examples_text += "Usa QUESTI esempi come modello assoluto per la logica e i nomi delle proprietà.\n"
            for i, ex in enumerate(few_shot_examples, 1):
                examples_text += f"\nDomanda Utente: {ex['question']}\nQuery Attesa:\n{ex['query']}\n"

        # Creazione del prompt finale
        full_system_content = f"{system_instructions}\n\nDATABASE SCHEMA\n{schema_context}{examples_text}"
        
        messages: List[ChatRequestMessage] = [
            SystemMessage(content=full_system_content),
            UserMessage(content=user_question)
        ]
        
        try:
            response = self.client.complete(
                messages=messages,
                temperature=0.0,
                model=self.model_name
            )
            query = response.choices[0].message.content.strip()
            
            # Pulizia automatica markdown
            if query.startswith("```"):
                lines = query.splitlines()
                if len(lines) >= 3: query = "\n".join(lines[1:-1])
            return query.strip()
        except Exception as e:
            print(f"Errore API: {e}")
            return "Errore nella generazione della query."