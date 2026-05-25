import os
import numpy as np
from typing import List, Dict
from azure.ai.inference import EmbeddingsClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

load_dotenv()

class FewShotSelector:
    def __init__(self, ground_truth_examples: List[Dict[str, str]], 
                 model_name: str = "cohere-embed-v3-multilingual"):
        self.examples = ground_truth_examples
        self.model_name = model_name
        
        token = os.getenv("GITHUB_API_KEY_DeepSeek") or os.getenv("GITHUB_TOKEN")
        if not token:
            raise ValueError("Token di accesso GitHub non trovato nel file .env")
            
        self.client = EmbeddingsClient(
            endpoint="https://models.inference.ai.azure.com",
            credential=AzureKeyCredential(token)
        )
        
        # INDICIZZAZIONE: Viene fatta solo una volta all'avvio del programma
        print(f"Indicizzazione di {len(self.examples)} esempi per il Few-Shot...")
        self.example_embeddings = self._batch_embed([ex["question"] for ex in self.examples])

    def _batch_embed(self, texts: List[str]) -> np.ndarray:
        try:
            response = self.client.embed(input=texts, model=self.model_name)
            return np.array([item.embedding for item in response.data])
        except Exception as e:
            print(f"Errore durante il calcolo degli embedding: {e}")
            return np.zeros((len(texts), 1024))

    def select_top_k(self, user_question: str, k: int = 3) -> List[Dict[str, str]]:
        # Calcola l'embedding per la singola domanda dell'utente
        user_vec = self._batch_embed([user_question])[0]
        
        # Calcolo similarità del Coseno rispetto agli esempi caricati
        norm_a = np.linalg.norm(user_vec)
        norms_b = np.linalg.norm(self.example_embeddings, axis=1)
        similarities = np.dot(self.example_embeddings, user_vec) / (norm_a * norms_b)
        
        top_indices = np.argsort(similarities)[::-1][:k]
        return [self.examples[idx] for idx in top_indices]