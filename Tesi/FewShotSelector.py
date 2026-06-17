import os
import numpy as np
from typing import List, Dict
from azure.ai.inference import EmbeddingsClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

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

        # --- MOTORE 2: INDICIZZAZIONE LESSICALE (TF-IDF) ---
        print("Indicizzazione Lessicale (TF-IDF)...")
        self.tfidf_vectorizer = TfidfVectorizer()
        # Creiamo la matrice matematica delle parole esatte per tutti gli esempi
        self.tfidf_matrix = self.tfidf_vectorizer.fit_transform([ex["question"] for ex in self.examples])

    def _batch_embed(self, texts: List[str]) -> np.ndarray:
        try:
            response = self.client.embed(input=texts, model=self.model_name)
            return np.array([item.embedding for item in response.data])
        except Exception as e:
            print(f"Errore durante il calcolo degli embedding: {e}")
            return np.zeros((len(texts), 1024))

    def select_top_k(self, user_question: str, k: int = 5, alpha: float = 0.5) -> List[Dict[str, str]]:
        """
        Seleziona i top-k esempi usando una Ricerca Ibrida.
        alpha = 0.5 bilancia equamente significato (Cohere) e parole esatte (TF-IDF).
        """
        
        # 1. CALCOLO PUNTEGGIO SEMANTICO (Cohere)
        user_vec = self._batch_embed([user_question])[0]
        
        norm_user = np.linalg.norm(user_vec)
        norms_examples = np.linalg.norm(self.example_embeddings, axis=1)
        
        if norm_user == 0 or np.any(norms_examples == 0):
            semantic_scores = np.zeros(len(self.examples))
        else:
            semantic_scores = np.dot(self.example_embeddings, user_vec) / (norms_examples * norm_user)

        # Normalizzazione Min-Max (scala 0-1) per il punteggio semantico
        sem_min, sem_max = semantic_scores.min(), semantic_scores.max()
        if sem_max > sem_min:
            semantic_scores = (semantic_scores - sem_min) / (sem_max - sem_min)
            
        # 2. CALCOLO PUNTEGGIO LESSICALE (TF-IDF)
        user_tfidf = self.tfidf_vectorizer.transform([user_question])
        lexical_scores = cosine_similarity(user_tfidf, self.tfidf_matrix).flatten()
        
        # Normalizzazione Min-Max (scala 0-1) per il punteggio lessicale
        lex_min, lex_max = lexical_scores.min(), lexical_scores.max()
        if lex_max > lex_min:
            lexical_scores = (lexical_scores - lex_min) / (lex_max - lex_min)

        # 3. FUSIONE IBRIDA (HYBRID RANKING)
        hybrid_scores = (alpha * semantic_scores) + ((1.0 - alpha) * lexical_scores)
        
        # Ordiniamo gli indici in base al punteggio decrescente e prendiamo i primi k
        top_indices = np.argsort(hybrid_scores)[::-1][:k]
        
        return [self.examples[i] for i in top_indices]