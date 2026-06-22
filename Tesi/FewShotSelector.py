import os
import pickle
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
                 token: str,
                 model_name: str = "cohere-embed-v3-multilingual",
                 cache_file: str = "tesi_embeddings_cache.pkl"):
        
        self.examples = ground_truth_examples
        self.model_name = model_name
        self.cache_file = cache_file
        
        if not token:
            raise ValueError("Token di accesso non fornito al FewShotSelector")
            
        self.client = EmbeddingsClient(
            endpoint="https://models.inference.ai.azure.com",
            credential=AzureKeyCredential(token)
        )
        
        #Load/salvataggio in cache dedicata per non sprecare chiamate
        cache_loaded = False
        if os.path.exists(self.cache_file):
            print(f"Tentativo di caricamento cache locale ({self.cache_file})")
            try:
                with open(self.cache_file, 'rb') as f:
                    self.example_embeddings = pickle.load(f)
                cache_loaded = True
                print("Cache caricata con successo!")
            except (EOFError, pickle.UnpicklingError) as e:
                print(f"ache corrotta o vuota ({e}). Ricalcolo")
                cache_loaded = False

        #Ricacolo degli embedding tramite choere
        if not cache_loaded:
            print(f"Calcolo embedding di {len(self.examples)} esempi tramite API ({self.model_name}).")
            self.example_embeddings = self._batch_embed([ex["question"] for ex in self.examples])
            
            # Salva i risultati in un file locale per la prossima volta
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.example_embeddings, f)
            print("Embedding salvati in cache! Le prossime esecuzioni saranno immediate.")

        #Uso di TF-IDF
        print("Indicizzazione Lessicale (TF-IDF)")
        self.tfidf_vectorizer = TfidfVectorizer()
        # Creiamo la matrice matematica delle parole esatte per tutti gli esempi
        self.tfidf_matrix = self.tfidf_vectorizer.fit_transform([ex["question"] for ex in self.examples])

    def _batch_embed(self, texts: List[str]) -> np.ndarray:
        try:
            response = self.client.embed(input=texts, model=self.model_name)
            return np.array([item.embedding for item in response.data])
        except Exception as e:
            print(f"Errore durante il calcolo degli embedding: {e}")
            # Ritorna array vuoti in caso di fallimento per non far crashare lo script
            return np.zeros((len(texts), 1024))

    def select_top_k(self, user_question: str, k: int, alpha: float = 0.5) -> List[Dict[str, str]]:

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
            
        
        user_tfidf = self.tfidf_vectorizer.transform([user_question])
        lexical_scores = cosine_similarity(user_tfidf, self.tfidf_matrix).flatten()
        
        # Normalizzazione Min-Max (scala 0-1) per il punteggio lessicale
        lex_min, lex_max = lexical_scores.min(), lexical_scores.max()
        if lex_max > lex_min:
            lexical_scores = (lexical_scores - lex_min) / (lex_max - lex_min)

        #Somma punteggi ottenuti
        hybrid_scores = (alpha * semantic_scores) + ((1.0 - alpha) * lexical_scores)
        
        # Ordiniamo gli indici in base al punteggio decrescente e prendiamo i primi k
        top_indices = np.argsort(hybrid_scores)[::-1][:k]
        
        return [self.examples[i] for i in top_indices]