# Analisi-LLM

L'obbiettivo è valutare la capacità delle LLM nel comprendere e lavorare con db grafo; Per fare ciò è stato estratto un sottografo "Grafo_agricolo", che rappresenterà il db da passare alle varie LLM.

Sono stati preparati vari file txt che ricoprono il ruolo di promt per l'analisi/risposte delle varie AI, ma questo non basta!
Per ottenere la miglior risposta possibile i vari file sono stati passati anche con un messaggio aggiuntivo uguale per tutte:

"Ti do il contesto: ovvero abbiamo il nostro db in PgAdmin, è un pg_age, quindi lavoraimo con query hybride sql e cypher;
 Il grafo è ottenuto da questa query:

 SELECT * FROM cypher('agri_graph', $$ match (m:AgriFarm {name : 'ZESPRI AZ. AGR. DALLE FABBRICHE ANDREA'}) - [r*] - (n) RETURN m, r, n $$) as (m agtype, r agtype, n agtype);

 Quindi quando ti chiedo di mostrare la query con il quale hai trovato la risposta devi considerare che questa è quella di partenza in più devi aggiungere la nuova."

Successivamente tutte le rispsote sono state salvate in file txt, Presenti nella cartella "Risposte LLM", per poi essere valutate sia da umani sia da un'altra LLM la quale conosceva esattamente quali risposte erano corrette! Ovvero DeepSeek.

L'unione di tutto ciò ha composto il Pdf Report presente.
