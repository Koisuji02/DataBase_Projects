/* RICHIESTE:
RETE-DISTRIBUTIVA(IDReteDistributiva, ReteDistributiva, Città, Provincia, Regione, AreaGeografica)
PRODOTTO-ASSICURATIVO(IDProdottoAssicurativo, ProdottoAssicurativo, Compagnia, RamoAssicurativo)
JUNK-CARATTERISTICHE-CLIENTE(IDJCC, FasciaEtà, Genere, StatoCivile, Professione)
TEMPO(IDTempo, Mese, 2-Mesi, 3-Mesi, 6-Mesi, Anno, Triennio, Quinquennio)
SOTTOSCRIZIONI(IDReteDistributiva, IDProdottoAssicurativo, IDJCC, IDTempo, #Sottoscrizioni, ImportoPremi, Durata)

Dato lo schema logico precedente, scrivere nel box sottostante in SQL esteso le seguenti interrogazioni, separate da uno spazio:

(3 punti) Query 1
Visualizzare per ogni prodotto assicurativo il numero di sottoscrizioni totali e l'importo complessivo dei premi. Associare ad ogni record un rango:
che identifica la posizione del prodotto assicurativo in funzione dell’importo complessivo dei premi (1 per il prodotto con il più basso valore complessivo dei premi)
che identifica la posizione del prodotto assicurativo in ordine decrescente del numero di sottoscrizioni totali, separatamente per compagnia 

(4 punti) Query 2
Per il prodotto assicurativo salute (ProdottoAssicurativo=’Salute’) emesso dalla compagnia AXA (Compagnia =’AXA’), visualizzare per ogni trimestre e genere del cliente
l'importo complessivo dei premi, 
l’importo cumulativo dei premi dall’inizio dell’anno al trascorrere dei trimestri, separatamente per genere,
il numero medio di sottoscrizioni mensili

(4 punti) Query 3
Visualizzare separatamente per rete distributiva, prodotto assicurativo e trimestre
il numero di sottoscrizioni
l'importo medio dei premi per sottoscrizione
l’importo complessivo dei premi indipendentemente dalla rete distributiva
la percentuale dell’importo dei premi rispetto all’importo complessivo delle reti di distribuzione site nella stessa regione, separatamente per prodotto assicurativo e trimestre
*/

---QUERY1
SELECT ProdottoAssicurativo, Compagnia,
  SUM(SUM((#Sottoscrizioni)) OVER(PARTITION BY ProdottoAssicurativo),
  SUM(SUM(ImportoPremi))  OVER(PARTITION BY ProdottoAssicurativo),
  RANK() OVER(PARTITION BY ProdottoAssicurativo ORDER BY SUM(ImportoPremi)),
  RANK() OVER(PARTITION BY Compagnia ORDER BY SUM(#Sottoscrizioni) DESC)
FROM PRODOTTO-ASSICURATIVO P, SOTTOSCRIZIONI S
WHERE P.IDProdottoAssicurativo = S.IDProdottoAssicurativo
GROUP BY ProdottoAssicurativo, Compagnia;

---QUERY2
SELECT 3-Mesi, Genere, Anno,
  SUM(ImportoPremi),
  SUM(SUM(ImportoPremi)) OVER(PARTITION BY Anno, Genere
                              ORDER BY 3-Mesi
                              ROWS UNBOUNDED PRECEDING),
  SUM(#Sottoscrizioni)/COUNT(DISTINCT Mese)
FROM PRODOTTO-ASSICURATIVO P, SOTTOSCRIZIONI S,
  JUNK-CARATTERISTICHE-CLIENTE C, TEMPO T
WHERE P.IDProdottoAssicurativo = S.IDProdottoAssicurativo AND
  ProdottoAssicurativo = 'Salute' AND Compagnia = 'AXA' AND
  C.IDJCC = S.IDJCC AND T.IDTempo = S.IDTempo
GROUP BY 3-Mesi, Genere, Anno;

---QUERY3
SELECT ReteDistributiva, ProdottoAssicurativo, 3-Mesi, Regione,
  SUM(#Sottoscrizioni),
  SUM(ImportoPremi)/SUM(#Sottoscrizioni),
  SUM(SUM(ImportoPremi)) OVER(PARTITION BY ProdottoAssicurativo, 3-Mesi),
  100*SUM(ImportoPremi)/SUM(SUM(ImportoPremi)) OVER(
    PARTITION BY ProdottoAssicurativo, 3-Mesi, Regione)
FROM RETE-DISTRIBUTIVA R, PRODOTTO-ASSICURATIVO P,
  SOTTOSCRIZIONI S, TEMPO T
WHERE P.IDProdottoAssicurativo = S.IDProdottoAssicurativo AND
  T.IDTempo = S.IDTempo AND
  R.IDReteDistributiva = S.IDReteDistributiva
GROUP BY ReteDistributiva, ProdottoAssicurativo, 3-Mesi, Regione;