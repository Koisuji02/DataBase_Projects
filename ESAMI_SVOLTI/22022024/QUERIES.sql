/*VILLAGGIO(CodV, Villaggio, CittàV, ProvinciaV, RegioneV, StatoV, Ristorazione, Animazione, BabyParking)
TEMPO(CodT, Data, Mese, 3M, 4M, 6M, Anno, Settimana, Periodo)
JUNK-ALLOGGIO-PAGAMENTO(CodAP, TipologiaAlloggio, ModalitàPagamento)
CARATTERISTICHE-CLIENTE(CodCC, CittàCliente, ProvinciaC, RegioneC, StatoC)
GESTIONE-VILLAGGI(CodV, CodT, CodAP, CodCC, Incasso, NumOspiti)*/
/*Considerando la gestione dei villaggi dal 2014 al 2023, visualizzare per ogni tipologia alloggio, regione in cui è sito il villaggio e semestre (6M),

l’incasso medio giornaliero e il numero complessivo di ospiti 
la percentuale dell’incasso rispetto al complessivo annuale.
Associare ad ogni record:

un rango rispetto all’incasso medio giornaliero (il valore 1 al record con il più basso valore di incasso medio giornaliero), separatamente per Stato in cui è sito il villaggio
Un rango per numero complessivo di ospiti (il valore 1 al record con il più alto).*/
SELECT TipologiaAlloggio, RegioneV, 6M, Anno, StatoV,
  SUM(Incasso)/COUNT(DISTINCT Data), SUM(NumOspiti),
  100*SUM(Incasso)/SUM(SUM(Incasso)) OVER(PARTITION BY TipologiaAlloggio, RegioneV, Anno),
  RANK() OVER(PARTITION BY StatoV
              ORDER BY SUM(Incasso)/COUNT(DISTINCT Data)),
  RANK() OVER(ORDER BY SUM(NumOspiti) DESC)
FROM GESTIONE-VILLAGGI G, VILLAGGIO V, TEMPO T,
  JUNK-ALLOGGIO-PAGAMENTO P
WHERE V.CodV = G.CodV AND T.CodT = G.CodT AND P.CodAP = G.CodAP
  AND (Anno >= 2014 AND Anno <= 2023)
GROUP BY TipologiaAlloggio, RegioneV, 6M, Anno, StatoV;

/*Considerando i villaggi con servizio animazione o baby parking, visualizzare per ogni città del villaggio e trimestre (3M),



l’incasso totale e il numero medio mensile di ospiti 
la percentuale dell’incasso rispetto al complessivo considerando tutti i villaggi siti nella stessa regione
l’incasso cumulativo dall’inizio dell’anno al trascorrere dei trimestri.
Si effettui l’analisi separatamente per modalità di pagamento.*/
SELECT CittàV, 3M, ModalitàPagamento, RegioneV, Anno,
  SUM(Incasso), SUM(NumOspiti)/COUNT(DISTINCT Mese),
  100*SUM(Incasso)/SUM(SUM(Incasso)) OVER(PARTITION BY 3M, ModalitàPagamento, RegioneV),
  SUM(SUM(Incasso)) OVER(PARTITION BY Anno, ModalitàPagamento
                         ORDER BY 3M
                         ROWS UNBOUNDED PRECEDING)
FROM GESTIONE-VILLAGGI G, VILLAGGIO V, TEMPO T,
  JUNK-ALLOGGIO-PAGAMENTO P
WHERE V.CodV = G.CodV AND T.CodT = G.CodT AND P.CodAP = G.CodAP
  AND (Animazione = 'Y' OR BabyParking = 'Y')
GROUP BY CittàV, 3M, ModalitàPagamento, RegioneV, Anno;

/*Considerando i clienti italiani, visualizzare per ogni provincia in cui è sito il villaggio e mese,

l’incasso medio per ospite 
la percentuale del numero di ospiti  rispetto al complessivo semestrale
l’incasso complessivo indipendentemente dalla provincia in cui è sito il villaggio e mese
Il numero complessivo di ospiti indipendentemente dalla provincia in cui è sito il villaggio
Si effettui l’analisi separatamente per Città del Cliente.*/
SELECT ProvinciaV, Mese, 6M, CittàCliente
  SUM(Incasso)/SUM(NumOspiti),
  100*SUM(NumOspiti)/SUM(SUM(NumOspiti)) OVER(PARTITION BY ProvinciaV, 6M),
  SUM(SUM(Incasso)) OVER(PARTITION BY CittàCliente),
  SUM(SUM(NumOspiti)) OVER(PARTITION BY Mese, CittàCliente)
FROM CARATTERISTICHE-CLIENTE C, GESTIONE-VILLAGGI G, VILLAGGIO V,
  TEMPO T
WHERE C.CodCC = G.CodCC AND V.CodV = G.CodV AND T.CodT = G.CodT
  AND StatoC = 'Italia'
GROUP BY ProvinciaV, Mese, 6M, CittàCliente;