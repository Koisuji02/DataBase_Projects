/*COPISTERIA(CodC, Copisteria, CittàC, ProvinciaC, RegioneC, StatoC, Brochure, Volantini, Calendari, Stampa_foto, Fotocopie)
TEMPO(CodT, Data, GiornoSettimana,  Settimana, Mese, 3-Mesi, 4-Mesi, 6-Mesi, Anno)
JUNK- DIMENSION(CodJK, TipoPagamento, TipoConsegna, TipoServizioRichiesto, ModalitàRealizzazione)
TIPOLOGIA-AZIENDA(CodTA, TipologiaAzienda, SettoreAzienda)
LOCALIZZAZIONE-AZIENDA (CodCA,  CittàAzienda, ProvinciaAzienda, RegioneAzienda, StatoAzienda)
ORDINI-EVASI(CodC, CodT, CodJK, CodTA, CodCA, #Ordini, TempoRealizzazione, Costo, QuantitàDiCopie)*/
/*Considerando gli ordini evasi per servizio richiesto di tipo stampa volantini (TipoServizioRichiesto='Stampa di volantini') effettuati da aziende site in Italia (StatoAzienda=’Italia’), separatamente per tipologia azienda, mese e tipo di pagamento, calcolare:

La quantità media di copie per ordine  
Il numero complessivo di ordini separatamente per quadrimestre (4-Mesi)
Il numero complessivo di copie indipendentemente dalla tipologia azienda e dal mese
Il tempo medio di realizzazione per ordine indipendentemente dal tipo di pagamento.
Si effettui l’analisi separatamente per modalità di realizzazione.*/
SELECT TipologiaAzienda, Mese, TipoPagamento, ModalitàRealizzazione,
  4-Mesi,
  SUM(QuantitàDiCopie)/SUM(#Ordini),
  SUM(SUM(#Ordini)) OVER(PARTITION BY 4-Mesi, ModalitàRealizzazione),
  SUM(SUM(QuantitàDiCopie)) OVER(PARTITION BY TipoPagamento, ModalitàRealizzazione),
  SUM(SUM(TempoRealizzazione)) OVER(PARTITION  BY TipologiaAzienda, Mese, ModalitàRealizzazione)/SUM(SUM(#Ordini)) OVER(PARTITION  BY TipologiaAzienda, Mese, ModalitàRealizzazione)
FROM JUNK- DIMENSION J, ORDINI-EVASI O, LOCALIZZAZIONE-AZIENDA LA,
  TIPOLOGIA-AZIENDA TA, TEMPO T
WHERE TipoServizioRichiesto='Stampa di volantini' AND StatoAzienda=’Italia’
  AND O.CodJK = J.CodJK AND LA.CodCA = O.CodCA AND
  TA.CodTA = O.CodTA AND T.CodT = O.CodT
GROUP BY TipologiaAzienda, Mese, TipoPagamento,
  ModalitàRealizzazione, 4-Mesi;

/*Considerando le copisterie che offrono esclusivamente servizi di  realizzazione di brochure e calendari, separatamente per semestre (6-Mesi) e regione dell’azienda, calcolare:

Il costo medio per ordine e il tempo medio di realizzazione per ordine
La percentuale del numero di ordini rispetto al numero totale per stato dell’azienda
Il numero cumulativo di copie al trascorrere dei semestri (6-Mesi).
Si effettui l’analisi separatamente per settore dell’azienda.*/
SELECT 6-Mesi, RegioneAzienda, SettoreAzienda, StatoAzienda,
  SUM(Costo)/SUM(#Ordini), SUM(TempoRealizzazione)/SUM(#Ordini),
  100*SUM(#Ordini)/SUM(SUM(#Ordini)) OVER(PARTITION BY StatoAzienda),
  SUM(SUM(QuantitàDiCopie)) OVER(PARTITION BY RegioneAzienda, SettoreAzienda
                                 ORDER BY 6-Mesi
                                 ROWS UNBOUNDED PRECEDING)
FROM COPISTERIA C, TEMPO T, ORDINI-EVASI O,
  LOCALIZZAZIONE-AZIENDA LA, TIPOLOGIA-AZIENDA TA
WHERE Brochure = 'Y' AND Calendari = 'Y' AND Volantini = 'N' AND
  Stampa_foto = 'N' AND Fotocopie = 'N' AND
  C.CodC = O.CodC AND T.CodT = O.CodT AND LA.CodCA = O.CodCA AND
  TA.CodTA = O.CodTA
GROUP BY 6-Mesi, RegioneAzienda, SettoreAzienda, StatoAzienda;

/*Separatamente per mese e tipologia azienda visualizzare:

La quantità media di copie per ordine e il tempo medio di realizzazione per ordine,
La percentuale di ordini rispetto al complessivo annuale
Il numero complessivo di ordini separatamente per settore dell’azienda 
Assegnare ad ogni record:

Un rank separatamente per settore dell’azienda. La posizione 1 va assegnata al record con il più basso tempo medio di realizzazione per ordine
Un rank separatamente per anno. La posizione 1 va assegnata al record con il più alto numero di ordini*/
SELECT Mese, TipologiaAzienda, Anno,
  SUM(QuantitàDiCopie)/SUM(#Ordini), SUM(TempoRealizzazione)/SUM(#Ordini),
  100*SUM(#Ordini)/SUM(SUM(#Ordini)) OVER(PARTITION BY Anno),
  SUM(SUM(#Ordini)) OVER(PARTITION BY SettoreAzienda),
  RANK() OVER(PARTITION BY SettoreAzienda
              ORDER BY SUM(TempoRealizzazione)/SUM(#Ordini)),
  RANK() OVER(PARTITION BY Anno
              ORDER BY SUM(#Ordini) DESC)
FROM TEMPO T, TIPOLOGIA-AZIENDA A, ORDINI-EVASI O
WHERE T.CodT = O.CodT AND A.CodTA = O.CodTA
GROUP BY Mese, TipologiaAzienda, Anno, SettoreAzienda;
