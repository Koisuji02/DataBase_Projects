/*NEGOZIO(IDNegozio, Negozio, Città, Regione, AreaGeografica)
MODELLO-DISP-ELETTRONICO (IDModDispEle, ModelloDispElettronico, Categoria)
JUNK-CARAT-COPERTURE-ASSICURATIVE (IDJCCA, GaranziaLegale, Estensione3Anni, DanniAccidentali, Furto)
JUNK-CARAT-ACQUIRENTE (IDJCA, Genere, Residenza, FasciaEtà)
TEMPO(IDTempo, Data, Mese, 2-Mesi, 3-Mesi, 6-Mesi, Anno,)
RICHIESTE-RIMBORSO (IDNegozio, IDModDispEle, IDJCCA, IDJCA, IDTempo, #RichiesteRicevute, #RichiesteConcluse, ImportoTotRichiesto, ImportoTotApprovato, DurataTotProcesso)
Considerando gli anni dal 2010 al 2022, separatamente per modello del dispositivo elettronico, trimestre (attributo 3-Mesi) e residenza dell’acquirente, visualizzare: 

l'importo complessivo approvato, 
la durata media per richiesta conclusa
l’importo cumulativo approvato al trascorrere dei trimestre, separatamente per anno, residenza e modello del dispositivo elettronico
l’importo complessivo richiesto indipendentemente dal dispositivo elettronico.*/
SELECT ModelloDispElettronico, 3-Mesi, Residenza,
  SUM(ImportoTotApprovato),
  SUM(DurataTotProcesso)/SUM(#RichiesteConcluse),
  SUM(SUM(ImportoTotApprovato)) OVER(PARTITION BY ModelloDispElettronico, Residenza, Anno
                  ORDER BY 3-Mesi),
  SUM(SUM(ImportoTotRichiesto)) OVER(PARTITION BY 3-Mesi, Residenza, Anno)
FROM TEMPO T, RICHIESTE-RIMBORSO R, JUNK-CARAT-ACQUIRENTE A,
  MODELLO-DISP-ELETTRONICO M
WHERE T.IDTempo = R.IDTempo AND (Anno >= 2010 AND Anno <= 2022) AND
  A.IDJCA = R.IDJCA AND M.IDModDispEle = R.IDModDispEle
GROUP BY ModelloDispElettronico, 3-Mesi, Residenza, Anno;

/*Visualizzare separatamente per modello del dispositivo elettronico, città del negozio e semestre (attributo 6-mesi):

L'importo complessivo richiesto e quello approvato
Il numero di richieste ricevute e quelle concluse
L’importo complessivo approvato indipendentemente dal semestre
Il rapporto tra l’importo complessivo richiesto e l’importo complessivo richiesto di tutti i modelli dei dispositivi elettronici appartenenti alla stessa categoria, separatamente per città del negozio e semestre.
Testo della risposta Domanda 7*/
SELECT ModelloDispElettronico, Città, 6-Mesi, Categoria,
  SUM(ImportoTotRichiesto), SUM(ImportoTotApprovato),
  SUM(#RichiesteRicevute), SUM(#RichiesteConcluse),
  SUM(SUM(ImportoTotApprovato)) OVER(PARTITION BY ModelloDispElettronico, Città),
  SUM(ImportoTotRichiesto)/SUM(SUM(ImportoTotRichiesto)) OVER(
     PARTITION BY Città, 6-Mesi, Categoria)
FROM RICHIESTE-RIMBORSO R,
  NEGOZIO N, TEMPO T, MODELLO-DISP-ELETTRONICO M
WHERE T.IDTempo = R.IDTempo AND N.IDNegozio = R.IDNegozio AND
  M.IDModDispEle = R.IDModDispEle
GROUP BY ModelloDispElettronico, Città, 6-Mesi, Categoria;

/*Considerando le coperture assicurative che includono l'estensione di garanzia per 3 anni (attributo Estensione3Anni) oppure l'estensione per i danni accidentali (attributo DanniAccidentali), separatamente per bimestre (2-Mesi) e negozio, visualizzare:

La differenze tra il numero complessivo di richieste concluse e quelle ricevute
La durata media del tempo di processamento per richiesta conclusa 
Associare ad ogni record visualizzato la posizione in un ranking in funzione della durata media del tempo di processamento per richiesta conclusa separatamente per regione del negozio (1 per il record con il più basso valore durata media del tempo di processamento per richiesta).

Testo della risposta Domanda 10*/
SELECT 2-Mesi, Negozio, Regione,
  SUM(#RichiesteConcluse) - SUM(#RichiesteRicevute),
  SUM(DurataTotProcesso)/SUM(#RichiesteConcluse),
  RANK() OVER(PARTITION BY Regione
              ORDER BY SUM(DurataTotProcesso)/SUM(#RichiesteConcluse))
FROM JUNK-CARAT-COPERTURE-ASSICURATIVE C, RICHIESTE-RIMBORSO R,
  NEGOZIO N, TEMPO T
WHERE (Estensione3Anni = 'Y' OR DanniAccidentali = 'Y') AND C.IDJCCA = R.IDJCCA AND
  T.IDTempo = R.IDTempo AND N.IDNegozio = R.IDNegozio
GROUP BY 2-Mesi, Negozio, Regione;