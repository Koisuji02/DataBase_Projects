/* RICHIESTE:
NEGOZIO(IDNegozio, Negozio, Città, Regione, AreaGeografica)
MODELLO-DISP-ELETTRONICO (IDModDispEle, ModelloDispElettronico, Categoria)
JUNK-CARAT-COPERTURE-ASSICURATIVE (IDJCCA, GaranziaLegale, Estensione3Anni, DanniAccidentali, Furto)
JUNK-CARAT-ACQUIRENTE (IDJCA, Genere, Residenza, FasciaEtà)
TEMPO(IDTempo, Data, Mese, 2-Mesi, 3-Mesi, 6-Mesi, Anno,)
RICHIESTE-RIMBORSO (IDNegozio, IDModDispEle, IDJCCA, IDJCA, IDTempo, #RichiesteRicevute, #RichiesteConcluse, ImportoTotRichiesto, ImportoTotApprovato, DurataTotProcesso)
*/
/*Considerando gli anni precedenti al 2020, separatamente per modello del dispositivo elettronico, semestre (attributo 6-Mesi) e genere dell’acquirente, visualizzare: 

L’importo approvato medio per richiesta conclusa 
L’importo cumulativo approvato al trascorrere dei semestri, separatamente per modello del dispositivo elettronico
L’importo complessivo richiesto indipendentemente dal modello del dispositivo, semestre e Genere */
SELECT ModelloDispElettronico, 6-Mesi, Genere,
  SUM(ImportoTotApprovato)/SUM(#RichiesteConcluse),
  SUM(SUM(ImportoTotApprovato)) OVER(PARTITION BY ModelloDispElettronico
                                     ORDER BY 6-Mesi
                                     ROWS UNBOUNDED PRECEDING),
  SUM(SUM(ImportoTotRichiesto)) OVER()
FROM RICHIESTE-RIMBORSO R,
  TEMPO T, MODELLO-DISP-ELETTRONICO M, JUNK-CARAT-ACQUIRENTE A
WHERE M.IDModDispEle = R.IDModDispEle AND
  T.IDTempo = R.IDTempo AND A.IDJCA = R.IDJCA
  AND Anno < 2020
GROUP BY ModelloDispElettronico, 6-Mesi, Genere;

/*Visualizzare separatamente per negozio, categoria del dispositivo elettronico e bimestre (attributo 2-mesi):

La percentuale dell'importo richiesto che è stato approvato
La differenza tra il numero di richieste ricevute e quelle concluse
L’importo complessivo approvato indipendentemente dal negozio
Il rapporto tra l’importo approvato e l’importo complessivo di tutti i negozi siti nella stessa città, separatamente per categoria e bimestre */
SELECT Negozio, Categoria, 2-Mesi, Città,
  100*SUM(ImportoTotApprovato)/SUM(ImportoTotRichiesto),
  SUM(#RichiesteRicevute) - SUM(#RichiesteConcluse),
  SUM(SUM(ImportoTotApprovato)) OVER(PARTITION BY Categoria, 2-Mesi),
  SUM(ImportoTotApprovato)/SUM(SUM(ImportoTotApprovato)) OVER(PARTITION BY Categoria, 2-Mesi, Città)
FROM RICHIESTE-RIMBORSO R,
  NEGOZIO N, TEMPO T, MODELLO-DISP-ELETTRONICO M
WHERE M.IDModDispEle = R.IDModDispEle AND
  N.IDNegozio = R.IDNegozio AND T.IDTempo = R.IDTempo
GROUP BY Negozio, Categoria, 2-Mesi, Città;

/*Considerando le coperture assicurative che includono solo la garanzia legale (attributo GaranziaLegale), separatamente per trimestre e città, visualizzare:

La percentuale di richieste concluse rispetto a quelle ricevute
La durata media del tempo di processamento per richiesta ricevuta 
Associare ad ogni record visualizzato la posizione in un ranking:

in funzione dell’importo complessivo approvato separatamente per area geografica (1 per il record con il più basso valore complessivo approvato)
in funzione della differenza tra importo richiesto e importo approvato (1 per il record con il più alto valore della differenza tra importo richiesto e importo approvato complessivi) separatamente per anno
*/
SELECT 3-Mesi, Città, AreaGeografica, Anno,
  100*SUM(#RichiesteConcluse)/SUM(#RichiesteRicevute),
  SUM(DurataTotProcesso)/SUM(#RichiesteRicevute),
  RANK() OVER(PARTITION BY AreaGeografica ORDER BY SUM(ImportoTotApprovato)),
  RANK() OVER(PARTITION BY Anno ORDER BY (SUM(ImportoTotRichiesto)-SUM(ImportoTotApprovato)) DESC)
FROM JUNK-CARAT-COPERTURE-ASSICURATIVE CA, RICHIESTE-RIMBORSO R,
  NEGOZIO N, TEMPO T
WHERE CA.IDJCCA = R.IDJCCA AND GaranziaLegale = True AND Estensione3Anni = False AND DanniAccidentali = False AND  Furto= False AND
  N.IDNegozio = R.IDNegozio AND T.IDTempo = R.IDTempo
GROUP BY 3-Mesi, Città, AreaGeografica, Anno;