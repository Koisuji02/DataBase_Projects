/* RICHIESTE:
CARATTERISTICHE-CORSO-CERTIFICAZIONE (IDCarattCorsoCert, Tematica, TipoCertificazione, LivelloCertificazione, PresenzaRequisitiAmmissione, ModalitàErogazione) 
CARATTERISTICHE-ISCRITTI (IDCarattIscritti, FasciaEtà, Professione, Genere)
SEDE-CERTIFICAZIONE (IDSedeCertificazione, CittàSede, RegioneSede, StatoSede, NumeroAule, NumeroDocenti) 
TEMPO (IDTempo, Mese, 2-Mesi, 3-Mesi, 4-Mesi, 6-Mesi, Anno) 
EROGAZIONE-CORSI (IDCarattCorsoCert, IDCarattIscritti, IDSedeCertificazione, IDTempo, IncassoTotale, NumeroOreErogate, NumeroIscritti, NumeroSuperi)*/

/*Considerando le sedi di certificazione site in Italia, separatamente per modalità di erogazione del corso e semestre (attributo 6-Mesi) visualizzare

Il numero complessivo di ore erogate 
Il numero medio di superi per mese
Il rapporto tra il numero di iscritti rispetto al numero complessivo di iscritti per anno e modalità di erogazione
la posizione in una graduatoria (rank) in ordine decrescente rispetto al numero di ore complessive erogate.
Si effettui l'analisi separatamente per Tematica.*/
SELECT ModalitàErogazione, 6-Mesi, Anno, Tematica,
  SUM(NumeroOreErogate),
  SUM(NumeroSuperi)/COUNT(DISTINCT Mese),
  SUM(NumeroIscritti)/SUM(SUM(NumeroIscritti)) OVER(PARTITION BY ModalitàErogazione, Anno),
  RANK() OVER(ORDER BY SUM(NumeroOreErogate) DESC)
FROM CARATTERISTICHE-CORSO-CERTIFICAZIONE C, 
  TEMPO T, SEDE-CERTIFICAZIONE S, EROGAZIONE-CORSI E
WHERE C.IDCarattCorsoCert = E.IDCarattCorsoCert AND
  T.IDTempo = E.IDTempo AND
  S.IDSedeCertificazione = E.IDSedeCertificazione AND
  StatoSede = 'Italia'
GROUP BY ModalitàErogazione, 6-Mesi, Anno, Tematica;

/*Separatamente per tipo certificazione e bimestre (attributo 2-Mesi), visualizzare:
l’incasso, 
il rapporto tra il numero di superi e il numero di iscritti,
la percentuale del numero di superi rispetto al totale semestrale di superi per tipo di certificazione
il totale cumulativo dell’incasso al trascorrere dei bimestri, separatamente per semestre e tipo di certificazione.
Si effettui l'analisi separatamente per genere.*/
SELECT TipoCertificazione, 2-Mesi, 6-Mesi
  SUM(IncassoTotale),
  SUM(NumeroSuperi)/SUM(NumeroIscritti),
  100*SUM(NumeroSuperi)/SUM(SUM(NumeroSuperi)) OVER(PARTITION BY TipoCertificazione, 6-Mesi),
  SUM(SUM(IncassoTotale)) OVER(PARTITION BY TipoCertificazione, 6-Mesi
                               ORDER BY 2-Mesi
                               ROWS UNBOUNDED PRECEDING)
FROM CARATTERISTICHE-CORSO-CERTIFICAZIONE C,
  TEMPO T, EROGAZIONE-CORSI E
WHERE C.IDCarattCorsoCert = E.IDCarattCorsoCert AND T.IDTempo = E.IDTempo
GROUP BY TipoCertificazione, 2-Mesi, 6-Mesi;

/*Considerando i corsi con modalità di erogazione online, separatamente per professione degli iscritti, trimestre (3-Mesi) e stato della sede di certificazione visualizzare
Il numero di iscritti e il numero di superi
l'incasso medio per numero di iscritti 
l'incasso medio mensile
l'incasso complessivo separatamente per professione degli iscritti, stato della sede di certificazione e anno,
la posizione in una graduatoria (rank) in ordine decrescente rispetto al numero di superi, separatamente per anno.
*/
SELECT Professione, 3-Mesi, StatoSede, Anno,
  SUM(NumeroIscritti), SUM(NumeroSuperi),
  SUM(IncassoTotale)/SUM(NumeroIscritti),
  SUM(IncassoTotale)/COUNT(DISTINCT Mese),
  SUM(SUM(IncassoTotale)) OVER(PARTITION BY Professione, 3-Mesi, StatoSede, Anno),
  RANK() OVER(PARTITION BY Anno ORDER BY SUM(NumeroSuperi) DESC)
FROM CARATTERISTICHE-CORSO-CERTIFICAZIONE C, CARATTERISTICHE-ISCRITTI I,
  TEMPO T, SEDE-CERTIFICAZIONE S, EROGAZIONE-CORSI E
WHERE C.IDCarattCorsoCert = E.IDCarattCorsoCert AND
  I.IDCarattIscritti = E.IDCarattIscritti AND T.IDTempo = E.IDTempo AND
  S.IDSedeCertificazione = E.IDSedeCertificazione AND
  ModalitàErogazione = 'online'
GROUP BY Professione, 3-Mesi, StatoSede, Anno;