/* RICHIESTE:
INTERMEDIARIO-FINANZIARIO(IDIntermediarioFinanziario, IntermediarioFinanziario, Città, Regione, Stato)
PRODOTTO-FINANZIARIO(IDProdottoFinanziario, ProdottoFinanziario, Società, SedeLegale, CategoriaP)
JUNK-CARATTERISTICHE-CLIENTE(IDJCC, CategoriaC, PropensioneRischio, Professione, Nazionalità)
TEMPO(IDTempo, Mese, Mese-Anno, 2-Mesi, 3-Mesi, 4-Mesi, 6-Mesi, Anno)
ACQUISTO(IDIntermediarioFinanziario, IDProdottoFinanziario, IDJCC, IDTempo, #Acquisti, ValoreAcquisti)

Dato lo schema logico precedente, considerare le seguenti query di interesse: 

a. Considerando gli acquisti effettuati da clienti con nazionalità italiana e propensione al rischio ‘alta’ (attributo PropensioneRischio), visualizzare il numero complessivo di acquisti, il valore complessivo corrispondente e il valore medio per acquisto, separatamente per bimestre (attributo 2-Mesi) e regione dell’intermediario. 

b. Considerando gli anni 2021 e 2022, separatamente per anno e città dell’intermediario, visualizzare il valore complessivo e il valore medio bimestrale degli acquisti. 

c. Separatamente per nazionalità del cliente e semestre (attributo 6-Mesi), visualizzare il valore medio per acquisto.  



Dato lo schema logico precedente, si svolgano le seguenti attività:

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
CREATE MATERIALIZED VIEW ViewAcquisti

BUILD IMMEDIATE

REFRESH FAST ON COMMIT

AS

		Blocco A


2. Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewAcquisti..


3. Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewAcquisti in caso di inserimento di un nuovo record nella tabella dei fatti ACQUISTI. 
*/

--- VISTA
CREATE MATERIALIZED VIEW ViewAcquisti
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS (
SELECT Nazionalità, PropensioneRischio, 2-Mesi, Regione, Anno,
  Città, 6-Mesi
  SUM(#Acquisti) AS ATot, SUM(ValoreAcquisti) AS VTot
FROM JUNK-CARATTERISTICHE-CLIENTE C, ACQUISTO A,
  INTERMEDIARIO-FINANZIARIO I, TEMPO T
WHERE I.IDIntermediarioFinanziario = A.IDIntermediarioFinanziario AND
  C.IDJCC = A.IDJCC AND T.IDTempo = A.IDTempo
GROUP BY 2-Mesi, Regione, Anno, Città, Nazionalità, 6-Mesi
);

--- INSIEME MINIMALE
Nazionalità, PropensioneRischio, 2-Mesi, Città

---TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewAcquisti
AFTER INSERT ON ACQUISTI
FOR EACH ROW
DECLARE
N NUMBER;
VNazionalità, VPropensioneRischio, V2-Mesi, VRegione, VAnno, VCittà, V6-Mesi VARCHAR(20);
BEGIN

SELECT Nazionalità, PropensioneRischio INTO VNazionalità, VPropensioneRischio
FROM JUNK-CARATTERISTICHE-CLIENTE 
WHERE C.IDJCC = :NEW.IDJCC;

SELECT 2-Mesi, Anno, 6-Mesi INTO V2-Mesi, VAnno, V6-Mesi
FROM TEMPO
WHERE T.IDTempo = :NEW.IDTempo;

SELECT Regione, Città INTO VRegione, VCittà
FROM INTERMEDIARIO-FINANZIARIO
WHERE I.IDIntermediarioFinanziario = :NEW.IDIntermediarioFinanziario;

SELECT COUNT(*) INTO N
FROM ViewAcquisti
WHERE Nazionalità = VNazionalità AND PropensioneRischio = VPropensioneRischio AND 2-Mesi = V2-Mesi AND Città = VCittà;

IF(N>0) THEN
---UPDATE
UPDATE ViewAcquisti
SET ATot = ATot + :NEW.#Acquisti, VTot = VTot + :NEW.ValoreAcquisti
WHERE Nazionalità = VNazionalità AND PropensioneRischio = VPropensioneRischio AND 2-Mesi = V2-Mesi AND Città = VCittà;

ELSE
---INSERT
INSERT INTO ViewAcquisti(Nazionalità, PropensioneRischio, 2-Mesi, Regione, Anno, Città, 6-Mesi,ATot,VTot)
VALUES(VNazionalità, VPropensioneRischio, V2-Mesi, VRegione, VAnno, VCittà, V6-Mesi, :NEW.#Acquisti, #NEW.ValoreAcquisti);

ENDIF

END