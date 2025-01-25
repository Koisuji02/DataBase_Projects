/*COPISTERIA(CodC, Copisteria, CittàC, ProvinciaC, RegioneC, StatoC, Brochure, Volantini, Calendari, Stampa_foto, Fotocopie)
TEMPO(CodT, Data, GiornoSettimana,  Settimana, Mese, 3-Mesi, 4-Mesi, 6-Mesi, Anno)
JUNK- DIMENSION(CodJK, TipoPagamento, TipoConsegna, TipoServizioRichiesto, ModalitàRealizzazione)
TIPOLOGIA-AZIENDA(CodTA, TipologiaAzienda, SettoreAzienda)
LOCALIZZAZIONE-AZIENDA (CodCA,  CittàAzienda, ProvinciaAzienda, RegioneAzienda, StatoAzienda)
ORDINI-EVASI(CodC, CodT, CodJK, CodTA, CodCA, #Ordini, TempoRealizzazione, Costo, QuantitàDiCopie)*/
/*Dato lo schema logico precedente, considerare le seguenti query di interesse: 

a. Separatamente per anno e tipo di servizio (attributo TipoServizioRichiesto), visualizzare la percentuale del numero di ordini (attributo #Ordini) rispetto al complessivo degli ordini per anno e la percentuale del tempo di realizzazione rispetto al tempo totale di realizzazione per anno.

b. Separatamente per semestre (attributo 6-Mesi), tipo di pagamento (attributo TipoPagamento) e regione dell’azienda (RegioneAzienda), visualizzare il numero complessivo di ordini, il costo complessivo e il costo medio per ordine.

c. Per le aziende localizzate in provincia di Torino (attributo ProvinciaAzienda), visualizzare il costo cumulativo annuale al trascorrere dei quadrimestri (attributo 4-Mesi).



Dato lo schema logico precedente, si svolgano le seguenti attività

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
            CREATE MATERIALIZED VIEW ViewOrdini
            BUILD IMMEDIATE
            REFRESH FAST ON COMMIT
            AS
		Blocco A
2. Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewOrdini.

3. Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewOrdini in caso di inserimento di un nuovo record nella tabella dei fatti ORDINI_EVASI. 

*/
CREATE MATERIALIZED VIEW ViewOrdini
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS(
SELECT 4-Mesi, 6-Mesi, Anno, TipoServizioRichiesto, TipoPagamento, ProvinciaAzienda, RegioneAzienda,
  SUM(#Ordini) AS Od, SUM(TempoRealizzazione) AS Tr,
  SUM(Costo) AS Co
FROM TEMPO T, JUNK- DIMENSION J, LOCALIZZAZIONE-AZIENDA LA,
  ORDINI-EVASI O
WHERE T.CodT = O.CodT AND J.CodJK = O.CodJK AND LA.CodCA = O.CodCA
GROUP BY
);
--- INSIEME MINIMALE
4-Mesi, 6-Mesi, TipoServizioRichiesto, TipoPagamento, ProvinciaAzienda

--- TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewOrdini
AFTER INSERT ON ORDINI-EVASI
FOR EACH ROW
DECLARE
N NUMBER;
V4-Mesi, V6-Mesi, VAnno, VTipoServizioRichiesto, VTipoPagamento, VProvinciaAzienda, VRegioneAzienda VARCHAR(20);
BEGIN

SELECT 4-Mesi, 6-Mesi, Anno INTO V4-Mesi, V6-Mesi, VAnno
FROM TEMPO T
WHERE T.CodT = :NEW.CodT;

SELECT TipoServizioRichiesto, TipoPagamento INTO VTipoServizioRichiesto, VTipoPagamento
FROM JUNK- DIMENSION J
WHERE J.CodJK = :NEW.CodJK;

SELECT ProvinciaAzienda, RegioneAzienda INTO VProvinciaAzienda, VRegioneAzienda
FROM LOCALIZZAZIONE-AZIENDA LA
WHERE LA.CodCA = :NEW.CodCA;

SELECT COUNT(*) INTO N
FROM ViewOrdini
WHERE 4-Mesi = V4-Mesi, 6-Mesi = V6-Mesi, TipoServizioRichiesto = VTipoServizioRichiesto, TipoPagamento = VTipoPagamento, ProvinciaAzienda = VProvinciaAzienda;

IF(N>0) THEN
---UPDATE
UPDATE ViewOrdini
SET Od = Od +:NEW.#Ordini, Tr = Tr + :NEW.TempoRealizzazione, Co = Co + :NEW.Costo
WHERE 4-Mesi = V4-Mesi, 6-Mesi = V6-Mesi, TipoServizioRichiesto = VTipoServizioRichiesto, TipoPagamento = VTipoPagamento, ProvinciaAzienda = VProvinciaAzienda;

ELSE
---INSERT
INSERT INTO ViewOrdini(4-Mesi, 6-Mesi, Anno, TipoServizioRichiesto, TipoPagamento, ProvinciaAzienda, RegioneAzienda, Od, Tr, Co)
VALUES(V4-Mesi, V6-Mesi, VAnno, VTipoServizioRichiesto, VTipoPagamento, VProvinciaAzienda, VRegioneAzienda, :NEW.#Ordini, :NEW.TempoRealizzazione, :NEW.Costo);

ENDIF

END