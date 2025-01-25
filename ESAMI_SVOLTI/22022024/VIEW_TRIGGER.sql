/*VILLAGGIO(CodV, Villaggio, CittàV, ProvinciaV, RegioneV, StatoV, Ristorazione, Animazione, BabyParking)
TEMPO(CodT, Data, Mese, 3M, 4M, 6M, Anno, Settimana, Periodo)
JUNK-ALLOGGIO-PAGAMENTO(CodAP, TipologiaAlloggio, ModalitàPagamento)
CARATTERISTICHE-CLIENTE(CodCC, CittàCliente, ProvinciaC, RegioneC, StatoC)
GESTIONE-VILLAGGI(CodV, CodT, CodAP, CodCC, Incasso, NumOspiti)*/
/*Dato lo schema logico precedente, considerare le seguenti query di interesse: 

a. Separatamente per trimestre (attributo 3M), considerando la modalità di pagamento con bonifico, visualizzare il numero totale di ospiti e il numero medio di ospiti per regione del villaggio (attributo RegioneV).

b. Separatamente per anno, regione di localizzazione del villaggio (attributo RegioneV), considerando i villaggio che dispongono di servizio di ristorazione, visualizzare il numero complessivo di ospiti e l'incasso totale.

c. Per i villaggi localizzati in Italia (attributo StatoV), visualizzare l'incasso cumulativo annuale al trascorrere dei quadrimestri (attributo 4M).



Dato lo schema logico precedente, si svolgano le seguenti attività

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
            CREATE MATERIALIZED VIEW ViewVillaggi
            BUILD IMMEDIATE
            REFRESH FAST ON COMMIT
            AS
		Blocco A
2. Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewVillaggi.

3. Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewVillaggi in caso di inserimento di un nuovo record nella tabella dei fatti GESTIONE-VILLAGGI. */
--- VISTA
CREATE MATERIALIZED VIEW ViewVillaggi
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS(
SELECT RegioneV, StatoV, Ristorazione, 3M, 4M, Anno, ModalitàPagamento,
  SUM(NumOspiti) AS No, SUM(Incasso) AS In
FROM GESTIONE-VILLAGGI G, VILLAGGIO V, TEMPO T,
  JUNK-ALLOGGIO-PAGAMENTO P
WHERE V.CodV = G.CodV AND T.CodT = G.CodT AND P.CodAP = G.CodAP
GROUP BY RegioneV, StatoV, Ristorazione, 3M, 4M, Anno, ModalitàPagamento
);
--- INSIEME MINIMALE
RegioneV, Ristorazione, 3M, 4M, ModalitàPagamento

--- TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewVillaggi
AFTER INSERT ON GESTIONE-VILLAGGI
FOR EACH ROW
DECLARE
N NUMBER;
VRegioneV, VStatoV, VRistorazione, V3M, V4M, VAnno, VModalitàPagamento VARCHAR(20);
BEGIN

SELECT RegioneV, StatoV, Ristorazione INTO VRegioneV, VStatoV, VRistorazione
FROM VILLAGGIO V
WHERE V.CodV = :NEW.CodV;

SELECT 3M, 4M, Anno INTO V3M, V4M, VAnno
FROM TEMPO T
WHERE T.CodT = :NEW.CodT;

SELECT ModalitàPagamento INTO VModalitàPagamento
FROM JUNK-ALLOGGIO-PAGAMENTO P
WHERE P.CodAP = :NEW.CodAP;

SELECT COUNT(*) INTO N
FROM ViewVillaggi
WHERE RegioneV = VRegioneV, Ristorazione = VRistorazione, 3M = V3M, 4M = V4M, ModalitàPagamento = VModalitàPagamento;

IF(N>0) THEN
---UPDATE
UPDATE ViewVillaggi
SET No = No + :NEW.NumOspiti, In = In + :NEW.Incasso
WHERE RegioneV = VRegioneV, Ristorazione = VRistorazione, 3M = V3M, 4M = V4M, ModalitàPagamento = VModalitàPagamento;

ELSE
---INSERT
INSERT INTO ViewVillaggi(RegioneV, StatoV, Ristorazione, 3M, 4M, Anno, ModalitàPagamento, No, In)
VALUES(VRegioneV, VStatoV, VRistorazione, V3M, V4M, VAnno, VModalitàPagamento, :NEW.NumOspiti, :NEW.Incasso);

ENDIF

END