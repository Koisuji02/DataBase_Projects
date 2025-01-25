/*NEGOZIO(IDNegozio, Negozio, Città, Regione, AreaGeografica)
MODELLO-DISP-ELETTRONICO (IDModDispEle, ModelloDispElettronico, Categoria)
JUNK-CARAT-COPERTURE-ASSICURATIVE (IDJCCA, GaranziaLegale, Estensione3Anni, DanniAccidentali, Furto)
JUNK-CARAT-ACQUIRENTE (IDJCA, Genere, Residenza, FasciaEtà)
TEMPO(IDTempo, Data, Mese, 2-Mesi, 3-Mesi, 6-Mesi, Anno,)
RICHIESTE-RIMBORSO (IDNegozio, IDModDispEle, IDJCCA, IDJCA, IDTempo, #RichiesteRicevute, #RichiesteConcluse, ImportoTotRichiesto, ImportoTotApprovato, DurataTotProcesso)*/
/*Dato lo schema logico precedente, considerare le seguenti query di interesse: 

a. Separatamente per città del negozio e semestre, visualizzare la differenza tra il numero complessivo di richieste ricevute e quelle concluse e l’importo complessivo approvato.

b. Considerando le coperture assicurative che includono furto (Attributo Furto) o estensione di tre anni (attributo Estensione3Anni), separatamente per anno e città del negozio, visualizzare il rapporto tra il numero di richieste concluse rispetto a quelle ricevute, e la durata media trimestrale (attributo 3-Mesi) di processamento delle richieste.

c. Separatamente per anno e regione del negozio, visualizzare il numero complessivo di richieste concluse e il corrispondente importo complessivo approvato.

Dato lo schema logico precedente, si svolgano le seguenti attività

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
            CREATE MATERIALIZED VIEW ViewRichiesteRimborso
            BUILD IMMEDIATE
            REFRESH FAST ON COMMIT
            AS
		Blocco A
2. Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewRichiesteRimborso.

3. Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewRichiesteRimborso in caso di inserimento di un nuovo record nella tabella dei fatti RICHIESTE-RIMBORSO. */
--- VISTA
CREATE MATERIALIZED VIEW ViewRichiesteRimborso
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS (
SELECT Città, Regione, 3-Mesi, 6-Mesi, Anno, Furto, Estensione3Anni,
  SUM(#RichiesteRicevute) AS Rr, SUM(#RichiesteConcluse) AS Rc,
  SUM(ImportoTotApprovato) AS Ia, SUM(DurataTotProcesso) AS Dp
FROM RICHIESTE-RIMBORSO R,
  NEGOZIO N, TEMPO T, JUNK-CARAT-COPERTURE-ASSICURATIVE A
WHERE T.IDTempo = R.IDTempo AND N.IDNegozio = R.IDNegozio AND
  A.IDJCCA = R.IDJCCA
GROUP BY Città, Regione, 3-Mesi, 6-Mesi, Anno, Furto, Estensione3Anni
);

--- INSIEME MINIMALE
Città, 3-Mesi, Furto, Estensione3Anni

--- TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewRichiesteRimborso
AFTER INSERT ON RICHIESTE-RIMBORSO
FOR EACH ROW
DECLARE
N NUMBER;
VCittà, VRegione, V3-Mesi, V6-Mesi, VAnno, VFurto, VEstensione3Anni VARCHAR(20);
BEGIN

SELECT Città, Regione INTO VCittà, VRegione
FROM NEGOZIO N
WHERE N.IDNegozio = :NEW.IDNegozio;

SELECT 3-Mesi, 6-Mesi, Anno INTO V3-Mesi, V6-Mesi, VAnno
FROM TEMPO T
WHERE T.IDTempo = :NEW.IDTempo;

SELECT Furto, Estensione3Anni INTO VFurto, VEstensione3Anni
FROM JUNK-CARAT-COPERTURE-ASSICURATIVE A
WHERE A.IDJCCA = :NEW.IDJCCA;

SELECT COUNT(*) INTO N
FROM ViewRichiesteRimborso
WHERE VCittà = Città, VRegione = Regione, V3-Mesi = 3-Mesi, V6-Mesi = 6-Mesi, VAnno = Anno, VFurto = Furto, VEstensione3Anni = Estensione3Anni;

IF(N>0) THEN
---UPDATE
UPDATE ViewRichiesteRimborso
SET Rr = Rr + :NEW.#RichiesteRicevute, Rc = Rc + :NEW.#RichiesteConcluse,
    Ia = Ia + :NEW.ImportoTotApprovato, Dp = Dp + :NEW.DurataTotProcesso
WHERE VCittà = Città, VRegione = Regione, V3-Mesi = 3-Mesi, V6-Mesi = 6-Mesi, VAnno = Anno, VFurto = Furto, VEstensione3Anni = Estensione3Anni;

ELSE
---INSERT
INSERT INTO ViewRichiesteRimborso(Città, Regione, 3-Mesi, 6-Mesi, Anno, Furto, Estensione3Anni, Rr, Rc, Ia, Dp)
VALUES(VCittà, VRegione, V3-Mesi, V6-Mesi, VAnno, VFurto, VEstensione3Anni,
  :NEW.#RichiesteRicevute, :NEW.#RichiesteConcluse, :NEW.ImportoTotApprovato, :NEW.DurataTotProcesso);

ENDIF

END