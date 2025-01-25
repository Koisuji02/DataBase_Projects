/*RICHIESTE:
NEGOZIO(IDNegozio, Negozio, Città, Regione, AreaGeografica)
MODELLO-DISP-ELETTRONICO (IDModDispEle, ModelloDispElettronico, Categoria)
JUNK-CARAT-COPERTURE-ASSICURATIVE (IDJCCA, GaranziaLegale, Estensione3Anni, DanniAccidentali, Furto)
JUNK-CARAT-ACQUIRENTE (IDJCA, Genere, Residenza, FasciaEtà)
TEMPO(IDTempo, Data, Mese, 2-Mesi, 3-Mesi, 6-Mesi, Anno,)
RICHIESTE-RIMBORSO (IDNegozio, IDModDispEle, IDJCCA, IDJCA, IDTempo, #RichiesteRicevute, #RichiesteConcluse, ImportoTotRichiesto, ImportoTotApprovato, DurataTotProcesso)
Dato lo schema logico precedente, considerare le seguenti query di interesse: 

a. Considerando l’area geografica nord, separatamente per regione e anno, visualizzare il numero complessivo di richieste concluse, l’importo complessivo approvato e la durata media mensile di processamento delle richieste.

b. Considerando le coperture assicurative che includono i danni accidentali (attributo DanniAccidentali), ma non il furto (attributo Furto), separatamente per mese e area geografica, visualizzare il numero complessivo di richieste concluse e la differenza tra l’importo complessivo richiesto e quello approvato.

c. Considerando gli anni 2021 e 2022, separatamente per semestre (attributo 6-Mesi) e regione, visualizzare il numero complessivo di richieste concluse e il corrispondente importo medio approvato.



Dato lo schema logico precedente, si svolgano le seguenti attività

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
            CREATE MATERIALIZED VIEW ViewRimborsi
            BUILD IMMEDIATE
            REFRESH FAST ON COMMIT
            AS
		Blocco A
2. Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewRimborsi.

3. Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewRimborsi in caso di inserimento di un nuovo record nella tabella dei fatti RICHIESTE-RIMBORSO. */

--- VISTA
CREATE MATERIALIZED VIEW ViewRimborsi
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS(
SELECT AreaGeografica, Regione, Anno, Mese, DanniAccidentali,
  Furto, 6Mesi,
  SUM(#RichiesteConcluse) AS Rc, SUM(ImportoTotApprovato) AS Ia,
  SUM(DurataTotProcesso) AS Dp, SUM(ImportoTotRichiesto) AS Ir
FROM NEGOZIO N, TEMPO T, RICHIESTE-RIMBORSO R,
  JUNK-CARAT-COPERTURE-ASSICURATIVE A
WHERE N.IDNegozio = R.IDNegozio AND T.IDTempo = R.IDTempo AND
  A.IDJCCA = R.IDJCCA
GROUP BY Regione, Anno, AreaGeografica, Mese, 6Mesi
);
--- INSIEME MINIMALE DI ATTRIBUTI
Regione, Mese, DanniAccidentali, Furto
--- TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewRimborsi
AFTER INSERT ON RICHIESTE-RIMBORSO
FOR EACH ROW
DECLARE
N NUMBER;
VAreaGeografica, VRegione, VAnno, VMese, VDanniAccidentali, VFurto, V6Mesi VARCHAR(20);
BEGIN

SELECT AreaGeografica, Regione INTO VAreaGeografica, VRegione
FROM NEGOZIO N
WHERE N.IDNegozio = :NEW.IDNegozio;

SELECT Anno, Mese, 6Mesi INTO VAnno, VMese, V6Mesi
FROM TEMPO T
WHERE T.IDTempo = :NEW.IDTempo;

SELECT DanniAccidentali, Furto INTO VDanniAccidentali, VFurto
FROM JUNK-CARAT-COPERTURE-ASSICURATIVE A
WHERE A.IDJCCA = :NEW.IDJCCA;

SELECT COUNT(*) INTO N
FROM ViewRimborsi
WHERE AreaGeografica = VAreaGeografica, Regione = VRegione, Anno = VAnno, Mese = VMese, DanniAccidentali = VDanniAccidentali, Furto = VFurto;

IF(N>0) THEN
---UPDATE
UPDATE ViewRimborsi
SET Rc = Rc + :NEW.#RichiesteConcluse, Ia = Ia + :NEW.ImportoTotApprovato,
  Dp = Dp + :NEW.DurataTotProcesso, Ir = Ir + :NEW.ImportoTotRichiesto
WHERE AreaGeografica = VAreaGeografica, Regione = VRegione, Anno = VAnno, Mese = VMese, DanniAccidentali = VDanniAccidentali, Furto = VFurto;

ELSE
---INSERT
INSERT INTO ViewRimborsi(AreaGeografica, Regione, Anno, Mese, DanniAccidentali, Furto, 6Mesi, Rc, Ia, Dp, Ir)
VALUES(VAreaGeografica, VRegione, VAnno, VMese, VDanniAccidentali, VFurto, V6Mesi, :NEW.#RichiesteConcluse, :NEW.ImportoTotApprovato, :NEW.DurataTotProcesso, :NEW.ImportoTotRichiesto);

ENDIF
END