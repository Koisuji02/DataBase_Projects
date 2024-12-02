CREATE OR REPLACE TRIGGER updateTipoServizio
AFTER
    UPDATE OF TipologiaServizio
    ON Servizio
FOR EACH ROW
DECLARE N NUMBER;

BEGIN
--- verifico se la TipologiaServizio è presente nella vista materializzata

SELECT COUNT(*) INTO N
FROM ViewIncassi
WHERE TipologiaServizio = :OLD.TipologiaServizio;

IF (N > 0) THEN
    --- nella VM c'è la vecchia TipologiaServizio da aggiornare
    UPDATE ViewIncassi
    SET TipologiaServizio = :NEW.TipologiaServizio
    WHERE TipologiaServizio = :OLD.TipologiaServizio;
END IF;
--- N = 0, ovvero non ho righe da aggiornare nella VM, non faccio nulla
END;