/* QUERY 1 */
SELECT Tipo, Mese, Anno, SUM(Prezzo)/COUNT(DISTINCT Data) AS EntrateMedieGiornaliere,
    SUM(SUM(Prezzo)) OVER(PARTITION BY Anno ORDER BY Mese) AS EntrateCumulative,
    100*COUNT(*)/(SUM(COUNT(*)) OVER(PARTITION BY Mese)) AS PercTipoMese
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
GROUP BY Tipo, Mese, Anno;

/* QUERY 2 */
SELECT Tipo, NomeM, Categoria, AVG(Prezzo) AS RicavoMedioBiglietto,
    100*(SUM(SUM(Prezzo)) OVER(PARTITION BY Categoria, Tipo))/SUM(SUM(Prezzo)) OVER() AS PercRicavoTot,
    RANK() OVER(PARTITION BY Tipo ORDER BY COUNT(*) DESC) AS RankTipoBiglietto
FROM BIGLIETTO B, MUSEO M, TEMPO T
WHERE B.CodM = M.CodM
    AND B.CodT = T.CodT
    AND Anno = 2021
GROUP BY Tipo, NomeM, Categoria;

/* QUERY DI INTERESSE */
SELECT Tipo, Mese, 6M, AVG(Prezzo) AS EntrateMedieMensili,
    SUM(SUM(Prezzo))/SUM(COUNT(*)) OVER(PARTITION BY Tipo, 6M) AS EntrateMedieSemestrali
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
GROUP BY Tipo, Mese, 6M;

SELECT Tipo, Mese, SUM(SUM(Prezzo)) OVER(PARTITION BY Anno ORDER BY Mese UNBOUNDED PRECEDING) AS SommaCumulativaAnnuale
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
GROUP BY Tipo, Mese, Anno;

SELECT Tipo, Mese, COUNT(*) AS TotBiglietti, SUM(Prezzo) AS EntrateTotali,
    SUM(SUM(Prezzo))/SUM(COUNT(*)) OVER(PARTITION BY Tipo) AS EntrateTipoBiglietto
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
    AND Acquisto = "Online"
GROUP BY Tipo, Mese;

SELECT Tipo, Mese, COUNT(*) AS TotBiglietti, SUM(Prezzo) AS EntrateTotali,
    SUM(SUM(Prezzo))/SUM(COUNT(*)) OVER(PARTITION BY Tipo) AS EntrateTipoBiglietto
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
    AND Anno = 2021
GROUP BY Tipo, Mese;

SELECT Tipo, Mese, 100*COUNT(*)/(SUM(COUNT(*)) OVER(PARTITION BY Mese)) AS PercTipoMese
FROM BIGLIETTO B, TEMPO T
WHERE B.CodT = T.CodT
GROUP BY Tipo, Mese;

/* VM */
CREATE MATERIALIZED VIEW Vm1
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
AS
    SELECT Tipo, Mese, 6M, Anno, Acquisto, SUM(Prezzo) AS EntrateTotali, AVG(Prezzo) AS EntrateMedieMensili
    FROM BIGLIETTO B, TEMPO T
    WHERE B.CodT = T.CodT
    GROUP BY Tipo, Mese, 6M, Anno, Acquisto;

/* LOG */
CREATE MATERIALIZED VIEW LOG ON TEMPO
WITH SEQUENCE, ROWID
(CodT, Mese, 6M, Anno)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON BIGLIETTO
WITH SEQUENCE, ROWID
(CodM, CodT, Tipo, Acquisto, Prezzo)
INCLUDING NEW VALUES;
/*tutti gli insert e update su tempo e biglietto che cambiano gli attributi qua scritti*/

/* TRIGGER */