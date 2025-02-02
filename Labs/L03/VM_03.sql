/*QUERY 1*/
SELECT PHONERATETYPE, DATEMONTH, SUM(PRICE) AS IncassoTotTariffaMese,
    SUM(SUM(PRICE)) OVER () AS IncassoTot,
    SUM(SUM(PRICE)) OVER (PARTITION BY PHONERATETYPE) AS IncassoTariffa,
    SUM(SUM(PRICE)) OVER (PARTITION BY DATEMONTH) AS IncassoMese
FROM TIMEDIM T, PHONERATE P, FACTS F
WHERE T.ID_TIME = F.ID_TIME
    AND P.ID_PHONERATE = F.ID_PHONERATE
    AND DATEYEAR = 2003
GROUP BY PHONERATETYPE, DATEMONTH;

/*QUERY 2*/
SELECT DATEMONTH, SUM(NUMBEROFCALLS) AS ChiamateTot,
    SUM(PRICE) AS IncassoTot,
    RANK() OVER(ORDER BY SUM(PRICE) DESC) AS RankPrice
FROM TIMEDIM T, FACTS F
WHERE T.ID_TIME = F.ID_TIME
GROUP BY DATEMONTH;

/*QUERY 3*/
SELECT DATEMONTH, SUM(NUMBEROFCALLS) AS ChiamateTot,
    RANK() OVER(ORDER BY SUM(NUMBEROFCALLS) DESC) AS RankNCalls
FROM TIMEDIM T, FACTS F
WHERE T.ID_TIME = F.ID_TIME
    AND DATEYEAR = 2003
GROUP BY DATEMONTH;

/*QUERY 4*/
SELECT PHONERATETYPE, SUM(PRICE) AS IncassoTot
FROM TIMEDIM T, FACTS F, PHONERATE P
WHERE T.ID_TIME = F.ID_TIME
    AND P.ID_PHONERATE = F.ID_PHONERATE
    AND DATEMONTH = '7-2003'
GROUP BY PHONERATETYPE;

/*QUERY 5*/
SELECT DATEMONTH, SUM(PRICE) AS IncassoTot,
    SUM(SUM(PRICE)) OVER(PARTITION BY DATEYEAR
    ORDER BY DATEYEAR, DATEMONTH
    ROWS UNBOUNDED PRECEDING) AS IncassoAnno
FROM TIMEDIM T, FACTS F
WHERE T.ID_TIME = F.ID_TIME
GROUP BY DATEMONTH, DATEYEAR;

/*QUERY 6*/
SELECT PHONERATETYPE, DATEMONTH, SUM(PRICE) AS IncassoTotTariffaMese,
    100*SUM(PRICE)/ SUM(SUM(PRICE)) OVER (PARTITION BY DATEMONTH) AS PercTutteTariffe,
    100*SUM(PRICE)/ SUM(SUM(PRICE)) OVER (PARTITION BY PHONERATETYPE) AS PercTuttiMesi
FROM TIMEDIM T, FACTS F, PHONERATE P
WHERE T.ID_TIME = F.ID_TIME
    AND P.ID_PHONERATE = F.ID_PHONERATE
    AND DATEYEAR = 2003
GROUP BY PHONERATETYPE,DATEMONTH;

/* CREAZIONE VISTA MATERIALIZZATA QUERY 1-3-6 */
CREATE MATERIALIZED VIEW  VM136
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
ENABLE QUERY REWRITE 
AS 
SELECT PHONERATETYPE, DATEMONTH, DATEYEAR, SUM(PRICE) AS IncassoTotTariffaMese, SUM(NUMBEROFCALLS) AS ChiamateTot
FROM TIMEDIM T, FACTS F, PHONERATE P
WHERE T.ID_TIME = F.ID_TIME AND P.ID_PHONERATE = F.ID_PHONERATE
GROUP BY PHONERATETYPE, DATEMONTH, DATEYEAR;
--- IDENTIFICATORE MINIMO: PHONERATETYPE, DATEMONTH

/* CREAZIONE VISTA MATERIALIZZATA QUERY 2-4-5 */
CREATE MATERIALIZED VIEW  VM245
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
ENABLE QUERY REWRITE 
AS 
SELECT DATEMONTH, DATEYEAR, PHONERATETYPE, SUM(NUMBEROFCALLS) AS ChiamateTot, SUM(PRICE) AS IncassoTotTariffaMese
FROM TIMEDIM T, FACTS F, PHONERATE P
WHERE T.ID_TIME = F.ID_TIME AND P.ID_PHONERATE = F.ID_PHONERATE
GROUP BY PHONERATETYPE, DATEMONTH, DATEYEAR;
--- uguale alla prima, quindi uso la prima per tutte le query

/* TRIGGER */
CREATE OR REPLACE TRIGGER InsertRecord
AFTER INSERT ON FACTS
FOR EACH ROW
DECLARE
    N NUMBER;
    VarPhType VARCHAR(20);
    VarDateMonth VARCHAR(20);
    VarDateYear VARCHAR(20);
    VarIncassoTot NUMBER;
    VarChiamateTot NUMBER;


BEGIN

SELECT PHONERATETYPE INTO VarPhType
FROM PHONERATE P
WHERE P.ID_PHONERATE = :NEW.ID_PHONERATE;

SELECT DATEMONTH, DATEYEAR INTO VarDateMonth, VarDateYear
FROM TIMEDIM T
WHERE T.ID_TIME = :NEW.ID_TIME;

SELECT COUNT(*), IncassoTotTariffaMese+:NEW.PRICE, ChiamateTot+:NEW.NUMBEROFCALLS INTO N, VarIncassoTot, VarChiamateTot
FROM VM1
WHERE PHONERATETYPE = VarPhType AND DATEMONTH = VarDateMonth;

IF (N > 0) THEN
    UPDATE VM1
    SET IncassoTotTariffaMese = VarIncassoTot, ChiamateTot = VarChiamateTot
    WHERE PHONERATETYPE = VarPhType AND DATEMONTH = VarDateMonth;
ELSE
    INSERT INTO VM1(PHONERATETYPE, DATEMONTH, DATEYEAR, IncassoTotTariffaMese, ChiamateTot) VALUES(VarPhType, VarDateMonth, VarDateYear, :NEW.PRICE, :NEW.NUMBEROFCALLS);
END IF;

END;