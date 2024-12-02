--- ---                         PROGETTAZIONE LOGICA
--- DIMENSIONI
TEMPO(IdTempo*, Mese, 2M, 3M, 4M, 6M, Anno)
TIPO_VINO(IdTipo*, Tipo, Doc, Dop, Docg)
CONFEZIONE(IdConf*, Confezione)
DESTINAZIONE(IdDest*, Stato, Continente)
--- non metto DIM_ORDINE(IdDO, DimOrdine) ma faccio pushdown perchè ho solo 3 valori
GEO(IdGeo*, Provincia, Regione, Area)
--- TABELLA DEI FATTI
VINI(IdTempo*, IdTipo*, IdConf*, IdDestinazione*, DimOrdine*, IdGeo*, Importo, Litri)

/* QUERY 1 */
SELECT Confezione, Anno, SUM(Importo)/SUM(Litri) AS PrezzoMedioAlLitro,
    100*SUM(Litri)/SUM(SUM(Litri)) OVER(PARTITION BY Confezione) AS %LitriEsportati,
    SUM(SUM(Importo)) OVER(PARTITION BY Confezione
                            ORDER BY anno
                            ROWS UNBOUNDED PRECEDING) AS PrezzoCumulativo
FROM CONFEZIONE C, TEMPO T, VINI V, TIPO_VINO TV
WHERE C.IdConf = V.IdConf
    AND T.IdTempo = V.IdTempo
    AND TV.IdTipo = V.IdTipo
    AND Doc == 1 --- DA FINIRE
GROUP BY Confezione, Anno;