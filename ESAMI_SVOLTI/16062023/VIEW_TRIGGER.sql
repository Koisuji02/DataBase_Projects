/*RICHIESTE:
CARATTERISTICHE-CORSO-CERTIFICAZIONE (IDCarattCorsoCert, Tematica, TipoCertificazione, LivelloCertificazione, PresenzaRequisitiAmmissione, ModalitàErogazione) 
CARATTERISTICHE-ISCRITTI (IDCarattIscritti, FasciaEtà, Professione, Genere)
SEDE-CERTIFICAZIONE (IDSedeCertificazione, CittàSede, RegioneSede, StatoSede, NumeroAule, NumeroDocenti) 
TEMPO (IDTempo, Mese, 2-Mesi, 3-Mesi, 4-Mesi, 6-Mesi, Anno) 
EROGAZIONE-CORSI (IDCarattCorsoCert, IDCarattIscritti, IDSedeCertificazione, IDTempo, IncassoTotale, NumeroOreErogate, NumeroIscritti, NumeroSuperi)
Dato lo schema logico precedente, considerare le seguenti query di interesse:

Considerando solo gli iscritti di genere maschile nella fascia d'età > 45, separatamente per regione sede di certificazione e professione, visualizzare numero di iscritti totale, numero di superi totale e incasso totale.
Separatamente per stato sede di certificazione e mese, visualizzare l'incasso totale mensile e l'incasso cumulativo annuale al trascorrere dei mesi.
Separatamente per professione, considerando solo la regione sede di certificazione Piemonte, visualizzare il numero medio mensile di ore erogate e l'incasso medio mensile.

Dato lo schema logico precedente, si svolgano le seguenti attività

Definire una vista materializzata con CREATE MATERIALIZED VIEW, in modo da ridurre il tempo di risposta delle query di interesse da (a) a (c) sopra riportate. In particolare si specifichi la query SQL associata al Blocco A nella seguente istruzione:
            CREATE MATERIALIZED VIEW ViewCorsi
            BUILD IMMEDIATE
            REFRESH FAST ON COMMIT
            AS
                Blocco A



Definire l’insieme minimale di attributi che permette di identificare le tuple appartenenti alla vista materializzata ViewCorsi.
Si ipotizzi che la gestione della vista materializzata (tabella derivata) sia svolta mediante trigger. Scrivere il trigger per propagare le modifiche alla vista materializzata ViewCorsi in caso di inserimento di un nuovo record nella tabella dei fatti EROGAZIONE-CORSI.*/

---VISTA
CREATE MATERIALIZED VIEW ViewCorsi
BUILD IMMEDIATE
REFRESH FAST ON COMMIT
AS(
SELECT Genere, FasciaEtà, Professione, RegioneSede, StatoSede,
  Mese, Anno,
  SUM(NumeroIscritti) AS Ni, SUM(NumeroSuperi) AS Ns,
  SUM(IncassoTotale) AS It, SUM(NumeroOreErogate) AS No
FROM CARATTERISTICHE-ISCRITTI I,
  TEMPO T, SEDE-CERTIFICAZIONE S, EROGAZIONE-CORSI E
WHERE I.IDCarattIscritti = E.IDCarattIscritti AND T.IDTempo = E.IDTempo AND
  S.IDSedeCertificazione = E.IDSedeCertificazione
GROUP BY Genere, FasciaEtà, Professione, RegioneSede, StatoSede,
  Mese, Anno
);
---INSIEME MINIMALE
Genere, FasciaEtà, RegioneSede, Professione, Mese
---TRIGGER
CREATE OR REPLACE TRIGGER TriggerViewCorsi
AFTER INSERT ON EROGAZIONE-CORSI
FOR EACH ROW
DECLARE
N NUMBER;
VGenere, VFasciaEtà, VProfessione, VRegioneSede, VStatoSede, VMese, VAnno VARCHAR(20);
BEGIN

SELECT Genere, FasciaEtà, Professione INTO VGenere, VFasciaEtà, VProfessione
FROM CARATTERISTICHE-ISCRITTI I
WHERE I.IDCarattIscritti = :NEW.IDCarattIscritti;

SELECT RegioneSede, StatoSede INTO VRegioneSede, VStatoSede
FROM SEDE-CERTIFICAZIONE S
WHERE S.IDSedeCertificazione = :NEW.IDSedeCertificazione;

SELECT Mese, Anno INTO VMese, VAnno
FROM TEMPO T
WHERE T.IDTempo = :NEW.IDTempo;

SELECT COUNT(*) INTO N
FROM ViewCorsi
WHERE Genere = VGenere , FasciaEtà = VFasciaEtà, Professione = VProfessione, RegioneSede = VRegioneSede, Mese = VMese;

IF(N>0) THEN
---UPDATE
UPDATE ViewCorsi
SET Ni = Ni + :NEWNumeroIscritti, Ns = Ns + :NEW.NumeroSuperi,
  It = It + :NEW.IncassoTotale, No = No + :NEW.NumeroOreErogate
WHERE Genere = VGenere , FasciaEtà = VFasciaEtà, Professione = VProfessione, RegioneSede = VRegioneSede, Mese = VMese;

ELSE
---INSERT
INSERT INTO ViewCorsi(Genere, FasciaEtà, Professione, RegioneSede, StatoSede,
  Mese, Anno, Ni, Ns, It, No)
VALUES(VGenere, VFasciaEtà, VProfessione, VRegioneSede, VStatoSede, VMese, VAnno,
  :NEWNumeroIscritti, :NEW.NumeroSuperi, :NEW.IncassoTotale, :NEW.NumeroOreErogate);

ENDIF
END
