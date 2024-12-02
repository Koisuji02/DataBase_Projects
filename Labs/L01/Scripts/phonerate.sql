CREATE TABLE PHONERATE(
   ID_phoneRate  INTEGER  NOT NULL PRIMARY KEY 
  ,phoneRateType VARCHAR(12) NOT NULL
);
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (1,'Notte');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (2,'Giorno');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (3,'Mattino');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (4,'24 ore su 24');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (5,'CartaNatale');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (6,'Festivi');
INSERT INTO PHONERATE(ID_phoneRate,phoneRateType) VALUES (7,'Business');
