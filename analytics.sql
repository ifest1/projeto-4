-- REFERENCE DATA STREAM
CREATE OR REPLACE STREAM "FINAL_STREAM" (
    CODIGO_ESTACAO VARCHAR(4),
    DATA_MEDICAO VARCHAR(16),
    NOME_ESTACAO VARCHAR(16),
    HORARIO_COLETA INTEGER,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    PRECIPITACAO DECIMAL(1,1),
    TEMPERATURA_MAXIMA DECIMAL(1,1),
    TEMPERATURA_MINIMA DECIMAL(1,1), 
    RADIACAO_GLOBAL DECIMAL(4,2),
    PONTO_DE_ORVALHO DECIMAL(1,1)
);

CREATE OR REPLACE STREAM "INITIAL_STREAM" (
    CODIGO_ESTACAO VARCHAR(4),
    DATA_MEDICAO VARCHAR(16),
    NOME_ESTACAO VARCHAR(16),
    HORARIO_COLETA INTEGER,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    PRECIPITACAO DECIMAL(1,1),
    TEMPERATURA_MAXIMA DECIMAL(1,1),
    TEMPERATURA_MINIMA DECIMAL(1,1), 
    RADIACAO_GLOBAL DECIMAL(4,2),
    PONTO_DE_ORVALHO DECIMAL(1,1)
);

CREATE OR REPLACE STREAM "INTERMEDIATE_STREAM_TYPE_1" (
    CODIGO_ESTACAO VARCHAR(4),
    DATA_MEDICAO VARCHAR(16),
    NOME_ESTACAO VARCHAR(16),
    HORARIO_COLETA INTEGER,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    PRECIPITACAO DECIMAL(1,1),
    TEMPERATURA_MAXIMA DECIMAL(1,1),
    TEMPERATURA_MINIMA DECIMAL(1,1), 
    RADIACAO_GLOBAL DECIMAL(4,2),
    PONTO_DE_ORVALHO DECIMAL(1,1)
);

CREATE OR REPLACE STREAM "INTERMEDIATE_STREAM_TEMP_MAX" (
    CODIGO_ESTACAO VARCHAR(4),
    DATA_MEDICAO VARCHAR(16),
    NOME_ESTACAO VARCHAR(16),
    HORARIO_COLETA INTEGER,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    PRECIPITACAO DECIMAL(1,1),
    TEMPERATURA_MAXIMA DECIMAL(1,1),
    TEMPERATURA_MINIMA DECIMAL(1,1), 
    RADIACAO_GLOBAL DECIMAL(4,2),
    PONTO_DE_ORVALHO DECIMAL(1,1)
);

CREATE OR REPLACE STREAM "INTERMEDIATE_STREAM_TEMP_MIN" (
    CODIGO_ESTACAO VARCHAR(4),
    DATA_MEDICAO VARCHAR(16),
    NOME_ESTACAO VARCHAR(16),
    HORARIO_COLETA INTEGER,
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE,
    PRECIPITACAO DECIMAL(1,1),
    TEMPERATURA_MAXIMA DECIMAL(1,1),
    TEMPERATURA_MINIMA DECIMAL(1,1), 
    RADIACAO_GLOBAL DECIMAL(4,2),
    PONTO_DE_ORVALHO DECIMAL(1,1)
);
CREATE OR REPLACE PUMP "PUMP_INITIAL_STREAM" AS
INSERT INTO "INITIAL_STREAM"
SELECT STREAM
SOURCE.CODIGO_ESTACAO,
SOURCE.DATA_MEDICAO,
SOURCE.NOME_ESTACAO,
SOURCE.HORARIO_COLETA,
SOURCE.LATITUDE,
SOURCE.LONGITUDE,
SOURCE.PRECIPITACAO,
SOURCE.TEMPERATURA_MAXIMA,
SOURCE.TEMPERATURA_MINIMA,
SOURCE.RADIACAO_GLOBAL,
SOURCE.PONTO_DE_ORVALHO
FROM SOURCE_SQL_STREAM_001 AS SOURCE
LEFT JOIN REFERENCE_DATA AS REF_DATA
ON SOURCE.DATA_MEDICAO = REF_DATA.DATA_MEDICAO;

CREATE OR REPLACE PUMP "PUMP_TEMP_MAX" AS 
INSERT INTO "INTERMEDIATE_STREAM_TEMP_MAX" (
ROWTIME,
CODIGO_ESTACAO,
DATA_MEDICAO,
NOME_ESTACAO,
HORARIO_COLETA,
LATITUDE,
LONGITUDE,
TEMPERATURA_MAXIMA
)
SELECT STREAM 
FLOOR(SOURCE.ROWTIME TO HOUR),
SOURCE.CODIGO_ESTACAO,
SOURCE.DATA_MEDICAO,
SOURCE.NOME_ESTACAO,
SOURCE.HORARIO_COLETA,
SOURCE.LATITUDE,
SOURCE.LONGITUDE,
SOURCE.TEMPERATURA_MAXIMA
FROM 
INITIAL_STREAM AS SOURCE
GROUP BY
SOURCE.ROWTIME,
SOURCE.CODIGO_ESTACAO,
SOURCE.DATA_MEDICAO,
SOURCE.NOME_ESTACAO,
SOURCE.HORARIO_COLETA,
SOURCE.LATITUDE,
SOURCE.LONGITUDE,
SOURCE.TEMPERATURA_MAXIMA,
FLOOR(SOURCE.ROWTIME TO HOUR)
ORDER BY
FLOOR(SOURCE.ROWTIME TO HOUR),
TEMPERATURA_MAXIMA DESC;

CREATE OR REPLACE PUMP "PUMP_TEMP_MIN" AS 
INSERT INTO "INTERMEDIATE_STREAM_TEMP_MIN" (
ROWTIME,
CODIGO_ESTACAO,
DATA_MEDICAO,
NOME_ESTACAO,
HORARIO_COLETA,
LATITUDE,
LONGITUDE,
TEMPERATURA_MINIMA
)
SELECT STREAM 
FLOOR(SOURCE.ROWTIME TO HOUR),
SOURCE.CODIGO_ESTACAO,
SOURCE.DATA_MEDICAO,
SOURCE.NOME_ESTACAO,
SOURCE.HORARIO_COLETA,
SOURCE.LATITUDE,
SOURCE.LONGITUDE,
SOURCE.TEMPERATURA_MINIMA
FROM 
INITIAL_STREAM AS SOURCE
GROUP BY
SOURCE.ROWTIME,
SOURCE.CODIGO_ESTACAO,
SOURCE.DATA_MEDICAO,
SOURCE.NOME_ESTACAO,
SOURCE.HORARIO_COLETA,
SOURCE.LATITUDE,
SOURCE.LONGITUDE,
SOURCE.TEMPERATURA_MINIMA,
FLOOR(SOURCE.ROWTIME TO HOUR)
ORDER BY
FLOOR(SOURCE.ROWTIME TO HOUR),
TEMPERATURA_MINIMA ASC;

-- VARIABLE 1 INTERMEDIATE STREAM
CREATE OR REPLACE PUMP "PUMP_INTERMEDIATE_TYPE_1" AS 
INSERT INTO "INTERMEDIATE_STREAM_TYPE_1" (
PRECIPITACAO,
RADIACAO_GLOBAL
)
SELECT STREAM 
SUM(SOURCE.PRECIPITACAO) OVER (PARTITION BY SOURCE.DATA_MEDICAO RANGE INTERVAL '1' HOUR PRECEDING) AS PRECIPITACAO,
SUM(SOURCE.RADIACAO_GLOBAL) OVER (PARTITION BY SOURCE.DATA_MEDICAO RANGE INTERVAL '1' HOUR PRECEDING) AS RADIACAO_GLOBAL
FROM INITIAL_STREAM AS SOURCE;

-- FINAL STREAM
CREATE OR REPLACE PUMP "PUMP_FINAL_STREAM" AS 
INSERT INTO "FINAL_STREAM" 
SELECT STREAM *
FROM
INTERMEDIATE_STREAM_TEMP_MAX
UNION ALL
SELECT STREAM *
FROM
INTERMEDIATE_STREAM_TEMP_MIN
UNION ALL
SELECT STREAM *
FROM INTERMEDIATE_STREAM_TYPE_1;
