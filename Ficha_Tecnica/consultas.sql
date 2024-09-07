-----Correlación-------
---Query para calcular correlacion de las variables consolidado completo
SELECT
CORR(DEP_DELAY, ARR_DELAY) AS CORR_DEP_ARR,
CORR(ARR_DELAY, DELAY_DUE_CARRIER_NN) AS CORR_ARR_CARRIER,
CORR(ARR_DELAY,DELAY_DUE_LATE_AIRCRAFT_NN) AS CORR_ARR_AIRCRAFT,
CORR(ARR_DELAY, DELAY_DUE_NAS_NN) AS CORR_ARR_NAS,
CORR(ARR_DELAY, DELAY_DUE_SECURITY_NN) AS CORR_ARR_SECURITY,
CORR(ARR_DELAY,DELAY_DUE_WEATHER_NN) AS CORR_ARR_WEATHER,
CORR(DEP_DELAY, DELAY_DUE_CARRIER_NN) AS CORR_DEP_CARRIER,
CORR(DEP_DELAY,DELAY_DUE_LATE_AIRCRAFT_NN) AS CORR_DEP_AIRCRAFT,
CORR(DEP_DELAY, DELAY_DUE_NAS_NN) AS CORR_DEP_NAS,
CORR(DEP_DELAY, DELAY_DUE_SECURITY_NN) AS CORR_DEP_SECURITY,
CORR(DEP_DELAY,DELAY_DUE_WEATHER_NN) AS CORR_DEP_WEATHER,
CORR(DEP_DELAY, TAXI_OUT) AS CORR_DEP_TAXI_OUT,
CORR(ARR_DELAY, TAXI_OUT) AS CORR_ARR_TAXI_OUT,
CORR(DEP_DELAY, TAXI_IN) AS CORR_DEP_TAXI_IN,
CORR(ARR_DELAY,TAXI_IN) AS CORR_ARR_TAXI_IN,
CORR(DEP_DELAY, DISTANCE) AS CORR_DEP_DISTANCE,
CORR(ARR_DELAY, DISTANCE) AS CORR_ARR_DISTANCE
FROM `Dataset.view_flights_completo`;

-----cant_variables_delay-----------
SELECT 
  'DELAY_DUE_CARRIER_NN' AS TIPO_DE_RETRASO,
  COUNT(*) AS CANTIDAD
FROM `Dataset.view_flights_completo`
WHERE DELAY_DUE_CARRIER_NN > 0
UNION ALL
SELECT 
  'DELAY_DUE_WEATHER_NN' AS TIPO_DE_RETRASO,
  COUNT(*) AS CANTIDAD
FROM `Dataset.view_flights_completo`
WHERE DELAY_DUE_WEATHER_NN > 0
UNION ALL
SELECT 
  'DELAY_DUE_LATE_AIRCRAFT_NN' AS TIPO_DE_RETRASO,
  COUNT(*) AS CANTIDAD
FROM `Dataset.view_flights_completo`
WHERE DELAY_DUE_LATE_AIRCRAFT_NN > 0
UNION ALL
SELECT 
  'DELAY_DUE_NAS_NN' AS TIPO_DE_RETRASO,
  COUNT(*) AS CANTIDAD
FROM `Dataset.view_flights_completo`
WHERE DELAY_DUE_NAS_NN > 0
UNION ALL
SELECT 
  'DELAY_DUE_SECURITY_NN' AS TIPO_DE_RETRASO,
  COUNT(*) AS CANTIDAD
FROM `Dataset.view_flights_completo`
WHERE DELAY_DUE_SECURITY_NN > 0;


-----------consolidado_retrasos__________
SELECT
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  ROUND(AVG(DELAY_DUE_CARRIER),2) AS AVG_DELAY_CARRIER,
  ROUND(AVG(DELAY_DUE_LATE_AIRCRAFT),2) AS AVG_DELAY_AIRCRAFT,
  ROUND(AVG(DELAY_DUE_NAS),2) AS DELAY_NAS,
  ROUND(AVG(DELAY_DUE_SECURITY),2) AS DELAY_SECURITY,
  ROUND(AVG(DELAY_DUE_WEATHER),2) AS DELAY_WEATHER,
  CONCAT(ORIGIN, '-', DEST) AS ruta,
  round(AVG(TAXI_OUT),2) AS promedio_taxi_out,
  COUNT(*) AS total_vuelos,
  ROUND(AVG(ARR_DELAY), 2) AS promedio_retraso,
  SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS total_cancelados,
  SUM(CASE WHEN DIVERTED = 1 THEN 1 ELSE 0 END) AS total_desviados
FROM
  `Dataset.flights_202301`
WHERE
  ARR_DELAY > 0  -- Filtramos solo los retrasos (ARR_DELAY > 0)
  AND DELAY_DUE_CARRIER IS NOT NULL
  AND DELAY_DUE_LATE_AIRCRAFT IS NOT NULL
  AND DELAY_DUE_NAS IS NOT NULL
  AND DELAY_DUE_SECURITY IS NOT NULL
  AND DELAY_DUE_WEATHER IS NOT NULL
GROUP BY
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY
ORDER BY
  total_vuelos DESC

---------------contar_etiquetas_causa-----------------------

SELECT 
  CAUSAS_DEMORA,
  COUNT(*) AS CANTIDAD
FROM (
  SELECT 
    IF(CC.DELAY_DUE_CARRIER_NN > 0 , 'DEMORADO POR OPERADOR',
      IF(CC.DELAY_DUE_WEATHER_NN > 0, 'DEMORADO POR CLIMA',
        IF(CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0, 'DEMORADO AERONAVE TARDIA',
          IF(CC.DELAY_DUE_NAS_NN > 0, 'DEMORADO NAS',
            IF(CC.DELAY_DUE_SECURITY_NN > 0, 'DEMORADO POR SEGURIDAD',
              IF(CC.ESTATUS_VUELO = 'DEMORADO', 'OTRAS CAUSAS', 'A TIEMPO')
            ))))) AS CAUSAS_DEMORA
  FROM `Dataset.view_consolidado_completo` AS CC
  JOIN `Dataset.view_airline_code` AS ACD
  ON CC.AIRLINE_CODE = ACD.AIRLINE_CODE
  JOIN `Dataset.dot_code_dictionary` AS DCD
  ON CC.DOT_CODE = DCD.CODE
) AS SUBQUERY
GROUP BY CAUSAS_DEMORA;

---------------duplicados-----------------------------
---Query para buscar valores nulos en la vista_arline_code
SELECT
  AIRLINE_CODE,
  NAME_AIRLINE,
  COUNT(*) AS count_duplicates
FROM
  `Dataset.view_airline_code`
GROUP BY
  AIRLINE_CODE,
  NAME_AIRLINE
HAVING
  COUNT(*) > 1
ORDER BY
  count_duplicates DESC;
--Query para buscar duplicados en dot_code_dictionary
SELECT
  code,
  name,
  description,
  COUNT(*) AS count_duplicates
FROM
  `Dataset.dot_code_dictionary`
GROUP BY
  code,
  name,
  description
HAVING
  COUNT(*) > 1
ORDER BY
  count_duplicates DESC;
---Query para buscar duplicados en flights_202301
SELECT
  FL_DATE,
  AIRLINE_CODE,
  DOT_CODE,
  FL_NUMBER,
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  CRS_DEP_TIME,
  DEP_TIME,
  DEP_DELAY,
  TAXI_OUT,
  WHEELS_OFF,
  WHEELS_ON,
  TAXI_IN,
  CRS_ARR_TIME,
  ARR_TIME,
  ARR_DELAY,
  CANCELLED,
  CANCELLATION_CODE,
  DIVERTED,
  CRS_ELAPSED_TIME,
  ELAPSED_TIME,
  AIR_TIME,
  DISTANCE,
  DELAY_DUE_CARRIER,
  DELAY_DUE_WEATHER,
  DELAY_DUE_SECURITY,
  DELAY_DUE_LATE_AIRCRAFT,
  FL_YEAR,
  FL_MONTH,
  FL_DAY,
  COUNT(*) AS count_duplicates
FROM
  `Dataset.flights_202301`
GROUP BY
  FL_DATE,
  AIRLINE_CODE,
  DOT_CODE,
  FL_NUMBER,
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  CRS_DEP_TIME,
  DEP_TIME,
  DEP_DELAY,
  TAXI_OUT,
  WHEELS_OFF,
  WHEELS_ON,
  TAXI_IN,
  CRS_ARR_TIME,
  ARR_TIME,
  ARR_DELAY,
  CANCELLED,
  CANCELLATION_CODE,
  DIVERTED,
  CRS_ELAPSED_TIME,
  ELAPSED_TIME,
  AIR_TIME,
  DISTANCE,
  DELAY_DUE_CARRIER,
  DELAY_DUE_WEATHER,
  DELAY_DUE_SECURITY,
  DELAY_DUE_LATE_AIRCRAFT,
  FL_YEAR,
  FL_MONTH,
  FL_DAY
HAVING
  COUNT(*) > 1
ORDER BY
  count_duplicates DESC;

---------------nulos------------------------------
---query manejo de nulos tabla flights_202301---
SELECT
  SUM(IF(FL_DATE IS NULL, 1, 0)) AS FL_DATE_nulos,
  SUM(IF(AIRLINE_CODE IS NULL, 1, 0)) AS AIRLINE_CODE_nulos,
  SUM(IF(DOT_CODE IS NULL, 1, 0)) AS DOT_CODE_nulos,
  SUM(IF(FL_NUMBER IS NULL, 1, 0)) AS FL_NUMBER_nulos,
  SUM(IF(ORIGIN IS NULL, 1, 0)) AS ORIGIN_nulos,
  SUM(IF(ORIGIN_CITY IS NULL, 1, 0)) AS ORIGIN_CITY_nulos,
  SUM(IF(DEST IS NULL, 1, 0)) AS DEST_nulos,
  SUM(IF(DEST_CITY IS NULL, 1, 0)) AS DEST_CITY_nulos,
  SUM(IF(CRS_DEP_TIME IS NULL, 1, 0)) AS CRSDEPTIME_nulos,
  SUM(IF(DEP_TIME IS NULL, 1, 0)) AS DEP_TIME_nulos,
  SUM(IF(DEP_DELAY IS NULL, 1, 0)) AS DEP_DELAY_nulos,
  SUM(IF(TAXI_OUT IS NULL, 1, 0)) AS TAXI_OUT_nulos,
  SUM(IF(WHEELS_OFF IS NULL, 1, 0)) AS WHEELS_OFF_nulos,
  SUM(IF(WHEELS_ON IS NULL, 1, 0)) AS WHEELS_ON_nulos,
  SUM(IF(TAXI_IN IS NULL, 1, 0)) AS TAXI_IN_nulos,
  SUM(IF(CRS_ARR_TIME IS NULL, 1, 0)) AS CRSARRTIME_nulos,
  SUM(IF(ARR_TIME IS NULL, 1, 0)) AS ARR_TIME_nulos,
  SUM(IF(ARR_DELAY IS NULL, 1, 0)) AS ARR_DELAY_nulos,
  SUM(IF(CANCELLED IS NULL, 1, 0)) AS CANCELLED_nulos,
  SUM(IF(CANCELLATION_CODE IS NULL, 1, 0)) AS CANCELLATION_CODE_nulos,
  SUM(IF(DIVERTED IS NULL, 1, 0)) AS DIVERTED_nulos,
  SUM(IF(CRS_ELAPSED_TIME IS NULL, 1, 0)) AS CRSELAPSEDTIME_nulos,
  SUM(IF(ELAPSED_TIME IS NULL, 1, 0)) AS ELAPSED_TIME_nulos,
  SUM(IF(AIR_TIME IS NULL, 1, 0)) AS AIR_TIME_nulos,
  SUM(IF(DISTANCE IS NULL, 1, 0)) AS DISTANCE_nulos,
  SUM(IF(DELAY_DUE_CARRIER IS NULL, 1, 0)) AS DELAYDUECARRIER_nulos,
  SUM(IF(DELAY_DUE_WEATHER IS NULL, 1, 0)) AS DELAYDUEWEATHER_nulos,
  SUM(IF(DELAY_DUE_NAS IS NULL, 1, 0)) AS DELAYDUENAS_nulos,
  SUM(IF(DELAY_DUE_SECURITY IS NULL, 1, 0)) AS DELAYDUESECURITY_nulos,
  SUM(IF(DELAY_DUE_LATE_AIRCRAFT IS NULL, 1, 0)) AS DELAYDUELATE_AIRCRAFT_nulos
FROM
 `Dataset.flights_202301`

 ---query manejo de nulos tabla airline_cod_dictionary---
SELECT
  COUNTIF(string_field_0 IS NULL) AS null_string_field0,
  COUNTIF(string_field_1 IS NULL) AS null_string_field1
FROM
  `Dataset.airline_code_dictionary`;
  --query manejo de nulos tabla flights_202301---
SELECT
  COUNTIF(FL_DATE IS NULL) AS NULL_FL_DATE,
  COUNTIF(AIRLINE_CODE IS NULL) AS null_AIR_LINE,
  COUNTIF(FL_NUMBER IS NULL) AS null_FL_NUMBER,
  COUNTIF(ORIGIN IS NULL) AS null_ORIGIN,
  COUNTIF(DEST IS NULL) AS null_DEST,
  COUNTIF(CRS_DEP_TIME IS NULL) AS null_CRS_DEP_TIME,
  COUNTIF(DEP_TIME IS NULL) AS null_DEP_TIME,
  COUNTIF(DEP_DELAY IS NULL) AS null_DEP_DELAY,
  COUNTIF(DISTANCE IS NULL) AS null_DISTANCE,
  COUNTIF(AIR_TIME IS NULL) AS null_AIR_TIME
FROM
  `Dataset.flights_202301`;

---ARR_DELAY IS NULL 11,640
SELECT
*
FROM `Dataset.flights_202301`
WHERE ARR_DELAY IS NULL;
---VUELOS CANCELADOS 10,295
SELECT
*
FROM `Dataset.flights_202301`
WHERE CANCELLED = 1;
------VUELOS DESVIADOS 1,345
SELECT
*
FROM `Dataset.flights_202301`
WHERE DIVERTED = 1;
---Vuelos cancelados 9978
SELECT
*
FROM `Dataset.flights_202301`
WHERE DEP_TIME IS NULL;
---Vuelos cancelados 10,197
SELECT
*
FROM `Dataset.flights_202301`
WHERE TAXI_OUT IS NULL and
WHEELS_OFF is null
---Vuelos cancelados
SELECT
*
FROM `Dataset.flights_202301`
WHERE WHEELS_ON IS NULL and
TAXI_OUT is null and
ARR_TIME is null;

--- Vuelo cancelado
SELECT
*
FROM `Dataset.flights_202301`
WHERE CRS_ELAPSED_TIME IS NULL;

-------------outliers-------------------

-----OUTLIERS

----OUTLIERS DEP_DELAY
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DEP_DELAY, 4)[OFFSET(1)] AS Q1_DEP_DELAY,
   APPROX_QUANTILES(DEP_DELAY, 4)[OFFSET(3)] AS Q3_DEP_DELAY
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DEP_DELAY,
   Q3_DEP_DELAY,
   (Q3_DEP_DELAY - Q1_DEP_DELAY) AS IQR_DEP_DELAY
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DEP_DELAY,
   Q3_DEP_DELAY,
   IQR_DEP_DELAY,
   Q1_DEP_DELAY - 1.5 * IQR_DEP_DELAY AS lower_bound,
   Q3_DEP_DELAY + 1.5 * IQR_DEP_DELAY AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS dep_delay_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DEP_DELAY > bounds.upper_bound
ORDER BY
 DEP_DELAY DESC;
-----OUTLIERS ARR_DELAY
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(ARR_DELAY, 4)[OFFSET(1)] AS Q1_ARR_DELAY,
   APPROX_QUANTILES(ARR_DELAY, 4)[OFFSET(3)] AS Q3_ARR_DELAY
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_ARR_DELAY,
   Q3_ARR_DELAY,
   (Q3_ARR_DELAY - Q1_ARR_DELAY) AS IQR_ARR_DELAY
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_ARR_DELAY,
   Q3_ARR_DELAY,
   IQR_ARR_DELAY,
   Q3_ARR_DELAY + 1.5 * IQR_ARR_DELAY AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS arr_delay_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 ARR_DELAY > bounds.upper_bound
ORDER BY
 ARR_DELAY DESC;
---OUTLIERS DELAY_DUE_WEATHER
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DELAY_DUE_WEATHER, 4)[OFFSET(1)] AS Q1_DELAY_DUE_WEATHER,
   APPROX_QUANTILES(DELAY_DUE_WEATHER, 4)[OFFSET(3)] AS Q3_DELAY_DUE_WEATHER
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DELAY_DUE_WEATHER,
   Q3_DELAY_DUE_WEATHER,
   (Q3_DELAY_DUE_WEATHER - Q1_DELAY_DUE_WEATHER) AS IQR_DELAY_DUE_WEATHER
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DELAY_DUE_WEATHER,
   Q3_DELAY_DUE_WEATHER,
   IQR_DELAY_DUE_WEATHER,
   Q1_DELAY_DUE_WEATHER - 1.5 * IQR_DELAY_DUE_WEATHER AS lower_bound,
   Q3_DELAY_DUE_WEATHER + 1.5 * IQR_DELAY_DUE_WEATHER AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS delay_due_weather_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DELAY_DUE_WEATHER > bounds.upper_bound
ORDER BY
 DELAY_DUE_WEATHER DESC;
---OUTLIERS DELAY_DUE_NAS
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DELAY_DUE_NAS, 4)[OFFSET(1)] AS Q1_DELAY_DUE_NAS,
   APPROX_QUANTILES(DELAY_DUE_NAS, 4)[OFFSET(3)] AS Q3_DELAY_DUE_NAS
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DELAY_DUE_NAS,
   Q3_DELAY_DUE_NAS,
   (Q3_DELAY_DUE_NAS - Q1_DELAY_DUE_NAS) AS IQR_DELAY_DUE_NAS
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DELAY_DUE_NAS,
   Q3_DELAY_DUE_NAS,
   IQR_DELAY_DUE_NAS,
   Q1_DELAY_DUE_NAS - 1.5 * IQR_DELAY_DUE_NAS AS lower_bound,
   Q3_DELAY_DUE_NAS + 1.5 * IQR_DELAY_DUE_NAS AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS delay_due_nas_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DELAY_DUE_NAS > bounds.upper_bound
ORDER BY
 DELAY_DUE_NAS DESC;
----OUTLIERS DELAY_DUE_SECURITY
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DELAY_DUE_SECURITY, 4)[OFFSET(1)] AS Q1_DELAY_DUE_SECURITY,
   APPROX_QUANTILES(DELAY_DUE_SECURITY, 4)[OFFSET(3)] AS Q3_DELAY_DUE_SECURITY
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DELAY_DUE_SECURITY,
   Q3_DELAY_DUE_SECURITY,
   (Q3_DELAY_DUE_SECURITY - Q1_DELAY_DUE_SECURITY) AS IQR_DELAY_DUE_SECURITY
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DELAY_DUE_SECURITY,
   Q3_DELAY_DUE_SECURITY,
   IQR_DELAY_DUE_SECURITY,
   Q1_DELAY_DUE_SECURITY - 1.5 * IQR_DELAY_DUE_SECURITY AS lower_bound,
   Q3_DELAY_DUE_SECURITY + 1.5 * IQR_DELAY_DUE_SECURITY AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS delay_due_security_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DELAY_DUE_SECURITY > bounds.upper_bound
ORDER BY
 DELAY_DUE_SECURITY DESC;
----OUTLIERS DELAY_DUE_LATE_AIRCRAFT
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DELAY_DUE_LATE_AIRCRAFT, 4)[OFFSET(1)] AS Q1_DELAY_DUE_LATE_AIRCRAFT,
   APPROX_QUANTILES(DELAY_DUE_LATE_AIRCRAFT, 4)[OFFSET(3)] AS Q3_DELAY_DUE_LATE_AIRCRAFT
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DELAY_DUE_LATE_AIRCRAFT,
   Q3_DELAY_DUE_LATE_AIRCRAFT,
   (Q3_DELAY_DUE_LATE_AIRCRAFT - Q1_DELAY_DUE_LATE_AIRCRAFT) AS IQR_DELAY_DUE_LATE_AIRCRAFT
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DELAY_DUE_LATE_AIRCRAFT,
   Q3_DELAY_DUE_LATE_AIRCRAFT,
   IQR_DELAY_DUE_LATE_AIRCRAFT,
   Q1_DELAY_DUE_LATE_AIRCRAFT - 1.5 * IQR_DELAY_DUE_LATE_AIRCRAFT AS lower_bound,
   Q3_DELAY_DUE_LATE_AIRCRAFT + 1.5 * IQR_DELAY_DUE_LATE_AIRCRAFT AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS delay_due_late_aircraft_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DELAY_DUE_LATE_AIRCRAFT > bounds.upper_bound
ORDER BY
 DELAY_DUE_LATE_AIRCRAFT DESC;
---- OUTLIERS DELAY_DUE_CARRIER
WITH quartiles AS (
 SELECT
   APPROX_QUANTILES(DELAY_DUE_CARRIER, 4)[OFFSET(1)] AS Q1_DELAY_DUE_CARRIER,
   APPROX_QUANTILES(DELAY_DUE_CARRIER, 4)[OFFSET(3)] AS Q3_DELAY_DUE_CARRIER
 FROM
   `Dataset.flights_202301`
),
iqr AS (
 SELECT
   Q1_DELAY_DUE_CARRIER,
   Q3_DELAY_DUE_CARRIER,
   (Q3_DELAY_DUE_CARRIER - Q1_DELAY_DUE_CARRIER) AS IQR_DELAY_DUE_CARRIER
 FROM
   quartiles
),
bounds AS (
 SELECT
   Q1_DELAY_DUE_CARRIER,
   Q3_DELAY_DUE_CARRIER,
   IQR_DELAY_DUE_CARRIER,
   Q1_DELAY_DUE_CARRIER - 1.5 * IQR_DELAY_DUE_CARRIER AS lower_bound,
   Q3_DELAY_DUE_CARRIER + 1.5 * IQR_DELAY_DUE_CARRIER AS upper_bound
 FROM
   iqr
)
SELECT
 flights_202301.*,
 'Above Upper Bound' AS delay_due_carrier_outlier_status
FROM
 `Dataset.flights_202301` AS flights_202301,
 bounds
WHERE
 DELAY_DUE_CARRIER > bounds.upper_bound
ORDER BY
 DELAY_DUE_CARRIER DESC;

-----------------quartiles-----------------------------------

----Segmentación por cuartiles para los vuelos retrasados 116 713 vuelos
WITH delay_datos AS (
    SELECT
        ARR_DELAY
    FROM `Dataset.view_vuelos_retrasados`
),
--- Calcular los cuartiles
quartiles AS (
    SELECT
        ARR_DELAY,
        NTILE(4) OVER (ORDER BY ARR_DELAY) AS cuartiles_delay
    FROM delay_datos
),
-- Calcula el número total de vuelos retrasados
quartile_risk AS (
    SELECT
        cuartiles_delay,
        COUNT(*) AS total_vuelos,
        SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS total_vuelos_retrasados
    FROM quartiles
    GROUP BY cuartiles_delay
),
--- Rango (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        cuartiles_delay,
        MIN(ARR_DELAY) AS min_delay,
        MAX(ARR_DELAY) AS max_delay
    FROM quartiles
    GROUP BY cuartiles_delay
)
SELECT
    q.cuartiles_delay,
    q.total_vuelos,
    q.total_vuelos_retrasados,
    r.min_delay,
    r.max_delay
FROM quartile_risk q
JOIN quartile_ranges r
ON q.cuartiles_delay = r.cuartiles_delay
ORDER BY cuartiles_delay ASC;




--------------retraso_aeropuerto------------------

----RUTAS CON MAS RETRASO---
SELECT
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  COUNT(*) AS total_retrasos,
  ROUND(AVG(ARR_DELAY),2)AS promedio_retraso
FROM
  `Dataset.view_flights_completo`
WHERE
  ESTATUS_VUELO = 'DEMORADO'  -- Filtramos solo los retrasos
GROUP BY
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY
ORDER BY
  total_retrasos DESC
LIMIT
  15;
  ---AEROPUERTOS CON MAYOR RETRASO---
SELECT
  ORIGIN,
  ORIGIN_CITY,
  COUNT(*) AS total_retrasos,
  ROUND(AVG(ARR_DELAY),2)AS promedio_retraso
FROM
  `Dataset.view_flights_completo`
WHERE
  ESTATUS_VUELO = 'DEMORADO'  -- Filtramos solo los retrasos (ARR_DELAY > 0)
GROUP BY
  ORIGIN,
  ORIGIN_CITY
ORDER BY
  total_retrasos DESC
LIMIT
  15;
  ----AEROLINEA CON MÁS RETRASOS---
SELECT
  AIRLINE_CODE,
  AIRLINE_DESCRIPTION,
  COUNT(*) AS total_retrasos,
  ROUND(AVG(ARR_DELAY),2)AS promedio_retraso
FROM
  `Dataset.view_flights_completo`
WHERE
  ESTATUS_VUELO = 'DEMORADO' -- Filtramos solo los retrasos
GROUP BY
  AIRLINE_CODE,
  AIRLINE_DESCRIPTION
ORDER BY
  total_retrasos DESC
LIMIT
  15;

------------------------unir_tablas------------------
--QUERY PARA UNIR LAS VARIABLES NOMBRE DE AEROLINEA Y DESCRIPCION DOT---
SELECT 
  CC.FL_DATE,
  CC.AIRLINE_CODE,
  ACD.NAME_AIRLINE AS AIRLINE_DESCRIPTION, 
  CC.DOT_CODE,
  DCD.Description AS DOT_DESCRIPTION,
  CC.FL_NUMBER,
  CC.ORIGIN,
  CC.ORIGIN_CITY,
  CC.DEST,
  CC.DEST_CITY,
  CC.CRS_DEP_TIME,
  CC.DEP_TIME,
  CC.DEP_DELAY,
  CC.TAXI_OUT,
  CC.WHEELS_OFF,
  CC.WHEELS_ON,
  CC.TAXI_IN,
  CC.CRS_ARR_TIME,
  CC.ARR_TIME,
  CC.ARR_DELAY,
  CC.CANCELLED,
  CC.CANCELLATION_CODE,
  CC.DIVERTED,
  CC.CRS_ELAPSED_TIME,
  CC.ELAPSED_TIME,
  CC.AIR_TIME,
  CC.DISTANCE,
  CC.FL_YEAR,
  CC.FL_MONTH,
  CC.FL_DAY,
  CC.DELAY_DUE_CARRIER_NN,
  CC.DELAY_DUE_WEATHER_NN,
  CC.DELAY_DUE_LATE_AIRCRAFT_NN,
  CC.DELAY_DUE_NAS_NN,
  CC.DELAY_DUE_SECURITY_NN,
  CC.ETIQUETA_RETRASO,
  CC.ETIQUETA_NUM
FROM `Dataset.view_consolidado_completo` AS CC
JOIN `Dataset.view_airline_code` AS ACD
ON CC.AIRLINE_CODE = ACD.AIRLINE_CODE
JOIN `Dataset.dot_code_dictionary` AS DCD
ON CC.DOT_CODE = DCD.CODE

-----------------------variables_delay_sin_nulos-------------------

SELECT * 
FROM `Dataset.flights_202301`
WHERE 
  DELAY_DUE_CARRIER IS NOT NULL
  AND DELAY_DUE_LATE_AIRCRAFT IS NOT NULL
  AND DELAY_DUE_NAS IS NOT NULL
  AND DELAY_DUE_SECURITY IS NOT NULL
  AND DELAY_DUE_WEATHER IS NOT NULL;

---------------------------variables_retraso----------------------

SELECT
  CANCELLED,
  SUM(CANCELLED) AS total_retraso_carrier,
  DIVERTED,
  SUM(DIVERTED) AS total_retraso_security,
  ARR_DELAY,
  SUM(ARR_TIME) AS total_retraso_aircraft
FROM
  `Dataset.flights_202301`
WHERE
  ARR_DELAY > 0  -- Consideramos solo los vuelos con retraso
  
GROUP BY
  CANCELLED, DIVERTED,ARR_DELAY
ORDER BY
  total_retraso_carrier DESC, 
  total_retraso_security DESC,
  total_retraso_aircraft DESC

------------------------view_airline_code--------------

SELECT string_field_0 AS AIRLINE_CODE, 
       string_field_1 AS NAME_AIRLINE
FROM `Dataset.airline_code_dictionary`
WHERE string_field_0 != 'AIRLINE CODE' 
  AND string_field_1 != 'DESCRIPTION';


-----------view_completa_ubicacion----------------
select
FC.FL_DATE,		
FC.AIRLINE_CODE,			
FC.AIRLINE_DESCRIPTION,			
FC.DOT_CODE,			
FC.DOT_DESCRIPTION,			
FC.FL_NUMBER,			
FC.ORIGIN,			
FC.ORIGIN_CITY,	
O.LATITUD_ORIG AS LATITUD_ORIGEN,
O.LONGITUD_ORIG AS LONGITUD_ORIGEN,		
FC.DEST,			
FC.DEST_CITY,	
D.LATITUD_DEST AS LATITUD_DESTINO,
D.LONGITUD_DEST AS LONGITUD_DESTINO,	
FC.CRS_DEP_TIME,			
FC.DEP_TIME,			
FC.DEP_DELAY,			
FC.TAXI_OUT,			
FC.WHEELS_OFF,			
FC.WHEELS_ON,			
FC.TAXI_IN,			
FC.CRS_ARR_TIME,			
FC.ARR_TIME,			
FC.ARR_DELAY,			
FC.CANCELLED,			
FC.CANCELLATION_CODE,			
FC.DIVERTED,			
FC.CRS_ELAPSED_TIME,			
FC.ELAPSED_TIME,			
FC.AIR_TIME,			
FC.DISTANCE,			
FC.FL_YEAR,			
FC.FL_MONTH,			
FC.FL_DAY,			
FC.DELAY_DUE_CARRIER_NN,			
FC.DELAY_DUE_WEATHER_NN,		
FC.DELAY_DUE_LATE_AIRCRAFT_NN,			
FC.DELAY_DUE_NAS_NN,			
FC.DELAY_DUE_SECURITY_NN,			
FC.CAUSAS_DEMORA,			
FC.ESTATUS_VUELO,			
FC.ETIQUETA_NUM,			
FC.DAY_OF_WEEK
from `Dataset.view_flights_completo` FC
left join `Dataset.ciudad_origen` O	
on FC.ORIGIN_CITY = O.ciudad
LEFT JOIN `Dataset.ciudad_destino` D
ON FC.DEST_CITY = D.ciudad	

----------view_consolidado_completo-------------------
---Consolidado 538,837 vuelos, se une con las otras dos tablas, con 0 en los valores nulos y las columnas con las etiquetas considerando vuelos cancelados, desviados, demorados y a tiempo
SELECT
  *,
  IFNULL(DELAY_DUE_CARRIER, 0) AS DELAY_DUE_CARRIER_NN,
  IFNULL(DELAY_DUE_WEATHER, 0) AS DELAY_DUE_WEATHER_NN,
  IFNULL(DELAY_DUE_LATE_AIRCRAFT, 0) AS DELAY_DUE_LATE_AIRCRAFT_NN,
  IFNULL(DELAY_DUE_NAS, 0) AS DELAY_DUE_NAS_NN,
  IFNULL(DELAY_DUE_SECURITY, 0) AS DELAY_DUE_SECURITY_NN,
  CASE
    WHEN ARR_DELAY IS NULL AND CANCELLED = 1 THEN 'CANCELADO'
    WHEN ARR_DELAY IS NULL AND DIVERTED = 1 THEN 'DESVIADO'
    WHEN ARR_DELAY <= 0 THEN 'A TIEMPO'
    WHEN ARR_DELAY > 0 AND DELAY_DUE_CARRIER IS NULL AND DELAY_DUE_WEATHER IS NULL AND DELAY_DUE_LATE_AIRCRAFT IS NULL AND DELAY_DUE_NAS IS NULL AND DELAY_DUE_SECURITY IS NULL THEN 'A TIEMPO'
    ELSE 'DEMORADO'
  END AS ESTATUS_VUELO,
  CASE
    WHEN ARR_DELAY IS NULL AND CANCELLED = 1 THEN 2  -- Etiqueta numérica para "Cancelado"
    WHEN ARR_DELAY IS NULL AND DIVERTED = 1 THEN 3  -- Etiqueta numérica para "Desviado"
    WHEN ARR_DELAY < 0 THEN 0 -----Etiqueta numérica para "A Tiempo"
    WHEN ARR_DELAY > 0 AND DELAY_DUE_CARRIER IS NULL AND DELAY_DUE_WEATHER IS NULL AND DELAY_DUE_LATE_AIRCRAFT IS NULL AND DELAY_DUE_NAS IS NULL AND DELAY_DUE_SECURITY IS NULL THEN 0 ----Etiqueta numérica para "A Tiempo"
    ELSE 1 -- Etiqueta numérica para "Demorado"
  END AS ETIQUETA_NUM
FROM
  `Dataset.flights_202301`;

-----------------view_consolidado_flights----------------------
---Consolidado con 0 en los valores nulos y las columnas con las etiquetas
SELECT
  *,
  IFNULL(DELAY_DUE_CARRIER, 0) AS DELAY_DUE_CARRIER_NN,
  IFNULL(DELAY_DUE_WEATHER, 0) AS DELAY_DUE_WEATHER_NN,
  IFNULL(DELAY_DUE_LATE_AIRCRAFT, 0) AS DELAY_DUE_LATE_AIRCRAFT_NN,
  IFNULL(DELAY_DUE_NAS, 0) AS DELAY_DUE_NAS_NN,
  IFNULL(DELAY_DUE_SECURITY, 0) AS DELAY_DUE_SECURITY_NN,
CASE
    WHEN ARR_DELAY > 0 THEN 'Retraso'
    ELSE 'No Retraso'
  END AS ETIQUETA_RETRASO,
CASE
WHEN ARR_DELAY > 0 THEN 1
ELSE 0
END AS ETIQUETA_NUM
FROM `Dataset.view_consolidado_vuelos`


-------------view_consolidado_vuelos--------------
--Consolidado vuelos
SELECT
*
FROM `Dataset.flights_202301`
WHERE NOT ( ARR_DELAY > 0
AND
DELAY_DUE_CARRIER IS NULL AND
DELAY_DUE_WEATHER IS NULL);

--------------------view_flights_completo----------------
---Query consolidado completo 538,837 con las etiquetas considerando cancelados, desviados, demorados, a tiempo y tipo de cancelacion
SELECT
  CC.FL_DATE,
  CC.AIRLINE_CODE,
  ACD.NAME_AIRLINE AS AIRLINE_DESCRIPTION,
  CC.DOT_CODE,
  DCD.Description AS DOT_DESCRIPTION,
  CC.FL_NUMBER,
  CC.ORIGIN,
  CC.ORIGIN_CITY,
  CC.DEST,
  CC.DEST_CITY,
  CC.CRS_DEP_TIME,
  CC.DEP_TIME,
  CC.DEP_DELAY,
  CC.TAXI_OUT,
  CC.WHEELS_OFF,
  CC.WHEELS_ON,
  CC.TAXI_IN,
  CC.CRS_ARR_TIME,
  CC.ARR_TIME,
  CC.ARR_DELAY,
  CC.CANCELLED,
  CC.CANCELLATION_CODE,
  CC.DIVERTED,
  CC.CRS_ELAPSED_TIME,
  CC.ELAPSED_TIME,
  CC.AIR_TIME,
  CC.DISTANCE,
  CC.FL_YEAR,
  CC.FL_MONTH,
  CC.FL_DAY,
  CC.DELAY_DUE_CARRIER_NN,
  CC.DELAY_DUE_WEATHER_NN,
  CC.DELAY_DUE_LATE_AIRCRAFT_NN,
  CC.DELAY_DUE_NAS_NN,
  CC.DELAY_DUE_SECURITY_NN,
  CASE
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'A' THEN 'CANCELADO POR OPERADOR'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'B' THEN 'CANCELADO POR CLIMA'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'C' THEN 'CANCELADO POR NAS'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'D' THEN 'CANCELADO POR SEGURIDAD'
    WHEN CC.DIVERTED = 1 THEN 'DESVIADO'
    WHEN DELAY_DUE_CARRIER_NN > 0 AND DELAY_DUE_WEATHER_NN = 0 AND DELAY_DUE_NAS_NN = 0 AND DELAY_DUE_SECURITY_NN = 0 AND DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR OPERADOR'
    WHEN DELAY_DUE_WEATHER_NN > 0 AND DELAY_DUE_CARRIER_NN = 0 AND DELAY_DUE_NAS_NN = 0 AND DELAY_DUE_SECURITY_NN = 0 AND DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR CLIMA'
    WHEN DELAY_DUE_NAS_NN > 0 AND DELAY_DUE_CARRIER_NN = 0 AND DELAY_DUE_WEATHER_NN = 0 AND DELAY_DUE_SECURITY_NN = 0 AND DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR NAS'
    WHEN DELAY_DUE_SECURITY_NN > 0 AND DELAY_DUE_CARRIER_NN = 0 AND DELAY_DUE_WEATHER_NN = 0 AND DELAY_DUE_NAS_NN = 0 AND DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR SEGURIDAD'
    WHEN DELAY_DUE_LATE_AIRCRAFT_NN > 0 AND DELAY_DUE_CARRIER_NN = 0 AND DELAY_DUE_WEATHER_NN = 0 AND DELAY_DUE_NAS_NN = 0 AND DELAY_DUE_SECURITY_NN = 0 THEN 'DEMORADO AERONAVE TARDÍA'
    WHEN (DELAY_DUE_CARRIER_NN > 0 AND (DELAY_DUE_WEATHER_NN > 0 OR DELAY_DUE_NAS_NN > 0 OR DELAY_DUE_SECURITY_NN > 0 OR DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (DELAY_DUE_WEATHER_NN > 0 AND (DELAY_DUE_CARRIER_NN > 0 OR DELAY_DUE_NAS_NN > 0 OR DELAY_DUE_SECURITY_NN > 0 OR DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (DELAY_DUE_NAS_NN > 0 AND (DELAY_DUE_CARRIER_NN > 0 OR DELAY_DUE_WEATHER_NN > 0 OR DELAY_DUE_SECURITY_NN > 0 OR DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (DELAY_DUE_SECURITY_NN > 0 AND (DELAY_DUE_CARRIER_NN > 0 OR DELAY_DUE_WEATHER_NN > 0 OR DELAY_DUE_NAS_NN > 0 OR DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (DELAY_DUE_LATE_AIRCRAFT_NN > 0 AND (DELAY_DUE_CARRIER_NN > 0 OR DELAY_DUE_WEATHER_NN > 0 OR DELAY_DUE_NAS_NN > 0 OR DELAY_DUE_SECURITY_NN > 0)) THEN 'DEMORA MULTIFACTOR'
    ELSE 'A TIEMPO'
  END AS CAUSAS_DEMORA,
  CC.ESTATUS_VUELO,
  CC.ETIQUETA_NUM,
  FORMAT_DATE('%A', CC.FL_DATE) AS DAY_OF_WEEK
FROM `Dataset.view_consolidado_completo` AS CC
JOIN `Dataset.view_airline_code` AS ACD
  ON CC.AIRLINE_CODE = ACD.AIRLINE_CODE
JOIN `Dataset.dot_code_dictionary` AS DCD
  ON CC.DOT_CODE = DCD.CODE;


------------view_ retraso_consolidado------------------
SELECT
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  ROUND(AVG(DELAY_DUE_CARRIER),2) AS AVG_DELAY_CARRIER,
  ROUND(AVG(DELAY_DUE_LATE_AIRCRAFT),2) AS AVG_DELAY_AIRCRAFT,
  ROUND(AVG(DELAY_DUE_NAS),2) AS DELAY_NAS,
  ROUND(AVG(DELAY_DUE_SECURITY),2) AS DELAY_SECURITY,
  ROUND(AVG(DELAY_DUE_WEATHER),2) AS DELAY_WEATHER,
  CONCAT(ORIGIN, '-', DEST) AS ruta,
  round(AVG(TAXI_OUT),2) AS promedio_taxi_out,
  COUNT(*) AS total_vuelos,
  ROUND(AVG(ARR_DELAY), 2) AS promedio_retraso,
  SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS total_cancelados,
  SUM(CASE WHEN DIVERTED = 1 THEN 1 ELSE 0 END) AS total_desviados
FROM
  `Dataset.flights_202301`
WHERE
  ARR_DELAY > 0  -- Filtramos solo los retrasos (ARR_DELAY > 0)
  AND DELAY_DUE_CARRIER IS NOT NULL
  AND DELAY_DUE_LATE_AIRCRAFT IS NOT NULL
  AND DELAY_DUE_NAS IS NOT NULL
  AND DELAY_DUE_SECURITY IS NOT NULL
  AND DELAY_DUE_WEATHER IS NOT NULL
GROUP BY
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY
ORDER BY
  total_vuelos DESC

  ---------------view_riesgo_relativo------------------------
  ----Query para calcular el riesgo relativo, total de expuestos, no expuestos y las tasas de incidencia.
WITH WeatherRisk AS (
  SELECT
    'Weather' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_WEATHER_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_flight`
    GROUP BY Grupo
  )
),
CarrierRisk AS (
  SELECT
    'Carrier' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_CARRIER_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_flight`
    GROUP BY Grupo
  )
),
SecurityRisk AS (
  SELECT
    'Security' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_SECURITY_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_flight`
    GROUP BY Grupo
  )
),
AircraftRisk AS (
  SELECT
    'Aircraft' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_LATE_AIRCRAFT_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_flight`
    GROUP BY Grupo
  )
),
NASRisk AS (
  SELECT
    'NAS' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_NAS_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ARR_DELAY > 0 THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_flight`
    GROUP BY Grupo
  )
)
SELECT * FROM WeatherRisk
UNION ALL
SELECT * FROM CarrierRisk
UNION ALL
SELECT * FROM SecurityRisk
UNION ALL
SELECT * FROM AircraftRisk
UNION ALL
SELECT * FROM NASRisk;

---------------view_rr_completo-------------------------
----Riesgo relativo con 538,837 vuelos
WITH WeatherRisk AS (
  SELECT
    'Weather' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_WEATHER_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_completo`
    GROUP BY Grupo
  )
),
CarrierRisk AS (
  SELECT
    'Carrier' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_CARRIER_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_completo`
    GROUP BY Grupo
  )
),
SecurityRisk AS (
  SELECT
    'Security' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_SECURITY_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_completo`
    GROUP BY Grupo
  )
),
AircraftRisk AS (
  SELECT
    'Aircraft' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_LATE_AIRCRAFT_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_completo`
    GROUP BY Grupo
  )
),
NASRisk AS (
  SELECT
    'NAS' AS Variable,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Total_No_Expuesto,
    SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_Expuesto,
    SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END) AS Tasa_Incidencia_No_Expuesto,
    SAFE_DIVIDE(
      SUM(CASE WHEN Grupo = 'Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'Expuesto' THEN Total_Vuelos ELSE 0 END),
      SUM(CASE WHEN Grupo = 'No Expuesto' THEN Vuelos_Retrasados ELSE 0 END) / SUM(CASE WHEN Grupo = 'No Expuesto' THEN Total_Vuelos ELSE 0 END)
    ) AS Riesgo_Relativo
  FROM (
    SELECT
      CASE
        WHEN DELAY_DUE_NAS_NN > 0 THEN 'Expuesto'
        ELSE 'No Expuesto'
      END AS Grupo,
      COUNT(*) AS Total_Vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Vuelos_Retrasados
    FROM `Dataset.view_consolidado_completo`
    GROUP BY Grupo
  )
)
SELECT * FROM WeatherRisk
UNION ALL
SELECT * FROM CarrierRisk
UNION ALL
SELECT * FROM SecurityRisk
UNION ALL
SELECT * FROM AircraftRisk
UNION ALL
SELECT * FROM NASRisk;


-------------------view_vuelos_retrasados-----------

--Vuelos que llegan retrasados 116,713 los 85,862 se clasificaron a tiempo por referencia US department transportation
SELECT
  *
FROM `Dataset.view_flights_completo`
WHERE ESTATUS_VUELO = 'DEMORADO';

-----------view_ rr_aerolineas----------------------------------
----Riesgo relativo aerolineas
WITH Totales AS (
  SELECT
    SUM(total_vuelos) AS Total_Vuelos_Todas_Aerolineas,
    SUM(total_retrasos) AS Total_Retrasos_Todas_Aerolineas
  FROM (
    SELECT
      AIRLINE_CODE,
      COUNT(*) AS total_vuelos,
      SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS total_retrasos
    FROM
      `Dataset.view_flights_completo`
    GROUP BY
      AIRLINE_CODE
  )
),
CalculosPorAerolinea AS (
  SELECT
    AIRLINE_CODE,
    AIRLINE_DESCRIPTION,
    COUNT(*) AS total_vuelos,
    SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS total_retrasos,
    SAFE_DIVIDE(SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END), COUNT(*)) AS Tasa_Incidencia_Aerolinea,
    (SELECT Total_Retrasos_Todas_Aerolineas FROM Totales) - SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Total_Retrasos_Otras_Aerolineas,
    (SELECT Total_Vuelos_Todas_Aerolineas FROM Totales) - COUNT(*) AS Total_Vuelos_Otras_Aerolineas,
    SAFE_DIVIDE(
      (SELECT Total_Retrasos_Todas_Aerolineas FROM Totales) - SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END),
      (SELECT Total_Vuelos_Todas_Aerolineas FROM Totales) - COUNT(*)
    ) AS Tasa_Incidencia_Otras_Aerolineas
  FROM
    `Dataset.view_flights_completo`
  GROUP BY
    AIRLINE_CODE, AIRLINE_DESCRIPTION
)
SELECT
  AIRLINE_CODE,
  AIRLINE_DESCRIPTION,
  total_vuelos,
  total_retrasos,
  Tasa_Incidencia_Aerolinea,
  Tasa_Incidencia_Otras_Aerolineas,
  SAFE_DIVIDE(Tasa_Incidencia_Aerolinea, Tasa_Incidencia_Otras_Aerolineas) AS Riesgo_Relativo
FROM
  CalculosPorAerolinea
ORDER BY
  Riesgo_Relativo DESC
LIMIT 15;
 
-------------------view_rr_aeropuertos-------------------------------------


---RIESGO RELATIVO AEROPUERTOS
WITH Totales AS (
  SELECT
    COUNT(*) AS Total_Vuelos_Todos_Aeropuertos,
    SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Total_Demorados_Todos_Aeropuertos
  FROM
    `Dataset.view_flights_completo`
),
CalculosPorAeropuerto AS (
  SELECT
    ORIGIN,
    ORIGIN_CITY,
    COUNT(*) AS Total_Vuelos_Aeropuerto,
    SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Total_Demorados_Aeropuerto,
    SAFE_DIVIDE(SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END), COUNT(*)) AS Tasa_Incidencia_Aeropuerto,
    (SELECT Total_Demorados_Todos_Aeropuertos FROM Totales) - SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END) AS Total_Demorados_Otros_Aeropuertos,
    (SELECT Total_Vuelos_Todos_Aeropuertos FROM Totales) - COUNT(*) AS Total_Vuelos_Otros_Aeropuertos,
    SAFE_DIVIDE(
      (SELECT Total_Demorados_Todos_Aeropuertos FROM Totales) - SUM(CASE WHEN ESTATUS_VUELO = 'DEMORADO' THEN 1 ELSE 0 END),
      (SELECT Total_Vuelos_Todos_Aeropuertos FROM Totales) - COUNT(*)
    ) AS Tasa_Incidencia_Otros_Aeropuertos
  FROM
    `Dataset.view_flights_completo`
  GROUP BY
    ORIGIN, ORIGIN_CITY
)
SELECT
  ORIGIN,
  ORIGIN_CITY,
  Total_Vuelos_Aeropuerto,
  Total_Demorados_Aeropuerto,
  Tasa_Incidencia_Aeropuerto,
  Tasa_Incidencia_Otros_Aeropuertos,
  SAFE_DIVIDE(Tasa_Incidencia_Aeropuerto, Tasa_Incidencia_Otros_Aeropuertos) AS Riesgo_Relativo
FROM
  CalculosPorAeropuerto
ORDER BY
  Riesgo_Relativo DESC;

  ----------------rr_rutas-------------------------------------

  -----Riesgo relativo por rutas
WITH Totales AS (
  SELECT
    SUM(total_vuelos) AS Total_Vuelos_Todas_Rutas,
    SUM(total_retrasos) AS Total_Retrasos_Todas_Rutas
  FROM `proyecto4-flightdelay.Dataset.view_rutas_demoradas`
),
CalculosPorRuta AS (
  SELECT
    CONCAT(ORIGIN, ' - ', DEST) AS RUTA,
    ORIGIN,
    ORIGIN_CITY,
    DEST,
    DEST_CITY,
    total_vuelos AS Total_Vuelos_Ruta,
    total_retrasos AS Total_Retrasos_Ruta,
    SAFE_DIVIDE(total_retrasos, total_vuelos) AS Tasa_Incidencia_Ruta,
    (SELECT Total_Retrasos_Todas_Rutas FROM Totales) - total_retrasos AS Total_Retrasos_Otras_Rutas,
    (SELECT Total_Vuelos_Todas_Rutas FROM Totales) - total_vuelos AS Total_Vuelos_Otras_Rutas,
    SAFE_DIVIDE(
      (SELECT Total_Retrasos_Todas_Rutas FROM Totales) - total_retrasos,
      (SELECT Total_Vuelos_Todas_Rutas FROM Totales) - total_vuelos
    ) AS Tasa_Incidencia_Otras_Rutas
  FROM `proyecto4-flightdelay.Dataset.view_rutas_demoradas`
)
SELECT
  RUTA,
  ORIGIN,
  ORIGIN_CITY,
  DEST,
  DEST_CITY,
  Total_Vuelos_Ruta,
  Total_Retrasos_Ruta,
  Tasa_Incidencia_Ruta,
  Tasa_Incidencia_Otras_Rutas,
  SAFE_DIVIDE(Tasa_Incidencia_Ruta, Tasa_Incidencia_Otras_Rutas) AS Riesgo_Relativo
FROM CalculosPorRuta
ORDER BY Riesgo_Relativo DESC
LIMIT 15;

-----------------view_rr_vuelos_retrasados-------------

---- RIESGO RELATIVO VUELOS RETRASADOS
WITH delay_counts AS (
  SELECT
    -- Contar los casos con retraso por operador y otros retrasos
    COUNTIF(DELAY_DUE_CARRIER_NN > 0) AS exposed_carrier_delay,
    COUNTIF(EXCLUDING_CARRIER > 0 AND DELAY_DUE_CARRIER_NN = 0) AS unexposed_carrier_delay,
    -- Contar los casos con retraso por clima y otros retrasos
    COUNTIF(DELAY_DUE_WEATHER_NN > 0) AS exposed_weather_delay,
    COUNTIF(EXCLUDING_WEATHER > 0 AND DELAY_DUE_WEATHER_NN = 0) AS unexposed_weather_delay,
    -- Contar los casos con retraso por NAS y otros retrasos
    COUNTIF(DELAY_DUE_NAS_NN > 0) AS exposed_nas_delay,
    COUNTIF(EXCLUDING_NAS > 0 AND DELAY_DUE_NAS_NN = 0) AS unexposed_nas_delay,
    -- Contar los casos con retraso por seguridad y otros retrasos
    COUNTIF(DELAY_DUE_SECURITY_NN> 0) AS exposed_security_delay,
    COUNTIF(EXCLUDING_SECURITY > 0 AND DELAY_DUE_SECURITY_NN = 0) AS unexposed_security_delay,
    -- Contar los casos con retraso por aeronave tardía y otros retrasos
    COUNTIF(DELAY_DUE_LATE_AIRCRAFT_NN > 0) AS exposed_late_aircraft_delay,
    COUNTIF(EXCLUDING_LATE_AIRCRAFT > 0 AND DELAY_DUE_LATE_AIRCRAFT_NN = 0) AS unexposed_late_aircraft_delay,
    COUNT(*) AS total_flights
  FROM
    `Dataset.view_flights_completo_rr`
  WHERE
    -- Solo considerar vuelos con algún tipo de retraso en total
    (DELAY_DUE_CARRIER_NN > 0 OR
     DELAY_DUE_WEATHER_NN > 0 OR
     DELAY_DUE_NAS_NN > 0 OR
     DELAY_DUE_SECURITY_NN > 0 OR
     DELAY_DUE_LATE_AIRCRAFT_NN > 0)
)
SELECT
  'Operador' AS motivo,
  exposed_carrier_delay AS exposed_count,
  unexposed_carrier_delay AS unexposed_count,
  (exposed_carrier_delay / total_flights) AS exposed_rate,
  (unexposed_carrier_delay / total_flights) AS unexposed_rate,
  IF(unexposed_carrier_delay > 0, exposed_carrier_delay / unexposed_carrier_delay, NULL) AS relative_risk
FROM delay_counts
UNION ALL
SELECT
  'Clima' AS motivo,
  exposed_weather_delay AS exposed_count,
  unexposed_weather_delay AS unexposed_count,
  (exposed_weather_delay / total_flights) AS exposed_rate,
  (unexposed_weather_delay / total_flights) AS unexposed_rate,
  IF(unexposed_weather_delay > 0, exposed_weather_delay / unexposed_weather_delay, NULL) AS relative_risk
FROM delay_counts
UNION ALL
SELECT
  'NAS' AS motivo,
  exposed_nas_delay AS exposed_count,
  unexposed_nas_delay AS unexposed_count,
  (exposed_nas_delay / total_flights) AS exposed_rate,
  (unexposed_nas_delay / total_flights) AS unexposed_rate,
  IF(unexposed_nas_delay > 0, exposed_nas_delay / unexposed_nas_delay, NULL) AS relative_risk
FROM delay_counts
UNION ALL
SELECT
  'Seguridad' AS motivo,
  exposed_security_delay AS exposed_count,
  unexposed_security_delay AS unexposed_count,
  (exposed_security_delay / total_flights) AS exposed_rate,
  (unexposed_security_delay / total_flights) AS unexposed_rate,
  IF(unexposed_security_delay > 0, exposed_security_delay / unexposed_security_delay, NULL) AS relative_risk
FROM delay_counts
UNION ALL
SELECT
  'Aeronave Tardía' AS motivo,
  exposed_late_aircraft_delay AS exposed_count,
  unexposed_late_aircraft_delay AS unexposed_count,
  (exposed_late_aircraft_delay / total_flights) AS exposed_rate,
  (unexposed_late_aircraft_delay / total_flights) AS unexposed_rate,
  IF(unexposed_late_aircraft_delay > 0, exposed_late_aircraft_delay / unexposed_late_aircraft_delay, NULL) AS relative_risk
FROM delay_counts;

--------view_rutas_demoradas------------------------

----RUTAS CON MAS RETRASO---
WITH TotalVuelos AS (
  SELECT
    ORIGIN,
    ORIGIN_CITY,
    DEST,
    DEST_CITY,
    COUNT(*) AS total_vuelos  -- Calculamos el total de vuelos por cada ruta
  FROM
    `Dataset.view_flights_completo`
  GROUP BY
    ORIGIN,
    ORIGIN_CITY,
    DEST,
    DEST_CITY
)
SELECT
  r.ORIGIN,
  r.ORIGIN_CITY,
  r.DEST,
  r.DEST_CITY,
  COUNT(*) AS total_retrasos,  -- Número total de retrasos por ruta
  ROUND(AVG(r.ARR_DELAY), 2) AS promedio_retraso,  -- Promedio de retraso por ruta
  tv.total_vuelos  -- Número total de vuelos por ruta
FROM
  `Dataset.view_flights_completo` r
JOIN
  TotalVuelos tv
ON
  r.ORIGIN = tv.ORIGIN AND r.DEST = tv.DEST  -- Unimos por ORIGIN y DEST para obtener el total de vuelos
WHERE
  r.ESTATUS_VUELO = 'DEMORADO'  -- Filtramos solo los retrasos
GROUP BY
  r.ORIGIN,
  r.ORIGIN_CITY,
  r.DEST,
  r.DEST_CITY,
  tv.total_vuelos
ORDER BY
  total_retrasos DESC
LIMIT
  15;


-------------------view_ flights_completo_rr--------------------------

SELECT
  CC.FL_DATE,
  CC.AIRLINE_CODE,
  ACD.NAME_AIRLINE AS AIRLINE_DESCRIPTION,
  CC.DOT_CODE,
  DCD.Description AS DOT_DESCRIPTION,
  CC.FL_NUMBER,
  CC.ORIGIN,
  CC.ORIGIN_CITY,
  CC.DEST,
  CC.DEST_CITY,
  CC.CRS_DEP_TIME,
  CC.DEP_TIME,
  CC.DEP_DELAY,
  CC.TAXI_OUT,
  CC.WHEELS_OFF,
  CC.WHEELS_ON,
  CC.TAXI_IN,
  CC.CRS_ARR_TIME,
  CC.ARR_TIME,
  CC.ARR_DELAY,
  CC.CANCELLED,
  CC.CANCELLATION_CODE,
  CC.DIVERTED,
  CC.CRS_ELAPSED_TIME,
  CC.ELAPSED_TIME,
  CC.AIR_TIME,
  CC.DISTANCE,
  CC.FL_YEAR,
  CC.FL_MONTH,
  CC.FL_DAY,
  CC.DELAY_DUE_CARRIER_NN,
  CC.DELAY_DUE_WEATHER_NN,
  CC.DELAY_DUE_LATE_AIRCRAFT_NN,
  CC.DELAY_DUE_NAS_NN,
  CC.DELAY_DUE_SECURITY_NN,
  -- Agregar columnas de EXCLUDING (exclusión de causas)
  IF(GREATEST(CC.DELAY_DUE_WEATHER_NN, CC.DELAY_DUE_NAS_NN, CC.DELAY_DUE_SECURITY_NN, CC.DELAY_DUE_LATE_AIRCRAFT_NN) > 0, 1, 0) AS EXCLUDING_CARRIER,
  IF(GREATEST(CC.DELAY_DUE_CARRIER_NN, CC.DELAY_DUE_NAS_NN, CC.DELAY_DUE_SECURITY_NN, CC.DELAY_DUE_LATE_AIRCRAFT_NN) > 0, 1, 0) AS EXCLUDING_WEATHER,
  IF(GREATEST(CC.DELAY_DUE_CARRIER_NN, CC.DELAY_DUE_WEATHER_NN, CC.DELAY_DUE_SECURITY_NN, CC.DELAY_DUE_LATE_AIRCRAFT_NN) > 0, 1, 0) AS EXCLUDING_NAS,
  IF(GREATEST(CC.DELAY_DUE_CARRIER_NN, CC.DELAY_DUE_WEATHER_NN, CC.DELAY_DUE_NAS_NN, CC.DELAY_DUE_LATE_AIRCRAFT_NN) > 0, 1, 0) AS EXCLUDING_SECURITY,
  IF(GREATEST(CC.DELAY_DUE_CARRIER_NN, CC.DELAY_DUE_WEATHER_NN, CC.DELAY_DUE_NAS_NN, CC.DELAY_DUE_SECURITY_NN) > 0, 1, 0) AS EXCLUDING_LATE_AIRCRAFT,
  -- Definir la causa de la demora
  CASE
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'A' THEN 'CANCELADO POR OPERADOR'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'B' THEN 'CANCELADO POR CLIMA'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'C' THEN 'CANCELADO POR NAS'
    WHEN CC.CANCELLED = 1 AND CC.CANCELLATION_CODE = 'D' THEN 'CANCELADO POR SEGURIDAD'
    WHEN CC.DIVERTED = 1 THEN 'DESVIADO'
    WHEN CC.DELAY_DUE_CARRIER_NN > 0 AND CC.DELAY_DUE_WEATHER_NN = 0 AND CC.DELAY_DUE_NAS_NN = 0 AND CC.DELAY_DUE_SECURITY_NN = 0 AND CC.DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR OPERADOR'
    WHEN CC.DELAY_DUE_WEATHER_NN > 0 AND CC.DELAY_DUE_CARRIER_NN = 0 AND CC.DELAY_DUE_NAS_NN = 0 AND CC.DELAY_DUE_SECURITY_NN = 0 AND CC.DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR CLIMA'
    WHEN CC.DELAY_DUE_NAS_NN > 0 AND CC.DELAY_DUE_CARRIER_NN = 0 AND CC.DELAY_DUE_WEATHER_NN = 0 AND CC.DELAY_DUE_SECURITY_NN = 0 AND CC.DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR NAS'
    WHEN CC.DELAY_DUE_SECURITY_NN > 0 AND CC.DELAY_DUE_CARRIER_NN = 0 AND CC.DELAY_DUE_WEATHER_NN = 0 AND CC.DELAY_DUE_NAS_NN = 0 AND CC.DELAY_DUE_LATE_AIRCRAFT_NN = 0 THEN 'DEMORADO POR SEGURIDAD'
    WHEN CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0 AND CC.DELAY_DUE_CARRIER_NN = 0 AND CC.DELAY_DUE_WEATHER_NN = 0 AND CC.DELAY_DUE_NAS_NN = 0 AND CC.DELAY_DUE_SECURITY_NN = 0 THEN 'DEMORADO AERONAVE TARDÍA'
    WHEN (CC.DELAY_DUE_CARRIER_NN > 0 AND (CC.DELAY_DUE_WEATHER_NN > 0 OR CC.DELAY_DUE_NAS_NN > 0 OR CC.DELAY_DUE_SECURITY_NN > 0 OR CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (CC.DELAY_DUE_WEATHER_NN > 0 AND (CC.DELAY_DUE_CARRIER_NN > 0 OR CC.DELAY_DUE_NAS_NN > 0 OR CC.DELAY_DUE_SECURITY_NN > 0 OR CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (CC.DELAY_DUE_NAS_NN > 0 AND (CC.DELAY_DUE_CARRIER_NN > 0 OR CC.DELAY_DUE_WEATHER_NN > 0 OR CC.DELAY_DUE_SECURITY_NN > 0 OR CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (CC.DELAY_DUE_SECURITY_NN > 0 AND (CC.DELAY_DUE_CARRIER_NN > 0 OR CC.DELAY_DUE_WEATHER_NN > 0 OR CC.DELAY_DUE_NAS_NN > 0 OR CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0)) OR
         (CC.DELAY_DUE_LATE_AIRCRAFT_NN > 0 AND (CC.DELAY_DUE_CARRIER_NN > 0 OR CC.DELAY_DUE_WEATHER_NN > 0 OR CC.DELAY_DUE_NAS_NN > 0 OR CC.DELAY_DUE_SECURITY_NN > 0)) THEN 'DEMORA MULTIFACTOR'
    ELSE 'A TIEMPO'
  END AS CAUSAS_DEMORA,
  CC.ESTATUS_VUELO,
  CC.ETIQUETA_NUM,
  FORMAT_DATE('%A', CC.FL_DATE) AS DAY_OF_WEEK
FROM `Dataset.view_consolidado_completo` AS CC
JOIN `Dataset.view_airline_code` AS ACD
  ON CC.AIRLINE_CODE = ACD.AIRLINE_CODE
JOIN `Dataset.dot_code_dictionary` AS DCD
  ON CC.DOT_CODE = DCD.CODE;
































































