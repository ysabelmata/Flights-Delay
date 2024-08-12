-------------Consultas Proyecto---------------------

-----CORR_DESVIAC_ESTANDAR----
#Query para excluir la variable sex de user_info
SELECT
  * EXCEPT (sex)
FROM
  `Dataset.user_info`;

#Query para calcular la correlación en loans_detail
SELECT
  CORR (more_90_days_overdue,number_times_delayed_payment_loan_30_59_days) AS r_90_30_59,
  CORR (more_90_days_overdue,number_times_delayed_payment_loan_60_89_days) AS r_90_60_89,
  CORR (number_times_delayed_payment_loan_30_59_days,number_times_delayed_payment_loan_60_89_days) AS r_30_60
FROM
  `Dataset.loans_detail`;
  
#Query para calcular la desviación estándar de las variables
SELECT
  STDDEV_POP(more_90_days_overdue) AS stddev_more_90,
  STDDEV_POP(number_times_delayed_payment_loan_30_59_days) AS stddev_30_59,
  STDDEV_POP(number_times_delayed_payment_loan_60_89_days) AS stddev_60_89
FROM
  `Dataset.loans_detail`;

------CORRELACION------
#Query para calcular la correlación entre variables
SELECT
  CORR(age,last_month_salary) AS correlation_age_salary,
  CORR(age,more_90_days_overdue) AS correlation_age_more90,
  CORR(age,total_loans) AS correlation_age_total_loans,
  CORR(last_month_salary,total_loans) AS correlation_salary_total_loans
FROM
 `Dataset.view_consolidado`

 #Datos Inconsistentes
    #Query para identificar datos incosistentes
SELECT
  DISTINCT loan_type
FROM
  `Dataset.loans_outstanding`;
#Query para contar los valores incosistentes
SELECT
  loan_type,
  COUNT(*) as Cantidad
FROM
  `Dataset.loans_outstanding`
GROUP BY
  loan_type
ORDER BY
  Cantidad DESC;
# Query para cambiar los datos inconsistentes
CREATE or replace table `Dataset.loans_outstanding_standar` AS
SELECT
  loan_id,
  user_id,
  CASE
    WHEN LOWER(loan_type) = 'real estate' THEN 'real estate'
    WHEN LOWER(loan_type) IN ('other', 'others') THEN 'others'
    ELSE LOWER(loan_type)
  END AS loan_type
FROM `Dataset.loans_outstanding`;

----DUPLICADOS----
#Query para buscar duplicados en loans_outstanding
SELECT
  loan_id,
  COUNT(*) AS cantidad
FROM
  `Dataset.loans_outstanding`
GROUP BY
  loan_id
HAVING
  COUNT(*) >1;
#Query para buscar duplicados en user_info
SELECT
  user_id,
  COUNT(*) AS cantidad
FROM
  `Dataset.user_info`
GROUP BY
  user_id
HAVING
  COUNT(*) >1;
#Query para buscar duplicados en loans_detail
SELECT
  user_id,
  COUNT(*) AS cantidad
FROM
  `Dataset.loans_detail`
GROUP BY
  user_id
HAVING
  COUNT(*) >1;
#Query para buscar duplicados en default
SELECT
  user_id,
  COUNT(*) AS cantidad
FROM
  `Dataset.default`
GROUP BY
  user_id
HAVING
  COUNT(*) >1;  

------MANEJO_NULOS--------
#Query para contar valores nulos en user_info
SELECT
  COUNT (*)
FROM
  `Dataset.user_info`
WHERE
  user_id IS NULL
  OR age IS NULL
  OR sex IS NULL
  OR last_month_salary IS NULL
  OR number_dependents IS NULL;
#Query para buscar los valores nulos en user_info
SELECT
  *
FROM
  `Dataset.user_info`
WHERE
  user_id IS NULL
  OR age IS NULL
  OR sex IS NULL
  OR last_month_salary IS NULL
  OR number_dependents IS NULL;
#Query para contar valores nulos en loans_outstanding
SELECT
  COUNT (*)
FROM
  `Dataset.loans_outstanding`
WHERE
  loan_id IS NULL
  OR user_id IS NULL
  OR loan_type IS NULL;
#Query para contar valores nulos en loans_details
SELECT
  COUNT (*)
FROM
  `Dataset.loans_detail`
WHERE
  user_id IS NULL
  OR more_90_days_overdue IS NULL
  OR using_lines_not_secured_personal_assets IS NULL
  OR 
  number_times_delayed_payment_loan_30_59_days IS NULL
  OR
  debt_ratio
  IS NULL
  OR
  debt_ratio
  IS NULL
  OR
  number_times_delayed_payment_loan_60_89_days
  IS NULL;
#Query para contar valores nulos en default
  SELECT
  COUNT (*)
FROM
  `Dataset.default`
WHERE
  user_id IS NULL
  OR
  default_flag is null;

-----Query para contar cantidad de usuarios---
  SELECT
  COUNT (*)
FROM `Dataset.user_info`;

#Query de la union de tablas para clientes que pagan
SELECT *
FROM
`Dataset.view_user_default`
WHERE
  default_flag = 0 AND
  last_month_salary IS NULL;

#Query de la union de tablas donde los clientes no pagan 
SELECT *
FROM
 `Dataset.view_user_default`
WHERE
  default_flag = 1 AND
  last_month_salary IS NULL;


--Moda_num_dependientes-----
WITH dependents_counts AS (
  SELECT
    default_flag,
    number_dependents,
    COUNT(*) AS count_dependents
  FROM
    `Dataset.view_user_default`
  WHERE
    last_month_salary <= 400000
  GROUP BY
    default_flag,
    number_dependents
),
dependents_mode AS (
  SELECT
    default_flag,
    number_dependents,
    count_dependents,
    RANK() OVER (PARTITION BY default_flag ORDER BY count_dependents DESC) AS rank
  FROM
    dependents_counts
)
SELECT
  default_flag,
  number_dependents AS mode_number_dependents
FROM
  dependents_mode
WHERE
  rank = 1;

---Outliers----

WITH quartiles AS (
  SELECT
    APPROX_QUANTILES(age, 4)[OFFSET(1)] AS Q1_age,
    APPROX_QUANTILES(age, 4)[OFFSET(3)] AS Q3_age
  FROM
    `Dataset.user_info`
),
iqr AS (
  SELECT
    Q1_age,
    Q3_age,
    (Q3_age - Q1_age) AS IQR_age
  FROM
    quartiles
),
bounds AS (
  SELECT
    Q1_age,
    Q3_age,
    IQR_age,
    Q1_age - 1.5 * IQR_age AS lower_bound,
    Q3_age + 1.5 * IQR_age AS upper_bound
  FROM
    iqr
)
SELECT
  user_info_limpia.*,
  CASE
    WHEN age < bounds.lower_bound THEN 'Below Lower Bound'
    WHEN age > bounds.upper_bound THEN 'Above Upper Bound'
    ELSE 'Within Range'
  END AS age_outlier_status
FROM
  `Dataset.user_info` AS user_info_limpia,
  bounds
WHERE
  age < bounds.lower_bound
  OR age > bounds.upper_bound
ORDER BY
  age;
#Query para indentificar outliers en user_info variable last_month_salary
WITH quartiles AS (
  SELECT
    APPROX_QUANTILES(last_month_salary, 4)[OFFSET(1)] AS Q1_salary,
    APPROX_QUANTILES(last_month_salary, 4)[OFFSET(3)] AS Q3_salary
  FROM
    `Dataset.user_info`
),
iqr AS (
  SELECT
    Q1_salary,
    Q3_salary,
    (Q3_salary - Q1_salary) AS IQR_salary
  FROM
    quartiles
),
bounds AS (
  SELECT
    Q1_salary,
    Q3_salary,
    IQR_salary,
    Q1_salary - 1.5 * IQR_salary AS lower_bound,
    Q3_salary + 1.5 * IQR_salary AS upper_bound
  FROM
    iqr
)
SELECT
  user_info_limpia.*,
  CASE
    WHEN last_month_salary < bounds.lower_bound THEN 'Below Lower Bound'
    WHEN last_month_salary > bounds.upper_bound THEN 'Above Upper Bound'
    ELSE 'Within Range'
  END AS salary_outlier_status
FROM
  `Dataset.user_info` AS user_info_limpia,
  bounds
WHERE
  last_month_salary < bounds.lower_bound
  OR last_month_salary > bounds.upper_bound
ORDER BY
  last_month_salary;
#Query para indentificar outliers en user_info variable number_dependents
WITH quartiles AS (
  SELECT
    APPROX_QUANTILES(number_dependents, 4)[OFFSET(1)] AS Q1_dependents,
    APPROX_QUANTILES(number_dependents, 4)[OFFSET(3)] AS Q3_dependents
  FROM
    `Dataset.user_info`
),
iqr AS (
  SELECT
    Q1_dependents,
    Q3_dependents,
    (Q3_dependents - Q1_dependents) AS IQR_dependents
  FROM
    quartiles
),
bounds AS (
  SELECT
    Q1_dependents,
    Q3_dependents,
    IQR_dependents,
    Q1_dependents - 1.5 * IQR_dependents AS lower_bound,
    Q3_dependents + 1.5 * IQR_dependents AS upper_bound
  FROM
    iqr
)
SELECT
  user_info_limpia.*,
  CASE
    WHEN number_dependents < bounds.lower_bound THEN 'Below Lower Bound'
    WHEN number_dependents > bounds.upper_bound THEN 'Above Upper Bound'
    ELSE 'Within Range'
  END AS dependents_outlier_status
FROM
  `Dataset.user_info` AS user_info_limpia,
  bounds
WHERE
  number_dependents< bounds.lower_bound
  OR number_dependents > bounds.upper_bound
ORDER BY
  number_dependents;


--Promedios_flags--

#Query para calcular los promedios por flags sin considerar outliers para los valores nulos en user_info
SELECT
 default_flag,
 AVG(last_month_salary) AS avg_last_month_salary,
 AVG(number_dependents) AS avg_number_dependents
FROM
 `Dataset.view_user_default`
WHERE
 last_month_salary <= 400000
GROUP BY
 default_flag;

 #Riego_relativo_totalloans#
 #Query para calcular el riesgo relativo de total_loans
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        total_loans,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable total_loans
quartiles AS (
    SELECT
        total_loans,
        default_flag,
        NTILE(4) OVER (ORDER BY total_loans) AS loans_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        loans_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers
    FROM quartiles
    GROUP BY loans_quartile
),
-- Paso 4: Obtener el rango de total_loans (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        loans_quartile,
        MIN(total_loans) AS min_loans,
        MAX(total_loans) AS max_loans
    FROM quartiles
    GROUP BY loans_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.loans_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_loans,
        r.max_loans,
        CASE
            WHEN q.loans_quartile = 1 THEN (q1.total_bad_payers / q1.total_count) / ((q2.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q2.total_count + q3.total_count + q4.total_count))
            WHEN q.loans_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count + q4.total_count))
            WHEN q.loans_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count + q4.total_count))
            WHEN q.loans_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count + q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.loans_quartile = r.loans_quartile
    LEFT JOIN quartile_risk q1 ON q1.loans_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.loans_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.loans_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.loans_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    loans_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_loans,
    max_loans
FROM risk_relative
ORDER BY loans_quartile ASC;

#Riesgo_realtivo_ratiocredito#

#Query para calcular el riesgo relativo de Using_lines_not_secured_personal_assets
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        Using_lines_not_secured_personal_assets,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable Using_lines_not_secured_personal_assets
quartiles AS (
    SELECT
        Using_lines_not_secured_personal_assets,
        default_flag,
        NTILE(4) OVER (ORDER BY Using_lines_not_secured_personal_assets) AS unsecured_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        unsecured_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers
    FROM quartiles
    GROUP BY unsecured_quartile
),
-- Paso 4: Obtener el rango de Using_lines_not_secured_personal_assets (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        unsecured_quartile,
        MIN(Using_lines_not_secured_personal_assets) AS min_unsecured,
        MAX(Using_lines_not_secured_personal_assets) AS max_unsecured
    FROM quartiles
    GROUP BY unsecured_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.unsecured_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_unsecured,
        r.max_unsecured,
        CASE
            WHEN q.unsecured_quartile = 1 THEN (q1.total_bad_payers / q1.total_count) / ((q2.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q2.total_count + q3.total_count + q4.total_count))
            WHEN q.unsecured_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count + q4.total_count))
            WHEN q.unsecured_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count + q4.total_count))
            WHEN q.unsecured_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count + q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.unsecured_quartile = r.unsecured_quartile
    LEFT JOIN quartile_risk q1 ON q1.unsecured_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.unsecured_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.unsecured_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.unsecured_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    unsecured_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_unsecured,
    max_unsecured
FROM risk_relative
ORDER BY unsecured_quartile ASC;

--Riesgo_relativo_age--
#Query para calcular el riesgo relativo de age
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        age,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable age
quartiles AS (
    SELECT
        age,
        default_flag,
        NTILE(4) OVER (ORDER BY age) AS age_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        age_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers,
    FROM quartiles
    GROUP BY age_quartile
),
-- Paso 4: Obtener el rango de edad (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        age_quartile,
        MIN(age) AS min_age,
        MAX(age) AS max_age
    FROM quartiles
    GROUP BY age_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.age_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_age,
        r.max_age,
        CASE
            WHEN q.age_quartile = 1 THEN (q1.total_bad_payers/q1.total_count)/((q2.total_bad_payers+q3.total_bad_payers+q4.total_bad_payers)/(q2.total_count+q3.total_count+q4.total_count))
            WHEN q.age_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count +q4.total_count))
            WHEN q.age_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count +q4.total_count))
            WHEN q.age_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count +q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.age_quartile = r.age_quartile
    LEFT JOIN quartile_risk q1 ON q1.age_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.age_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.age_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.age_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    age_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_age,
    max_age
FROM risk_relative
ORDER BY age_quartile ASC;

---Riesgo_relativo_debratio----
#Query para calcular el riesgo relativo de debt_ratio
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        debt_ratio,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable debt_ratio
quartiles AS (
    SELECT
        debt_ratio,
        default_flag,
        NTILE(4) OVER (ORDER BY debt_ratio) AS debt_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        debt_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers
    FROM quartiles
    GROUP BY debt_quartile
),
-- Paso 4: Obtener el rango de debt_ratio (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        debt_quartile,
        MIN(debt_ratio) AS min_debt_ratio,
        MAX(debt_ratio) AS max_debt_ratio
    FROM quartiles
    GROUP BY debt_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.debt_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_debt_ratio,
        r.max_debt_ratio,
        CASE
            WHEN q.debt_quartile = 1 THEN (q1.total_bad_payers / q1.total_count) / ((q2.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q2.total_count + q3.total_count + q4.total_count))
            WHEN q.debt_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count + q4.total_count))
            WHEN q.debt_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count + q4.total_count))
            WHEN q.debt_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count + q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.debt_quartile = r.debt_quartile
    LEFT JOIN quartile_risk q1 ON q1.debt_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.debt_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.debt_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.debt_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    debt_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_debt_ratio,
    max_debt_ratio
FROM risk_relative
ORDER BY debt_quartile ASC;


---Riesgo_relativo_lastmonthsalary---
#Query para calcular el riesgo relativo de last_month_salary
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        last_month_salary,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable last_month_salary
quartiles AS (
    SELECT
        last_month_salary,
        default_flag,
        NTILE(4) OVER (ORDER BY last_month_salary) AS salary_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        salary_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers
    FROM quartiles
    GROUP BY salary_quartile
),
-- Paso 4: Obtener el rango de salario (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        salary_quartile,
        MIN(last_month_salary) AS min_salary,
        MAX(last_month_salary) AS max_salary
    FROM quartiles
    GROUP BY salary_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.salary_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_salary,
        r.max_salary,
        CASE
            WHEN q.salary_quartile = 1 THEN (q1.total_bad_payers / q1.total_count) / ((q2.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q2.total_count + q3.total_count + q4.total_count))
            WHEN q.salary_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count + q4.total_count))
            WHEN q.salary_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count + q4.total_count))
            WHEN q.salary_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count + q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.salary_quartile = r.salary_quartile
    LEFT JOIN quartile_risk q1 ON q1.salary_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.salary_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.salary_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.salary_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    salary_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_salary,
    max_salary
FROM risk_relative
ORDER BY salary_quartile ASC;

---Riesgo_relativo_numberdepent----

#Query para calcular el riesgo relativo de number_dependents
-- Paso 1: Crear una tabla temporal con los datos base
WITH base_data AS (
    SELECT
        number_dependents,
        default_flag
    FROM `Dataset.view_consolidado`
),
-- Paso 2: Calcular los cuartiles para la variable number_dependents
quartiles AS (
    SELECT
        number_dependents,
        default_flag,
        NTILE(4) OVER (ORDER BY number_dependents) AS dependents_quartile
    FROM base_data
),
-- Paso 3: Calcular el número total de malos y buenos pagadores por cuartil
quartile_risk AS (
    SELECT
        dependents_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
        COUNT(*) - SUM(default_flag) AS total_good_payers
    FROM quartiles
    GROUP BY dependents_quartile
),
-- Paso 4: Obtener el rango de number_dependents (mínimo y máximo) para cada cuartil
quartile_ranges AS (
    SELECT
        dependents_quartile,
        MIN(number_dependents) AS min_dependents,
        MAX(number_dependents) AS max_dependents
    FROM quartiles
    GROUP BY dependents_quartile
),
-- Paso 5: Calcular el riesgo relativo usando la nueva fórmula
risk_relative AS (
    SELECT
        q.dependents_quartile,
        q.total_count,
        q.total_bad_payers,
        q.total_good_payers,
        r.min_dependents,
        r.max_dependents,
        CASE
            WHEN q.dependents_quartile = 1 THEN (q1.total_bad_payers / q1.total_count) / ((q2.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q2.total_count + q3.total_count + q4.total_count))
            WHEN q.dependents_quartile = 2 THEN (q2.total_bad_payers / q2.total_count) / ((q1.total_bad_payers + q3.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q3.total_count + q4.total_count))
            WHEN q.dependents_quartile = 3 THEN (q3.total_bad_payers / q3.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q4.total_bad_payers) / (q1.total_count + q2.total_count + q4.total_count))
            WHEN q.dependents_quartile = 4 THEN (q4.total_bad_payers / q4.total_count) / ((q1.total_bad_payers + q2.total_bad_payers + q3.total_bad_payers) / (q1.total_count + q2.total_count + q3.total_count))
        END AS riesgo_relativo
    FROM quartile_risk q
    JOIN quartile_ranges r ON q.dependents_quartile = r.dependents_quartile
    LEFT JOIN quartile_risk q1 ON q1.dependents_quartile = 1
    LEFT JOIN quartile_risk q2 ON q2.dependents_quartile = 2
    LEFT JOIN quartile_risk q3 ON q3.dependents_quartile = 3
    LEFT JOIN quartile_risk q4 ON q4.dependents_quartile = 4
)
-- Paso 6: Seleccionar los resultados finales
SELECT
    dependents_quartile,
    total_count,
    total_bad_payers,
    total_good_payers,
    riesgo_relativo,
    min_dependents,
    max_dependents
FROM risk_relative
ORDER BY dependents_quartile ASC;

---quartiles_deb_ratio----
----------DEBT RATIO
WITH base_data AS (
    SELECT 
        debt_ratio,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        debt_ratio,
        default_flag,
        NTILE(4) OVER (ORDER BY debt_ratio) AS debt_ratio_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        debt_ratio_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY debt_ratio_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
       debt_ratio_quartile,
        MIN(debt_ratio) AS min_debt_ratio,
        MAX(debt_ratio) AS max_debt_ratio
    FROM quartiles
    GROUP BY debt_ratio_quartile
)
SELECT 
    q.debt_ratio_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_debt_ratio,
    r.max_debt_ratio
FROM quartile_risk q
JOIN quartile_ranges r
ON q.debt_ratio_quartile = r.debt_ratio_quartile
ORDER BY debt_ratio_quartile ASC;

----quartiles_edad----

#CALCULAR LOS CUARTILES DE MALOS PAGADORES POR VARIABLE
WITH base_data AS (
    SELECT 
        age,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        age,
        default_flag,
        NTILE(4) OVER (ORDER BY age) AS age_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        age_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY age_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
        age_quartile,
        MIN(age) AS min_age,
        MAX(age) AS max_age
    FROM quartiles
    GROUP BY age_quartile
)
SELECT 
    q.age_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_age,
    r.max_age
FROM quartile_risk q
JOIN quartile_ranges r
ON q.age_quartile = r.age_quartile
ORDER BY age_quartile ASC;

-----quartiles_last_month_salary-----

#CALCULAR LOS CUARTILES DE MALOS PAGADORES POR VARIABLE
-----LAST MONTH SALARY
WITH base_data AS (
    SELECT 
        last_month_salary,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        last_month_salary,
        default_flag,
        NTILE(4) OVER (ORDER BY last_month_salary) AS last_salary_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        last_salary_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY last_salary_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
       last_salary_quartile,
        MIN(last_month_salary) AS min_last_month_salary,
        MAX(last_month_salary) AS max_last_month_salary
    FROM quartiles
    GROUP BY last_salary_quartile
)
SELECT 
    q.last_salary_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_last_month_salary,
    r.max_last_month_salary
FROM quartile_risk q
JOIN quartile_ranges r
ON q.last_salary_quartile = r.last_salary_quartile
ORDER BY last_salary_quartile ASC;

------quartiles_numero_dependientes---

#CALCULAR LOS CUARTILES DE MALOS PAGADORES POR VARIABLE
--NUMBER OF DEPENDENTS
WITH base_data AS (
    SELECT 
        number_dependents,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        number_dependents,
        default_flag,
        NTILE(4) OVER (ORDER BY number_dependents) AS dependents_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        dependents_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY dependents_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
       dependents_quartile,
        MIN(number_dependents) AS min_number_dependents,
        MAX(number_dependents) AS max_number_dependents
    FROM quartiles
    GROUP BY dependents_quartile
)
SELECT 
    q.dependents_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_number_dependents,
    r.max_number_dependents
FROM quartile_risk q
JOIN quartile_ranges r
ON q.dependents_quartile = r.dependents_quartile
ORDER BY dependents_quartile ASC;

------quartiles_ratio_credito-----
----------using_lines_not_secured_personal_assets
WITH base_data AS (
    SELECT 
        using_lines_not_secured_personal_assets,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        using_lines_not_secured_personal_assets,
        default_flag,
        NTILE(4) OVER (ORDER BY using_lines_not_secured_personal_assets) AS lines_not_secured_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        lines_not_secured_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY lines_not_secured_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
       lines_not_secured_quartile,
        MIN(using_lines_not_secured_personal_assets) AS min_using_lines_not_secured_personal_assets,
        MAX(using_lines_not_secured_personal_assets) AS max_using_lines_not_secured_personal_assets
    FROM quartiles
    GROUP BY lines_not_secured_quartile
)
SELECT 
    q.lines_not_secured_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_using_lines_not_secured_personal_assets,
    r.max_using_lines_not_secured_personal_assets
FROM quartile_risk q
JOIN quartile_ranges r
ON q.lines_not_secured_quartile = r.lines_not_secured_quartile
ORDER BY lines_not_secured_quartile ASC;

-----quartiles_total_loans---
----------total_loans
WITH base_data AS (
    SELECT 
        total_loans,
        default_flag
    FROM `Dataset.view_consolidado`
),
---Calcular los cuartiles dependiendo la edad.
quartiles AS (
    SELECT 
        total_loans,
        default_flag,
        NTILE(4) OVER (ORDER BY total_loans) AS lines_not_secured_quartile
    FROM base_data ---datos de donde provienen las variables
),
-- Calcula el número total de malos pagadores 
quartile_risk AS (
    SELECT 
        lines_not_secured_quartile,
        COUNT(*) AS total_count,
        SUM(default_flag) AS total_bad_payers,
    FROM quartiles
    GROUP BY lines_not_secured_quartile
),
---rango de edad (mínimo y máximo) para cada cuartil.
quartile_ranges AS (
    SELECT
       lines_not_secured_quartile,
        MIN(total_loans) AS min_total_loans,
        MAX(total_loans) AS max_total_loans
    FROM quartiles
    GROUP BY lines_not_secured_quartile
)
SELECT 
    q.lines_not_secured_quartile,
    q.total_count,
    q.total_bad_payers,
    r.min_total_loans,
    r.max_total_loans
FROM quartile_risk q
JOIN quartile_ranges r
ON q.lines_not_secured_quartile = r.lines_not_secured_quartile
ORDER BY lines_not_secured_quartile ASC;


--------score_var1-------
SELECT 'deb_ratio' AS variable, * ,
IF(dummy = 1, 6, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL
SELECT 'mas_90dias' AS variable, * ,
IF(dummy = 1, 5, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL
SELECT 'ratio_credito' AS variable, * ,
IF(dummy = 1, 7, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL;

-------score_var2----------

SELECT 'total_loans' AS variable, * ,
IF(dummy = 1, 4, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL
SELECT 'dependientes' AS variable, * ,
IF(dummy = 1, 4, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL
SELECT 'edad' AS variable, * ,
IF(dummy = 1, 4, 0) AS score
FROM `Dataset.view_dummys`
UNION ALL
SELECT 'last_month_salary' AS variable, * ,
IF(dummy = 1, 4, 0) AS score
FROM `Dataset.view_dummys`

-------view consolidado-----

#Query union de tablas
SELECT
  u.user_id,
  u.age,
  u.number_dependents,
  u.last_month_salary,
  u.default_flag,
  lo.tot_real_estate_loans,
  lo.tot_other_loans,
  lo.total_loans,
  ld.more_90_days_overdue,
  ld.using_lines_not_secured_personal_assets,
  ld.number_times_delayed_payment_loan_30_59_days,
  ld.debt_ratio,
  ld.number_times_delayed_payment_loan_60_89_days
FROM
  `Dataset.view_user_default_limpia` AS u
INNER JOIN
  `Dataset.view_loans_out_tot` AS lo
ON
  u.user_id = lo.user_id
INNER JOIN
  `Dataset.view_loans_detail_limpia` AS ld
ON
  u.user_id = ld.user_id
WHERE
  age < 97
ORDER BY
  u.user_id ASC;

--------view_consolidado_generacion
SELECT C.*, G.* EXCEPT (age, user_id)
FROM `Dataset.view_consolidado` AS C
INNER JOIN `Dataset.view_generacion` AS G
ON C.user_id = G.user_id;

------view_dummys--------
SELECT 'deb_ratio' AS variable, * ,
IF(riesgo_relativo > 1.4, 1, 0) AS dummy
FROM `Dataset.view_rr_debratio`
UNION ALL
SELECT 'mas_90dias' AS variable, * ,
IF(riesgo_relativo > 1, 1, 0) AS dummy
FROM `Dataset.view_rr_mas90dias`
UNION ALL
SELECT 'ratio_credito' AS variable, * ,
IF(riesgo_relativo > 1, 1, 0) AS dummy
FROM `Dataset.view_rr_ratiocredito`
UNION ALL
SELECT 'total_loans' AS variable, * ,
IF(riesgo_relativo > 2, 1, 0) AS dummy
FROM `Dataset.view_rr_totloans`


-------view_generacion----

SELECT user_id, 
    CASE
      WHEN age >= 79 THEN 'Silent'
      WHEN age >= 60 AND age <= 78 THEN 'Baby Boomers'
      WHEN age >= 44 AND age <= 59 THEN 'Generation X'
      WHEN age >= 28 AND age <= 43 THEN 'Millennials'
      WHEN age <= 27 THEN 'Centennials'
    END AS generacion,
    age,

  FROM
    `Dataset.view_consolidado`

---view_rr_perfil----
SELECT 'deb_ratio' AS variable, * 
FROM `Dataset.view_rr_debratio`
WHERE riesgo_relativo > 1.40
UNION ALL
SELECT 'mas_90dias' AS variable, * 
FROM `Dataset.view_rr_mas90dias`
WHERE riesgo_relativo > 40
UNION ALL
SELECT 'ratio_credito' AS variable, * 
FROM `Dataset.view_rr_ratiocredito`
WHERE riesgo_relativo > 42
UNION ALL
SELECT 'total_loans' AS variable, * 
FROM `Dataset.view_rr_totloans`
WHERE riesgo_relativo > 2
UNION ALL
SELECT 'dependientes' AS variable, * 
FROM `Dataset.view_rr_dependientes`
WHERE riesgo_relativo > 1.38
UNION ALL
SELECT 'edad' AS variable, * 
FROM `Dataset.view_rr_edad`
WHERE riesgo_relativo > 2
UNION ALL
SELECT 'last_month_salary' AS variable, * 
FROM `Dataset.view_rr_last_month_salary`
WHERE riesgo_relativo > 2;

----view_loans_out----
SELECT
  loan_id,
  user_id,
  CASE
    WHEN LOWER(loan_type) = 'real estate' THEN 'real estate'
    WHEN LOWER(loan_type) IN ('other', 'others') THEN 'others'
    ELSE LOWER(loan_type)
  END AS loan_type
FROM `Dataset.loans_outstanding`;

-------Matriz de confusión-----
WITH confusion_matrix AS (
  SELECT
    COUNTIF(clasificacion = "Buen pagador" AND default_flag = 0) AS true_positive,
    COUNTIF(clasificacion = "Buen pagador" AND default_flag = 1) AS false_positive,
    COUNTIF(clasificacion = "Mal pagador" AND default_flag = 0) AS false_negative,
    COUNTIF(clasificacion = "Mal pagador" AND default_flag = 1) AS true_negative
  FROM
    `Dataset.Tabla_score`
)
SELECT
  true_positive,
  false_positive,
  false_negative,
























  
