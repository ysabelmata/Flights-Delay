## Riesgo Relativo: "Super Caja" 

El riesgo relativo es una medida estadística que proporciona una indicación de cuánto más probable es que ocurra un resultado en el grupo expuesto en comparación con el grupo no expuesto. 
Un riesgo relativo igual a 1 sugiere que no hay diferencia en la incidencia entre los dos grupos, mientras que un riesgo relativo mayor que 1 indica un mayor riesgo en el grupo expuesto, y un riesgo relativo menor que 1 indica un menor riesgo en el grupo expuesto.

## Temas

- :bulb: [Introducción](#introducción)
- :hammer_and_wrench: [Herramientas](#herramientas)
- </> [Lenguajes](#lenguajes)
- :gear: [Procesamiento y preparación de datos](#procesamiento-y-preparación-de-datos)
- :bar_chart: [Visualización y Análisis de Datos](/Visualizacion/README.md)
- :bookmark: [Pruebas y Resultados](/Jupiter_Notebook/README.md)
- :heavy_check_mark: [Conclusiones y Recomendaciones](/%20Presentacion%20%20/README.md)

## Introducción
En el actual escenario financiero, la disminución de las tasas de interés ha generado un notable aumento en la demanda de crédito en el banco "Super Caja". Sin embargo, esta creciente demanda ha sobrecargado al equipo de análisis de crédito, que se encuentra actualmente inmerso en un proceso manual ineficiente y demorado para evaluar las numerosas solicitudes de préstamo. Frente a este desafío, se propone una solución innovadora: la automatización del proceso de análisis mediante avanzadas técnicas de análisis de datos. 

Esta propuesta también destaca la integración de una métrica existente de pagos atrasados, fortaleciendo así la capacidad del modelo. Este proyecto no solo brinda la oportunidad de sumergirse en el análisis de datos, sino que también ofrece la adquisición de habilidades clave en la clasificación de clientes, el uso de la matriz de confusión y la realización de consultas complejas en BigQuery, preparandote para enfrentar desafíos analíticos en diversos campos.

## Objetivo

El objetivo principal es mejorar la eficiencia y la precisión en la evaluación del riesgo crediticio, permitiendo al banco tomar decisiones informadas sobre la concesión de crédito y reducir el riesgo de préstamos no reembolsables.
El objetivo del análisis es armar un score crediticio a partir de un análisis de datos y la evaluación del riesgo relativo que pueda clasificar a los solicitantes en diferentes categorías de riesgo basadas en su probabilidad de incumplimiento.

   
## Procesamiento y preparación de datos

1. Conectar/importar datos a herramientas:

* Se creó el proyecto-riesgo-relativo y el conjunto de datos Dataset en BigQuery.
* Tablas importadas: 

    * Tabla 1 user_info: datos del usuario/cliente. 
    * Tabla 2 loans_outstanding (préstamos pendientes): datos de tipo de préstamos. 
    * Tabla 3 loans_details:comportamiento de pago de los préstamos. 
    * Tabla 4 default: identificación de clientes incluyendo morosos.


2. Identificar y manejar valores nulos:

* Se identifican valores nulos a través de comandos SQL COUNT, WHERE y IS NULL.
* loans_outstanding: 0 valores nulos.
* loans_details: 0 valores nulos.
* default: 0 valores nulos.
* user_info: 7199 valores nulos en la columna last_month_salary y number_dependents.

Los datos nulos (7199) representan el 20% del total (36,000). Nuestro objetivo en este análisis es encontrar el perfil de los clientes que pagan mal para generar un motor de reglas de aprobación de crédito, la variable last_month_salary es importante para nuestro análisis.

  * Con los comandos AVG, WHERE y GROUP BY, se calculó el promedio a la variable last_month_salary para cada categoría de cliente (buen pagador/mal pagador), sin considerar datos outliers, salarios mayores a 400,000.
  * Con los comandos IFNULL, CASE, WHEN, THEN, ELSE, se IMPUTARON los valores nulos de la variable last_month_salary colocando el promedio por categoría.
  * Con los comandos WITH, RANK se calculó la moda para la variable number_dependents para cada categoría de cliente (buen pagador/mal pagador).
  * Con los comandos IFNULL, CASE, WHEN, THEN, ELSE, se IMPUTARON los valores nulos de la variable number_dependents colocando la moda por categoría.


3. Identificar y manejar valores duplicados:

* Se identifican duplicados a través de comandos SQL COUNT, GROUP BY, HAVING.
* user_info: no hay valores duplicados.
* loans_outstanding: no hay valores duplicados.
* loans_details: no hay valores duplicados.
* default: no hay valores duplicados.

4. Identificar y manejar datos fuera del alcance del análisis:

* Se manejan variables que no son útiles para el análisis a través de comandos SQL SELECT EXCEPT.
* track_tecnical_info: se excluyó la columna key por tener muchos datos nulos (95) y la columna mode por no tener información relevante para el análisis.
* Se manejan variables que no son útiles para el análisis a través de comandos SQL SELECT EXCEPT.
* Se excluye la variable sex de la tabla user_info.
* Con el comando CORR y STDDEV, se calcula la correlación y la desviación estándar entre las variables more_90_days_overdue y number_times_delayed_payment_loan_30_59_days, y more_90_days_overdue y number_times_delayed_payment_loan_60_89_days. 
* Se  identifican las variables con alta correlación.
* more_90_days_overdue y number_times_delayed_payment_loan_60_89_days tienen la correlación más alta con 0.99
* Number_times_delayed_payment_loan_60_89_days tiene la desviación estándar más baja 4.1.


5. Identificar y manejar datos inconsistentes en variables numéricas:

* Con los comandos WITH, APPROX_QUANTILES, CASE, WHEN, ELSE, WHERE, se identifican los datos outliers de las tablas user_info y de loans_detail. Se utilizó la metodología de rango intercuartil.

* Se realizaron box plots e histogramas en google colab usando python para visualizar mejor los resultados encontrados, adicional se hicieron nuevas consultas para encontrar los valores más extremos un top 30 y top 70, concluyendo lo siguiente:

 * Age: se mantienen 10 datos.
 * Last_month_salary: en los gráficos y en los datos se observan 5 valores muy por encima de los demás, por lo que se descartaran los registros arriba de 400,000.
 * Number_dependents: en las gráficas y en los datos se observa un valor muy alejado.
 * Number_times_deleyed_payment_loan_30_59_days: en los gráficos y en los datos se observan 63 valores muy por encima de los demás (98 y 96) y con datos inconsistentes en las otras variables, por lo que se descartaran los registros mayores a 20.
 * Number_times_deleyed_payment_loan_60_89_days: en los gráficos y en los datos se observan 63 valores muy por encima de los demás (98 y 96) y con datos inconsistentes en las otras variables, por lo que se descartaran los registros mayores a 20.
 * Using_lines_not_secured: se observan 4 valores por encima de los demás.
 * Debt_ratio: se observa un valor por encima de los demás.


6. Crear nuevas variables:

* Con los comandos DISTINCT, SUM, CASE, WHEN, GROUP BY, se hizo una tabla agrupada por usuario, con una fila para cada cliente, mostrando el tipo de préstamo y la cantidad total.

7. Unir tablas:

* Con el comando INNER JOIN se unieron las vistas user_default_limpia, loans_out_totales, loans_detail_limpia.

8. Agrupar datos según variables categóricas:

* Se conectaron los datos a Looker studio desde BigQuery.
* Se creó un campo calculado en Looker studio para crear una clasificaciónde edad por Generaciones.
* Se creó un grupo categoría de pago para buen pagador y mal pagador de acuerdo al campo default_flag.

9. Visualizar las variables categóricas:

* Se utilizaron graficos de barra, torta, score cards, y diferentes tablas dinamicas  para la visualización de variables y exploración de datos en looker studio. 

10. Aplicar medidas de tendencia central y aplicar medidas de dispersión

* Se crearon tablas en looker studio con las medidas de tendencia central (mediana, promedio) para comparar los datos por edad y categoría de pago.
* Se crearon tablas en looker studio con la desviación estándar para comparar los datos por edad y por categoría de pago.

11. Visualizar distribución:

* Se crearon box plot para visualizar la distribución de las variables por rango de edad y categoría de pago en looker studio.
* Se realizaron box plot e histogramas para las variables en google colab usando python.

12. Aplicar correlación entre las variables numéricas:

Se creó una matriz de correlación de todas las variables en google colab utilizando Python.
Con el comando CORR se calculó la correlación entre variables en BigQuery.

13. Calcular cuartiles, deciles o percentiles:

* Con los comandos WITH, NTILE, COUNT, GROUP BY, MIN, MAX, JOIN, se calcularon los cuartiles de cada variable, se contabilizó el número de usuarios por cuartil, el total de malos pagadores y se calculó el rango de cada cuartil.

14. Calcular riesgo relativo: 

* Con los comando WITH, NTILE, COUNT, MIN, MAX, CASE, WHEN, LEFT JOIN, se calculó el riesgo relativo en BigQuery para las variables, obteniendo una tabla con los cuartiles, total de usuarios, total de malos y buenos pagadores, riesgo relativo, y el rango de los cuartiles.

Este proceso es fundamental para asegurar la calidad y precisión del análisis subsiguiente.

## Herramientas

* BigQuery
* Looker Studio
* Google Docs
* Google Slide
* Jupyter Notebook

## Lenguajes

* SQL
* Python



