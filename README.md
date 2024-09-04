## Flights  Delay : 

Conjunto de datos sobre cancelaciones y retrasos de vuelos de aerolíneas de enero de 2023, datos extraídos del Departamento de Transporte de EE.UU., Oficina de Estadísticas de Transporte (https://www.transtats.bts.gov) y disponibles en Kaggle.

## Temas

- :bulb: [Objetivo](#Objetivo)
- :hammer_and_wrench: [Herramientas](#herramientas)
- </> [Lenguajes](#lenguajes)
- :gear: [Procesamiento y preparación de datos](#procesamiento-y-preparación-de-datos)
- :bar_chart: [Visualización y Análisis de Datos](/Visualizacion/README.md)
- :bookmark: [Pruebas y Resultados](/Google%20_colab/README.md)
- :heavy_check_mark: [Conclusiones y Recomendaciones](/Presentacion/README.md)


## Objetivo

Analizar y predecir los retrasos de vuelos mediante técnicas de análisis de datos como el riesgo relativo, y regresión lineal, para identificar rutas, aeropuertos, y aerolíneas con alta frecuencia de demora, entender las principales causas de estos retrasos, y mejorar la toma de decisiones proactivas en la gestión de vuelos.

## Procesamiento y preparación de datos

1. Procesar y preparar la base de datos:

  Conectar/importar datos a otras herramientas

* Se creó el proyecto4-flightdelay y el conjunto de datos Dataset en BigQuery.
* Tablas importadas: DOT_CODE_DICTIONARY, AIRLINE_CODE_DICTIONARY y flights_202301.

2. Identificar y manejar valores nulos:

  Se identifican valores nulos a través de comandos SQL COUNTIF, IS NULL, AS.

  DOT_CODE_DICTIONARY: 4 valores nulos.

* Se separó la columna Description, en Name y Description.
*  Se eliminaron los siguientes códigos porque no hay información en la
columna descripción, 22114, 22115, 22116, 22117.

  AIRLINE_CODE_DICTIONARY: 0 valores nulos.
* Se cambió el encabezado de la tabla de string_field_0 a AIRLINE_CODE, y de
string_field_1 a NAME_AIRLINE, generando una vista de la tabla
view_airline_code.

  Flights_202301:
* DEP_TIME: 9,978 vuelos cancelados.
*  TAXI_OUT: 10,197 vuelos cancelados algunos tienen DEP_TIME y
  DEP_DELAY.
*  WHEELS_OFF: 10,197 son vuelos cancelados algunos tienen DEP_TIME
y DEP_DELAY.
* WHEELS_ON: 10,517 son vuelos cancelados.
* TAXI_IN: 10,517 son vuelos cancelados.
* ARR_TIME: 10,517 son vuelos cancelados.
*  ARR_DELAY: 11,640 son vuelos cancelados o desviados.
  * Vuelos cancelados=10,295.
  * Vuelos desviados= 1,345.
* CRS_ELAPSED_TIME: 1 vuelo cancelado
* ELAPSED_TIME: 11,640 vuelo cancelado o desviado
*  AIR_TIME: 11,640 vuelo cancelado o desviado
*  DELAY_DUE_CARRIER: 422,124 son los vuelos que llegan a tiempo.
*  DELAY_DUE_WEATHER: 422,124 son los vuelos que llegan a tiempo.
* DELAY_DUE_NAS: 422,124 son los vuelos que llegan a tiempo.
*  DELAY_DUE_SECURITY: 422,124 son los vuelos que llegan a tiempo.
*  DELAY_DUELATE_AIRCRAFT: 422,124 son los vuelos que llegan a
tiempo.

  Se IMPUTARON los valores nulos con el valor de 0 ya que son datos que no se
  obtuvieron debido a que los vuelos fueron cancelados o desviados, esto nos
  permite mantener la consistencia de la naturaleza de los datos.

  Para las variables DELAY_DUE_CARRIER, DELAY_DUE_WEATHER,
  DELAY_DUE_NAS, DELAY_DUE_SECURITY, DELAY_DUELATE_AIRCRAFT,
  se IMPUTARON colocando el valor de 0, ya que el número de datos nulos
  corresponde a los vuelos que llegaron a tiempo a su destino. Los valores nulos
  no indican falta de datos sino la ausencia de un evento, por ejemplo, retraso por
  clima, retrasó por la aerolínea.

3. Identificar y manejar valores duplicados:

  Se identifican valores duplicados a través de comandos SQL COUNT, GROUP BY,
  HAVING.
* DOT_CODE_DICTIONARY: no hay valores duplicados.
* AIRLINE_CODE_DICTIONARY: no hay valores duplicados.
* flights_202301: no hay valores duplicados.

4. Identificar y manejar datos fuera del alcance del análisis: 

  Se manejan variables que no son útiles para el análisis a través de comandos SQL SELECT
  , EXCEPT.
Se excluyen del análisis las variables DELAY_DUE_CARRIER,DELAY_DUE_WEATHER, DELAY_DUE_NAS, DELAY_DUE_SECURITY,DELAY_DUELATE_AIRCRAFT, porque tienen muchos datos nulos, en su lugar
se utilizan las variables en las que se imputaron los datos.

5. Identificar y manejar datos discrepantes en variables categóricas
* DOT_CODE_DICTIONARY: Separamos la columna Description, en Name y
Description, utilizando las fórmulas =LEFT(A1, FIND("/", A1) - 1) y =RIGHT(A1,
LEN(A1) - FIND("/", A1)) en google sheets.
* AIRLINE_CODE_DICTIONARY: Se cambió el encabezado de la tabla de
string_field_0 a AIRLINE_CODE, y de string_field_1 a NAME_AIRLINE,
generando una vista de la tabla view_airline_code, con los comandos WHERE y
AS.
* Con los comandos REGEXP_CONTAINS, WHEN, ELSE, CASE, END, se
comprobó la presencia o ausencia de caracteres especiales en las variables
categóricas, y en caso de tenerlos que fueran adecuados para su definición.

6. Identificar y manejar datos discrepantes en variables numéricas
(OUTLIERS)
Con los comandos WITH, APPROX_QUANTILES, CASE, WHEN, ELSE, WHERE, se
identifican los datos outliers de la tabla flights_202301. Se utilizó la metodología de
rango intercuartil.
* Se realizaron box plots e histogramas en google colab usando python para
visualizar mejor los resultados encontrados.

7. Crear nuevas variables:

* Con los comandos WHEN, CASE, ELSE se crearon las variables
ESTATUS_VUELO para agregar las siguientes etiquetas de identificación “A
TIEMPO”, “DEMORADO”, “CANCELADO”, “DESVIADO” a cada vuelo. Adicional
se creó la variable ETIQUETA_NUM para asignar el valor de “1” a los vuelos
demorados, “0” a los no demorados, “2” a los vuelos cancelados y “3” a los
vuelos desviados.
* Con el comando IF se creó la variable CAUSAS_DEMORA, que asigna una
etiqueta de acuerdo al tipo de demora de los vuelos, etiqueta a los vuelos
desviados, etiqueta los vuelos a tiempo, y para los vuelos cancelados especifica
el motivo.
* Con el comando FORMAT_DATE se creó la variable DAY_OF_WEEK.

8. Unir tablas: 

* Con el comando JOIN se unieron las vistas view_consolidado_completo,
view_airline_code y dot_code_dictionary

9. Agrupar datos según variables categóricas: 

* Se conectaron los datos a looker studio desde Bigquery.
*  Se crearon campos calculados para la visualización de variable y elaboración de
gráficos.

10. Visualizar las variables categóricas: 
* Se realizaron gráficos de barras para la visualización de variables y exploración
de datos en looker studio.
* Se crearon listas drop-down list para filtrar y explorar la información y se agregaron score cards con datos relevantes.

11. Aplicar medidas de tendencia central y aplicar medidas de dispersión: 

* Se crearon tablas en looker studio con las medidas de tendencia central y de
dispersión (media, promedio, rango, desviación estándar) de la variable
ARR_DELAY para explorar los datos considerando todos los vuelos y los vuelos
demorados.

12. Visualizar distribución: 

* Se crearon box plot para visualizar la distribución de las variables ARR_DELAY y
DEP_DELAY por causa de demora y por estatus de vuelo.
*  Se realizaron box plot e histogramas para las variables en google colab usando python.

13. Visualizar el comportamiento de los datos a lo largo del tiempo:

* Se crearon gráficos de línea para observar el comportamiento de los datos a lo
largo del mes y por día de la semana.

14. Calcular cuartiles, deciles o percentiles:

* Con los comandos WITH, NTILE, COUNT, GROUP BY, MIN, MAX, JOIN, se
calcularon los cuartiles de la variable ARR_DELAY para los vuelos demorados y
para el consolidado con todos los vuelos, se contabilizó el número de vuelos por
cuartil, el total de vuelos retrasados y se calculó el rango de cada cuartil.
Vuelos demorados
Todos los vuelos

15. Calcular correlación entre variables:

* Con el comando CORR se calculó la correlación entre las variables en BigQuery.
* En google colab usando python se creó una matriz de correlación incluyendo
todas las variables numéricas.

16. Calcular riesgo relativo:

* Con los comandos WITH, SUM, CASE, WHEN, THEN, ELSE, END,
SAFE_DIVIDE, COUNT, GROUP BY, se calculó el riesgo relativo en BigQuery
para las variables DELAY_DUE_CARRIER_NN, DELAY_DUE_SECURTY_NN,
DELAY_DUE_WEATHER_NN, DELAY_DUE_LATE_AIRCRAFT_NN,
DELAY_DUE_NAS_NN, adicional se hizo el cálculo por Aerolínea, Aeropuerto y
Ruta, obteniendo una tabla con el total de vuelos expuestos, total de vuelos no
expuestos, las tasas de incidencia de ambos grupos y el riesgo relativo.

17. Validar hipótesis: 

* ¿Las aerolíneas con mayor número de vuelos demorados tienen mayor riesgo de
que sus vuelos se retrasen?

Conclusión: Se refuta la hipótesis, la aerolínea con más cantidad de vuelos
demorados es WN (Southwest Airlines Co.) con 21, 830, tiene un riesgo relativo de
0.87, por lo que hay menor riesgo de demora en comparación con el resto. La aerolínea
que tiene el mayor riesgo relativo es F9 (Frontier Airlines Inc.) con un riesgo relativo de
1.59

* ¿Los vuelos que enfrentan condiciones meteorológicas adversas (como tormentas
o niebla) tienen un riesgo relativo significativamente mayor de sufrir retrasos en
comparación con aquellos que no enfrentan condiciones meteorológicas
adversas?

Conclusión: Se refuta la hipótesis, los vuelos demorados por condiciones
meteorológicas tiene un riesgo relativo menor a otras causas de demora, por ejemplo,
retrasos por operador, retrasos por aeronave tardía y retrasos por NAS.

* ¿Las demoras por causa del operador son más frecuentes en las aerolíneas con
mayor riesgo relativo?

Conclusión: Se válida la hipótesis, la causa de demora con mayor riesgo relativo es la
demora del operador.

18. Regresión lineal

Se aplicó la técnica de regresión lineal para entender la relación entre el tiempo de
retraso de llegada de un vuelo y las diversas causas de retraso representadas por las
variables del análisis. Para más información puedes revisar este docuemento [Pruebas y Resultados](/Google%20_colab/Proyecto4.ipynb)


## Resultados y Conclusiones:

* Existe una relación lineal muy fuerte entre DEP_DELAY y ARR_DELAY (retraso
en la llegada), lo que significa que la mayoría del retraso en la llegada se debe
directamente al retraso en la salida.
* Los retrasos causados por la aerolínea tienen una correlación moderada con los
retrasos en la llegada. Aunque estos retrasos son significativos, hay otros
factores que también influyen, como el clima, la congestión del tráfico aéreo, y
problemas operativos.
* Existe una correlación moderada entre los retrasos debidos a la llegada tardía
de una aeronave y los retrasos en la llegada. Este es un factor más relevante
que el clima, pero no el más determinante.
* Las condiciones meteorológicas adversas influyen en los retrasos, pero la
correlación es débil. Esto sugiere que, aunque el mal clima puede contribuir, no
es un factor predominante.
* Hay una correlación débil entre los retrasos causados por problemas en el
Sistema de Navegación Aérea (NAS) y los retrasos en la llegada. Aunque existe
una relación, no es suficientemente fuerte como para ser un factor clave.
* Los retrasos debidos a problemas de seguridad no tienen un impacto
significativo en los retrasos de llegada. La correlación es prácticamente
inexistente.
* Frontier Airlines (F9): Esta aerolínea tiene el mayor riesgo relativo de 1.59. Esto
sugiere que sus vuelos tienen un 59% más de probabilidades de experimentar
retrasos en comparación con la tasa de referencia.
* Spirit Air Lines (NK) y Allegiant Air (G4): Ambas aerolíneas también presentan
un riesgo relativo elevado, con valores de 1.33 y 1.30 respectivamente. Esto
indica un riesgo significativamente alto de retrasos comparado con otras
aerolíneas.
* Los pasajeros que viajan con Frontier, Spirit, y Allegiant pueden esperar un
mayor riesgo de retrasos, y estas aerolíneas podrían beneficiarse de iniciativas
para mejorar la puntualidad.
* Pago Pago, TT (PPG) y Clarksburg/Fairmont, WV (CKB) son aeropuertos que
tienen los riesgos relativos más altos, de 2.52 y 2.31 respectivamente. Los
vuelos desde estos aeropuertos tienen más del doble de probabilidad de
retrasarse en comparación con otros.
*  Lincoln, NE (LNK) y Niagara Falls, NY (IAG), tienen riesgo relativo alto, de
alrededor de 1.98, lo que sugiere que estos aeropuertos tienen problemas
similares con la puntualidad.

Para mas detalle de las conclusiones y recomendaciones te invito a revisar este documento [Conclusiones y Recomendaciones](/Presentacion/Presentación.pdf)


## Herramientas

* BigQuery
* Looker Studio
* Google Docs
* Google Slide
* Google Colab
* Tableau

## Lenguajes

* SQL
* Python



