# ETL IMSSB

## Descripcion general
Este repositorio contiene una aplicacion ETL que automatiza la captura, depuracion, integracion y carga de informacion para el seguimiento de contratos IMSS/INSABI. El flujo parte de fuentes web (SAI/Camunda, SAGI y PREI) combinadas con catalogos y facturas internas, y finaliza actualizando un esquema historico en PostgreSQL ademas de habilitar reportes de inteligencia de negocios.

## Arquitectura y componentes
- `main.py`: orquesta el flujo mediante un menu interactivo (extraccion, transformacion, carga y BI).
- `modules/config.py`: crea o lee `config.yaml` con credenciales, rutas, columnas y definicion de pasos de navegacion.
- `modules/web_automation_driver.py`: prepara Chrome for Testing y configura el WebDriver de Selenium.
- `modules/orders_management.py`: automatiza la descarga de ordenes (SAI/Camunda, SAGI) siguiendo los pasos definidos en el YAML.
- `modules/payments_status_management.py`: descarga cortes PREI por rangos de fechas y valida archivos esperados.
- `modules/facturas.py`: consolida facturas desde Excel y XML/PDF, cruza estatus SAT y genera bases por lote.
- `modules/downloaded_files_manager.py`: inspecciona los archivos descargados en el dia, fusiona por encabezado y renombra con prefijos fecha-hora.
- `modules/data_integration.py`: enlaza ordenes, facturas y tesoreria (mas logistica) por llave de orden y guarda la integracion diaria.
- `modules/sql_connexion_updating.py`: normaliza columnas y reemplaza la tabla destino en PostgreSQL; tambien puede ejecutar scripts SQL.
- `modules/data_warehouse.py`: consulta el historico, construye reportes DOCX/CSV y graficas para toma de decisiones.

## Requisitos previos
- Python 3.10 o superior.
- Dependencias principales: `pandas`, `numpy`, `openpyxl`, `pyyaml`, `selenium`, `lxml`, `PyPDF2`, `sqlalchemy`, `matplotlib`, `python-docx` (opcional para reportes).
- Chrome for Testing y Chromedriver instalados en el directorio del usuario (`Documents/chrome-win64` y `Documents/chromedriver-win64`).
- Acceso a las fuentes web (credenciales validas) y a las rutas locales de facturas y comprobantes.

Para instalar dependencias basicas:
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install pandas numpy openpyxl pyyaml selenium lxml PyPDF2 sqlalchemy matplotlib python-docx
```
Ajusta la lista segun los modulos que planees ejecutar (las funciones de BI requieren `matplotlib` y `python-docx`).

## Preparacion del entorno
1. Clona o descarga el repositorio en tu equipo Windows.
2. Crea la carpeta `Implementacion` (el script puede generarla con caracteres especiales) y dentro define la estructura:
   - `Camunda/`
   - `Facturas/`
   - `SAGI/`
   - `PREI/`
   - `Logistica/`
   - `Estatus SAT/Comprobantes SAT/`
   - `Integracion/`
3. Ejecuta `python main.py` una primera vez para que `ConfigManager` cree `config.yaml` si no existe.
4. Llena `Implementacion/config.yaml` con credenciales, rutas de origen, columnas esperadas y pasos de automatizacion.

## Configuracion (`config.yaml`)
El archivo incluye:
- Credenciales y URL de SAI, PREI y SAGI (`SAI_user`, `SAI_password`, etc.).
- Diccionarios `PAQS_IMSS` y `PAQS_INSABI` con rutas de archivos Excel, hojas y columnas a importar.
- Listados de columnas esperadas (`columns_IMSS_altas`, `columns_IMSS_orders`, `columns_PREI`).
- Parametros de conexion SQL (`sql_url`, `sql_target`).
- Rutas de origen para facturas y reportes (`facturas_path`, `jupyterlab_files`).
- Definicion de pasos de Selenium para cada sitio (`CAMUNDA`, `SAGI`), incluyendo acciones `click`, `send_keys`, `wait_user` y `call_function`.

Mantiene credenciales sensibles fuera del codigo fuente y permite ajustar los flujos de navegacion sin modificar Python.

## Carpetas y archivos generados
- `Implementacion/Camunda` y `Implementacion/SAGI`: contienen subcarpetas `Temporal downloads` y los consolidados diarios renombrados por `DownloadedFilesManager`.
- `Implementacion/Facturas`: guarda los excels consolidados `YYYY-MM-DD-HHh_PAQS_*.xlsx` creados por `FACTURAS` junto al inventario `xmls_extraidos.xlsx`.
- `Implementacion/Estatus SAT`: almacena los PDF descargados y `estatus_facturas.xlsx` generado al leer los acuses.
- `Implementacion/Integracion`: recibe el libro `YYYY-MM-DD HHh_integracion.xlsx` con pestanas `order_df`, `invoice_df`, `accounts_df`, `logistic_df`.
- `sql_queries/`: opcional, coloca `.sql` para que el menu ejecute consultas posteriores a la carga.

## Ejecucion del flujo
Lanza `python main.py` y utiliza el menu interactivo:
1. Descargar `CAMUNDA`: abre la sesion definida y permite al usuario filtrar antes de fusionar los descargables.
2. Descargar `SAGI`: replica el proceso y llama a `export_results` para recorrer todas las paginas.
3. Cargar `PAQS_INSABI`: consolida catalogos internos y cruza XML/PDF.
4. Integrar informacion: toma los archivos mas recientes de ordenes, facturas, tesoreria y logistica para generar el libro de integracion.
5. Actualizar SQL: valida columnas, convierte fechas y reemplaza `eseotres_warehouse.altas_historicas`.
6. Ejecutar consultas SQL: recorre `sql_queries/*.sql` y muestra resultados o mensajes.
7. Inteligencia de negocios: descarga la tabla historica y construye reportes comparativos PTYCSA vs CPI.
`auto`: intenta disparar todo el flujo de manera encadenada.
`0`: salir.

Cada opcion reaprovecha la informacion almacenada en `Implementacion`, por lo que es importante revisar que las carpetas esten limpias antes de iniciar una nueva corrida diaria.

## Detalle de las etapas
### Extraccion
- `orders_management` se apoya en Selenium para seguir los pasos definidos en `config.yaml`, controlar paginadores y guardar los Excel descargados.
- `ACCOUNTS_MANAGEMENT` valida rangos de fechas de PREI, reintenta hasta completar los archivos faltantes y documenta diferencias.
- `FACTURAS` ingiere catalogos locales, extrae datos de XML y copia acuses SAT relevantes.

### Transformacion
- `DownloadedFilesManager` agrupa archivos del mismo dia con encabezados identicos y crea un unico archivo timestamped por origen.
- `DataIntegration` detecta el archivo mas reciente por prefijo `YYYY-MM-DD-HH`, calcula importes, cruza ordenes-facturas-estatus y deja cada conjunto en una hoja del libro final.

### Carga y analitica
- `SQL_CONNEXION_UPDATING` normaliza nombres de columnas, crea el esquema si no existe y ejecuta un reemplazo completo en la tabla destino.
- `data_warehouse.Business_Intelligence` lee el historico, solicita fechas de comparacion y genera reportes (DOCX o CSV) con graficas y tablas de variacion.

## Consultas SQL posteriores
Coloca scripts en `sql_queries/` para ejecutar cortes adicionales (por ejemplo, agregaciones o vistas materializadas). El menu notifica resultados y, si aplica, imprime totales agrupados.

## Buenas practicas
- Ejecuta el flujo en un ambiente virtual dedicado y versiona `config.yaml` solo en repositorios privados.
- Revisa que Chrome for Testing y Chromedriver esten actualizados antes de correr Selenium.
- Valida que los archivos en `Temporal downloads` correspondan al mismo dia; el administrador solo fusiona archivos con la fecha de creacion de hoy.
- Respeta el formato `YYYY-MM-DD-HHh_ORIGEN.xlsx` si subes archivos manualmente al pipeline.
- Antes de correr la opcion 5, confirma que el archivo de integracion contiene las columnas esperadas para evitar cargas incompletas.

## Soporte y extensiones
El diseno modular permite:
- Agregar nuevos origenes replicando el esquema de acciones en `config.yaml` y creando clases dedicadas.
- Ajustar joins del integrador modificando `modules/data_integration.py` sin tocar el resto del flujo.
- Automatizar reportes adicionales en `modules/data_warehouse.py` reutilizando la conexion existente a PostgreSQL.

Cualquier cambio operativo deberia probarse primero en un entorno de pruebas, validando descargas y conciliaciones antes de impactar el historico principal.
