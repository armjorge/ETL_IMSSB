import os
import pandas as pd
import glob 
# Módulos propios
from modules.data_warehouse import DataWarehouse
from modules.config import ConfigManager
from modules.helpers import message_print, create_directory_if_not_exists
from modules.web_automation_driver import WebAutomationDriver
from modules.orders_management import orders_management
from modules.payments_status_management import ACCOUNTS_MANAGEMENT
from modules.facturas import FACTURAS
from modules.downloaded_files_manager import DownloadedFilesManager
from modules.data_integration import DataIntegration
from modules.sql_connexion_updating import SQL_CONNEXION_UPDATING


class ETL_APP:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.config_manager = ConfigManager(self.working_folder)
        self.web_driver = None
        self.data_access = None 
        self.integration_path = os.path.join(self.working_folder, "Integración")
        
        
    def update_sql_historico(self):
        print("🔄 Integrando información...")
        print("Fuente de las altas históricas")
        df_final, esquema, tabla = self.altas_historicas()
        print(df_final.head(2))
        try:
            df_final[['fechaAltaTrunc', 'fpp']] = df_final[['fechaAltaTrunc', 'fpp']].apply(pd.to_datetime, errors='coerce', format='%d/%m/%Y')
            df_final = self.sql_integration.sql_column_correction(df_final)         
            self.sql_integration.update_sql(df_final, esquema, tabla)
            # Cambio a diccionario
            print(f"✅ Actualización {esquema}.{tabla} completada")
        except Exception as e:
            print(f"❌ Error durante la actualización: {e}")
        
        print("✅ Integración completada")    
          
    def initialize(self):
        """Inicializa los managers principales"""
        print("🚀 Inicializando aplicación...")
        
        # Inicializar configuración
        self.data_access = self.config_manager.yaml_creation(self.working_folder)
        
        if self.data_access is None:
            print("⚠️ Configura el archivo YAML antes de continuar")
            return False
        
        # Inicializar web driver manager (sin crear el driver aún)
        downloads_path = os.path.join(self.working_folder)
        self.web_driver_manager = WebAutomationDriver(downloads_path)
        # Inicializar SAI manager
        self.orders_manager = orders_management(self.working_folder, self.web_driver_manager, self.data_access)
        self.prei_manager = ACCOUNTS_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)
        self.facturas_manager = FACTURAS(self.working_folder, self.data_access)
        self.downloaded_files_manager = DownloadedFilesManager(self.working_folder, self.data_access)
        self.data_integration = DataIntegration(self.working_folder, self.data_access, self.integration_path)
        self.sql_integration = SQL_CONNEXION_UPDATING(self.working_folder, self.data_access)
        self.data_warehouse = DataWarehouse(self.data_access, self.working_folder)
        print("✅ Inicialización completada")
        return True
    def altas_historicas(self):
        print("🔄 Actualizando información en SQL: longitudinal en el tiempo")

        # Buscar archivos .xlsx en la carpeta de integración

        integration_files = glob.glob(os.path.join(self.integration_path, "*.xlsx"))
        schema = 'eseotres_warehouse'
        table_name = 'altas_historicas'       
        

        # Columnas esperadas: base + integración (sin duplicados, preservando orden)
        base_cols = list(self.data_access['columns_IMSS_altas'])
        columnas_integracion = ['file_date', 'UUID', 'Estado C.R.']
        columnas = list(dict.fromkeys(base_cols + columnas_integracion))

        # Debug
        print(f"🔍 Carpeta de integración: {self.integration_path}")
        print(f"🗂️ Archivos encontrados: {len(integration_files)}")
        print(f"🧩 Columnas esperadas ({len(columnas)}): {columnas}")

        # Filtrar: aceptar archivos que contengan al menos todas las columnas esperadas
        valid_files = []
        for path in integration_files:
            try:
                cols = list(pd.read_excel(path, nrows=0).columns)
                if set(columnas).issubset(set(cols)):
                    valid_files.append(path)
                else:
                    missing = [c for c in columnas if c not in cols]
                    extra = [c for c in cols if c not in columnas]
                    print(f"⚠️ {os.path.basename(path)} faltan: {missing} | extras: {extra}")
            except Exception as e:
                print(f"⚠️ No se pudo leer {os.path.basename(path)}: {e}")

        if not valid_files:
            print("❌ No hay archivos válidos con columnas esperadas")
            return pd.DataFrame(columns=columnas)

        # Cargar cada Excel, quedarnos solo con las columnas esperadas y concatenar
        partes = []
        for p in valid_files:
            try:
                df = pd.read_excel(p)
                df = df.loc[:, columnas]  # solo esperadas, en el orden definido
                partes.append(df)
            except Exception as e:
                print(f"⚠️ Error leyendo {os.path.basename(p)}: {e}")

        if not partes:
            print("❌ No se pudo cargar ningún archivo válido")
            return pd.DataFrame(columns=columnas)

        df_final = pd.concat(partes, ignore_index=True)
        print(f"✅ {len(valid_files)} archivos válidos concatenados: {len(df_final)} filas")
        return df_final, schema, table_name

        
    def run(self):
        """Ejecuta el menú principal de la aplicación"""
        if not self.initialize():
            return
        camunda_steps = 'CAMUNDA'
        sagi_steps = 'SAGI'
        facturas = 'PAQS_INSABI'
        orders_path = os.path.join(self.working_folder, "Camunda")
        temporal_orders_path = os.path.join(orders_path, "Temporal downloads")
        temporal_sagi_path = os.path.join(self.working_folder, "SAGI", "Temporal downloads")
        create_directory_if_not_exists(temporal_orders_path)
        accounts_path = os.path.join(self.working_folder, "SAGI")
        temporal_accounts_path = os.path.join(accounts_path, 'Temporal downloads')
        create_directory_if_not_exists(temporal_accounts_path)
        logistica_path = os.path.join(self.working_folder, "Logística")
        create_directory_if_not_exists(logistica_path)
        #ORDERS_processed_path = os.path.join(self.working_folder, "SAI", "Orders_Procesados")
        FACTURAS_processed_path = os.path.join(self.working_folder, "Facturas")
        PREI_processed_path = os.path.join(self.working_folder, "PREI", "PREI_files")
        ALTAS_processed_path = os.path.join(self.working_folder, "SAI", "SAI Altas_files")
        queries_folder = os.path.join(self.folder_root, "sql_queries")
        while True:
            print("\n" + "="*50)
            choice = input(message_print(

                "Elige una opción:\n"
                "EXTRACCIÓN\n"
                f"\t1) Descargar {camunda_steps}\n"
                f"\t2) Descargar {sagi_steps}\n"
                f"\t3) Cargar {facturas}\n"
                "\t3.1) Información logística -> En desarrollo\n"
                "TRANSFORMACIÓN\n"
                "\t4) Integrar información\n"
                "CARGA\n"
                "\t5) Actualizar SQL (Longitudinal)\n"                
                "\t6) Ejecutar consultas SQL\n"
                "\t7) Inteligencia de negocios\n"
                "\tauto Ejecutar todo automáticamente\n"
                "\t0) Salir"
            )).strip()
        
            if choice == "1":
                # Probar fusión de archivos descargados 
                self.downloaded_files_manager.manage_downloaded_files(temporal_orders_path, camunda_steps)
                exito_descarga_ordenes = self.orders_manager.execute_download_session(temporal_orders_path, camunda_steps)
                if exito_descarga_ordenes:
                    print("✅ Descarga de Órdenes completada")
                else:
                    print("❌ Error en descarga de Órdenes")

                self.downloaded_files_manager.manage_downloaded_files(temporal_orders_path, camunda_steps)

            elif choice == "2":
                self.downloaded_files_manager.manage_downloaded_files(temporal_sagi_path, sagi_steps)   
                exito_descarga_sagi = self.orders_manager.execute_download_session(temporal_sagi_path, sagi_steps)
                if exito_descarga_sagi:
                    print("✅ Descarga de SAGI completada")
                else:
                    print("⚠️ Descarga de SAGI incompleta con archivos pendientes")

                self.downloaded_files_manager.manage_downloaded_files(temporal_sagi_path, sagi_steps)    
            elif choice == "3":
                print("📄 Cargando facturas...")
                exito_facturas = self.facturas_manager.cargar_facturas(facturas)
                if exito_facturas:
                    print("✅ Carga de facturas completada")
                else:
                    print("⚠️ Carga de facturas pendientes")
            elif choice == "4":
                print("🔄 Integrando información...")
                ordenes_fuente = orders_path
                facturas_fuente = FACTURAS_processed_path
                sagi_fuente = accounts_path
                logistica_fuente = logistica_path  # En desarrollo
                self.data_integration.integrar_datos(ordenes_fuente, facturas_fuente, sagi_fuente, logistica_fuente)

            elif choice == "5":
                print("🔄 Actualizando SQL (Longitudinal)")
                self.update_sql_historico()
                print("Generación de agrupaciones y reportes")


            elif choice == "6":
                print("Ejecutando consultas SQL...")
                # Ensure the queries folder exists
                if not os.path.exists(queries_folder):
                    print(f"⚠️ Queries folder not found: {queries_folder}")
                else:
                    self.sql_integration.run_queries(queries_folder)
                
            elif choice == "7":
                print("Inteligencia de negocios.")
                self.data_warehouse.Business_Intelligence()

            elif choice == 'auto':
                exito_descarga_altas = self.orders_manager.descargar_altas(temporal_altas_path)
                if exito_descarga_altas:
                    exito_descarga_prei = self.prei_manager.descargar_PREI(temporal_prei_path)
                    self.downloaded_files_manager.manage_downloaded_files(temporal_altas_path)
                    print("✅ Descarga de Órdenes completada")
                    if exito_descarga_prei:
                        print("✅ Descarga de PREI completada")
                        self.downloaded_files_manager.manage_downloaded_files(temporal_prei_path)
                        exito_facturas = self.facturas_manager.cargar_facturas()
                        if exito_facturas:
                            print("✅ Carga de facturas completada")
                        self.data_integration.integrar_datos(PREI_processed_path, ALTAS_processed_path, FACTURAS_processed_path)
                        print("✅ Integración completada")
                        self.update_sql_historico()
                        self.sql_integration.run_queries(queries_folder)
                    else:
                        print("⚠️ No pudimos continuar con el proceso ETL en automático")
            elif choice == "0":
                print("Saliendo de la aplicación...")
                break


if __name__ == "__main__":
    app = ETL_APP()
    app.run()
