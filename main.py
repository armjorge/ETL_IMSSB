import os
import pandas as pd
import glob 
# Módulos propios
from modules.data_warehouse import DataWarehouse
from modules.config import ConfigManager
from modules.web_automation_driver import WebAutomationDriver
from modules.orders_management import orders_management
from modules.payments_status_management import ACCOUNTS_MANAGEMENT
from modules.facturas import FACTURAS
from modules.downloaded_files_manager import DownloadedFilesManager
from modules.data_integration import DataIntegration
from modules.sql_connexion_updating import SQL_CONNEXION_UPDATING
from modules.helpers import HELPERS

class ETL_APP:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.config_manager = ConfigManager(self.working_folder)
        self.web_driver = None
        self.data_access = None 
        self.integration_path = os.path.join(self.working_folder, "Integración")
        
        
          
    def initialize(self):
        """Inicializa los managers principales"""
        print("🚀 Inicializando aplicación...")
        
        # Inicializar configuración
        self.data_access = self.config_manager.yaml_creation(self.working_folder)
        
        if self.data_access is None:
            print("⚠️ Configura el archivo YAML antes de continuar")
            return False
        
        # Inicializar web driver manager (sin crear el driver aún)
        self.helpers = HELPERS()
        downloads_path = os.path.join(self.working_folder)
        self.web_driver_manager = WebAutomationDriver(downloads_path)
        # Inicializar SAI manager
        self.orders_manager = orders_management(self.working_folder, self.web_driver_manager, self.data_access)
        self.prei_manager = ACCOUNTS_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)
        self.facturas_manager = FACTURAS(self.working_folder, self.data_access,self.helpers)
        self.downloaded_files_manager = DownloadedFilesManager(self.working_folder, self.data_access)
        self.data_integration = DataIntegration(self.working_folder, self.data_access, self.integration_path, self.helpers)
        self.sql_integration = SQL_CONNEXION_UPDATING(self.working_folder, self.data_access)
        self.data_warehouse = DataWarehouse(self.data_access, self.working_folder)
        
        print("✅ Inicialización completada")
        return True
        
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
        os.makedirs(temporal_orders_path, exist_ok=True)
        accounts_path = os.path.join(self.working_folder, "SAGI")
        temporal_accounts_path = os.path.join(accounts_path, 'Temporal downloads')
        os.makedirs(temporal_accounts_path, exist_ok=True)
        logistica_path = os.path.join(self.working_folder, "Logística")
        os.makedirs(logistica_path, exist_ok=True)
        #ORDERS_processed_path = os.path.join(self.working_folder, "SAI", "Orders_Procesados")
        FACTURAS_processed_path = os.path.join(self.working_folder, "Facturas")
        PREI_processed_path = os.path.join(self.working_folder, "PREI", "PREI_files")
        ALTAS_processed_path = os.path.join(self.working_folder, "SAI", "SAI Altas_files")
        queries_folder = os.path.join(self.folder_root, "sql_queries")


        while True:
            print("\n" + "="*50)
            choice = input(

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
            ).strip()
        
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
