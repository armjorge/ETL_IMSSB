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
from modules.db_payments_feed import DB_PAYMENTS_FEED

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
        web_driver_root = os.path.join(self.folder_root, "imssb_files", "web_driver")
        self.web_driver_manager = WebAutomationDriver(
            downloads_path, web_driver_root=web_driver_root
        )
        # Inicializar SAI manager
        self.orders_manager = orders_management(self.working_folder, self.web_driver_manager, self.data_access)
        self.prei_manager = ACCOUNTS_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)
        self.facturas_manager = FACTURAS(self.working_folder, self.data_access,self.helpers)
        self.downloaded_files_manager = DownloadedFilesManager(self.working_folder, self.data_access)
        self.data_integration = DataIntegration(self.working_folder, self.data_access, self.integration_path, self.helpers)
        self.sql_integration = SQL_CONNEXION_UPDATING(self.integration_path, self.data_access)
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
        os.makedirs(queries_folder, exist_ok=True)


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
                "\t8) Actualizar relación de Oficina de atención de proveedores\n"
                "\t0) Salir\n"
            ).strip()
        
            if choice == "1":
                # Probar fusión de archivos descargados 
                exito_descarga_ordenes = self.orders_manager.execute_download_session(temporal_orders_path, camunda_steps)
                if exito_descarga_ordenes:
                    print("✅ Descarga de Órdenes completada")
                else:
                    print("❌ Error en descarga de Órdenes")

                self.downloaded_files_manager.manage_downloaded_files(temporal_orders_path, camunda_steps)

            elif choice == "2":

                exito_descarga_sagi = self.orders_manager.execute_download_session(temporal_sagi_path, sagi_steps)
                if exito_descarga_sagi:
                    print("✅ Descarga de SAGI completada")
                    self.downloaded_files_manager.manage_downloaded_files(temporal_sagi_path, sagi_steps)   


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
                self.data_integration.integrar_datos()

            elif choice == "5":
                print("🔄 Actualizando SQL")
                self.sql_integration.load_menu()

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
                # CAMUNDA
                exito_descarga_ordenes = self.orders_manager.execute_download_session(temporal_orders_path, camunda_steps)
                self.downloaded_files_manager.manage_downloaded_files(temporal_orders_path, camunda_steps)

                print(f"✅ Descarga de {camunda_steps}")
                # SAGI
                exito_descarga_sagi = self.orders_manager.execute_download_session(temporal_sagi_path, sagi_steps)
                self.downloaded_files_manager.manage_downloaded_files(temporal_sagi_path, sagi_steps)   

                print("✅ Descarga de SAGI completada")
                # Facturas
                exito_facturas = self.facturas_manager.cargar_facturas(facturas)
                print("✅ Descarga de Facturas completada")
                self.data_integration.integrar_datos()
                self.sql_integration.load_menu()
                self.sql_integration.run_queries(queries_folder)
            elif choice == "8":
                print("Actualizando relación de Oficina de atención de proveedores...") 
                db_payments_feed = DB_PAYMENTS_FEED(self.working_folder, self.data_access)
                db_payments_feed.menu_db_payments_feed()
                print("✅ Actualización completada")

            elif choice == "0":
                print("Saliendo de la aplicación...")
                break


if __name__ == "__main__":
    app = ETL_APP()
    app.run()
