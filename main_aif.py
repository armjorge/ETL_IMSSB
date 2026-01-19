import os
import pandas as pd
from modules.proof_of_delivery import SAI_PROOF_OF_DELIVERY
import sys
from dotenv import load_dotenv
from modules.web_automation_driver import WebAutomationDriver
from modules.data_integration import DataIntegration

from modules.config import ConfigManager


class ORCHESTRATOR:
    def __init__(self, root_path):
        self.folder_root =root_path
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.downloads_path = os.path.join(self.working_folder, "Proof of Delivery")
        self.cancel_path = os.path.join(self.working_folder, "Cancelaciones")
        os.makedirs(self.downloads_path, exist_ok=True)
        os.makedirs(self.cancel_path, exist_ok=True)

        self.web_driver_manager = WebAutomationDriver(self.downloads_path)
        
        self.config_manager = ConfigManager(self.working_folder)
        self.data_access = self.config_manager.yaml_creation(self.working_folder)
        
        
    def run(self):
        print("🚀 Inicializando aplicación...")
        print("1. Descargar archivos de SAGI")
        print("2. Cancelar facturas en SAGI")
        while True: 
            choice = input("Elige 1 para descargar archivos SAGI o 2 para cancelar facturas en SAGI: ")
            if choice == "1":  
                # Cargar SAI Manager Proof of Delivery
                from modules.proof_of_delivery import SAI_PROOF_OF_DELIVERY
                # Inicializar SAI manager
                sagi_proof_delivery = 'SAGI_PROOF_DELIVERY'
                self.proof_of_delivery = SAI_PROOF_OF_DELIVERY(self.working_folder, self.web_driver_manager, self.data_access)    
                self.proof_of_delivery.execute_download_session(self.downloads_path,sagi_proof_delivery)
            if choice == "2":
                # Cargar SAI Manager Proof of Delivery
                from modules.cancel_loaded_sagi import SAGI_CANCEL_UPLOADED
                # Inicializar SAI manager
                sagi_proof_delivery = 'SAGI_CANCEL_UPLOADED'
                self.proof_of_delivery = SAGI_CANCEL_UPLOADED(self.working_folder, self.web_driver_manager, self.data_access)    
                self.proof_of_delivery.execute_cancel_session(self.cancel_path,sagi_proof_delivery)                

if __name__ == "__main__":
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    # Aseguramos que BASE_PATH esté en sys.path
    if BASE_PATH not in sys.path:
        sys.path.insert(0, BASE_PATH) 
    env_file = os.path.join(BASE_PATH, ".env")

    if os.path.exists(env_file):
        # Modo desarrollo local: leemos .env
        load_dotenv(dotenv_path=env_file)
        env_main_path = os.getenv("MAIN_PATH")
        if env_main_path:
            root_path = env_main_path
            print(f"✅ MAIN_PATH tomado desde .env: {root_path}")
            ORCHESTRATOR(root_path).run()
        else:
            print(
                f"⚠️ Se encontró .env en {env_file} pero la variable 'MAIN_PATH' no está definida.\n"
            )

    