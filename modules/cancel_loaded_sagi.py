import os
import time
import datetime
import platform
from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time

from io import StringIO
import pandas as pd 
import json 
#Basado en SAI_MANAGEMENT
class SAGI_CANCEL_UPLOADED:
    def __init__(self, working_folder, web_driver_manager, data_access):
        """
        Docstring for __init__
        
        :param self: Description
        :param working_folder: Description
        :param web_driver_manager: Description
        :param data_access: Description
        """
        self.working_folder = working_folder
        self.web_driver_manager = web_driver_manager
        self.data_access = data_access
        self.timeout = 30  # Agregar timeout para WebDriverWait
        self.SAGI_CANCEL_UPLOADED = {
            "https://sistemas.insabi.gob.mx/contratos/login": [
                {
                    "type": "send_keys",
                    "by": "XPATH",
                    "locator": "/html/body/div[1]/div/div[2]/div/div/form[1]/div[3]/label/div/div[1]/div/input",
                    "value": os.getenv("SAGI_user"),  # Usar placeholder para usuario
                },
                {
                    "type": "send_keys",
                    "by": "XPATH",
                    "locator": "/html/body/div[1]/div/div[2]/div/div/form[1]/div[4]/label/div/div[1]/div[1]/input",
                    "value": os.getenv('SAGI_password')
                },
                {
                    "type": "click",
                    "by": "XPATH",
                    "locator": "/html/body/div[1]/div/div[2]/div/div/form[1]/div[5]/button",
                },
                {
                    "type": "click", 
                    "by": "XPATH",
                    "locator": '//*[@id="Facturas"]/div'
                },
                {
                    "type": "click", 
                    "by": "XPATH",
                    "locator": '//*[@id="facturas2023"]'
                },
                {
                    "type": "call_function",
                    "function": "cancel_loaded",
                    "args": [],
                    #"kwargs": {"download_directory": "{temporal_sagi_path}"},
                },
            ]
        }

    def cancel_loaded(self):
        print("Iniciando la descarga de órdenes para todo el DataFrame")
        
        # 1. INITIAL PAGE LOAD GUARD
        first_table_xpath = '//*[@id="q-app"]/div/div[2]/div/div[2]/div/div[3]/div[2]/span[1]'
        WebDriverWait(self.driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, first_table_xpath))
        )
        
        xpath_dict = {
            'input_text': '//input[@aria-label="N° de Suministro/ N° de Remisión / N° de Factura"]',
            'loading_engine': "//div[contains(@class, 'text-orange') and contains(text(), 'Por favor espere')]",
            'table_rows': "//table[@class='q-table']/tbody/tr",
            'no_results': "//div[contains(@class, 'q-table__bottom--nodata')]",
            'pdf_viewers': "//div[contains(@class, 'q-dialog')]//iframe | //div[contains(@class, 'pdf-viewer')]", # Adjust based on actual modal container
            'open_menu': '//*[@id="secondaryToolbarToggle"]',
            'download_file': '//*[@id="secondaryDownload"]',
            'open_menu_pdf': '//*[@id="secondaryToolbarToggle"]', # Renamed to avoid conflict
            'download_file_pdf': '//*[@id="secondaryDownload"]', # Renamed to avoid conflict
            'close_modal': '/html/body/div[3]/div[2]/div/div[1]/button[3]',
            'pdf_containers': [
                    '/html/body/div[3]/div[2]/div/div[2]/div[1]/div[1]', # Top Left
                    '/html/body/div[3]/div[2]/div/div[2]/div[1]/div[2]', # Top Right
                    '/html/body/div[3]/div[2]/div/div[2]/div[2]/div[1]', # Bottom Left
                    '/html/body/div[3]/div[2]/div/div[2]/div[2]/div[2]'  # Bottom Right
                ],
            'iframe': './/iframe', # Relative search within container
            'open_menu': '//*[@id="secondaryToolbarToggle"]',
            'download_file': '//*[@id="secondaryDownload"]',
            'close_modal': '/html/body/div[3]/div[2]/div/div[1]/button[3]',
                  
            'iframe_pdf': './/iframe', # Relative search within container
        }

        # OUTER LOOP: Iterate through every row in your DataFrame
        for df_index, row in self.df_relacion.iterrows():
            search_terms = [row['Orden de Suministro']]
            found_for_this_row = False
            
            print(f"\n--- Procesando fila DF {df_index} ---")

            # INNER LOOP: Try different search terms for the current row
            for term in search_terms:
                if not term or str(term).lower() == 'nan': continue
                
                print(f"Probando término: {term}")
                
                # Input handling
                input_field = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath_dict['input_text']))
                )
                input_field.send_keys(Keys.CONTROL + "a")
                input_field.send_keys(Keys.BACKSPACE)
                input_field.send_keys(term)
                input_field.send_keys(Keys.ENTER)

                # Wait for Loading Engine
                try:
                    WebDriverWait(self.driver, 1.5).until(
                        EC.presence_of_element_located((By.XPATH, xpath_dict['loading_engine']))
                    )
                except TimeoutException:
                    pass

                WebDriverWait(self.driver, 15).until_not(
                    EC.presence_of_element_located((By.XPATH, xpath_dict['loading_engine']))
                )

                # Wait for Results or No Results
                try:
                    time.sleep(1)
                    WebDriverWait(self.driver, 10).until(
                        lambda d: d.find_elements(By.XPATH, xpath_dict['no_results']) or 
                                len(d.find_elements(By.XPATH, xpath_dict['table_rows'])) > 0
                    )
                except TimeoutException:
                    continue

                # Check for No Results
                time.sleep(1)
                no_results = self.driver.find_elements(By.XPATH, xpath_dict['no_results'])
                if no_results and no_results[0].is_displayed():
                    continue

                # Process Table Rows
                time.sleep(1)
                web_rows = self.driver.find_elements(By.XPATH, xpath_dict['table_rows'])
                target_row_index = -1
                for i in range(1, len(web_rows) + 1):
                    status_path = f"({xpath_dict['table_rows']})[{i}]/td[11]"
                    status_text = self.driver.find_element(By.XPATH, status_path).text
                    
                    if "Cancelado" not in status_text:
                        target_row_index = i
                        break
                
                # If valid result found, click and break INNER loop to move to next DF row

                if target_row_index != -1:
                    print(f"✅ Éxito para fila {df_index} con término {term}")
                    found_for_this_row = True
                    
                    # Click the eye/view button
                    button_path = f"({xpath_dict['table_rows']})[{target_row_index}]/td[12]//button[4]"
                    self.driver.find_element(By.XPATH, button_path).click()

                    # 1. Wait for modal to be visible                    
                    time.sleep(1)


                    # Wait for Loading Engine
                    try:
                        WebDriverWait(self.driver, 1.5).until(
                            EC.presence_of_element_located((By.XPATH, xpath_dict['loading_engine']))
                        )
                    except TimeoutException:
                        pass

                    WebDriverWait(self.driver, 15).until_not(
                        EC.presence_of_element_located((By.XPATH, xpath_dict['loading_engine']))
                    )
                    button_continuar = '/html/body/div[3]/div[2]/div/div[2]/button[2]'
                    self.driver.find_element(By.XPATH, button_continuar).click()

            if not found_for_this_row:
                print(f"❌ No se encontró resultado válido para la fila {df_index} después de agotar términos.")

        print("\nProcesamiento de DataFrame completado.")
        return True
    
    def execute_cancel_session(self, download_folder, actions_name):
        """Ejecuta una sesión completa de descarga"""
        if not self.web_driver_manager:
            print("❌ Driver de Chrome no disponible")
            return False
        success = False
        self.download_folder = download_folder  # Agregar para usar en _execute_step
        csv_relacion = os.path.join(self.download_folder, 'relacion_eliminar.csv')
        self.df_relacion = pd.read_csv(csv_relacion)
        

        if not os.path.exists(csv_relacion):
            print('No se localizó el archivo de descargas')

        try:
            # Inicializar driver
            #self.driver = self.chrome_driver_load(download_folder)
            self.driver = self.web_driver_manager.create_driver()
            
            # Configurar acciones según los datos de acceso
            #actions = self.data_access.get(actions_name, {})
            actions = self.SAGI_CANCEL_UPLOADED

            if not actions:
                print(f"❌ No se encontraron acciones para '{actions_name}'. Verifique que el nombre sea correcto y que las acciones estén definidas en data_access.")
                return False
            
            # Ejecutar navegación
            success = self._execute_navigation(actions)
            if success:
                print("✅ Navegación completada con éxito. Procediendo a procesar archivos...")
                return True
            else:
                print("❌ Navegación fallida.")
                return False
        except Exception as e:
            print(f"❌ Error en execute_download_session: {e}")
            return False
        finally:
            if success and hasattr(self, 'driver') and self.driver:
                self.driver.quit()  # Solo cerrar si fue exitoso

    def _execute_navigation(self, actions):
        """Ejecuta la navegación web paso a paso"""
        for url, steps in actions.items():
            print(f"\n🔗 Navegando a {url}")
            self.driver.get(url)
            time.sleep(10)  # Aumentar pausa para carga completa de página
            # Esperar a que el body esté presente
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
            try:
                for idx, step in enumerate(steps, start=1):
                    success = self._execute_step(step, idx)
                    if not success:
                        if step["type"] == "call_function":
                            print("⚠️ Reintentando la función personalizada...")
                            continue  # Reintentar la función personalizada
                        else:
                            return False
                            
            except TimeoutException as e:
                print(f"❌ Timeout durante la navegación: {e}")
                return False
            
        return True

    def _execute_step(self, step, step_number):
        """Ejecuta un paso individual de la automatización"""
        step_type = step["type"]
        print(f"  → Paso {step_number}: {step_type}")
        
        if step_type == "wait_user":
            msg = step.get("value", "Presiona enter para continuar...")
            print(f"\n    ⏸ {msg}")
            input()
            return True
        # Paso para llamar a la función. 
        elif step_type == "call_function":
            function_name = step.get("function")
            function = getattr(self, function_name)  # Obtener la función desde self
            args = step.get("args", [])
            kwargs = step.get("kwargs", {})
            # Reemplazar placeholders en kwargs con data_access y working_folder
            for key, value in kwargs.items():
                if isinstance(value, str):
                    # Reemplazar placeholders específicos
                    if "{temporal_sagi_path}" in value:
                        value = value.replace("{temporal_sagi_path}", self.download_folder)
                    if "{working_folder}" in value:
                        value = value.replace("{working_folder}", self.working_folder)
                    kwargs[key] = value.format(**self.data_access)
            print(f"  → Llamando a la función: {function.__name__}")
            try:
                result = function(*args, **kwargs)
                if result:
                    print(f"    ✓ Función {function.__name__} ejecutada con éxito.")
                    return True
                else:
                    print(f"    ⚠️ Función {function.__name__} no completada. Reintentando...")
                    return False
            except Exception as e:
                print(f"    ❌ Error al ejecutar la función {function.__name__}: {e}")
                return False
        # Operación en la web

        try:
            # Localizar elemento
            by_str = step["by"]
            by = getattr(By, by_str)  # Convertir string a objeto By (e.g., "XPATH" -> By.XPATH)
            locator = step["locator"]
            
            print(f"    🔍 Buscando elemento: {by_str} = {locator}")  # Debug: mostrar qué se busca
            
            # Esperar visibilidad del elemento antes de cualquier acción
            WebDriverWait(self.driver, self.timeout).until(
                EC.visibility_of_element_located((by, locator))
            )
            
            element = WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((by, locator))
            )
            
            # Ejecutar acción
            if step_type == "click":
                element.click()
                print(f"    ✓ Click ejecutado en {locator}")
                
            elif step_type == "send_keys":
                value = step["value"]
                # Reemplazar placeholders con valores dinámicos desde data_access
                value = value.format(**self.data_access)
                element.click()
                element.clear()
                element.send_keys(value)
                print(f"    ✓ Texto enviado a {locator}")
                
            else:
                print(f"    ⚠️ Tipo de paso desconocido: {step_type}")
                return False
            
            return True
            
        except TimeoutException:
            print(f"    ❌ Timeout en paso {step_number}: {locator}")
            return False
        except Exception as e:
            print(f"    ❌ Error en paso {step_number}: {e}")
            # Debug adicional: imprimir fuente de la página si falla
            try:
                page_source = self.driver.page_source[:2000]  # Primeros 2000 caracteres
                print(f"    📄 Fuente de la página (primeros 2000 chars): {page_source}")
            except Exception:
                print("    ❌ No se pudo obtener la fuente de la página")
            return False


