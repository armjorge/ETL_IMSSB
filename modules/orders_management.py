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
from modules.helpers import create_directory_if_not_exists
from io import StringIO
import pandas as pd 
import json 
#Basado en SAI_MANAGEMENT
class orders_management:
    def __init__(self, working_folder, web_driver_manager, data_access):
        self.working_folder = working_folder
        self.web_driver_manager = web_driver_manager
        self.data_access = data_access
        self.timeout = 30  # Agregar timeout para WebDriverWait

    def export_results(self, download_directory):
        """Extrae y exporta datos de la tabla de resultados para ambos sets sin input del usuario."""
        today_date = datetime.datetime.now()  # Usar datetime.datetime.now() para incluir hora
        today_yyyy_mm_dd_hh = today_date.strftime("%Y %m %d %Hh")
        
        xpath_dict = {
            'facturas_button': '//*[@id="Facturas"]/div/div[1]',
            'facturas_2023_2024': '//*[@id="facturas2023"]/div/div[1]',
            'main_menu': '//*[@id="q-app"]/div/header/div/div[1]',
            'facturas_2024': '//*[@id="facturas2024"]/div/div[1]',
            'down_arrow_menu': '//*[@id="q-app"]/div/div[2]/div/div[2]/div/div[3]/div[2]/label/div/div/div[2]/i',
            'choice_50': '/html/body/div[3]/div[2]/div[7]/div[2]/div',
            'next_page_button': '//*[@id="q-app"]/div/div[2]/div/div[2]/div/div[3]/div[3]/button[3]'
        }
        
        # Cargar páginas previas desde archivo
        pages_file = os.path.join(self.working_folder, "sagi_pages.json")
        previous_pages = {"2023-2024": 0, "2024": 0}
        if os.path.exists(pages_file):
            try:
                with open(pages_file, 'r') as f:
                    previous_pages = json.load(f)
            except (json.JSONDecodeError, ValueError):
                print(f"⚠️ Error al cargar {pages_file}, usando valores por defecto.")
                previous_pages = {"2023-2024": 0, "2024": 0}
        
        
        # Verificar archivos existentes
        existing_files = {}
        for downloaded_set in ["2023-2024", "2024"]:
            output_file_name = os.path.join(download_directory, f"{today_yyyy_mm_dd_hh} SAGI_{downloaded_set}.xlsx")
            if os.path.exists(output_file_name):
                print(f"El archivo para el data_set {downloaded_set} de hoy {today_yyyy_mm_dd_hh} ya existe, omitiendo")
                existing_files[downloaded_set] = True
            else:
                existing_files[downloaded_set] = False
        
        # Función auxiliar para extraer datos de un set
        def extract_set_data(downloaded_set, xpath_to_click):
            output_data = []
            previous_table_html = ""  # Para verificar cambios en la tabla
            
            # Hacer click en el menú correspondiente
            try:
                menu_element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath_to_click))
                )
                menu_element.click()
                time.sleep(10)  # Esperar carga de página
            except Exception as e:
                print(f"❌ Error al hacer click en {xpath_to_click}: {e}")
                return []
            
            # Seleccionar 50 filas por página antes de extraer
            try:
                down_arrow = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath_dict['down_arrow_menu']))
                )
                down_arrow.click()
                time.sleep(2)
                
                choice_50 = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath_dict['choice_50']))
                )
                choice_50.click()
                time.sleep(5)  # Esperar que se aplique el cambio
                print(f"✅ Seleccionadas 50 filas por página para {downloaded_set}")
            except Exception as e:
                print(f"⚠️ Error al seleccionar 50 filas por página para {downloaded_set}: {e}")
                # Continuar sin seleccionar, para no detener el proceso
            
            page_count = 0
            while True:
                page_count += 1
                try:
                    table_element = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="q-app"]/div/div[2]/div/div[2]/div/div[2]/table'))
                    )
                    table_html = table_element.get_attribute('outerHTML')
                    
                    # Verificar que la tabla haya cambiado (no idéntica a la anterior)
                    if table_html == previous_table_html and page_count > 1:
                        print(f"⚠️ La tabla no cambió para página {page_count} en {downloaded_set}, esperando más tiempo...")
                        time.sleep(5)  # Esperar adicional
                    
                    previous_table_html = table_html
                    table_df = pd.read_html(StringIO(table_html))[0]
                    output_data.append(table_df)
                    print(f"Data extracted for {downloaded_set}. Total pages so far: {len(output_data)}")
                    
                    # Intentar  click en el botón de siguiente página
                    try:
                        print(f"🔍 Intentando localizar next_page_button para {downloaded_set}")
                        next_page_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, xpath_dict['next_page_button']))
                        )
                        print(f"✅ Next page button localizado para {downloaded_set}")
                        if next_page_button.is_enabled():
                            print(f"🖱️ Next page button enabled, clicking para {downloaded_set}...")
                            # Usar ActionChains para un click más robusto
                            ActionChains(self.driver).move_to_element(next_page_button).click().perform()
                            time.sleep(2)  # Pequeña pausa antes de verificar cambio
                            
                            # Espera activa hasta que la tabla cambie (hasta 30 segundos)
                            start_time = time.time()
                            table_changed = False
                            while time.time() - start_time < 30:
                                try:
                                    table_element = WebDriverWait(self.driver, 5).until(
                                        EC.presence_of_element_located((By.XPATH, '//*[@id="q-app"]/div/div[2]/div/div[2]/div/div[2]/table'))
                                    )
                                    new_table_html = table_element.get_attribute('outerHTML')
                                    if new_table_html != previous_table_html:
                                        table_changed = True
                                        print(f"✅ Nueva página cargada para {downloaded_set} en {time.time() - start_time:.2f} segundos")
                                        break
                                    else:
                                        print(f"⏳ Tabla aún no cambió para {downloaded_set}, esperando... ({time.time() - start_time:.2f}s)")
                                except Exception as e:
                                    print(f"⚠️ Error al verificar tabla: {e}")
                                time.sleep(1)
                            
                            if not table_changed:
                                print(f"⚠️ La tabla no cambió después de 30 segundos para {downloaded_set}, deteniendo extracción.")
                                break
                        else:
                            print(f"❌ Next page button not enabled para {downloaded_set}. End of pages.")
                            break
                    except (TimeoutException, NoSuchElementException) as e:
                        print(f"❌ Error al localizar next_page_button para {downloaded_set}: {e}")
                        print(f"⚠️ Asumiendo que es la última página para {downloaded_set}.")
                        break  # Asumir que es la última página sin input manual

                except TimeoutException:
                    print(f"Timeout while trying to extract data for {downloaded_set}.")
                    break
            
            return output_data
        
        # Extraer para 2023-2024 si no existe
        data_2023_2024 = []
        if not existing_files["2023-2024"]:
            print("\n🔄 Extrayendo datos para 2023-2024...")
            facturas_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath_dict['facturas_button']))
            )
            facturas_button.click()
            time.sleep(5)
            
            data_2023_2024 = extract_set_data("2023-2024", xpath_dict['facturas_2023_2024'])
        
        # Volver al menú principal y extraer para 2024 si no existe
        data_2024 = []
        if not existing_files["2024"]:
            print("\n🔄 Extrayendo datos para 2024...")
            main_menu = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath_dict['main_menu']))
            )
            main_menu.click()
            time.sleep(5)
            
            facturas_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath_dict['facturas_button']))
            )
            facturas_button.click()
            time.sleep(5)
            
            data_2024 = extract_set_data("2024", xpath_dict['facturas_2024'])
        
        # Guardar ambos sets si se extrajeron y comparar con páginas previas
        for downloaded_set, output_data in [("2023-2024", data_2023_2024), ("2024", data_2024)]:
            if output_data:
                final_output_df = pd.concat(output_data, ignore_index=True)
                output_file_name = os.path.join(download_directory, f"{today_yyyy_mm_dd_hh} SAGI_{downloaded_set}.xlsx")
                final_output_df.to_excel(output_file_name, index=False)
                print(f"✅ Data for {downloaded_set} saved to {output_file_name}")
                
                # Comparar con páginas previas
                current_pages = len(output_data)
                if current_pages < previous_pages.get(downloaded_set, 0):
                    print(f"⚠️ ALERTA: Extracción parcial para {downloaded_set}. Páginas actuales: {current_pages}, previas: {previous_pages[downloaded_set]}. Posible pérdida de datos.")
                
                # Actualizar páginas previas
                previous_pages[downloaded_set] = current_pages
            elif not existing_files[downloaded_set]:
                print(f"⚠️ No data extracted for {downloaded_set}.")
        
        # Guardar páginas previas en archivo
        with open(pages_file, 'w') as f:
            json.dump(previous_pages, f)
        
        return True


    def _clear_and_type_date(self, input_element, value):
        """Limpia robustamente el input y escribe la fecha, validando el valor."""
        try:
            input_element.click()
            time.sleep(0.2)
            input_element.send_keys(Keys.ESCAPE)
            time.sleep(0.2)
            input_element.send_keys(Keys.CONTROL, 'a')
            time.sleep(0.1)
            input_element.send_keys(Keys.DELETE)
            time.sleep(0.2)
            input_element.send_keys(value)
            time.sleep(0.3)

            # Validar que quedó escrito
            current = input_element.get_attribute('value') or ''
            if current.strip() != value:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input'));",
                    input_element,
                    value,
                )
        except Exception:
            # Fallback directo por JS
            try:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input'));",
                    input_element,
                    value,
                )
            except Exception:
                pass
        

    def chrome_driver_load(self, directory):
        """Launch Chrome with OS-specific paths and consistent configuration."""

        # Detect OS
        system = platform.system()
        home = os.path.expanduser("~")
        # Set Chrome binary and ChromeDriver paths based on OS
        if system == "Windows":
            chrome_binary_path = os.path.join(home, "Documents", "chrome-win64", "chrome.exe")
            chromedriver_path = os.path.join(home, "Documents", "chromedriver-win64", "chromedriver.exe")
        elif system == "Darwin":  # macOS
            
            chrome_binary_path = os.path.join(home, "chrome_testing", "chrome-mac-arm64", "Google Chrome for Testing.app", "Contents", "MacOS", "Google Chrome for Testing")
            chromedriver_path = os.path.join(home, "chrome_testing", "chromedriver-mac-arm64", "chromedriver")
        else:
            print(f"Unsupported OS: {system}")
            return None

        # Set Chrome options
        chrome_options = Options()
        chrome_options.binary_location = chrome_binary_path

        prefs = {
            "download.default_directory": os.path.abspath(directory),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        # (Optional) Further reduce noise:
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-component-update")

        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")

        try:
            # Initialize ChromeDriver with the correct service path
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            return driver
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            return None
        
    def execute_download_session(self, download_folder, actions_name):
        """Ejecuta una sesión completa de descarga"""
        if not self.chrome_driver_load:
            print("❌ Driver de Chrome no disponible")
            return False
        success = False
        self.download_folder = download_folder  # Agregar para usar en _execute_step
        try:
            # Inicializar driver
            self.driver = self.chrome_driver_load(download_folder)
            
            # Configurar acciones según los datos de acceso
            actions = self.data_access.get(actions_name, {})

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
