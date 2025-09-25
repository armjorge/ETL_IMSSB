import os
import time
import datetime
import pandas as pd
from lxml import etree
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
#from modules.helpers import 
import numpy as np
import concurrent.futures  # Agregar para procesamiento paralelo
import yaml
from pathlib import Path
import shutil
from PyPDF2 import PdfReader
import re


class FACTURAS:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        
    def cargar_facturas(self, facturas):
        facturas_folder = os.path.join(self.working_folder, "Facturas")
        os.makedirs(facturas_folder, exist_ok=True)
        xlsx_database = os.path.join(facturas_folder, 'xmls_extraidos.xlsx')
        self.smart_xml_extraction(xlsx_database)   
        
        preffix = os.path.basename(facturas_folder)
        # DataFrame general vacío
        df_general = pd.DataFrame()

        # Iterar sobre los paquetes en PAQS_IMSS
        for paq_name, paq_info in self.data_access.get(facturas, {}).items():
            file_path = paq_info.get("file_path")
            sheet = paq_info.get("sheet")
            rows = paq_info.get("rows", [])

            print(f"\n🔍 Procesando {paq_name}")

            # 1. Intentar cargar archivo
            if not os.path.exists(file_path):
                print(f"⚠️ Archivo no encontrado: {file_path}")
                continue
            print(f"✅ Archivo encontrado: {file_path}")

            try:
                # 2. Intentar cargar hoja con columnas definidas
                df = pd.read_excel(file_path, sheet_name=sheet, usecols=rows)
                print(f"✅ Hoja '{sheet}' cargada con columnas {rows}")

                # Concatenar a df_general
                df_general = pd.concat([df_general, df], ignore_index=True)

            except ValueError as e:
                print(f"⚠️ Problema con la hoja o columnas: {e}")
                continue

        # Guardar resultado en carpeta local
        if not df_general.empty:
            today = datetime.datetime.today().strftime("%Y-%m-%d-%H")  # ✅ Formato de fecha corregido
            output_file = os.path.join(facturas_folder, f"{today}h_{facturas}.xlsx")  # ✅ Usar carpeta local
            df_xmls = pd.read_excel(xlsx_database)
            print(f"📊 Filas en df_xmls antes de limpiar: {df_xmls.shape[0]}")

            # Verificar duplicados por Folio
            duplicados = df_xmls['Folio'].duplicated().sum()
            if duplicados > 0:
                print(f"⚠️ Se encontraron {duplicados} folios duplicados en df_xmls")
                
                # Opción A: Eliminar duplicados manteniendo el primero
                df_xmls = df_xmls.drop_duplicates(subset=['Folio'], keep='first')
                print(f"✅ Duplicados eliminados. Filas restantes: {df_xmls.shape[0]}")
            print(f"Filas antes de la fusión con el XML {df_general.shape[0]}")
            print(df_general.head(),df_xmls.head())
            df_general = pd.merge(df_general, df_xmls, how='left', left_on='Factura', right_on='Folio')
            print(f"print filas después de la fusión con el XML {df_general.shape[0]}")
            print("\n✅ DataFrame fusionado con información XML con éxito.")
            # --- Cargar estatus SAT y eliminar Cancelados ---
            invoice_to_check = os.path.join(self.working_folder, "Estatus SAT", "estatus_facturas.xlsx")
            if os.path.isfile(invoice_to_check):
                df_status_SAT = pd.read_excel(invoice_to_check)

                # Asegurarnos de que las columnas existan
                if {'uuid', 'estado'}.issubset(df_status_SAT.columns):
                    # Lista de UUID cancelados
                    uuid_list = df_status_SAT.loc[df_status_SAT['estado'] == 'Cancelado', 'uuid'].astype(str).tolist()
                    print(f"⚠️  {len(uuid_list)} UUIDs marcados como Cancelado en SAT")

                    # Quitar de invoice_df los UUID que están en esa lista
                    # Marcar como Cancelado en invoice_df
                    mask = df_general['UUID'].astype(str).isin(uuid_list)
                    df_general.loc[mask, 'UUID Descripción'] = 'Cancelado'

                    print(f"📝 {mask.sum()} facturas actualizadas como Cancelado")

                    
                else:
                    print("⚠️ Archivo estatus_facturas.xlsx no tiene columnas esperadas: 'uuid', 'estado'")
            else:
                print("ℹ️ No existe estatus_facturas.xlsx, no se filtraron cancelados")

            try:
                df_general.to_excel(output_file, index=False)
                print(f"\n💾 Archivo guardado en {output_file}")
                print(f"📊 Total de filas procesadas: {len(df_general)}")
                return True
            except PermissionError as e:
                print(f"❌ Error de permisos: {e}")
                print(f"🔄 Intentando guardar en carpeta alternativa...")
                
                # Fallback: guardar en carpeta temporal
                import tempfile
                temp_dir = tempfile.gettempdir()
                fallback_file = os.path.join(temp_dir, f"{today}_facturas.xlsx")
                df_general.to_excel(fallback_file, index=False)
                print(f"💾 Archivo guardado en ubicación temporal: {fallback_file}")

                return False

        else:
            print("\n⚠️ No se generó DataFrame, revisar configuración.")        
        #self.check_invoice_status(facturas)

        
    def smart_xml_extraction(self, xlsx_database):
        print(("Extrayendo la información de los XMLs..."))
        invoice_paths = []
        for path in self.data_access['facturas_path']:
            if os.path.exists(path):
                invoice_paths.append(path)

        # Cargar base existente si existe
        if os.path.exists(xlsx_database):
            df_database = pd.read_excel(xlsx_database)
            existing_uuids = set(df_database['UUID'].dropna())  # Set de UUIDs existentes para check rápido
            existing_folios_files = set(zip(df_database['Folio'], df_database['Archivo']))  # Para check alternativo
        else:
            df_database = pd.DataFrame(columns=[
                'UUID', 'Folio', 'Fecha', 'Nombre', 'Rfc',
                'Descripcion', 'Cantidad', 'Importe', 'Archivo'
            ])
            existing_uuids = set()
            existing_folios_files = set()

        data = []

        # Función para procesar un archivo XML (para paralelizar)
        def process_xml_file(full_path, file):
            try:
                tree = etree.parse(full_path)
                root_element = tree.getroot()

                # Detectar namespace
                ns = None
                for ns_url in root_element.nsmap.values():
                    if "cfd/3" in ns_url:
                        ns = {
                            "cfdi": "http://www.sat.gob.mx/cfd/3",
                            "tfd": "http://www.sat.gob.mx/TimbreFiscalDigital"
                        }
                        break
                    elif "cfd/4" in ns_url:
                        ns = {
                            "cfdi": "http://www.sat.gob.mx/cfd/4",
                            "tfd": "http://www.sat.gob.mx/TimbreFiscalDigital"
                        }
                        break
                if ns is None:
                    return []  # No procesar si no hay namespace válido

                # Extraer Folio y Serie
                folio = root_element.get('Folio')
                serie = root_element.get('Serie')
                folio_completo = f"{serie}-{folio}" if serie and folio else folio or serie or ""

                # Extraer Fecha
                fecha = root_element.get('Fecha')

                # Extraer UUID
                uuid = None
                complemento = root_element.find('./cfdi:Complemento', ns)
                if complemento is not None:
                    timbre = complemento.find('./tfd:TimbreFiscalDigital', ns)
                    if timbre is not None:
                        uuid = timbre.get('UUID')

                # Check si ya existe
                if uuid and uuid in existing_uuids:
                    return []  # Omitir
                elif (folio_completo, file) in existing_folios_files:
                    return []  # Omitir

                # Extraer receptor
                rec = root_element.find('./cfdi:Receptor', ns)
                if rec is None:
                    return []
                nombre = rec.get('Nombre')
                rfc = rec.get('Rfc')

                # Extraer conceptos
                conceptos_data = []
                for concepto in root_element.findall('./cfdi:Conceptos/cfdi:Concepto', ns):
                    descripcion = concepto.get('Descripcion')
                    cantidad = concepto.get('Cantidad')
                    importe = concepto.get('Importe')

                    conceptos_data.append([
                        uuid,
                        folio_completo,
                        fecha,
                        nombre,
                        rfc,
                        descripcion,
                        cantidad,
                        importe,
                        file
                    ])

                return conceptos_data

            except Exception as e:
                print(f"[ERROR] Al procesar {file}: {e}")
                return []

        # Recopilar todos los archivos a procesar
        xml_files = []
        for folder in invoice_paths:
            print(f"\nExplorando carpeta: {folder}")
            for root_dir, dirs, files in os.walk(folder):
                for file in files:
                    if file.endswith('.xml'):
                        full_path = os.path.join(root_dir, file)
                        xml_files.append((full_path, file))

        # Procesar en paralelo usando ThreadPoolExecutor (para I/O bound)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:  # Ajusta max_workers según tu CPU
            futures = [executor.submit(process_xml_file, full_path, file) for full_path, file in xml_files]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    data.extend(result)

        # Si hay nuevos registros, agregarlos y guardar
        if data:
            df_nuevos = pd.DataFrame(data, columns=[
                'UUID', 'Folio', 'Fecha', 'Nombre', 'Rfc',
                'Descripcion', 'Cantidad', 'Importe', 'Archivo'
            ])
            df_database = pd.concat([df_database, df_nuevos], ignore_index=True)
            df_database[['Cantidad', 'Importe']] = df_database[['Cantidad', 'Importe']].astype(float)
            df_database.to_excel(xlsx_database, engine='openpyxl', index=False)
            print(f"\n✅ Se agregaron {len(df_nuevos)} nuevos registros a {xlsx_database}")
        else:
            print("\n✔️ No se encontraron nuevos XMLs para agregar.")


    # ==== 
    # SECCIÓN PARA CONFIRMAR EL ESTATUS DE LAS FACTURAS
    # ====

    def check_invoice_status(self, facturas):
        print("Ingresando a la sección para segregar los PDF's")

        # --- Folders ---
        invoice_to_check = os.path.join(self.working_folder, "Estatus SAT")
        invoice_temporal_files = os.path.join(invoice_to_check, "PDFs")
        os.makedirs(invoice_temporal_files, exist_ok=True)

        # --- Read Excel list of folios ---
        xlsx_database = os.path.join(invoice_to_check, "Estatus_SAT_Download.xlsx")
        if not os.path.isfile(xlsx_database):
            print(f"❌ No existe el Excel de folios: {xlsx_database}")
            return

        pdf_files_list = pd.read_excel(xlsx_database)
        if pdf_files_list.empty or "Folio" not in pdf_files_list.columns:
            print("❌ No folios encontrados en el archivo Excel.")
            return

        print(pdf_files_list.head())

        # --- Load YAML (ya está en la clase) -> usar carpeta del Excel como raíz de búsqueda ---
        paqs = self.data_access.get(facturas, {})
        base_dirs = []

        for name, cfg in paqs.items():
            excel_path = Path(cfg["file_path"])
            folder = excel_path.parent  # carpeta donde vive el Excel
            base_dirs.append(folder)

        # Deduplicar y normalizar
        base_dirs = list(dict.fromkeys([d.resolve() for d in base_dirs]))
        print("\n📂 Carpetas base a escanear:")
        for d in base_dirs:
            print(f"  - {d}  (exists: {d.exists()})")

        # --- Indexar TODOS los PDFs de forma recursiva (una sola pasada) ---
        pdf_index = []  # lista de tuplas: (nombre_base_lower, ruta_abs_str)
        total_indexed = 0
        for d in base_dirs:
            if not d.exists():
                print(f"⚠️  Carpeta inexistente: {d}")
                continue
            count = 0
            # rglob es recursivo: busca en subcarpetas (ej. "08 Agosto\...")
            for pdf in d.rglob("*.pdf"):
                pdf_index.append((pdf.name.lower(), str(pdf)))
                count += 1
            total_indexed += count
            print(f"   → {count} PDFs indexados bajo {d}")

        print(f"🔎 Total PDFs indexados: {total_indexed}")

        if total_indexed == 0:
            print("❌ No se indexó ningún PDF. Revisa rutas/permiso de Dropbox/drive.")
            return

        # (Debug opcional) mostrar algunos nombres para validar
        print("🧪 Ejemplos de PDFs indexados:")
        for name, path in pdf_index[:min(5, len(pdf_index))]:
            print(f"   - {name}")

        # --- Buscar por prefijo y copiar ---
        expected = len(pdf_files_list)
        found = 0
        not_found = []
        duplicates = {}

        for raw_folio in pdf_files_list["Folio"].astype(str):
            folio = raw_folio.strip().lower()  # normalizar
            matched_paths = [p for (name, p) in pdf_index if name.startswith(folio)]

            if matched_paths:
                # Si hay más de uno, reportar duplicados y tomar el primero
                if len(matched_paths) > 1:
                    duplicates[raw_folio] = matched_paths[:5]  # guarda hasta 5 para no saturar consola

                src = matched_paths[0]
                dst = os.path.join(invoice_temporal_files, f"{raw_folio}.pdf")
                try:
                    shutil.copy2(src, dst)
                    print(f"✔ Copiado: {raw_folio} -> {dst}")
                    found += 1
                except Exception as e:
                    print(f"❌ Error copiando {src} -> {dst}: {e}")
            else:
                not_found.append(raw_folio)

        # --- Summary ---
        print("\n--- Resumen ---")
        print(f"Expected: {expected}")
        print(f"Found:    {found}")
        if duplicates:
            print(f"⚠️ Duplicados detectados ({len(duplicates)} folios). Se usó el primero encontrado:")
            for fol, paths in duplicates.items():
                print(f"   · {fol}:")
                for p in paths:
                    print(f"      - {p}")
        if not_found:
            print(f"Not found ({len(not_found)}): {not_found}")
        self.extract_estatus_pdf()

    def extract_estatus_pdf(self):
        acuses_sat = os.path.join(self.working_folder, "Estatus SAT", "Comprobantes SAT")
        output_file = os.path.join(self.working_folder, "Estatus SAT", "estatus_facturas.xlsx")

        list_pdf_files = [f for f in os.listdir(acuses_sat) if f.lower().endswith(".pdf")]

        if not list_pdf_files:
            print("❌ No se encontraron PDFs en:", acuses_sat)
            return

        results = []

        for pdf_file in list_pdf_files:
            pdf_path = os.path.join(acuses_sat, pdf_file)
            try:
                reader = PdfReader(pdf_path)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() or ""

                # Normalizar texto
                clean_text = text.replace("\n", " ").replace("\r", " ")

                # Regex tolerante: capturar entre &id= y &re=
                match = re.search(r"&id=([\s\S]+?)&re=", clean_text, re.IGNORECASE)
                uuid = None
                if match:
                    uuid = match.group(1)
                    # limpiar espacios intermedios
                    uuid = uuid.replace(" ", "").replace("\n", "").replace("\r", "")

                # Estado
                if "Cancelado" in clean_text:
                    estado = "Cancelado"
                elif "Vigente" in clean_text:
                    estado = "Vigente"
                else:
                    estado = "Desconocido"

                results.append({
                    "uuid": uuid,
                    "estado": estado,
                    "filename": os.path.basename(pdf_file)
                })

            except Exception as e:
                print(f"❌ Error procesando {pdf_file}: {e}")

        # Guardar resultados
        df = pd.DataFrame(results)
        df.to_excel(output_file, index=False)

        print(f"✔ Resultados guardados en {output_file}")
        print(df.head())
if __name__ == "__main__":
    folder_root = os.getcwd()    
    working_folder = os.path.join(folder_root, "Implementación")
    config_path = os.path.join(working_folder, "config.yaml")
    yaml_exists = os.path.exists(config_path)
    if yaml_exists:
        # Abrir y cargar el contenido YAML en un diccionario
        with open(config_path, 'r', encoding='utf-8') as f:
            data_access = yaml.safe_load(f)
        print(f"✅ Archivo YAML cargado correctamente: {os.path.basename(config_path)}")
    facturas = 'PAQS_INSABI'
    # Inicializar web driver manager (sin crear el driver aún)
    app = FACTURAS(working_folder, data_access)
    app.cargar_facturas(facturas)
    
