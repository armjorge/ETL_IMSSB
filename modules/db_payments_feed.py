import os
import re
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from psycopg2.extras import execute_values
from pandas._libs.tslibs.nattype import NaTType
from colorama import Fore, Style, init
import platform
import subprocess

class DB_PAYMENTS_FEED:
    def __init__(self, working_folder, data_access):
        init(autoreset=True)
        print("Initializing DB_PAYMENTS_FEED...")
        self.working_folder = working_folder
        self.data_access = data_access

    def menu_db_payments_feed(self):
        print("Actualizando relación de UUID pagados por la Oficina de atención a proveedores...")
        source_folder = os.path.join(self.working_folder, "Oficina Atención Proveedores")
        os.makedirs(source_folder, exist_ok=True)
        # cada archivo tiene al menos folio_fiscal, referencia, importe y el nombre del archivo es la pk.
        df_pagos_consolidado = self.get_new_dataframe(source_folder)
        
        # Limpieza previa a la carga
        print(f"{Fore.RED}Iniciando la limpieza del dataframe generado")
        df = df_pagos_consolidado.copy()
        #    'coerce' turns string headers like "IMPORTE" into NaN.
        numeric_importe = pd.to_numeric(df['importe'], errors='coerce')

        # 2. Define the split condition: Is it a valid number?
        is_valid_number = numeric_importe.notna()

        # 3. Create the two DataFrames
        df_included_rows = df[is_valid_number].copy()
    
        df_excluded_rows = df[~is_valid_number].copy()

        # 4. CRITICAL: Assign the clean numeric values back to the included rows.
        #    This ensures 'importe' is actually float/int, not a string look-alike.
        df_included_rows['importe'] = numeric_importe[is_valid_number]

        # --- Logging / Feedback ---
        print(f"   📊 Rows kept: {len(df_included_rows)}")
        print(f"   🗑️ Rows dropped (repeated headers/invalid): {len(df_excluded_rows)}")

        if not df_excluded_rows.empty:
            # Optional: Preview what you are dropping to be safe
            print(f"{Fore.YELLOW}   ⚠️ Dropped sample values in 'importe': {df_excluded_rows['importe'].unique()[:5]}{Style.RESET_ALL}")
            output_excluded = os.path.join(source_folder, "excluded_rows.xlsx")
            df_excluded_rows.to_excel(output_excluded, index=False)
            print(f"{Fore.GREEN}\tFile saved to {output_excluded}")

        

        # Si no hay nada que subir, salimos sin romper
        if df_included_rows is None or df_included_rows.empty:
            print(f"{Fore.YELLOW}⏩ No hay pagos nuevos para subir (DataFrame vacío).{Style.RESET_ALL}")
            return True
        output_included = os.path.join(source_folder, "included_rows.xlsx")
        df_included_rows.to_excel(output_included, index=False)
        print(f"{Fore.GREEN}\tFile saved to {output_included}")

        schema = self.data_access.get("data_warehouse_schema")
        table_name = "dim_uuid_pagadas"
        primary_keys = ["file_name"]

        engine = self.sql_conexion()
        if engine is None:
            print("❌ No se pudo obtener el engine de SQL.")
            return False

        try:
            with engine.begin() as conn:
                self.upsert_dataframe(conn, df_included_rows, schema, table_name, primary_keys)
                print(f"{Fore.YELLOW}")

                
            return True
        except Exception as e:
            print(f"❌ Error en update_sql: {e}")
            return False

    def sql_conexion(self):
        sql_url = self.data_access["sql_url"]
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None

    def _normalize_identifier(self, name: str) -> str:
        name = str(name).strip().lower()
        name = re.sub(r"[^a-z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name)
        if re.match(r"^[0-9]", name):
            name = "col_" + name
        return name

    def upsert_dataframe(self, conn, df: pd.DataFrame, schema: str, table_name: str, primary_keys: list):
        if df.empty:
            print(f"{Fore.YELLOW}⏩ No hay filas para insertar en {schema}.{table_name}.{Style.RESET_ALL}")
            return

        # 1. Normalizar nombres de columnas
        df.columns = [self._normalize_identifier(c) for c in df.columns]
        
        # 2. Definir función de limpieza (Serialize values for Postgres)
        def _coerce_sql_value(value):
            if pd.isna(value) or value is None:
                return None
            
            # Fechas
            if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
                return value.to_pydatetime() if not pd.isna(value) else None
            if isinstance(value, np.datetime64):
                return pd.to_datetime(value).to_pydatetime()
            
            # Números (Numpy a Python nativo)
            if isinstance(value, (np.integer, int)):
                return int(value)
            if isinstance(value, (np.floating, float)):
                return float(value)
            
            # Strings (Limpieza específica)
            if isinstance(value, str):
                val_clean = value.strip()
                if not val_clean or val_clean.lower() in {"nan", "nat", "none", "null"}:
                    return None
                return val_clean
            
            return value

        # 3. Preparar Query SQL
        cols = list(df.columns)
        col_list_sql = ", ".join(cols)
        
        # Aunque validamos en BD, necesitamos los PKs para escribir la sintaxis ON CONFLICT
        norm_pks = [self._normalize_identifier(pk) for pk in primary_keys]
        pk_list_sql = ", ".join(norm_pks)

        insert_sql = f"""
            INSERT INTO {schema}.{table_name} ({col_list_sql})
            VALUES %s
            ON CONFLICT ({pk_list_sql}) 
            DO NOTHING
        """

        # 4. Generador: Transforma los datos fila por fila al vuelo (ahorra memoria)
        # Usamos itertuples con name=None para obtener tuplas puras rápidas
        data_generator = (
            tuple(_coerce_sql_value(x) for x in row) 
            for row in df.itertuples(index=False, name=None)
        )

        # 5. Ejecutar
        raw_conn = conn.connection
        with raw_conn.cursor() as cur:
            execute_values(cur, insert_sql, data_generator, page_size=10000)
            
        print(f"{Fore.GREEN}✅ {len(df)} filas procesadas hacia {schema}.{table_name}{Style.RESET_ALL}")

    def get_new_dataframe(self, source_folder):
        email_folder = os.path.join(source_folder, "Emails de OAP")
        os.makedirs(email_folder, exist_ok=True)

        # Archivos presentes en email_folder
        allowed_ext = {".xlsx", ".pdf"}

        files_in_folder = [
            os.path.join(email_folder, f)
            for f in os.listdir(email_folder)
            if os.path.isfile(os.path.join(email_folder, f))
            and not f.startswith("~")
            and os.path.splitext(f)[1].lower() in allowed_ext
        ]
        print('\n', files_in_folder,'\n')
        # Siempre regresar DF con columnas esperadas para evitar KeyError
        out_columns = ["folio_fiscal", "file_name"]

        prefix_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}-\d{2} Eseotres")

        pdf_by_prefix = {}
        xlsx_by_prefix = {}
        unprefixed_files = []

        for file in os.listdir(email_folder):
            match = prefix_pattern.match(file)
            if not match:
                unprefixed_files.append(file)
                continue

            prefix = match.group(0)
            ext = os.path.splitext(file)[1].lower()

            if ext == ".pdf":
                pdf_by_prefix.setdefault(prefix, []).append(file)
            elif ext == ".xlsx":
                xlsx_by_prefix.setdefault(prefix, []).append(file)

        # ======== VALIDACIÓN DE ARCHIVOS POR PREFIX ========
        all_prefixes = set(pdf_by_prefix.keys()) | set(xlsx_by_prefix.keys())

        print("\n📌 Validación de archivos por prefix:\n")
        for p in sorted(all_prefixes):
            pdfs = pdf_by_prefix.get(p, [])
            excels = xlsx_by_prefix.get(p, [])

            print(f"➡️  Prefix: {p}")
            print(f"   - PDF encontrados: {len(pdfs)} → {pdfs}")
            print(f"   - XLSX encontrados: {len(excels)} → {excels}")

            if len(pdfs) == 0:
                print("   ❌ ERROR: Falta el PDF")
            if len(excels) == 0:
                print("   ❌ ERROR: Falta el XLSX")
            if len(pdfs) > 1:
                print("   ⚠️ Advertencia: Hay más de un PDF")
            if len(excels) > 1:
                print("   ⚠️ Advertencia: Hay más de un XLSX")
            print()

        if unprefixed_files:
            print("\n⚠️ Archivos que NO cumplen el formato de prefix:")
            for f in unprefixed_files:
                print("   -", f)
            print("\n👉 Corrige o elimina estos archivos para mantener ordenada la carpeta.")

        print("\n✔️ Revisión completa.\n")

        # ========== Generar dataframe con todos los UUID enlistados como pagados ==========
        df_list = []
        print("📦 XLSX detectados por prefix:", xlsx_by_prefix)

        if not xlsx_by_prefix:
            # No hay excels: regresar df vacío con columnas esperadas
            return pd.DataFrame(columns=out_columns)

        
        for prefix, file_list in xlsx_by_prefix.items():
            if len(file_list) > 1:
                print(f"{Fore.YELLOW}⚠️ Más de un XLSX para prefix='{prefix}'. Tomando el primero: {file_list[0]}{Style.RESET_ALL}")

            file = file_list[0]
            full_path = os.path.join(email_folder, file)
            print(f"\n📄 Procesando archivo: {file}")

            # --- NUEVA LÓGICA DE REINTENTO ---
            while True:
                # 1. Leer preview para buscar cabeceras
                try:
                    preview = pd.read_excel(full_path, header=None, nrows=30)
                except Exception as e:
                    print(f"   ❌ Error leyendo archivo: {e}")
                    break # Salir si el archivo está corrupto o ilegible

                header_row = None
                
                # Regex para folio fiscal
                folio_pattern = re.compile(r"folio\s*fiscal", re.IGNORECASE)
                
                # Buscamos la fila que contenga LOS TRES elementos
                for i in range(preview.shape[0]):
                    # Convertimos la fila a strings limpios y minúsculas
                    row_values = preview.iloc[i].astype(str).str.strip().str.lower().tolist()
                    
                    # Chequeos
                    has_folio = any(folio_pattern.search(val) for val in row_values)
                    has_ref = "referencia" in row_values
                    has_importe = "importe" in row_values

                    if has_folio and has_ref and has_importe:
                        header_row = i
                        break

                # 2. Si encontramos el header, rompemos el bucle while y seguimos
                if header_row is not None:
                    print(f"   ✔ Header encontrado en fila: {header_row}")
                    break 
                
                # 3. Si NO encontramos el header, abrimos el archivo y pedimos ayuda
                print(f"{Fore.RED}   ❌ No se encontraron las columnas requeridas ('folio fiscal', 'referencia', 'importe').{Style.RESET_ALL}")
                print(f"   📂 Abriendo archivo para corrección manual...")

                current_os = platform.system()
                try:
                    if current_os == 'Windows':
                        os.startfile(full_path)
                    elif current_os == 'Darwin':  # macOS
                        subprocess.call(('open', full_path))
                    else: # Linux / Otro
                        subprocess.call(('xdg-open', full_path))
                except Exception as e:
                    print(f"   ⚠️ No se pudo abrir el archivo automáticamente ({e}). Por favor ábrelo manualmente.")

                print(f"{Fore.CYAN}   👉 Por favor, edita el Excel: asegúrate de que existan las columnas 'Folio Fiscal', 'Referencia' e 'Importe'.")
                print(f"   💾 Guarda, cierra el archivo y luego presiona [ENTER] aquí para reintentar...{Style.RESET_ALL}")
                input()
                print("   🔄 Reintentando lectura...")
                # El loop 'while True' volverá a empezar desde 'preview = ...'
            
            # --- FIN BLOQUE REINTENTO ---

            # Si salimos del while y header_row sigue siendo None (caso raro de break por error), saltamos
            if header_row is None:
                continue

            # Cargar DataFrame definitivo
            df = pd.read_excel(full_path, header=header_row)

            # Normalizar columnas
            df.columns = [self._normalize_identifier(c) for c in df.columns]

            # Re-verificar la columna folio fiscal (por si el nombre varía ligeramente)
            if "folio_fiscal" not in df.columns:
                candidates = [c for c in df.columns if "folio" in c and "fiscal" in c]
                if candidates:
                    df = df.rename(columns={candidates[0]: "folio_fiscal"})
            
            # Validación final de columnas antes de filtrar
            required_cols = ["folio_fiscal", "referencia", "importe"]
            missing_cols = [c for c in required_cols if c not in df.columns]

            if missing_cols:
                # Esto no debería pasar gracias al while loop, pero por seguridad:
                print(f"   ❌ Faltan columnas tras la carga normalizada: {missing_cols}. Se omite archivo.")
                continue

            df = df[required_cols].copy()
            df["file_name"] = file

            # Limpiar UUIDs
            df["folio_fiscal"] = df["folio_fiscal"].astype(str).str.strip()
            df.loc[df["folio_fiscal"].str.lower().isin(["nan", "nat", "none", "null", ""]), "folio_fiscal"] = pd.NA

            df_list.append(df)

        if not df_list:
            return pd.DataFrame(columns=out_columns)

        df_pagos_consolidado = pd.concat(df_list, ignore_index=True)

        # Aquí ya existe la columna sí o sí
        df_pagos_consolidado = df_pagos_consolidado.dropna(subset=["folio_fiscal"])

        print("\n🧾 Preview consolidado:")
        print(df_pagos_consolidado.head(5))
        print(f"\n📊 Total UUID válidos: {len(df_pagos_consolidado)}")

        return df_pagos_consolidado


if __name__ == "__main__":
    folder_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    working_folder = os.path.join(folder_root, "Implementación")
    from config import ConfigManager

    config_manager = ConfigManager(working_folder)
    data_access = config_manager.yaml_creation(working_folder)

    db_payments_feed = DB_PAYMENTS_FEED(working_folder, data_access)
    db_payments_feed.menu_db_payments_feed()
