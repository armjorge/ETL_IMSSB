import os
import re
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from psycopg2.extras import execute_values
from pandas._libs.tslibs.nattype import NaTType
from colorama import Fore, Style, init


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

        df_pagos_consolidado = self.get_new_dataframe(source_folder)

        # Si no hay nada que subir, salimos sin romper
        if df_pagos_consolidado is None or df_pagos_consolidado.empty:
            print(f"{Fore.YELLOW}⏩ No hay pagos nuevos para subir (DataFrame vacío).{Style.RESET_ALL}")
            return True

        schema = self.data_access.get("data_warehouse_schema")
        table_name = "imssb_pago_proveedores"
        primary_keys = ["folio_fiscal"]

        engine = self.sql_conexion()
        if engine is None:
            print("❌ No se pudo obtener el engine de SQL.")
            return False

        try:
            with engine.begin() as conn:
                self.upsert_dataframe(conn, df_pagos_consolidado, schema, table_name, primary_keys)
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
        df = df.copy()
        df.columns = [self._normalize_identifier(c) for c in df.columns]
        norm_pks = [self._normalize_identifier(pk) for pk in primary_keys]

        missing = [pk for pk in norm_pks if pk not in df.columns]
        if missing:
            raise ValueError(f"Primary keys not found in DataFrame columns: {missing}")

        df = df.drop_duplicates(subset=norm_pks, keep="last")
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)

        def _coerce_sql_value(value):
            if value is None:
                return None
            if isinstance(value, NaTType):
                return None
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"nat", "nan", "none", "null", ""}:
                    return None
                return value.strip()
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            if isinstance(value, np.datetime64):
                if pd.isna(value):
                    return None
                return pd.to_datetime(value).to_pydatetime()
            if pd.isna(value):
                return None
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                if np.isnan(value):
                    return None
                return float(value)
            return value

        for col in df.columns:
            df[col] = df[col].apply(_coerce_sql_value).astype(object)

        cols = list(df.columns)
        col_list_sql = ", ".join(cols)
        pk_list_sql = ", ".join(norm_pks)

        insert_sql = f"""
            INSERT INTO {schema}.{table_name} ({col_list_sql})
            VALUES %s
            ON CONFLICT ({pk_list_sql})
            DO NOTHING
        """

        total = len(df)
        if total == 0:
            print(f"{Fore.YELLOW}⏩ No hay filas para insertar en {schema}.{table_name}.{Style.RESET_ALL}")
            return

        raw_conn = conn.connection
        cur = raw_conn.cursor()
        try:
            sanitized_rows = (
                tuple(_coerce_sql_value(value) for value in row)
                for row in df.itertuples(index=False, name=None)
            )
            execute_values(cur, insert_sql, sanitized_rows, page_size=10000)
        finally:
            cur.close()

        print(f"{Fore.GREEN}✅ {total} filas insertadas en {schema}.{table_name} (ON CONFLICT DO NOTHING){Style.RESET_ALL}")

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
            # Si hay más de uno, tomamos el primero, pero avisamos
            if len(file_list) > 1:
                print(f"{Fore.YELLOW}⚠️ Más de un XLSX para prefix='{prefix}'. Tomando el primero: {file_list[0]}{Style.RESET_ALL}")

            file = file_list[0]
            full_path = os.path.join(email_folder, file)
            print(f"\n📄 Procesando archivo: {file}")

            # Buscar header: mejor escanear más filas por si el header no está en las primeras 5
            preview = pd.read_excel(full_path, header=None, nrows=30)

            header_row = None
            header_pattern = re.compile(r"folio\s*fiscal", re.IGNORECASE)

            for i in range(preview.shape[0]):
                row_values = preview.iloc[i].astype(str).str.strip()
                if row_values.str.contains(header_pattern).any():
                    header_row = i
                    break

            if header_row is None:
                print("   ❌ No se encontró header con 'folio fiscal'. Se omite archivo.")
                continue

            print(f"   ✔ Header encontrado en fila: {header_row}")
            df = pd.read_excel(full_path, header=header_row)

            # Normalizar columnas con tu función (más robusto que sólo lower/strip)
            df.columns = [self._normalize_identifier(c) for c in df.columns]

            if "folio_fiscal" not in df.columns:
                # Intento adicional: a veces queda como folio_fiscal_uuid o algo similar
                candidates = [c for c in df.columns if "folio" in c and "fiscal" in c]
                if candidates:
                    df = df.rename(columns={candidates[0]: "folio_fiscal"})
                else:
                    print(f"   ❌ No existe columna folio_fiscal (columnas detectadas: {df.columns.tolist()}). Se omite archivo.")
                    continue

            df = df[["folio_fiscal"]].copy()
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
