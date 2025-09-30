import pandas as pd 
from sqlalchemy import create_engine, text, insert
import os 
import glob
import re 
from psycopg2.extras import execute_values
import numpy as np
import math
from datetime import datetime
from pandas._libs.missing import NAType
from pandas._libs.tslibs.nattype import NaTType


class SQL_CONNEXION_UPDATING:
    def __init__(self, integration_path, data_access):
        self.integration_path = integration_path
        self.data_access = data_access
        # Create a DataIntegration instance to use its get_newest_file method
        #self.data_integration = DataIntegration(working_folder, data_access)
    
    def sql_conexion(self):
        sql_url = self.data_access['sql_url']
        #url example: 'postgresql://arXXXrge:XXX@ep-shy-darkness-10211313-poolXXXX.tech/neondb?sslmode=require&channel_binding=require'
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None

    


    def create_schema_if_not_exists(self, connexion, schema_name):
        """Create schema if it doesn't exist"""
        try:
            with connexion.connect() as conn:
                # Check if schema exists
                result = conn.execute(text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'"))
                if not result.fetchone():
                    # Schema doesn't exist, create it
                    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
                    conn.commit()
                    print(f"✅ Schema '{schema_name}' created successfully")
                else:
                    print(f"✅ Schema '{schema_name}' already exists")
                return True
        except Exception as e:
            print(f"❌ Error creating schema '{schema_name}': {e}")
            return False
 
    def force_sql_safe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Garantiza que los valores sean SQL-safe:
        - numpy.int64 → int
        - numpy.float64 → float
        - pd.Timestamp → datetime.datetime
        - NaN / NaT / pd.NA → None
        - strings → str limpio
        """
        def convert_cell(x):
            if x is None:
                return None
            if isinstance(x, (NAType, NaTType)):
                return None
            if pd.isna(x):  # cubre NaN, NaT, pd.NA
                return None
            if isinstance(x, (np.integer,)):
                return int(x)
            if isinstance(x, (np.floating,)):
                return float(x)
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()
            if isinstance(x, str):
                cleaned = x.strip()
                lowered = cleaned.lower()
                if not cleaned or lowered in {'nat', 'nan', 'none', 'null', 'n/a'} or lowered == '<na>':
                    return None
                return cleaned
            return x

        for col in df.columns:
            df[col] = df[col].apply(convert_cell)

        null_markers = {"", "nat", "nan", "none", "null", "n/a", "<na>"}
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                mask = df[col].apply(lambda v: isinstance(v, str) and v.strip().lower() in null_markers)
                if mask.any():
                    df.loc[mask, col] = None


        df = df.where(pd.notnull(df), None)

        # Debug para confirmar que no quedan "NaT"
        for col in df.columns:
            if any(val == "NaT" for val in df[col].dropna().unique() if isinstance(val, str)):
                print(f"⚠️ Columna {col} todavía tiene strings 'NaT'")

        return df

    
    
    def load_menu(self): 
        print("📂 Iniciando extracción de df_altas desde archivos Excel...")
        drop_columns = ['rfc_proveedor', 'razon_social', 'almacen_entrega', 'entidad_destino', 'nombre_unidad', ]
        primary_keys = ['numero_orden_suministro']
        schema = self.data_access.get('data_warehouse_schema')
        table_name = 'imssb_historico'
        sheet_name = 'CAMUNDA'
        # --- Transformaciones de tipos ---
        date_columns   = ['fecha_autorizacion','fecha_limite_entrega']                # fechas dd/mm/yyyy
        int_columns    = ['precio_unitario','cantidad_solicitada']        # enteros
        float_columns  = ['Importe', 'PENA']  # numéricos decimales
        string_columns = ['numero_orden_suministro', 'numero_contrato']
        nan_columns = []        
        # Buscar todos los Excel en la carpeta de integración
        xlsx_files = [
            f for f in glob.glob(os.path.join(self.integration_path, "*.xlsx"))
            if not os.path.basename(f).startswith("~")
        ]
        if not xlsx_files:
            print("⚠️ No se encontraron archivos Excel en la ruta de integración.")
            return

        # Concatenar todos los df_altas de cada archivo
        df_list = []
        for file in xlsx_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, engine="openpyxl")
                df_list.append(df)
                print(f"✅ Leído 'df_altas' de {os.path.basename(file)} con {len(df)} filas")
            except Exception as e:
                print(f"⚠️ No se pudo leer 'df_altas' de {file}: {e}")

        if not df_list:
            print("⚠️ Ninguna hoja 'df_altas' pudo ser cargada.")
            return

        df_altas = pd.concat(df_list, ignore_index=True)
        df_altas = df_altas.drop(columns=drop_columns, errors='ignore')
        df_altas = df_altas.loc[:, ~df_altas.columns.str.contains("^Unnamed", case=False)]
        #print(df_altas.info())

        # Nan Columns0
        for col in nan_columns:
            if col in df_altas.columns:
                if col in ['clasPtalDist', 'totalItems', 'resguardo']:
                    df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Float64')
                else:  # strings (ej. descDist)
                    df_altas[col] = df_altas[col].astype('string').str.strip()
                    df_altas[col] = df_altas[col].replace({'nan': pd.NA, 'NaN': pd.NA, 'None': pd.NA})

        # Convertir fechas
        dummy_date = pd.Timestamp('1900-01-01')
        for col in date_columns:
            if col in df_altas.columns:
                df_altas[col] = pd.to_datetime(
                    df_altas[col],
                    format="%d/%m/%Y",
                    errors="coerce"   # valores inválidos -> NaT
                )
                df_altas[col] = df_altas[col].fillna(dummy_date)

        # Convertir a enteros (mantener dtype entero nullable)
        for col in int_columns:
            if col in df_altas.columns:
                df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Int64')

        # Convertir a floats (mantener dtype flotante)
        for col in float_columns:
            if col in df_altas.columns:
                df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Float64')

        # Convertir a string (y limpiar "nan"/"None")
        for col in string_columns:
            if col in df_altas.columns:
                df_altas[col] = df_altas[col].astype('string').str.strip()
                df_altas[col] = df_altas[col].replace({'nan': pd.NA, 'NaN': pd.NA, 'None': pd.NA})

        df_altas = self.force_sql_safe_types(df_altas)

        self.update_postresql(df_altas, schema, table_name, primary_keys)
        
    
    def _normalize_identifier(self, name: str) -> str:
        # Forzar string
        name = str(name).strip().lower()
        # Reemplazar cualquier caracter n0o alfanumérico por "_"
        name = re.sub(r'[^a-z0-9_]', '_', name)
        # Evitar doble guion bajo
        name = re.sub(r'_+', '_', name)
        # Si empieza con número, prefijar con "col_"
        if re.match(r'^[0-9]', name):
            name = "col_" + name
        return name

    def _map_dtype_to_pg(self, dtype: str) -> str:
        # pandas dtype as string -> PG type
        d = dtype.lower()
        # nullable integer dtype in pandas can be 'int64' or 'int64'/'int32' or 'int'/'int64'/'Int64'
        if d in ("int64", "int32") or d == "int":
            return "BIGINT"
        if d in ("float64", "float32", "float"):
            return "DOUBLE PRECISION"
        if d.startswith("datetime64[ns"):
            # covers datetime64[ns] and datetime64[ns, tz]
            return "TIMESTAMP"
        if d == "bool":
            return "BOOLEAN"
        # fallback for 'object', 'string', 'category', etc.
        return "TEXT"

    def table_creation(self, conn, df_to_upload: pd.DataFrame, schema_name: str, table_name: str, primary_keys: list):
        # Normalize column names
        norm_cols = [ self._normalize_identifier(c) for c in df_to_upload.columns ]
        # Build column defs
        col_defs = []
        for col_name, dtype in zip(norm_cols, df_to_upload.dtypes.astype(str)):
            pg_type = self._map_dtype_to_pg(dtype)
            col_defs.append(f"{col_name} {pg_type}")

        # Normalize PKs and validate they exist
        norm_pks = [ self._normalize_identifier(pk) for pk in primary_keys ]
        missing_pks = [pk for pk in norm_pks if pk not in norm_cols]
        if missing_pks:
            raise ValueError(f"Primary keys not present in DataFrame columns after normalization: {missing_pks}")

        pk_clause = f", PRIMARY KEY ({', '.join(norm_pks)})" if norm_pks else ""

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(col_defs)}
            {pk_clause}
        )
        """
        conn.execute(text(create_sql))
        print(f"✅ Tabla '{schema_name}.{table_name}' creada con PK {norm_pks}")

    def update_postresql(self, df_to_upload: pd.DataFrame, schema: str, table_name: str, primary_keys: list):
        engine = self.sql_conexion()  # must return a SQLAlchemy Engine
        if engine is None:
            print("❌ No se pudo obtener el engine de SQL.")
            return False

        # Ensure DataFrame columns are normalized the same way they’ll be created in SQL
        df_to_upload = df_to_upload.copy()
        df_to_upload.columns = [ self._normalize_identifier(c) for c in df_to_upload.columns ]
        norm_pks = [ self._normalize_identifier(pk) for pk in primary_keys ]

        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                exists = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = :schema AND table_name = :table
                        )
                    """),
                    {"schema": schema, "table": table_name}
                ).scalar()

                if not exists:
                    self.table_creation(conn, df_to_upload, schema, table_name, norm_pks)

                print(f"⚡ Preparado para insertar datos en {schema}.{table_name}")

                # 👉 usar la misma conn aquí
                self.upsert_dataframe(conn, df_to_upload, schema, table_name, primary_keys)
            return True

        except Exception as e:
            print(f"❌ Error en update_sql: {e}")
            return False
    

    def upsert_dataframe(self, conn, df: pd.DataFrame, schema: str, table_name: str, primary_keys: list):
        df = df.copy()
        df.columns = [self._normalize_identifier(c) for c in df.columns]
        norm_pks = [self._normalize_identifier(pk) for pk in primary_keys]

        # Ensure PKs exist
        missing = [pk for pk in norm_pks if pk not in df.columns]
        if missing:
            raise ValueError(f"Primary keys not found in DataFrame columns: {missing}")

        # Drop duplicates on PK
        df = df.drop_duplicates(subset=norm_pks, keep="last")
        df = df.where(pd.notnull(df), None)

        null_markers = {"", "nat", "nan", "none", "null", "n/a", "<na>"}
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                mask = df[col].apply(lambda v: isinstance(v, str) and v.strip().lower() in null_markers)
                if mask.any():
                    df.loc[mask, col] = None


        #print("🔎 Column dtypes before fix:")
        #print(df.dtypes)
        #print("🔎 Sample row after conversion:")
        #print(df.iloc[0].to_dict())

        cols = list(df.columns)
        col_list_sql = ", ".join(cols)
        pk_list_sql  = ", ".join(norm_pks)

        insert_sql = f"""
            INSERT INTO {schema}.{table_name} ({col_list_sql})
            VALUES %s
            ON CONFLICT ({pk_list_sql})
            DO NOTHING
        """

        date_like_cols = {col for col in cols if 'fecha' in col or 'date' in col}
        dummy_date = datetime(1900, 1, 1)

        def sanitize_value(value, column_name):
            if isinstance(value, (NaTType, NAType)):
                return dummy_date if column_name in date_like_cols else None
            if value is None:
                return dummy_date if column_name in date_like_cols else None
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            if isinstance(value, str):
                cleaned = value.strip()
                lowered = cleaned.lower()
                if lowered in null_markers:
                    return dummy_date if column_name in date_like_cols else None
                return cleaned
            if pd.isna(value):
                return dummy_date if column_name in date_like_cols else None
            return value

        total = len(df)
        if total == 0:
            print(f"-- No hay filas para insertar en {schema}.{table_name}.")
            return

        raw_conn = conn.connection
        cur = raw_conn.cursor()
        try:
            values_iter = (
                tuple(sanitize_value(val, col) for val, col in zip(row, cols))
                for row in df.itertuples(index=False, name=None)
            )
            execute_values(cur, insert_sql, values_iter, page_size=10000)
        finally:
            cur.close()  # commit y close los maneja SQLAlchemy

        print(f"OK {total} filas insertadas en {schema}.{table_name} (ON CONFLICT DO NOTHING)")
