import os
import yaml
from modules.helpers import message_print, create_directory_if_not_exists
import pandas as pd
import datetime
import platform
import hashlib
import csv
from io import StringIO


class DownloadedFilesManager:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access

    def manage_downloaded_files(self, path_input, steps):
        #print("steps\n", steps)
        print(f"Procesando archivos desde\n\t{os.path.basename(path_input)}\n")
        # Obtener fecha de hoy
        today = datetime.date.today()
        current_hour = datetime.datetime.now().hour
        # Listar archivos de hoy con extensiones válidas
        valid_extensions = ['.csv', '.xlsx', '.xls']
        files_today = []
        for file in os.listdir(path_input):
            file_path = os.path.join(path_input, file)
            if os.path.isfile(file_path) and any(file.lower().endswith(ext) for ext in valid_extensions):
                creation_date = self.get_file_creation_date(file_path).date()
                if creation_date == today:
                    files_today.append(file_path)
        # Files_today tiene los archivos descargados de hoy 
        # Generamos las matrices por tipo de archivo
        csv = []
        xls = []
        xlsx = []
        for file in files_today:
            if file.lower().endswith('.csv'):
                csv.append(file)
            elif file.lower().endswith('.xls'):
                xls.append(file)
            elif file.lower().endswith('.xlsx'):
                xlsx.append(file)
        # Cargar dataframes para cada tipo
        csv_dfs = self.extract_dataframes(csv)
        xls_dfs = self.extract_dataframes(xls)
        xlsx_dfs = self.extract_dataframes(xlsx)
        if csv_dfs: 
            self.concatenate_dfs(csv_dfs, path_input, steps)
            for file in csv:
                os.remove(file)
        if xls_dfs:
            self.concatenate_dfs(xls_dfs, path_input, steps)
            for file in xls:
                os.remove(file)
        if xlsx_dfs:
            self.concatenate_dfs(xlsx_dfs, path_input, steps)
            for file in xlsx:
                os.remove(file)
        print("✅ Proceso de fusión y renombre de archivos descargados completado.\n")
        print("Se fusionan archivos siempre que sean del mismo día, mismos encabezados, contenido distinto")

    def concatenate_dfs(self, df_list, path_input, steps):
        if not df_list:
            return
        
        # Obtener fecha de hoy
        today = datetime.date.today()
        current_hour = datetime.datetime.now().hour

        # Group by columns (as tuple of sorted column names)
        groups = {}
        for df in df_list:
            cols = tuple(sorted(df.columns))
            if cols not in groups:
                groups[cols] = []
            groups[cols].append(df)
        
        date_obj = datetime.datetime.combine(today, datetime.time(hour=current_hour))
        date_str = self.format_date_for_filename(date_obj)
        base_path = os.path.join(path_input, '..')
        
        for i, (cols, dfs) in enumerate(groups.items()):
            # 🔑 Siempre concatenar, sin borrar duplicados
            result_df = pd.concat(dfs, ignore_index=True, sort=False)

            # Guardar archivo
            filename = f'{date_str}h {steps}_{i}.xlsx' if len(groups) > 1 else f'{date_str}h {steps}.xlsx'
            save_path = os.path.join(base_path, filename)
            result_df.to_excel(save_path, index=False)
            print(f"✅ Guardado: {save_path} ({len(result_df)} filas)")
        

    def extract_dataframes(self, file_list):
        """Carga archivos CSV/XLSX/XLS y devuelve una lista de DataFrames.
        - CSV: respeta comas escapadas con '\,' (escapechar='\\'), BOM utf-8-sig.
        - XLSX: lee todas las hojas; agrega un DF por hoja no vacía.
        - XLS: usa self.XLS_header_location(file). Acepta DF o dict de DFs.
        """
        def _clean_df(df: pd.DataFrame) -> pd.DataFrame | None:
            if df is None:
                return None
            # Normaliza encabezados y elimina columnas 'Unnamed'
            df.columns = (pd.Index(df.columns)
                        .astype(str)
                        .str.replace(r"\s+", " ", regex=True)
                        .str.strip())
            df = df.loc[:, ~df.columns.str.match(r"^Unnamed(\s*:\s*\d+)?$")]
            # Opcional: descartar hojas completamente vacías
            if df.empty or df.dropna(how="all").empty:
                return None
            return df

        dataframes: list[pd.DataFrame] = []
        if not file_list:
            return dataframes

        for file in file_list:
            file_type = os.path.splitext(file)[1].lower().lstrip('.')
            try:
                if file_type == 'csv':
                    # CSV robusto: respeta '\,' como coma literal dentro del campo
                    df = pd.read_csv(
                        file,
                        sep=",",
                        engine="c",              # soporta escapechar y es rápido
                        encoding="utf-8-sig",
                        quoting=csv.QUOTE_NONE,
                        escapechar="\\",
                        na_values=["\\N"],
                        dtype=str,
                        on_bad_lines="skip"
                    )
                    df = _clean_df(df)
                    if df is not None:
                        dataframes.append(df)
                elif file_type == 'xlsx':
                    with pd.ExcelFile(file) as xls:
                        for sheet in xls.sheet_names:
                            dfx = pd.read_excel(xls, sheet_name=sheet, dtype=str)
                            dfx = _clean_df(dfx)
                            if dfx is not None:
                                dataframes.append(dfx)

                elif file_type == 'xls':
                    # XLS: tu función especializada (puede devolver DF o dict de DFs)
                    out = self.XLS_header_location(file)
                    if isinstance(out, dict):
                        for dfx in out.values():
                            dfx = _clean_df(dfx)
                            if dfx is not None:
                                dataframes.append(dfx)
                    else:
                        dfx = _clean_df(out)
                        if dfx is not None:
                            dataframes.append(dfx)

                else:
                    print(f"⚠️ Formato no soportado: {file}")

            except Exception as e:
                print(f"❌ Error al procesar {file}: {e}")
                # continúa con el siguiente archivo

        return dataframes



    def _file_sha256(self, file_path, chunk_size=65536):
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _normalize_cols(self, cols):
        def norm_one(x):
            try:
                s = str(x)
            except Exception:
                s = ''
            # Normalizaciones básicas: trim, lower, colapsar espacios
            s = s.replace('\u00a0', ' ')  # NBSP a espacio normal
            s = ' '.join(s.strip().split())
            return s.lower()
        return [norm_one(c) for c in cols]

    def get_file_creation_date(self, file_path):
        """
        Extrae la fecha de creación del archivo de manera precisa en Windows y Mac
        Returns: datetime object with the creation date
        """
        try:
            file_stats = os.stat(file_path)

            if platform.system() == 'Windows':
                creation_timestamp = file_stats.st_ctime
            elif platform.system() == 'Darwin':  # macOS
                creation_timestamp = getattr(file_stats, 'st_birthtime', file_stats.st_ctime)
            else:  # Linux y otros Unix
                creation_timestamp = file_stats.st_ctime

            return datetime.datetime.fromtimestamp(creation_timestamp)

        except Exception as e:
            print(f"Error al obtener fecha de creación de {file_path}: {e}")
            # Fallback: usar fecha de modificación
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

    def format_date_for_filename(self, date_obj):
        """
        Formatea la fecha para usar en nombres de archivo
        Returns: string en formato YYYY-MM-DD-HH
        """
        return date_obj.strftime("%Y-%m-%d-%H")

    def XLS_header_location(self, filepath):
        """
        Busca en las primeras 10 filas del archivo XLS para encontrar los headers correctos
        que coincidan con columns_PREI y retorna el DataFrame con los headers correctos.
        """
        columns_PREI = self.data_access['columns_PREI']

        # Leer las primeras 11 filas (0-10) sin headers
        df_raw = pd.read_excel(filepath, header=None, nrows=11)

        header_row = None

        # Buscar en cada fila (0-10) los headers que coincidan
        for row_index in range(min(11, len(df_raw))):
            potential_headers = df_raw.iloc[row_index].tolist()
            # Limpiar valores None, NaN y convertir a string
            potential_headers = [str(col).strip() if pd.notna(col) else '' for col in potential_headers]
            # Filtrar solo valores no vacíos
            potential_headers = [col for col in potential_headers if col != '' and col != 'nan']

            print(f"Fila {row_index}: {potential_headers}")

            # Verificar si coincide con columns_PREI
            if potential_headers == columns_PREI:
                header_row = row_index
                print(f"Headers PREI encontrados en fila {row_index}")
                break

        if header_row is not None:
            # Leer el archivo completo usando la fila correcta como header
            df_final = pd.read_excel(filepath, header=header_row)
            print(f"DataFrame PREI creado con {len(df_final)} filas y columnas: {df_final.columns.tolist()}")
            return df_final
        else:
            print(f"No se encontraron headers que coincidan con columns_PREI: {columns_PREI}")
            print(f"Headers esperados: {columns_PREI}")
            return None
