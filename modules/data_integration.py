import pandas as pd
import datetime 
import os
import glob
import re
import json



class DataIntegration:
    def __init__(self, working_folder, data_access, integration_path, helpers=None):
        self.working_folder = working_folder
        self.data_access = data_access  
        self.integration_path = integration_path 
        self.order_df = None
        self.helpers = helpers
        self.accounts=os.path.join(self.working_folder, "SAGI")
        self.logistica=os.path.join(self.working_folder, "Logística")
        self.facturas_path=os.path.join(self.working_folder, "Facturas")
        self.ordenes_path=os.path.join(self.working_folder, "Camunda")
        self.folders = {
            "SAGI": self.accounts,
            #"Logistica": self.logistica,
            "Facturas": self.facturas_path,
            "Ordenes": self.ordenes_path
            }
        self.record_file=os.path.join(self.integration_path,"processed_file.db")   

    def generate_file_groups(self):
        print(self.folders)
        from datetime import datetime, timedelta
        print(f"🔍 Buscando archivos más recientes...")
        # Regex para extraer yyyy-mm-dd-hh
        ts_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2}-\d{2})")
        
        # 1. Escanear todos los archivos
        all_files = []
        for cat, folder in self.folders.items():
            if not os.path.exists(folder):
                continue
            for f in os.listdir(folder):
                if f.endswith(".xlsx"):
                    m = ts_pattern.match(f)
                    if m:
                        ts = datetime.strptime(m.group(1), "%Y-%m-%d-%H")
                        all_files.append((ts, cat, os.path.join(folder, f)))

        # 2. Ordenar por timestamp
        all_files.sort(key=lambda x: x[0])

        # 3. Agrupar con ventana de 2 horas
        groups = []
        current = []
        for ts, cat, path in all_files:
            if not current:
                current.append((ts, cat, path))
            else:
                delta = ts - current[0][0]
                if delta <= timedelta(hours=2):
                    current.append((ts, cat, path))
                else:
                    groups.append(current)
                    current = [(ts, cat, path)]
        if current:
            groups.append(current)

        # 4. Formar all_groups y complete_groups
        all_groups = []
        complete_groups = []
        for g in groups:
            min_ts = min(x[0] for x in g)
            max_ts = max(x[0] for x in g)
            group_id = f"{min_ts.strftime('%Y-%m-%d-%H')}_{max_ts.strftime('%H')}"
            record = {"group_id": group_id}
            for cat in self.folders.keys():
                record[cat] = ""
            for ts, cat, path in g:
                record[cat] = path
            all_groups.append(record)

            if all(record[cat] for cat in self.folders.keys()):
                complete_groups.append(record)

        print("📂 all_groups encontrados:", len(all_groups))
        print("✅ complete_groups completos:", len(complete_groups))
       # 🔎 Analizar si los date-hour coinciden dentro de cada grupo
        exact_match_count = 0
        different_count = 0
        for g in all_groups:
            hours = []
            for cat in self.folders.keys():
                if g[cat]:
                    # Tomamos solo yyyy-mm-dd-hh del path
                    fname = os.path.basename(g[cat])
                    ts_prefix = fname[:13]  # YYYY-MM-DD-HH
                    hours.append(ts_prefix)
            if hours and all(h == hours[0] for h in hours):
                exact_match = True
                exact_match_count += 1
            else:
                exact_match = False
                different_count += 1
            print(f"Grupo {g['group_id']} → same_datehour == {exact_match}")

        print(f"📊 Grupos con misma fecha-hora exacta: {exact_match_count}")
        print(f"📊 Grupos con diferencias de hora: {different_count}")
        complete_groups.sort(
            key=lambda x: x['group_id'][:10],  # yyyy-mm-dd
            reverse=True
        )

        return complete_groups
             
    def integrar_datos(self):
        print("\n🔗 Iniciando proceso de TRANSFORMACIÓN de datos...")
        group_preffix_file = self.generate_file_groups()

        for group in group_preffix_file:
            # Cargamos dataframes 
            #df_logistica = pd.read_excel(group['Logistica'])    if group['Logistica']    else pd.DataFrame()
            raw_accounts_df     = pd.read_excel(group['SAGI'])     if group['SAGI']     else pd.DataFrame()
            raw_invoice_df = pd.read_excel(group['Facturas']) if group['Facturas'] else pd.DataFrame()
            self.order_df  = pd.read_excel(group['Ordenes'])  if group['Ordenes']  else pd.DataFrame()
            # Generamos fecha de grupo de archivos 
            prefix = group['group_id'].split("_")[0]   # "2025-09-19-08"
            dt = datetime.datetime.strptime(prefix, "%Y-%m-%d-%H")
            group_date = dt.replace(minute=0, second=0, microsecond=0)

            #-- sECCIÓN PARA 
            self.order_df['Importe'] = self.order_df['precio_unitario'].astype(float) * self.order_df['cantidad_solicitada'].astype(float)
            # Versiones limpias de facturas, SAGI, órdenes se mantiene igual. 
            invoice_df = self.clean_invoice_df(raw_invoice_df)
            accounts_df = self.clean_accounts_df(raw_accounts_df, invoice_df)    
            # Carga de logística
            yaml_penalties_key = 'PENAS'
            # Carga de penas convencionales
            penalties_df = self.helpers.load_and_concat(self.data_access.get(yaml_penalties_key))
            
            # Unión de Órdenes con facturas        
            print(self.order_df['numero_orden_suministro'].nunique())
            print(self.order_df.shape)
            orders_invoice_join = {
                'left': ['numero_orden_suministro'],
                'right': ['Referencia'],
                'return': ['UUID', 'Folio']
            }        
            self.order_df = self.populate_df(self.order_df, invoice_df, orders_invoice_join)
            orders_sagi_join = {'left': ['numero_orden_suministro'], 'right': ['Orden de suministro'], 'return': ['Estado de la factura']}
            self.order_df = self.populate_df(self.order_df, accounts_df, orders_sagi_join)
            # Unión con penas convencionales
            orders_penalties_join = {
                        'left': ['numero_orden_suministro'],
                        'right': ['ORDEN DE SUMINISTRO'],
                        'return': ['PENA', 'OFICIO']
                    }              
            self.order_df = self.populate_df(self.order_df, penalties_df, orders_penalties_join)
            
            self.order_df['PENA'] = self.order_df['PENA'] = pd.to_numeric(self.order_df['PENA'], errors='coerce')
            self.order_df['file_date']= group_date
            # Unión con datos logísticos.

            # Guardar archivo de integración
            output_file_name = f'{prefix}_Integracion INSABI.xlsx' 
            output_file_path = os.path.join(self.integration_path, output_file_name)
            self.save_if_modified(output_file_path, {
                "CAMUNDA": self.order_df,
                "SAGI": accounts_df,
                "FACTURAS": invoice_df,
                #"LOGÍSTICA": df_logistica
            }, self.record_file)
                       
    def save_if_modified(self, output_file_path, df_dict, record_file):
        """
        Guarda múltiples DataFrames en un Excel solo si el archivo destino
        no tiene la misma fecha de modificación registrada.
        """

        # 1. Cargar registro si existe
        if os.path.exists(record_file):
            with open(record_file, "r") as f:
                record = json.load(f)
        else:
            record = {}

        file_key = os.path.abspath(output_file_path)
        last_mod_time = None

        if os.path.exists(output_file_path):
            last_mod_time = os.path.getmtime(output_file_path)

        # 2. Verificar si ya está registrado y coincide
        if file_key in record and last_mod_time is not None:
            if abs(record[file_key] - last_mod_time) < 1:  # tolerancia de 1 segundo
                mod_dt = datetime.datetime.fromtimestamp(last_mod_time)
                print(f"⏩ Archivo '{os.path.basename(output_file_path)}' no ha cambiado desde {mod_dt}, no se sobrescribe.")
                return

        # 3. Escribir el archivo
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            for name, df in df_dict.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=name, index=False)
                    print(f"✅ Hoja '{name}' guardada con {len(df)} filas")

        print(f"\n🎉 ¡Integración completada exitosamente!")
        print(f"📁 Archivo guardado en: {os.path.basename(output_file_path)}")

        # 4. Actualizar registro
        new_mod_time = os.path.getmtime(output_file_path)
        record[file_key] = new_mod_time
        with open(record_file, "w") as f:
            json.dump(record, f)

    def clean_accounts_df(self, accounts_df, invoice_df):
        accounts_df = accounts_df[accounts_df['Estado de la factura'] != 'Cancelado']
        account_df_nan = accounts_df[accounts_df['Orden de suministro'].isna()]
        accounts_invoice_join = {'left': ['Folio fiscal'], 'right': ['UUID'], 'return': ['Referencia']}
        account_df_nan = self.populate_df(account_df_nan, invoice_df, accounts_invoice_join)
        # Solo los Folio fiscal donde Referencia no es nula
        mask = account_df_nan['Referencia'].notna()
        for folio, referencia in zip(account_df_nan.loc[mask, 'Folio fiscal'], account_df_nan.loc[mask, 'Referencia']):
            accounts_df.loc[accounts_df['Folio fiscal'] == folio, 'Orden de suministro'] = referencia

        #print(account_df_nan.info())
        accounts_df['Total'] = accounts_df['Total'].replace('[\$,]', '', regex=True).astype(float)
        return accounts_df
    

    def clean_invoice_df(self, invoice_df):
        print("🔍 Valores únicos en 'UUID Descripción':", invoice_df['UUID Descripción'].astype(str).unique()[:20])

        debug_ref = "IMB-23-02-2025-23026576-U013"
        if debug_ref in invoice_df['Referencia'].astype(str).values:
            row_debug = invoice_df[invoice_df['Referencia'].astype(str) == debug_ref]
            print("🔍 Row antes del filtro:\n", row_debug[['Referencia', 'Factura', 'UUID Descripción']])
 
        # Eliminar facturas no vigentes
        invoice_df = invoice_df[invoice_df['UUID Descripción'] == 'Vigente']

        primary_keys = ['Referencia', 'Factura']

        # Retiramos espacios de las llaves
        for col in primary_keys:
            invoice_df[col] = invoice_df[col].astype(str).str.replace(r"\s+", "", regex=True)

        # 🔍 Debug: check if the problematic Referencia is still here
        debug_ref = "IMB-23-02-2025-23026576-U013"
        if debug_ref in invoice_df['Referencia'].values:
            print(f"✅ {debug_ref} found BEFORE drop_duplicates, rows: {invoice_df[invoice_df['Referencia']==debug_ref].shape[0]}")
        else:
            print(f"❌ {debug_ref} missing BEFORE drop_duplicates")

        # Drop duplicates
        invoice_df = invoice_df.drop_duplicates(subset=primary_keys)

        # 🔍 Debug again
        if debug_ref in invoice_df['Referencia'].values:
            print(f"✅ {debug_ref} survived AFTER drop_duplicates, rows: {invoice_df[invoice_df['Referencia']==debug_ref].shape[0]}")
        else:
            print(f"❌ {debug_ref} missing AFTER drop_duplicates")

        if self.order_df is not None: 
            print(f"🔍 Validando facturas VS órdenes de suministro...")
            invoice_order_validation = {
                'left': ['Referencia', 'Total'],
                'right': ['numero_orden_suministro', 'Importe'],
                'return': ['orden_remision']
            }
            invoice_df = self.populate_df(invoice_df, self.order_df, invoice_order_validation)

        return invoice_df


    def populate_df(self, left_df, right_df, query_dict):
        """
        Pobla columnas en left_df a partir de right_df según query_dict.
        
        query_dict:
            {
                'left': ['col1_left', 'col2_left'],
                'right': ['col1_right', 'col2_right'],
                'return': ['colX_right', 'colY_right']
            }
        """
        left_keys = query_dict['left']
        right_keys = query_dict['right']
        return_cols = query_dict['return']

        # Validación
        if len(left_keys) != len(right_keys):
            raise ValueError("Las llaves left y right deben tener la misma longitud")
        # Validación de existencia de columnas en left_df
        missing_left = [col for col in left_keys if col not in left_df.columns]
        if missing_left:
            print(f"⚠️ Columnas faltantes en left_df: {', '.join(missing_left)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para keys
        missing_right_keys = [col for col in right_keys if col not in right_df.columns]
        if missing_right_keys:
            print(f"⚠️ Columnas faltantes en right_df para keys: {', '.join(missing_right_keys)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para return
        missing_return = [col for col in return_cols if col not in right_df.columns]
        if missing_return:
            print(f"⚠️ Columnas faltantes en right_df para return: {', '.join(missing_return)}. No se puede proceder con el merge.")
            return left_df

        # Índice compuesto para búsquedas rápidas
        right_index = right_df.groupby(right_keys)[return_cols].agg(lambda x: ','.join(x.astype(str))).reset_index()

        # Hacer merge left→right (left join)
        merged = pd.merge(
            left_df,
            right_index,
            how="left",
            left_on=left_keys,
            right_on=right_keys,
            suffixes=('', '_right'),
            indicator=True
        )

        # Métricas de match
        left_unmatched = (merged["_merge"] == "left_only").sum()
        right_unmatched = (merged["_merge"] == "right_only").sum()  # casi siempre 0 en left join

        print(f"📊 No match in left_df → {left_unmatched} rows")
        print(f"📊 No match in right_df → {right_unmatched} rows")

        # Rellenar NaN con "no localizado"
        for col in return_cols:
            if col in merged.columns:
                merged[col] = merged[col].fillna("no localizado")

        # Eliminar columnas auxiliares de join (las right_keys y el indicador)
        merged = merged.drop(columns=right_keys + ["_merge"], errors="ignore")
        # Rellenar NaN con "no localizado"
        for col in return_cols:
            if col in merged.columns:
                merged[col] = merged[col].fillna("no localizado")

        # Eliminar columnas auxiliares de join (las right_keys)
        merged = merged.drop(columns=right_keys, errors="ignore")


        return merged

    def get_newest_file(self, path, pattern="*.xlsx"): 
        """
        Obtiene el archivo más reciente basado en la fecha en el nombre del archivo.
        Formatos soportados: 
        - YYYY-MM-DD-HH-nombre.xlsx (ej: 2025-08-25-13-PREI.xlsx)
        - YYYY-MM-DD-HH_nombre.xlsx (ej: 2025-08-25-13_PAQ_IMSS.xlsx)
        - YYYY-MM-DD-HH-nombre-extra.xlsx (ej: 2025-08-25-12-SAI Altas.xlsx)
        """
        today = datetime.date.today()
        
        if not os.path.exists(path):
            print(f"⚠️ La carpeta {path} no existe")
            return None, None
        
        # Obtener todos los archivos que coincidan con el patrón
        files = glob.glob(os.path.join(path, pattern))
        
        if not files:
            print(f"⚠️ No se encontraron archivos {pattern} en {os.path.basename(path)}")
            return None, None
        
        newest_file = None
        newest_date = None
        
        for file_path in files:
            filename = os.path.basename(file_path)
            
            try:
                # Dividir el nombre por guiones
                parts = filename.split('-')
                
                # Necesitamos al menos 4 partes: YYYY, MM, DD, HH
                if len(parts) >= 4:
                    year = parts[0]
                    month = parts[1] 
                    day = parts[2]
                    hour = parts[3]
                    
                    # Limpiar la hora si tiene underscore o caracteres extra
                    # Ej: "13_PAQ" -> "13", "12" -> "12"
                    if '_' in hour:
                        hour = hour.split('_')[0]
                    elif ' ' in hour:
                        hour = hour.split(' ')[0]
                    # Si tiene extensión o más texto, tomar solo los primeros dígitos
                    hour = ''.join(filter(str.isdigit, hour))
                    
                    # Validar que todos sean números
                    if (year.isdigit() and month.isdigit() and 
                        day.isdigit() and hour.isdigit()):
                        
                        year_int = int(year)
                        month_int = int(month)
                        day_int = int(day)
                        hour_int = int(hour)
                        
                        # Crear datetime
                        file_date = datetime.datetime(year_int, month_int, day_int, hour_int, 0)
                        
                        if newest_date is None or file_date > newest_date:
                            newest_date = file_date
                            newest_file = file_path
                        
                        print(f"🔍 {filename} → {file_date.strftime('%Y-%m-%d %H:%M')}")
                    else:
                        print(f"⚠️ Formato de fecha inválido en: {filename}")
                        
            except (ValueError, IndexError) as e:
                print(f"⚠️ No se pudo extraer fecha de {filename}: {e}")
                continue
        
        if newest_file:
            file_date_only = newest_date.date()
            
            # Verificar si el archivo es de hoy
            if file_date_only < today:
                print(f"⚠️ El archivo {os.path.basename(newest_file)} no es de hoy ({file_date_only}), se recomienda descargar")
            
            print(f"✅ Archivo más reciente: {os.path.basename(newest_file)} ({newest_date.strftime('%Y-%m-%d %H:%M')})")
            return newest_file, newest_date
        else:
            print(f"❌ No se pudo determinar el archivo más reciente en {os.path.basename(path)}")
            return None, None

    def run_queries(self, queries_folder, schema, table_name):
        """Ejecuta las consultas SQL en el folder especificado."""
        print(f"🔄 Ejecutando consultas en {queries_folder}...")
        for query_file in glob.glob(os.path.join(queries_folder, "*.sql")):
            with open(query_file, "r") as f:
                query = f.read()
                self.execute_query(query, schema, table_name)