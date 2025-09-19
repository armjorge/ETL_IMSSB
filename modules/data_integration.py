import pandas as pd
import datetime 
import os
import glob
from modules.helpers import message_print, create_directory_if_not_exists


class DataIntegration:
    def __init__(self, working_folder, data_access, integration_path):
        self.working_folder = working_folder
        self.data_access = data_access  
        self.integration_path = integration_path 
        self.order_df = None

    def integrar_datos(self, ordenes_fuente, facturas_fuente, tesoreria_fuente, logistica_fuente):
        print("\n🔗 Iniciando proceso de TRANSFORMACIÓN de datos...")

        print(f"🔍 Buscando archivos más recientes para órdenes, facturas, información tesorería y logística...")

        newest_orders_file, orders_date = self.get_newest_file(ordenes_fuente, "*.xlsx")
        newest_logistic_file, logistic_date = self.get_newest_file(logistica_fuente, "*.xlsx")
        newest_sagi_file, prei_date = self.get_newest_file(tesoreria_fuente, "*.xlsx")
        newest_facturas_file, facturas_date = self.get_newest_file(facturas_fuente, "*.xlsx")
        self.order_df = pd.read_excel(newest_orders_file) if newest_orders_file else pd.DataFrame()
        self.order_df['Importe'] = self.order_df['precio_unitario'].astype(float) * self.order_df['cantidad_solicitada'].astype(float)
        invoice_df = pd.read_excel(newest_facturas_file) if newest_facturas_file else pd.DataFrame()
        # De inmediato quiero agregar los 'Orden de suministro' faltantes si es que existen
        accounts_df = pd.read_excel(newest_sagi_file) if newest_sagi_file else pd.DataFrame()
        accounts_df = self.clean_accounts_df(accounts_df, invoice_df)
        invoice_df = self.clean_invoice_df(invoice_df)
        
        
        logistic_df = pd.read_excel(newest_logistic_file) if newest_logistic_file else pd.DataFrame()

        
        print(self.order_df['numero_orden_suministro'].nunique())
        print(self.order_df.shape)
        orders_invoice_join = {
            'left': ['numero_orden_suministro', 'Importe'],
            'right': ['Referencia', 'Total'],
            'return': ['UUID', 'Folio']
        }        
        self.order_df = self.populate_df(self.order_df, invoice_df, orders_invoice_join)
        orders_sagi_join = {'left': ['numero_orden_suministro'], 'right': ['Orden de suministro'], 'return': ['Estado de la factura']}
        self.order_df = self.populate_df(self.order_df, accounts_df, orders_sagi_join)
        # Unión con datos logísticos.

        # Guardar archivo de integración

        today = datetime.datetime.now()
        today_string_file = today.strftime('%Y-%m-%d %H') + 'h_integracion.xlsx'
        create_directory_if_not_exists(self.integration_path)
        with pd.ExcelWriter(os.path.join(self.integration_path, today_string_file)) as writer:
            self.order_df.to_excel(writer, sheet_name='order_df', index=False)
            invoice_df.to_excel(writer, sheet_name='invoice_df', index=False)
            accounts_df.to_excel(writer, sheet_name='accounts_df', index=False)
            logistic_df.to_excel(writer, sheet_name='logistic_df', index=False)
        print(f"✅ Archivo de integración guardado en {self.integration_path} como {today_string_file}")

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
        # Eliminar facturas no vigentes
        invoice_df = invoice_df[invoice_df['UUID Descripción'] == 'Vigente']
        # Eliminar sin orden vinculada
        #invoice_df = invoice_df[invoice_df['Referencia'].notna()]
        invoice_df = invoice_df.drop_duplicates(subset=['Referencia', 'Factura'])
        if self.order_df is not None: 
            print(f"🔍 Validando facturas VS órdenes de suministro...")
            invoice_order_validation = {'left': ['Referencia', 'Total'], 'right': ['numero_orden_suministro', 'Importe'], 'return': ['orden_remision']}
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
            suffixes=('', '_right')
        )

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