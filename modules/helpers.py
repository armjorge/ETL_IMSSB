import pandas as pd

class HELPERS:
    @staticmethod

    def load_and_concat(config_section: dict) -> pd.DataFrame:
        """
        Load and concatenate DataFrames from a config section.
        Concatenates by column **position**, ignoring column names,
        but restores the first file's column names at the end.
        """
        dfs = []
        standard_cols = None
        original_names = None

        for name, cfg in config_section.items():
            file_path = cfg.get("file_path")
            sheet = cfg.get("sheet")
            rows = cfg.get("rows")

            if not file_path or not sheet:
                print(f"⚠️ Skipping {name}, missing file_path or sheet")
                continue

            # Try to find the header row by checking first 10 rows
            df = None
            for skip in range(11):  # 0 to 10
                try:
                    temp_df = pd.read_excel(file_path, sheet_name=sheet, skiprows=skip, nrows=0)  # Read headers only
                    if all(col in temp_df.columns for col in rows):
                        df = pd.read_excel(file_path, sheet_name=sheet, skiprows=skip)
                        #print(f"✅ Header found at skiprows={skip} for {name}")
                        break
                except Exception as e:
                    print(f"⚠️ Error reading with skiprows={skip} for {name}: {e}")
                    continue
            else:
                print(f"⚠️ Could not find matching columns {rows} in first 10 rows for {name}, loading with skiprows=0")
                df = pd.read_excel(file_path, sheet_name=sheet)

            print(f"Loaded df for {name}: shape={df.shape}, columns={list(df.columns)}")

            # Keep only requested columns
            if rows:
                df = df[[col for col in rows if col in df.columns]]

            # Standardize columns by **position**
            if standard_cols is None:
                standard_cols = [f"col_{i}" for i in range(len(df.columns))]
                original_names = df.columns.tolist()
                print(f"📝 Using {len(standard_cols)} columns as standard: {original_names}")

            # Rename current df to match standard by index
            df.columns = standard_cols[:len(df.columns)]

            # Add a source tag
            #df["__source__"] = name

            dfs.append(df)

            # Print valuable info
            print(f"📂 {name}: {file_path}")
            print(f"📑 Sheet: {sheet}")
            print(f"✅ Shape: {df.shape[0]} rows × {df.shape[1]} cols")

        # Concatenate all dataframes
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)

            # Restore original column names + __source__
            final_df.columns = original_names #+ ["__source__"]

            print(f"\n🔗 Final concatenated DataFrame: {final_df.shape[0]} rows × {final_df.shape[1]} cols")
            return final_df
        else:
            print("⚠️ No dataframes loaded.")
            return pd.DataFrame()
