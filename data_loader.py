import pandas as pd

def load_data():
    print("🔄 Carregando dataframes...")
    try:
        df_equipes = pd.read_parquet("data/Equipes.parquet")
        df_tecnicos = pd.read_parquet("data/atendTec.parquet")
        df_comerciais = pd.read_parquet("data/ServCom.parquet")
        print("✅ Dataframes carregados com sucesso.")
    except FileNotFoundError as e:
        print(f"❌ Erro ao carregar arquivos Parquet: {e}")
        exit(1)
    
    df_equipes.columns = df_equipes.columns.str.lower()
    df_equipes["dt_ref"] = pd.to_datetime(df_equipes["dt_ref"])
    
    df_tecnicos = df_tecnicos.dropna(subset=['LATITUDE', 'LONGITUDE'])
    df_tecnicos['TE'] = df_tecnicos['TE'].fillna(0).round(0).astype(int)
    
    df_comerciais = df_comerciais.dropna(subset=['LATITUDE', 'LONGITUDE'])
    # Esta é a sua correção V15, que é excelente.
    df_comerciais = df_comerciais.loc[df_comerciais["DATA_VENC"] > df_comerciais["DATA_SOL"]]
    df_comerciais['TE'] = df_comerciais['TE'].fillna(0).round(0).astype(int)

    return df_equipes, df_tecnicos, df_comerciais

def verificar_e_converter_colunas(df, colunas_esperadas, tipos_datetime):
    for col_esperada, col_real in colunas_esperadas.items():
        if col_real not in df.columns:
            print(f"❌ Coluna '{col_real}' não encontrada. Colunas disponíveis: {df.columns.tolist()}")
            exit(1)
        if col_esperada in tipos_datetime:
            df[col_real] = pd.to_datetime(df[col_real], errors='coerce')

def prepare_columns(df_equipes, df_tecnicos, df_comerciais):
    print("🔄 Preparando colunas dos dataframes...")
    colunas_esperadas_equipes = {
        'dt_ref': 'dt_ref',
        'dthaps_ini': 'dthaps_ini',
        'dthaps_fim_ajustado': 'dthaps_fim_ajustado',
        'dthpausa_ini': 'dthpausa_ini',
        'dthpausa_fim': 'dthpausa_fim',
        'equipe': 'equipe'
    }
    tipos_datetime_equipes = ['dt_ref', 'dthaps_ini', 'dthaps_fim_ajustado', 'dthpausa_ini', 'dthpausa_fim']
    verificar_e_converter_colunas(df_equipes, colunas_esperadas_equipes, tipos_datetime_equipes)
    
    colunas_esperadas_tecnicos = {
        'DH_INICIO': 'DH_INICIO',
        'DH_FINAL': 'DH_FINAL',
        'EQUIPE': 'EQUIPE',
        'NUMOS': 'NUMOS',
        'TE': 'TE',
        'LONGITUDE': 'LONGITUDE',
        'LATITUDE': 'LATITUDE',
        'EUSD': 'EUSD'
    }
    verificar_e_converter_colunas(df_tecnicos, colunas_esperadas_tecnicos, ['DH_INICIO', 'DH_FINAL'])
    
    colunas_esperadas_comerciais = {
        'DATA_SOL': 'DATA_SOL',
        'DATA_VENC': 'DATA_VENC',
        'EQUIPE': 'EQUIPE',
        'NUMOS': 'NUMOS',
        'TE': 'TE',
        'LONGITUDE': 'LONGITUDE',
        'LATITUDE': 'LATITUDE',
        'CODSERV': 'CODSERV',
        'EUSD': 'EUSD'
    }
    verificar_e_converter_colunas(df_comerciais, colunas_esperadas_comerciais, ['DATA_SOL', 'DATA_VENC'])
    
    print("✅ Colunas preparadas.")
    return df_equipes, df_tecnicos, df_comerciais