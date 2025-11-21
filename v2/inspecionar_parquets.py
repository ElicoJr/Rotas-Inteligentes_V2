import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")

def peek(path, n=3):
    df = pd.read_parquet(path)
    cols = list(df.columns)
    sample = df.head(n).to_dict("records")
    return cols, sample

def main():
    print("\n🔎 Inspeção rápida dos .parquet em /data\n")
    for fname in ["atendTec.parquet","Equipes.parquet","ServCom.parquet"]:
        p = DATA_DIR / fname
        if not p.exists():
            print(f"⚠️  Não encontrado: {fname}")
            continue
        cols, sample = peek(p)
        print(f"📄 {fname}")
        print(f"➤ Colunas: {cols}")
        print(f"➤ Amostra: {sample}\n")

if __name__ == "__main__":
    main()
