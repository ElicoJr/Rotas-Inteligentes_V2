#!/usr/bin/env python3
"""
Script para testar se as colunas do V4 estão sendo preenchidas corretamente
"""
import sys
import pandas as pd
from pathlib import Path

sys.path.append('/app')

def test_v4_output():
    """Testa se os arquivos V4 têm as mesmas colunas do V3"""
    
    results_v3 = Path("/app/results_v3")
    results_v4 = Path("/app/results_v4")
    
    # Pegar primeiro arquivo de cada
    v3_files = list(results_v3.glob("*.parquet"))
    v4_files = list(results_v4.glob("*.parquet"))
    
    if not v3_files:
        print("❌ Nenhum arquivo V3 encontrado")
        return False
    
    if not v4_files:
        print("❌ Nenhum arquivo V4 encontrado")
        return False
    
    print("=" * 70)
    print("🔍 COMPARAÇÃO DE COLUNAS V3 vs V4")
    print("=" * 70)
    
    # Ler primeiro arquivo de cada
    df_v3 = pd.read_parquet(v3_files[0])
    df_v4 = pd.read_parquet(v4_files[0])
    
    print(f"\n📁 V3: {v3_files[0].name}")
    print(f"   Linhas: {len(df_v3)}")
    print(f"   Colunas: {len(df_v3.columns)}")
    
    print(f"\n📁 V4: {v4_files[0].name}")
    print(f"   Linhas: {len(df_v4)}")
    print(f"   Colunas: {len(df_v4.columns)}")
    
    # Colunas no V3 mas não no V4
    missing_in_v4 = set(df_v3.columns) - set(df_v4.columns)
    if missing_in_v4:
        print(f"\n❌ Colunas faltando no V4: {missing_in_v4}")
    else:
        print("\n✅ V4 tem todas as colunas do V3")
    
    # Colunas no V4 mas não no V3
    extra_in_v4 = set(df_v4.columns) - set(df_v3.columns)
    if extra_in_v4:
        print(f"\n➕ Colunas extras no V4: {extra_in_v4}")
    
    # Verificar colunas vazias
    print("\n" + "=" * 70)
    print("📊 VERIFICAÇÃO DE VALORES PREENCHIDOS (V4)")
    print("=" * 70)
    
    important_cols = [
        'dthaps_ini', 'dthaps_fim_ajustado', 'inicio_turno', 'fim_turno',
        'dthpausa_ini', 'dthpausa_fim', 'fim_turno_estimado', 'chegada_base',
        'base_lon', 'base_lat', 'distancia_vroom', 'duracao_vroom'
    ]
    
    all_ok = True
    for col in important_cols:
        if col in df_v4.columns:
            filled = df_v4[col].notna().sum()
            total = len(df_v4)
            pct = (filled / total * 100) if total > 0 else 0
            
            if pct == 0:
                print(f"❌ {col:25s}: {filled:4d}/{total:4d} ({pct:5.1f}%) - VAZIO!")
                all_ok = False
            elif pct < 50:
                print(f"⚠️  {col:25s}: {filled:4d}/{total:4d} ({pct:5.1f}%) - Parcial")
            else:
                print(f"✅ {col:25s}: {filled:4d}/{total:4d} ({pct:5.1f}%)")
        else:
            print(f"❌ {col:25s}: COLUNA NÃO EXISTE")
            all_ok = False
    
    print("\n" + "=" * 70)
    if all_ok and not missing_in_v4:
        print("✅ TODOS OS TESTES PASSARAM!")
    else:
        print("❌ ALGUNS TESTES FALHARAM")
    print("=" * 70)
    
    return all_ok and not missing_in_v4

if __name__ == "__main__":
    success = test_v4_output()
    sys.exit(0 if success else 1)
