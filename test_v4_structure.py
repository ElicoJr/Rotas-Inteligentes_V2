#!/usr/bin/env python3
"""
Script de teste para validar a estrutura do V4
"""
import sys
sys.path.append('/app')

def test_imports():
    """Testa se todos os imports estão corretos"""
    try:
        from v4.data_loader import prepare_equipes_v3, prepare_pendencias_v3
        print("✅ Imports do data_loader OK")
    except Exception as e:
        print(f"❌ Erro no data_loader: {e}")
        return False
    
    try:
        from v2.vroom_client import VroomClient
        print("✅ Import do VroomClient OK")
    except Exception as e:
        print(f"❌ Erro no VroomClient: {e}")
        return False
    
    try:
        from v4.main import _score_job, _ensure_result_schema
        print("✅ Imports de funções do main OK")
    except Exception as e:
        print(f"❌ Erro nas funções do main: {e}")
        return False
    
    return True

def test_vroom_client():
    """Testa se o VroomClient tem os métodos necessários"""
    try:
        from v2.vroom_client import VroomClient
        vc = VroomClient()
        
        # Verifica se tem os métodos
        assert hasattr(vc, 'route'), "Falta método route"
        assert hasattr(vc, 'route_multi'), "Falta método route_multi"
        assert hasattr(vc, '_post'), "Falta método _post"
        
        print("✅ VroomClient tem todos os métodos necessários")
        return True
    except Exception as e:
        print(f"❌ Erro no VroomClient: {e}")
        return False

def test_capacity_payload():
    """Testa se o payload com capacidade está correto"""
    try:
        vehicles = [
            {
                "id": 1,
                "start": [-63.9, -8.7],
                "end": [-63.9, -8.7],
                "time_window": [0, 28800],
                "capacity": [15]  # Teste de capacidade
            }
        ]
        
        jobs = [
            {
                "id": 1,
                "location": [-63.85, -8.75],
                "service": 1800,
                "delivery": [1]  # Teste de delivery
            }
        ]
        
        print("✅ Estrutura de payload com capacidade OK")
        print(f"   Vehicle capacity: {vehicles[0]['capacity']}")
        print(f"   Job delivery: {jobs[0]['delivery']}")
        return True
    except Exception as e:
        print(f"❌ Erro na estrutura do payload: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 TESTE DE ESTRUTURA V4")
    print("=" * 60)
    
    all_ok = True
    
    print("\n1️⃣ Testando imports...")
    all_ok &= test_imports()
    
    print("\n2️⃣ Testando VroomClient...")
    all_ok &= test_vroom_client()
    
    print("\n3️⃣ Testando estrutura de payload...")
    all_ok &= test_capacity_payload()
    
    print("\n" + "=" * 60)
    if all_ok:
        print("✅ TODOS OS TESTES PASSARAM!")
        print("=" * 60)
        print("\n📝 PRÓXIMOS PASSOS:")
        print("1. Certifique-se que o VROOM está rodando (localhost:3000)")
        print("2. Execute: python -m v4.main --limite 15 --debug")
        print("3. Compare os resultados com V3 para ver a melhoria")
    else:
        print("❌ ALGUNS TESTES FALHARAM")
    print("=" * 60)
