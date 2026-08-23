#!/usr/bin/env python
"""Script de Control de Calidad automatizado."""

import subprocess
import sys
import os


def run_command(command, description):
    """Ejecuta un comando y maneja el resultado."""
    print(f"\n{'='*60}")
    print(f"EJECUTANDO: {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - EXITOSO")
            if result.stdout:
                print(result.stdout[:500])  # Limitar output
            return True
        else:
            print(f"❌ {description} - FALLIDO")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error ejecutando {description}: {e}")
        return False


def main():
    """Flujo principal de control de calidad."""
    print("INICIANDO SISTEMA DE CONTROL DE CALIDAD")
    print("="*60)
    
    # 1. Instalar dependencias de testing
    run_command("pip install pytest pytest-cov mutpy flake8 black mypy", "Instalando dependencias de testing")
    
    # 2. Verificar estilo de código
    print("\nANALISIS ESTATICO DE CODIGO")
    print("-"*40)
    
    flake8_ok = run_command("flake8 . --max-complexity=10 --count", "Flake8 - Analisis de complejidad")
    black_ok = run_command("black --check .", "Black - Formato de codigo")
    mypy_ok = run_command("mypy .", "Mypy - Verificacion de tipos")
    
    # 3. Ejecutar pruebas unitarias
    print("\nPRUEBAS UNITARIAS")
    print("-"*40)
    
    pytest_ok = run_command("pytest tests/ -v", "Pytest - Pruebas unitarias")
    
    # 4. Ejecutar pruebas con cobertura
    print("\nCOBERTURA DE CODIGO")
    print("-"*40)
    
    coverage_ok = run_command("pytest tests/ --cov=./ --cov-report=term-missing", "Cobertura de codigo")
    
    # 5. Resumen final
    print("\n" + "="*60)
    print("RESUMEN FINAL DE CALIDAD")
    print("="*60)
    
    resultados = {
        "Flake8": flake8_ok,
        "Black": black_ok,
        "Mypy": mypy_ok,
        "Pytest": pytest_ok,
        "Cobertura": coverage_ok,
    }
    
    for test, ok in resultados.items():
        status = "PASO" if ok else "FALLO"
        print(f"{test}: {status}")
    
    exit_code = 0 if all(resultados.values()) else 1
    print(f"\n{'TODOS LOS TESTS PASARON' if exit_code == 0 else 'ALGUN TEST FALLO'}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()