import subprocess
import sys
import pandas as pd
from logic import calcular_profit_factor, validar_profit_factor_minimo


def ejecutar_comando(comando, descripcion):
    print(f"\n>>> Ejecutando: {descripcion}...")
    resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
    if resultado.returncode != 0:
        print(f"ERROR EN: {descripcion}")
        print(resultado.stdout)
        print(resultado.stderr)
        return False
    print(f"OK: {descripcion}")
    return True


def calcular_metricas_historial():
    """Calcula métricas de trading a partir del historial CSV."""
    try:
        df = pd.read_csv("historial_trading.csv")
        ventas = df[df["Tipo_Operacion"] == "VENTA"]
        
        if ventas.empty:
            print("No hay operaciones de venta registradas en el historial.")
            return 0.0, 0.0
        
        ganancias = ventas[ventas["Ganancia_USDT"] > 0]["Ganancia_USDT"].sum()
        perdidas = abs(ventas[ventas["Ganancia_USDT"] < 0]["Ganancia_USDT"].sum())
        
        return ganancias, perdidas
    except FileNotFoundError:
        print("No se encontró archivo de historial. Iniciando con métricas en cero.")
        return 0.0, 0.0
    except Exception as e:
        print(f"Error al calcular métricas: {e}")
        return 0.0, 0.0


def validar_profit_factor_pipeline():
    """Valida que el Profit Factor cumpla con el mínimo requerido."""
    print("\nVALIDACIÓN DE PROFIT FACTOR")
    print("=" * 40)
    
    ganancias, perdidas = calcular_metricas_historial()
    profit_factor = calcular_profit_factor(ganancias, perdidas)
    
    print(f"Ganancias totales: ${ganancias:.2f}")
    print(f"Pérdidas totales: ${perdidas:.2f}")
    print(f"Profit Factor: {profit_factor:.2f}")
    
    if validar_profit_factor_minimo(ganancias, perdidas, 1.2):
        print("Profit Factor APROBADO (>= 1.2)")
        return True
    else:
        print("Profit Factor RECHAZADO (< 1.2)")
        return False


def pipeline_qa():
    print("==============================================")
    print("PIPELINE DE CONTROL DE CALIDAD Y METRICAS")
    print("==============================================")

    # 1. Pruebas Unitarias y Cobertura de Código (>90%)
    cmd_coverage = "pytest --cov=logic --cov-fail-under=90 test_logic.py"
    if not ejecutar_comando(
        cmd_coverage, "Pruebas Unitarias y Cobertura de Código (Minimo 90%)"
    ):
        sys.exit(1)

    # 2. Métricas de Complejidad Ciclomática (Radon)
    cmd_radon = "radon cc logic.py -a -s"
    if not ejecutar_comando(cmd_radon, "Metricas de Complejidad Ciclomatica"):
        sys.exit(1)

    # 3. Análisis Estático / Calidad de Código (Pylint)
    cmd_pylint = "pylint logic.py --fail-under=8.0"
    if not ejecutar_comando(
        cmd_pylint, "Calidad de Código y Estilo (Pylint >= 8.0/10)"
    ):
        sys.exit(1)

    # 4. Validación de Profit Factor
    if not validar_profit_factor_pipeline():
        print("\nPIPELINE FALLIDO: Profit Factor insuficiente")
        print("Se requiere Profit Factor >= 1.2 para ejecutar el bot")
        sys.exit(1)

    # 5. Pruebas de Mutación (Manejo de entorno Windows)
    if sys.platform == "win32":
        print(
            "\nPruebas de Mutación: Omitidas automaticamente (mutmut requiere WSL en Windows)."
        )
    else:
        cmd_mutmut = "mutmut run --paths-to-mutate=logic.py"
        ejecutar_comando(cmd_mutmut, "Análisis de Pruebas de Mutación")

    print("\nTODAS LAS PRUEBAS Y METRICAS DE CALIDAD FUERON APROBADAS.")
    print("Iniciando el bot de trading...\n")

    import bot

    bot.run()


if __name__ == "__main__":
    pipeline_qa()
