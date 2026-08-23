# Sistema de Calidad para Bot de Trading

## Comandos de Control de Calidad

### 1. Ejecutar Pruebas Unitarias
```bash
pytest tests/ -v
```

### 2. Ejecutar con Cobertura
```bash
pytest tests/ --cov=./ --cov-report=html
```

### 3. Pruebas de Mutación (opcional - requiere mutpy)
```bash
pip install mutpy
mut.py --target ./logic.py --unit-test ./tests/test_filtros.py
```

### 4. Análisis Estático
```bash
pip install flake8 black mypy
flake8 . --max-complexity=10
black --check .
mypy .
```

## Métricas de Calidad Implementadas

1. **Cobertura de Código**: >85% requerido
2. **Complexidad Ciclomática**: <10 por función
3. **Tasa de Fallos**: <5% en pruebas
4. **Tiempo de Ejecución**: <2 segundos por prueba

## Procedimientos de Control

### Pre-commit
- Ejecutar pruebas automáticamente antes de cada commit
- Validar estilo de código con black/flake8
- Verificar tipos con mypy

### CI/CD (GitHub Actions/Render)
- Ejecutar suite completa en cada push
- Generar reportes de cobertura
- Alertas en degradación de calidad

## Estructura de Tests
```
tests/
├── test_filtros.py      # Pruebas unitarias filtros
├── test_integracion.py  # Pruebas integración
├── test_estrategias.py  # Pruebas estrategias
└── conftest.py         # Configuración pytest
```