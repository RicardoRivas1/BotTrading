{
    "name": "BotTrading",
    "version": "2.0.0",
    "description": "Bot de trading cuantitativo con filtros avanzados",
    "author": "Quant Trader",
    "license": "MIT",
    "python_requires": ">=3.8",
    "install_requires": [
        "ccxt>=4.2.0",
        "pandas>=2.0.0",
        "python-dotenv>=1.0.0",
        "requests>=2.31.0",
        "numpy>=1.24.0"
    ],
    "extras_require": {
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "mutpy>=0.6.1",
            "flake8>=6.1.0",
            "black>=23.7.0",
            "mypy>=1.5.0"
        ],
        "qa": [
            "coverage>=7.3.0",
            "pytest-xdist>=3.3.0"
        ]
    },
    "scripts": {
        "start": "python bot.py",
        "test": "pytest tests/ -v",
        "test-cov": "pytest tests/ --cov=./ --cov-report=html",
        "lint": "flake8 . --max-complexity=10",
        "format": "black .",
        "type-check": "mypy .",
        "qa": "python quality_check.py",
        "mutate": "mut.py --target ./logic.py --unit-test ./tests/test_filtros.py"
    },
    "package_dir": {"": "."},
    "packages": ["logic"],
    "include_package_data": True,
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11"
    ]
}