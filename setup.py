from setuptools import setup, find_packages

setup(
    name="data_extractor",
    version="1.0.0",
    description="Spark JDBC data extraction module for Oracle databases with parallel processing",
    author="Data Engineering Team",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.4.0",
        "py4j>=0.10.9.7",
        "cx_Oracle>=8.3.0",
        "configparser>=5.3.0",
        "python-dateutil>=2.8.2",
        "pytz>=2023.3",
        "pathlib>=1.0.1",
        "argparse>=1.4.0",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "data-extractor=data_extractor.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)