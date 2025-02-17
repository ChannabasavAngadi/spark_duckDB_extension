from setuptools import setup, find_packages

setup(
    name="duckdb_extension",  # The name of your package
    version="0.1",            # Version number
    packages=find_packages(), # Automatically find all packages
    install_requires=[        # Dependencies that your package needs
        "pyspark>=3.0.0",      # Make sure to set the correct PySpark version
        "duckdb>=0.3.0"
    ],
    author="Channabasav Angadi",       # Author's name
    author_email="channuangadi077@gmail.com",  # Author's email
    description="A custom PySpark extension for writing data to DuckDB",
    long_description=open("README.md").read(),  # Long description from README.md
    long_description_content_type="text/markdown",  # Markdown format for the README
    url="https://github.com/yourusername/duckdb_extension",  # Your GitHub repo URL
    classifiers=[  # Optional classifiers to categorize your project
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",  # Minimum Python version required
)
