# from setuptools import setup
# from Cython.Build import cythonize
# import os


# src_path = os.path.join(os.path.dirname(__file__), "src")
# print(src_path)

# setup(
#     name="fuzzrapid",
#     ext_modules=cythonize(os.path.join(src_path, "fuzzrapid.pyx")),
#     install_requires=["rapidfuzz"],
# )

from setuptools import setup, find_packages

setup(
    name="buscadorpartnumber",
    version="0.1.0",
    author="Gustavo Trillo",
    author_email="gustavo.edu.tb@gmail.com",
    description="Busqueda de partnumber en base a descripcion de producto",
    packages=find_packages(),  
    install_requires=[
        "pandas",
        "numpy",
        "datetime",
        "sys",
        "mysql-connector",
        "python-dotenv",
        "seaborn",
        "matplotlib",
        "ipywidgets",
    ],
    entry_points={
        "console_scripts": [
            "vs_code_busqueda_partnumber = vs_code_busqueda_partnumber:main",  
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "License :: Other/Proprietary License",
        "Operating System :: Microsoft :: Windows",
    ],
    python_requires=">=3.11",
)