from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("busqueda.pyx", language_level="3")
)