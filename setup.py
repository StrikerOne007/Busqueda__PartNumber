from setuptools import setup
from Cython.Build import cythonize
import os


src_path = os.path.join(os.path.dirname(__file__), "src")

setup(
    name = 'fuzzrapid',
    ext_modules=cythonize(os.path.join(src_path, "fuzzrapid.pyx")),
    install_requires=['rapidfuzz']
)


