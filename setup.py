from setuptools import setup, find_packages
from antelope_interface import ANTELOPE_VERSION as VERSION

requires = [
    "antelope_interface",
    "xlrd",
    "six",
    "lxml",
    "python-magic"
]

# optional: pylzma
"""
Version History
0.1.2 - 2020/12/28 - PyPI installation

0.1.1 - 2020/11/12 - Bug fixes all over the place.  
                     Catalogs implemented
                     LCIA computation + flat LCIA computation reworked
                     Improvements for OpenLCA LCIA methods

0.1.0 - 2020/07/31 - Initial release - JIE paper
"""


setup(
    name="antelope_core",
    version=VERSION,
    author="Brandon Kuczenski",
    author_email="bkuczenski@ucsb.edu",
    license=open('LICENSE').read(),
    install_requires=requires,
    url="https://github.com/AntelopeLCA/core",
    summary="A reference implementation of the Antelope interface for accessing a variety of LCA data sources",
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering"
    ],
    python_requires='>=3.6',
    packages=find_packages()
)
