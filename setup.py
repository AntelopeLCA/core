from setuptools import setup, find_packages

requires = [
    "antelope",
    "xlrd",
    "six",
    "lxml",
    "python-magic"
]

# optional: pylzma
"""
Version History
0.1.1 - 2020/11/12 - Bug fixes all over the place.  
                     Catalogs implemented
                     LCIA computation + flat LCIA computation reworked
                     Improvements for OpenLCA LCIA methods

0.1.0 - 2020/07/31 - Initial release - JIE paper
"""


VERSION = '0.1.1'

setup(
    name="antelope_core",
    version=VERSION,
    author="Brandon Kuczenski",
    author_email="bkuczenski@ucsb.edu",
    license=open('LICENSE').read(),
    install_requires=requires,
    url="https://github.com/AntelopeLCA/core",
    summary="A reference implementation of the Antelope interface for accessing a variety of LCA data sources",
    long_description=open('README.md').read(),
    packages=find_packages()
)
