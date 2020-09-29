from setuptools import setup

requires = [
    "antelope"
]

VERSION = '0.1.0'

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
    packages=['antelope_core']
)
