from setuptools import setup, find_packages

requires = [
    "antelope-interface",
    "xlrd",
    "six",
    "python-magic"
]

# optional: pylzma
"""
Version History
0.1.3 - 2020/12/30 - Move lxml into optional requirements.
0.1.3rc2           - include data files
0.1.3rc1           - update requirements

0.1.2b - 2020/12/29 - fix some minor items
0.1.2 - 2020/12/28 - PyPI installation; includes significant performance enhancements for LCIA 

0.1.1 - 2020/11/12 - Bug fixes all over the place.  
                     Catalogs implemented
                     LCIA computation + flat LCIA computation reworked
                     Improvements for OpenLCA LCIA methods

0.1.0 - 2020/07/31 - Initial release - JIE paper
"""

VERSION = '0.1.3rc2'

setup(
    name="antelope_core",
    version=VERSION,
    author="Brandon Kuczenski",
    author_email="bkuczenski@ucsb.edu",
    license="BSD 3-Clause",
    install_requires=requires,
    extras_require={
        'XML': ['lxml>=1.2.0']
    },
    include_package_data=True,
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
