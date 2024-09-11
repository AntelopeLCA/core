from setuptools import setup, find_packages

VERSION = '0.3.3'

requires = [
    "synonym_dict>=0.2.4",
    "antelope_interface>=0.2.7",
    "xlstools>=0.1.3",
    "python-magic>=0.4.18",
    "requests>=2.25",
    "pydantic>=2.5.0"
]

# optional: pylzma
"""
Version History
0.3.3   2024-08-10 - consumers implementation
                     sorting direction on LciaResults
                     clean up unobserved_lci in preparation for folding it into lci()
                     catch some errors
                     some new unit conversions
                     
0.3.2.3 2024-08-15 - lots of remote traversal compatibility changes
                     StringEntities in LciaResults
                     repair biogenic CO2 switching
                     ecoinvent 3.10 flowables
                     assign_ref for blackbook resources
                     zillions of tiny, costly bugs

0.3.2.2 2024-06-11 - get_context into basic interface
0.3.2   2024-06-06 - create ItemNotFound - bump interface version

0.3.1   2024-04-18 - update antelope-interface to 0.2.4

0.3.0   2024-01-05 - 0.3-branch development version, supporting end-user access to vault.lc resources 

# ^ 0.3.* 0.3-branch fork   
# v 0.2.* main / master for legacy projects

0.2.5   2024-05-06 - move bg_lcia() into basic interface; sys_lcia() as fully-featured background analogue

0.2.4.1 2024-04-24 - implement pagination in BasicArchive search
                     fix ecospold v1 to handle process-flow collisions
                     add_interfaces in LcResource
                     LcCatalog.add_resource(replace=True)

0.2.4   2024-04-17 - Remove antelope.ExteriorFlow in favor of antelope.models.ExteriorFlow

0.2.3   2024-03-21 - compatibility release for antelope_interface upgrades and terminology changes
                     OpenLCA v2 schema handled
                     lots of cloud work 

0.2.1   2023-04-10 - xdb passes benchmarks
                     pydantic models moved into interface
                     sys_lci and bg_lcia operational, both locally and remotely

0.2.0   2023-04-07 - "release" virtualize branch. 
                   - Add pydantic models for everything
                   - add an XDB client implementation for remote operation
                   - job-related changes to entity handling throughout

0.1.8   2022-04-08 - PyPI release to roll up a few small changes:
 * upgrade ecoinvent_lcia to use xlstools; port 3.8
 - pull out parse_math
 - windows path mgmt
 - background trivial impl needs to access entities and not refs
 - LciaResults can sum together if at least one has a unit node weight
 - cleanup reference quantities; finally add node activity as canonical quantity
 - quantity unit mismatch refactor


0.1.7.3 2021-08-11 - fix last edit
0.1.7.2 2021-08-11 - make lxml optional when running _find_providers
0.1.7.1 2021-08-11 - eliminate recursion error in basic get_reference()
0.1.7 - 2021-08-05 - merge configuration changes from [virtualize] in-progress
0.1.6 - 2021-03-10 - update to handle new synonym_dict 0.2.0, along with OLCA reference flow matching, ecoinvent 2.2,
                     a range of other improvements in performance and context handling
                     2021-03-10 post1
                     2021-03-17 post2 - get_context(); bump antelope version
                     
0.1.5 - 2021-02-05 - Updates to NullContext handling, flow term matching, fixed faulty requirements (add requests) 
0.1.4 - 2021/01/29 - background passing
0.1.3 - 2021/01/29 - Build passing (without bg tested)
0.1.3rc4           - fix last edit
0.1.3rc3           - xlrd removes support for xlsx in 2.0; _localize_file(None) return None
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

setup(
    name="antelope_core",
    version=VERSION,
    author="Brandon Kuczenski",
    author_email="bkuczenski@ucsb.edu",
    license="BSD 3-Clause",
    install_requires=requires,
    extras_require={
        'XML': ['lxml>=1.2.0'],
        'write_to_excel': ['xlsxwriter>=1.3.7']
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
