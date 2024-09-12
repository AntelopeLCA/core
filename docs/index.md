# Antelope core implementation

This package includes the reference version of Antelope life cycle assessment (LCA) software.  
Antelope simplifies the practice of LCA by creating different
[interfaces](https://antelopelca.github.io/antelope/interfaces.html) that handle different
parts of the computational problem.

## Conceptual Overview

The [antelope_interface](https://github.com/AntelopeLCA/antelope) declares a set of interfaces
and creates a set of `EntityRef` catalog-reference classes for referring to LCA data objects.
It is up to an *implementation* (i.e. this repo) to create working code for those objects.

The present package provides a reference implementation for quantities, flows, and processes, which
are the essential data types or "entities" for LCA computation.  It includes a set of providers, 
which translate different data sources (ecospold and ecospold2, ILCD, OpenLCA) into antelope entities.
Each provider class may include custom implementations of the different interfaces.

implements the [quantity] interface for performing LCIA operations.  It
also creates an [LciaResult](./api/lcia_results.md) object that provides sophisticated capabilities 
for reviewing and analyzing LCIA computations.  The core package also implements the [exchange]
interface, which is 

The [antelope_background](https://github.com/AntelopeLCA/background) package performs [Tarjan 
Ordering](https://link.springer.com/article/10.1007/s11367-015-0972-x) of data sources

The [antelope_foreground](https://github.com/AntelopeLCA/foreground) package provides the 
ability to construct dynamic, modular life cycle models that make reference to datasets from different
origins.

Other implementations can be created independently as long as they inherit from the interface.
