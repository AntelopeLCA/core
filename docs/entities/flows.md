## LcFlow

!!! tip "The Flow Interface"

    The `LcFlow` class inherits the [Flow]() interface defined in `antelope_interface`.  It includes 
    a name, a set of synonyms, and a context, defined as a tuple of hierarchical terms.  It also 
    includes a `lookup_cf` function where it performs the quantity relation on itself and stores
    the results in a cache.  

::: antelope_core.entities.flows.LcFlow
    handler: python
