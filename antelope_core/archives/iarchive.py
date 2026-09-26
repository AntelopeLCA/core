"""
Abstract interface for archive providers.

An archive is the object stored in ``BasicImplementation._archive`` (and all
subclass implementations).  Any object that satisfies this interface can be
used as a provider for the standard implementation classes.

The surface was derived by auditing every ``self._archive.*`` access across
``antelope_core/implementations/*.py``.
"""

from abc import abstractmethod
try:
    from antelope import ArchiveInterface
except ImportError:
    from abc import ABC

    class ArchiveInterface(ABC):
        """
        Minimal contract that a provider object must satisfy to work with the
        standard ``*Implementation`` classes in ``antelope_core``.

        Read-only requirements are marked with ``@property``; methods that may
        mutate the store are marked accordingly in their docstrings.
        """

        # ------------------------------------------------------------------
        # Identity / metadata
        # ------------------------------------------------------------------

        @property
        @abstractmethod
        def ref(self) -> str:
            """Semantic reference string used as the archive's origin identifier."""

        @property
        @abstractmethod
        def source(self) -> str:
            """Physical data source (file path, URL, etc.) — used for display."""

        # ------------------------------------------------------------------
        # Entity access
        # ------------------------------------------------------------------

        @abstractmethod
        def __getitem__(self, key):
            """
            Return the already-loaded entity for *key*, or ``None`` if not present.
            Must never raise on a cache miss — return ``None`` instead.
            """

        @abstractmethod
        def _fetch(self, key, **kwargs):
            """
            Obtain the entity for *key* in a provider-specific way.  Raise ``EntityNotFound`` on failure
            """

        # ------------------------------------------------------------------
        # Interface factory
        # ------------------------------------------------------------------

        @abstractmethod
        def make_interface(self, itype: str):
            """
            Return an implementation object for the named interface type string
            (``'basic'``, ``'index'``, ``'exchange'``, ``'quantity'``,
            ``'background'``, ``'configure'``).
            """


class AntelopeArchive(ArchiveInterface):
    """
    Minimal contract that a provider object must satisfy to work with the
    standard ``*Implementation`` classes in ``antelope_core``.

    Read-only requirements are marked with ``@property``; methods that may
    mutate the store are marked accordingly in their docstrings.
    """

    # ------------------------------------------------------------------
    # Identity / metadata
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def static(self) -> bool:
        """
        True if the full contents of the archive are already loaded into
        memory.  When True, ``BasicImplementation._fetch`` uses ``__getitem__``
        directly instead of calling ``retrieve_or_fetch_entity``.
        """

    # ------------------------------------------------------------------
    # Term manager
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def tm(self):
        """
        Return the archive's ``TermManager`` (or compatible object).

        Must satisfy :class:`TermManagerInterface`.
        """

    # ------------------------------------------------------------------
    # Entity access
    # ------------------------------------------------------------------
    @abstractmethod
    def retrieve_or_fetch_entity(self, key, **kwargs):
        """
        Return the entity for *key*.  If not cached, load it lazily via the
        provider's internal ``_fetch`` mechanism.  Raises ``KeyError`` (or a
        subclass) when the entity cannot be found.
        """

    # ------------------------------------------------------------------
    # Bulk access (used by Index and Background implementations)
    # ------------------------------------------------------------------

    @abstractmethod
    def entities_by_type(self, entity_type: str):
        """
        Generate all loaded entities of the given type string
        (``'process'``, ``'flow'``, ``'quantity'``).
        """

    @abstractmethod
    def count_by_type(self, entity_type: str) -> int:
        """Return the count of loaded entities of the given type."""


'''
    @abstractmethod
    def search(self, entity_type: str, **kwargs):
        """
        Generate entities of *entity_type* matching the supplied keyword
        filters.  Keyword semantics are implementation-defined (typically
        substring / regex matches on entity name fields).
        """

    # ------------------------------------------------------------------
    # Mutation (required by QuantityImplementation only)
    # ------------------------------------------------------------------

    def add(self, entity):
        """
        Add a single entity to the store.

        Required only when the archive must support
        ``QuantityImplementation.get_canonical`` (which may register foreign
        quantity refs).  Raise ``NotImplementedError`` for strictly read-only
        providers.
        """
        raise NotImplementedError

    def add_entity_and_children(self, entity):
        """
        Add an entity and all dependent sub-entities (e.g. a quantity and its
        unit group).

        Required only for ``QuantityImplementation.characterize``.
        Raise ``NotImplementedError`` for read-only providers.
        """
        raise NotImplementedError
'''
