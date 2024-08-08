from antelope import EntityNotFound, NoAccessToEntity, NullEntity, ItemNotFound
from ..contexts import NullContext


class BasicImplementation(object):
    def __init__(self, archive, **kwargs):
        """
        Provides common features for an interface implementation: namely, an archive and a privacy setting. Also
        provides access to certain common methods of the archive.  This should be the base class for interface-specific
        implementations.

        Requires an archive with the following attributes:
         - ref - report semantic reference
         - source - report physical data source
         - static - a boolean indicating whether the full contents of the archive are loaded into memory
         - get_uuid() - deprecated - only present for compatibility reasons
         - __getitem__ - retrieve already-loaded entity
         - retrieve_or_fetch_entity() - _fetch abstract method must be implemented

        All of these requirements are met by the standard ArchiveImplementation, with the exception of the _fetch
        abstract method.

        Since a recent change that removed the 'basic' interface as a default for all resources, this must be
        explicitly assigned to at least one resource in order for a query to be valid.  The basic interface should
        be assigned to the resource that meets the following requirements:
         - most comprehensive source of information about entity properties (e.g. documentary pseudo-interface)
         - easiest to load (e.g. a non-static)

        :param archive: an LcArchive
        :param privacy: No longer used. Privacy is enforced at the server and not the resource (where it was phony
        from the beginning)
        """
        self._archive = archive

    def validate(self):
        """
        way to check that a query implementation is valid without querying anything
        :return:
        """
        return self.origin

    def is_me(self, archive):
        return self._archive is archive

    @property
    def origin(self):
        return self._archive.ref

    def __str__(self):
        return '%s for %s (%s)' % (self.__class__.__name__, self.origin, self._archive.source)

    def __getitem__(self, item):
        return self._archive[item]

    def _dereference_entity(self, external_ref):
        """
        returns a real local entity (non ref) for which is_entity is True
        :param external_ref:
        :return:
        """
        if hasattr(external_ref, 'external_ref'):
            eref = external_ref
            external_ref = eref.external_ref
        else:
            eref = None
        entity = self.get(external_ref)
        # if entity:
        if not entity.is_entity:
            raise NoAccessToEntity(self.origin, entity.link)
        return entity

    def get_item(self, external_ref, item):
        """
        In this, we accept either an external_ref or an entity reference itself.  If the latter, we dereference via
        the archive to an actual entity, which we then ask for the item.  If the dereference and the reference are the
        same, throws an error.
        :param external_ref:
        :param item:
        :return:
        """
        entity = self._dereference_entity(external_ref)
        if entity.has_property(item):
            obj = entity[item]
            if obj is None:
                raise ItemNotFound
            return obj
        raise ItemNotFound('%s: %s [%s]' % (self.origin, external_ref, item))
        # raise EntityNotFound(external_ref)

    def get_reference(self, key):
        entity = self._dereference_entity(key)
        if entity is None:
            return None
        if entity.entity_type == 'process':
            # need to get actual references with exchange values-- not the reference_entity
            return [x for x in entity.references()]
        if entity.reference_entity is None:
            return NullEntity(self.origin)
        return entity.reference_entity

    def get_uuid(self, external_ref):
        u = self._fetch(external_ref)
        if u is None:
            return False
        if u.uuid is None:
            return False
        return u.uuid

    def _fetch(self, external_ref, **kwargs):
        if external_ref is None:
            return None
        if self._archive.static:
            return self._archive[external_ref]
        try:
            return self._archive.retrieve_or_fetch_entity(external_ref, **kwargs)
        except (KeyError, NotImplementedError, IndexError):
            return None

    '''
    def lookup(self, external_ref, **kwargs):
        if self._fetch(external_ref, **kwargs) is not None:
            return True
        return False
    '''

    def get(self, external_ref, **kwargs):
        """
        :param external_ref: may also be link, as long as requested origin is equal or lesser in specificity
        :param kwargs:
        :return: entity or None
        """
        if external_ref is None:
            raise EntityNotFound(None)
        e = self._fetch(external_ref, **kwargs)
        if e is not None:
            return e
        if isinstance(external_ref, int):  # we got away with this before by falling back on NSUUIDs
            external_ref = str(external_ref)
        e = self._fetch(external_ref, **kwargs)
        if e is not None:
            return e
        er_s = external_ref.split('/')
        if self.origin.startswith(er_s[0]):
            e = self._fetch('/'.join(er_s[1:]), **kwargs)
            if e is not None:
                return e
        raise EntityNotFound(external_ref)

    def get_context(self, term, **kwargs):
        """
        I think this needs to be moved into the quantity interface
        :param term:
        :param kwargs:
        :return:
        """
        cx = self._archive.tm[term]
        if cx is None:
            return NullContext
        if cx.fullname == cx.name:
            cx.add_origin(self.origin)
        return cx

    def is_lcia_engine(self, **kwargs):
        """
        suggests expansion to a graph-based TM
        :param kwargs:
        :return:
        """
        if hasattr(self._archive, 'tm'):
            return self._archive.tm.is_lcia_engine
        return False

    def synonyms(self, item, **kwargs):
        if hasattr(self._archive, 'tm'):
            return self._archive.tm.synonyms(item)
        # yield from ()

    def properties(self, external_ref, **kwargs):
        e = self._dereference_entity(external_ref)
        for i in e.properties():
            yield i

    def bg_lcia(self, process, query_qty, ref_flow=None, *kwargs):
        """
        This needs to be handled by a query with lci() access, or by a subclass
        :param process:
        :param query_qty:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        raise NotImplementedError
