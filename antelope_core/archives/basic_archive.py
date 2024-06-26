import os
import re
from collections import defaultdict
from .entity_store import EntityStore, SourceAlreadyKnown, EntityExists
from .term_manager import TermManager

from ..implementations import BasicImplementation, IndexImplementation, QuantityImplementation, ConfigureImplementation
from antelope import BasicQuery, EntityNotFound, FlowInterface
from ..entities import LcQuantity, LcUnit, LcFlow
from ..characterizations import DuplicateCharacterizationError

from ..from_json import from_json, to_json



class OldJson(Exception):
    pass


class ContextCollision(Exception):
    pass


class InterfaceError(Exception):
    pass


class ArchiveError(Exception):
    pass


LD_CONTEXT = 'https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld'

BASIC_ENTITY_TYPES = ('quantity', 'flow')


'''
LcArchive Stored Configuration.

add these objects with archive.add_config()
applied in sequence with archive.apply_config()
'''


class BasicArchive(EntityStore):
    """
    Adds on basic functionality to the archive interface: add new entities; deserialize entities.

    The Basic Archive also introduces the Term Manager, which is used to handle flow characterization information.

    Each archive must have a term manager; however, the same term manager can be shared among several archives.  If
    no TermManager is provided at instantiation as an input argument, then the archive will create a captive term
    manager that only includes entities from that archive.

    ALternatively, a term manager can be created first and supplied as an argument to each created archive, which would
    then allow them to share their terms in common with one another.

    The BasicArchive should be used for all archives that only contain flows and quantities (and contexts in the future)

    """
    _entity_types = BASIC_ENTITY_TYPES

    _drop_fields = defaultdict(list)  # dict mapping entity type to fields that should be omitted from serialization

    _skip_msg = set()
    _query = None

    @classmethod
    def from_file(cls, filename, ref=None, **init_args):
        """
        BasicArchive factory from minimal dictionary.  Must include at least one of 'dataSource' or 'dataReference'
        fields and 0 or more flows or quantities; but note that any flow present must have its reference
        quantities included. The method is inherited by LcArchives which permit processes as well; any process must
        have its exchanged flows (and their respective quantities) included.
        :param filename: The name of the file to be loaded
        :param ref: fallback reference to use if none is specified in source file
        :return:
        """
        j = from_json(filename)
        init_args.update(j.pop('initArgs', {}))

        old_ref = j.pop('dataReference', ref)
        ref = init_args.pop('dataReference', old_ref)
        ns_uuid = j.pop('nsUuid', j.pop('ns_uuid', None))  # this is for opening legacy files
        if ns_uuid is None:
            ns_uuid = init_args.pop('ns_uuid', None)
        ar = cls(filename, ref=ref, ns_uuid=ns_uuid, **init_args)
        ar.load_from_dict(j, jsonfile=filename)
        return ar

    @classmethod
    def from_already_open_file(cls, j, filename, ref=None, **kwargs):
        """
        This is an in-between function that should probably be refactored away / folded into archive_from_json (which
        is the only place it's used)
        :param j:
        :param filename:
        :param ref: incoming ref from catalog
        :param kwargs:
        :return:
        """
        init_args = j.pop('initArgs', {})
        ns_uuid = j.pop('nsUuid', j.pop('ns_uuid', None))  # this is for opening legacy files
        if ns_uuid is None:
            ns_uuid = init_args.pop('ns_uuid', None)
        kwargs.update(init_args)

        old_ref = j.pop('dataReference', ref)
        existing_ref = kwargs.pop('dataReference', old_ref)  # this will be the latest of init[dataRef], [dataRef], ref

        if filename is None:
            source = j.pop('dataSource')
        else:
            source = filename
        ar = cls(source, ref=existing_ref, ns_uuid=ns_uuid, static=True, **kwargs)
        if ref != ar.ref:
            ar.set_origin(ref)

        ar.load_from_dict(j, jsonfile=filename)

        return ar

    def __init__(self, *args, contexts=None, flowables=None, term_manager=None, **kwargs):
        super(BasicArchive, self).__init__(*args, **kwargs)
        self._tm = term_manager or TermManager(contexts=contexts, flowables=flowables)
        self._set_query()

    def _set_query(self):
        self._query = BasicQuery(self)

    @property
    def query(self):
        return self._query

    @property
    def tm(self):
        return self._tm

    '''
    def _check_key_unused(self, key):
        """
        If the key is unused, return the UUID. Else raise EntityExists
        :param key:
        :return:
        """
        u = self._ref_to_uuid(key)
        try:
            e = self._get_entity(u)
        except KeyError:
            return u
        raise EntityExists(str(e))

    def new_quantity(self, name, ref_unit, **kwargs):
        u = self._check_key_unused(name)
        q = LcQuantity(u, ref_unit=LcUnit(ref_unit), Name=name, origin=self.ref, external_ref=name, **kwargs)
        self.add(q)
        return q

    def new_flow(self, name, ref_qty, CasNumber='', **kwargs):
        u = self._check_key_unused(name)
        f = LcFlow(u, Name=name, ReferenceQuantity=ref_qty, CasNumber=CasNumber, origin=self.ref, external_ref=name,
                   **kwargs)
        self.add_entity_and_children(f)
        return f
    '''

    def make_interface(self, iface):
        if iface == 'basic':
            return BasicImplementation(self)
        elif iface == 'quantity':
            return QuantityImplementation(self)
        elif iface == 'index':
            return IndexImplementation(self)
        elif iface == 'configure':
            return ConfigureImplementation(self)
        elif iface == 'inventory':
            return ValueError('inventory is no longer a valid interface name')
        else:
            raise InterfaceError('Unable to create interface %s' % iface)

    def _ensure_valid_refs(self, entity):
        if self.tm.is_context(entity.external_ref):
            raise ContextCollision('Entity external_ref %s is already known as a context identifier' %
                                   entity.external_ref)
        super(BasicArchive, self)._ensure_valid_refs(entity)

    def add(self, entity):
        self._add(entity, entity.external_ref)
        self._add_to_tm(entity)

    def _add_to_tm(self, entity, merge_strategy=None):
        if entity.entity_type == 'quantity':
            self.tm.add_quantity(entity)
            if entity.is_entity and not entity.configured:  # not ref
                try:
                    entity.set_qi(self.make_interface('quantity'))
                except InterfaceError:
                    pass  # quantities in index archives will not be able to use the quantity impl
        elif isinstance(entity, FlowInterface):
            # characterization infrastructure
            self.tm.add_flow(entity, merge_strategy=merge_strategy)

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)
        (note that _get_entity does not get contexts)

        :param item:
        :return:
        """
        # cx = self.tm.__getitem__(item)
        # if cx is None:
        if hasattr(item, 'external_ref'):
            item = item.external_ref
        return super(BasicArchive, self).__getitem__(item)
        # return cx

    def _add_children(self, entity):
        if entity.entity_type == 'quantity' and entity.is_entity:
            # reset unit strings- units are such a hack
            if isinstance(entity.reference_entity, LcUnit):
                entity.reference_entity._external_ref = entity.reference_entity.unitstring
        elif entity.entity_type == 'flow':
            self.add_entity_and_children(entity.reference_entity)

    def add_entity_and_children(self, entity):
        try:
            self.add(entity)
        except EntityExists:
            return
        self._add_children(entity)

    def _create_unit(self, unitstring):
        """
        This returns two things: an LcUnit having the given unit string, and a dict of conversion factors
        (or None if the class doesn't support it).  The dict should have unit strings as keys, and the values should
        have the property that each key-value pair has the same real magnitude.  In other words, the [numeric] values
        should report the number of [keys] that is equal to the reference unit.  e.g. for a reference unit of 'kg',
        the UnitConversion entry for 'lb' should have the value 2.2046... because 2.2046 lb = 1 kg

        In many cases, this will require the supplied conversion value to be inverted.

        The conversion dict should be stored in the Quantity's UnitConversion property.  See IlcdArchive for an
        example implementation.
        :param unitstring:
        :return:
        """
        return LcUnit(unitstring), None

    def _quantity_from_json(self, entity_j, ext_ref):
        # can't move this to entity because we need _create_unit- so we wouldn't gain anything
        unit, _ = self._create_unit(entity_j.pop('referenceUnit'))
        entity_j['referenceUnit'] = unit
        quantity = LcQuantity(ext_ref, **entity_j)
        return quantity

    def _flow_from_json(self, entity_j, ext_ref):
        chars = entity_j.pop('characterizations', [])
        if 'referenceQuantity' in entity_j:
            rq = entity_j.pop('referenceQuantity')
        else:
            try:
                rq = next(c['quantity'] for c in chars if 'isReference' in c and c['isReference'] is True)
            except StopIteration:
                rq = None
        if rq is None:
            ref_q = None
        else:
            try:
                ref_q = self.tm.get_canonical(rq)
            except EntityNotFound:
                print('canonical reference quantity %s not found!' % rq)
                ref_q = None
        return LcFlow(ext_ref, referenceQuantity=ref_q, **entity_j)

    def _add_char(self, flow, q, v):
        self.tm.add_characterization(flow.name, flow.reference_entity, q, v, context=flow.context,
                                     origin=flow.origin)

    def _add_chars(self, flow, chars):
        for c in chars:
            if 'isReference' in c:
                if c['isReference'] is True:
                    continue
            v = None
            try:
                q = self.tm.get_canonical(c['quantity'])  # this is required because of foreground; _process_from_json unaffected
            except EntityNotFound:
                if c['quantity'] not in self._skip_msg:
                    print('skipping unrecognized quantity %s' % c['quantity'])
                    self._skip_msg.add(c['quantity'])
                continue
            if q is None:
                continue
                # import json
                # import sys
                # print(ext_ref)
                # json.dump(c, sys.stdout, indent=2)
                # raise KeyError
            if 'value' in c:
                v = c['value']
            try:
                self._add_char(flow, q, v)
            except DuplicateCharacterizationError:
                print('Skipping duplicate CF %3.6g %s %s' % (v, q, flow))

    def _make_entity(self, e, etype, ext_ref):
        if etype == 'quantity':
            entity = self._quantity_from_json(e, ext_ref)
        elif etype == 'flow':
            entity = self._flow_from_json(e, ext_ref)
        else:
            raise TypeError('Unknown entity type %s' % etype)
        return entity

    def entity_from_json(self, e):
        """
        Create an LcEntity subclass from a json-derived dict

        this could use some serious refactoring
        :param e:
        :return:
        """
        if 'tags' in e:
            raise OldJson('This file type is no longer supported.')
        e['entity_uuid'] = e.pop('entityId', None)
        ext_ref = e.pop('externalId', None)
        if ext_ref is None:
            if e['entity_uuid'] is None:
                raise OldJson('Missing both externalId and entityId')
            ext_ref = e['entity_uuid']
        ext_ref = str(ext_ref)
        if ext_ref in self._entities:
            return self[ext_ref]
        etype = e.pop('entityType')
        if etype == 'flow':
            # need to delay adding characterizations until after entity is registered with term manager
            try:
                chars = e['characterizations']
            except KeyError:
                chars = []
        else:
            chars = []
        e['origin'] = e.pop('origin', self.ref)

        entity = self._make_entity(e, etype, ext_ref)

        self.add(entity)
        if etype == 'flow':
            # characterization infrastructure
            self._add_chars(entity, chars)

        return entity

    def load_from_dict(self, j, _check=True, jsonfile=None):
        """
        Archives loaded from JSON files are considered static.
        :param j:
        :param _check: whether to run check_counter to print out statistics at the end
        :param jsonfile: [None] if present, add to the list of sources for the canonical ref
        :return:
        """

        if 'catalogNames' in j:
            for ref, l in j['catalogNames'].items():
                if isinstance(l, str) or l is None:
                    self._add_name(ref, l)
                else:
                    for s in l:
                        self._add_name(ref, s)

        if 'dataReference' in j:
            # new style
            source = j['dataSource']
            ref = j['dataReference']
            try:
                self._add_name(ref, source)
            except SourceAlreadyKnown:
                self._add_name(ref, None)

        if jsonfile is not None:
            self._add_name(self.ref, jsonfile, rewrite=True)

        q_map = dict()
        if 'quantities' in j:
            for e in j['quantities']:
                e['entityType'] = 'quantity'
                q = self.entity_from_json(e)
                if q.uuid is not None:
                    q_map[q.uuid] = q
                q_map[q.external_ref] = q
        if 'termManager' in j:
            self.tm.add_from_json(j['termManager'], q_map, self.ref)

        if 'flows' in j:
            for e in j['flows']:
                e['entityType'] = 'flow'
                self.entity_from_json(e)

        if 'loaded' in j:
            self._loaded = j['loaded']

        if _check:
            self.check_counter()

    def _load_all(self, **kwargs):
        if self.source is None:
            return
        if os.path.exists(self.source):
            self.load_from_dict(from_json(self.source))

    @staticmethod
    def _narrow_search(entity, **kwargs):
        """
        Narrows a result set using sequential keyword filtering
        :param entity:
        :param kwargs:
        :return: bool
        """
        def _recurse_expand_subtag(tag):
            if tag is None:
                return ''
            elif isinstance(tag, str):
                return tag
            else:
                return '\n'.join([_recurse_expand_subtag(t) for t in tag])
        keep = True
        for k, v in kwargs.items():
            if v is None:
                continue
            if not entity.has_property(k):
                return False
            if isinstance(v, str):
                v = [v]
            for vv in v:
                keep = keep and bool(re.search(vv, _recurse_expand_subtag(entity[k]),
                                               flags=(re.IGNORECASE|re.MULTILINE)))
        return keep

    def search(self, etype=None, count=None, offset=None, **kwargs):
        """
        Implement pagination
        :param etype:
        :param count:
        :param offset:
        :param kwargs:
        :return:
        """
        if offset:
            offset = int(offset)
        else:
            offset = 0
        if count is None:
            count = len(self._entities)  # just need something that is guaranteed not to run out
        for k in self._search(etype, **kwargs):
            if offset:
                offset -= 1
                continue
            yield k
            count -= 1
            if count == 0:
                break

    def _search(self, etype=None, **kwargs):
        """
        Find entities by search term, either full or partial uuid or entity property like 'Name', 'CasNumber',
        or so on.
        :param etype: optional first argument is entity type
        :param kwargs: regex search through entities' properties as named in the kw arguments
        :return: result set
        """
        if etype is None:
            if 'entity_type' in kwargs.keys():
                etype = kwargs.pop('entity_type')
        if etype is not None:
            for ent in self.entities_by_type(etype):
                if self._narrow_search(ent, **kwargs):
                    yield ent
        else:
            for ent in self._entities.values():
                if self._narrow_search(ent, **kwargs):
                    yield ent

    def _serialize_quantities(self, domesticate=False):
        return sorted([q.serialize(domesticate=domesticate, drop_fields=self._drop_fields['quantity'])
                       for q in self.entities_by_type('quantity')],
                      key=lambda x: x['externalId'])

    def serialize(self, characterizations=False, values=False, domesticate=False):
        """

        :param characterizations:
        :param values:
        :param domesticate: [False] if True, omit entities' origins so that they will appear to be from the new archive
         upon serialization
        :return:
        """
        j = super(BasicArchive, self).serialize()
        j['@context'] = LD_CONTEXT

        j['flows'] = sorted([f.serialize(domesticate=domesticate, drop_fields=self._drop_fields['flow'])
                             for f in self.entities_by_type('flow')],
                            key=lambda x: x['externalId'])
        if characterizations:
            j['termManager'], qqs, rqs = self.tm.serialize(self.ref, values=values)
            for q in qqs:
                if self[q] is None:
                    self.add(self.tm.get_canonical(q))

        j['quantities'] = self._serialize_quantities(domesticate=domesticate)

        return j

    def export_quantities(self, filename, *quantities, domesticate=True, values=True, gzip=False):
        """

        :param filename:
        :param quantities: positional args are quantities to serialize
        :param domesticate: [True- new archive becomes reference]  if False, the qty source origin will remain canonical
        :param values: [True- new archive stores cfs] if False the export will only include flowable+context lists
        :param gzip: [False] whether to gzip the file
        :return:
        """
        j = super(BasicArchive, self).serialize()
        j['dataSourceType'] = 'BasicArchive'
        j['dataSource'] = filename
        j['termManager'], qq, rq = self.tm.serialize(self.ref, *quantities, values=values)
        qs = [self[u] for u in rq]
        for ref in qq:
            q = self[ref]
            if q not in qs:
                qs.append(q)

        j['quantities'] = sorted([q.serialize(domesticate=domesticate)
                                  for q in qs],
                                 key=lambda x: x['externalId'])
        to_json(j, filename, gzip=gzip)

    def _serialize_all(self, **kwargs):
        return self.serialize(characterizations=True, values=True, **kwargs)
