"""
GreenDelta has updated OpenLCA JSON-LD to version 2 with a number of changes, tersely documented here:
https://greendelta.github.io/olca-schema/CHANGES.html

There are changes throughout, and this forces us to decide whether to kludge our code or branch it.

Data ETL is the forever bane of the LCA lone wolf.

For simple naming changes, we can just introduce a constant mapping, detect whether it's version 1 or version 2,
and use the correct one.

Whether there's a better way to do this is something we can think intensely about as we dive into deep interoperability
with OpenLCA.

The most immediate material change is that Categories are no longer entities with UUIDs but merely
forward-slash-separated strings. This is actually fine because this is much closer to our native approach. But we
do need to rework the plumbing around this.


"""


import json
import os
import re

import logging

from collections import defaultdict

from antelope import ConversionError, NoReference

from ...exchanges import AmbiguousReferenceError

from ...entities import LcQuantity, LcFlow, LcProcess, LcUnit, MetaQuantityUnit, ZeroAllocation
from ...archives import LcArchive
from ..file_store import FileStore
from ..parse_math import parse_math

from .schema_mapping import OLCA_MAPPING
from .olca_ps_interpreter import OlcaProductSystemInterpreter
from .parameters import OlcaParameterResolver


geog_tail = re.compile(',\\s([A-Z]+[o-]?[A-Z]*)$')  # capture, e.g. 'ZA', 'GLO', 'RoW', 'US-CA' but not 'PET-g'


def pull_geog(flowname):
    raise NotImplementedError
    '''
    try:
        return geog_tail.search(flowname).group(1)
    except AttributeError:
        return None
    '''


valid_types = {'processes', 'flows', 'flow_properties'}


class OpenLcaException(Exception):
    pass


class _NotAnRx(Exception):
    pass


SKIP_DURING_INDEX = ('context.json', 'meta.info')


class OpenLcaJsonLdArchive(LcArchive):
    """
    Opens JSON-LD archives formatted according to the OpenLCA schema
    """
    def _cat_as_list(self, cat_id):
        cat = self._cat_index[cat_id]
        if 'category' in cat:
            return self._cat_as_list(cat['category']['@id']) + [cat['name']]
        return [cat['name']]

    @property
    def schema_version(self):
        if self._type_index is None:
            return 0
        if 'openlca.json' in self._type_index:
            return 2
        return 1

    def _get_v_field(self, obj_type, fieldname):
        """
        Retrieves the proper field name given the schema version
        :param obj_type: the object whose field is being requested
        :param fieldname: field name according to the v1 schema
        :return:
        """
        if self.schema_version == 2:
            try:
                return OLCA_MAPPING[obj_type][fieldname]
            except KeyError:
                return fieldname
        else:
            return fieldname

    def _get_location(self, loc_id):
        if loc_id in self._type_index:
            if self._type_index[loc_id] == 'locations':
                return self._create_object('locations', loc_id)
            else:
                raise TypeError('%s is a %s, not a location' % (loc_id, self._type_index[loc_id]))
        else:
            raise KeyError('%s not in index' % loc_id)

    def _gen_index(self):
        self._print('Generating index')
        self._type_index = dict()
        self._lm_index = dict()
        # these are cheap- but we should really not need more than 3
        self._cat_index = dict()  # maps olca id to olca category object
        self._cat_lookup = dict()  # maps tuple of category-as-list to olca id
        self._cat_tuple = dict()  # maps olca id to tuple of category-as-list

        for f in self._archive.listfiles():
            if f in SKIP_DURING_INDEX:
                continue
            ff = f.split('/')
            if len(ff) < 2:
                self._type_index[ff[0]] = 'root'
                continue
            fg = ff[1].split('.')
            self._type_index[fg[0]] = ff[0]
            if ff[0] == 'lcia_methods':
                # we need to do a further indexing to generate a list of LCIA quantities aka categories by method.
                # that means actually loading the files
                obj = self._create_object(ff[0], fg[0])
                for c in obj['impactCategories']:
                    self._lm_index[c['@id']] = fg[0]
            elif ff[0] == 'categories':
                obj = self._create_object(ff[0], fg[0])
                self._cat_index[fg[0]] = obj
        if self.schema_version == 1:
            # old schema: build the category lists manually
            for cat_key in self._cat_index.keys():
                cat = self._cat_as_list(cat_key)
                lookup_key = tuple(cat)
                self._cat_tuple[cat_key] = lookup_key  # forward lookup key -> tuple
                self._cat_lookup[lookup_key] = cat_key  # reverse lookup of tuple -> key
                self.tm.add_context(lookup_key, cat_key)

    def __init__(self, source, prefix=None, skip_index=False, product_system=None, **kwargs):
        super(OpenLcaJsonLdArchive, self).__init__(source, **kwargs)

        self._drop_fields['process'].extend(['processDocumentation', 'OlcaParameterEngine'])

        self._archive = FileStore(source, internal_prefix=prefix)

        self._type_index = None
        self._unit_dict = dict()
        if not skip_index:
            self._gen_index()

        self._defined_ps = None
        if product_system:
            self.select_product_system(product_system)

    def select_product_system(self, product_system):
        if self._loaded:
            raise OpenLcaException('Database is already loaded!')
        if self._type_index[product_system] == 'product_systems':
            psj = self._create_object('product_systems', product_system)
            self._defined_ps = OlcaProductSystemInterpreter(psj)
        else:
            raise TypeError('product system %s not found' % product_system)


    @property
    def defined(self):
        return bool(self._defined_ps)

    def _check_id(self, _id):
        return self[_id] is not None

    def _create_object(self, typ, key):
        return json.loads(self._archive.readfile(os.path.join(typ, key + '.json')))

    def _process_from_json(self, entity_j, uid):
        process = super(OpenLcaJsonLdArchive, self)._process_from_json(entity_j, uid)
        if process.has_property('allocationFactors'):
            # we do not need to replicate 0-valued allocation factors, esp. when there are thousands of them
            process['allocationFactors'] = [k for k in process['allocationFactors'] if k['value'] != 0]
        return process

    def _clean_object(self, typ, key):
        """

        :param typ:
        :param key:
        :return: clean json, name, list of category IDs
        """
        j = self._create_object(typ, key)
        j.pop('@context', None)
        j.pop('@id', None)
        name = j.pop('name')

        if 'category' in j:
            c_j = j.pop('category')
            cat = self._get_category_list(c_j)
        else:
            cat = []
        return j, name, cat

    def _get_category_list(self, category):
        if self.schema_version == 2:
            return category.split('/')
        else:
            return self._recurse_category_list(category['@id'])

    def _recurse_category_list(self, cat_key):
        if cat_key in self._cat_tuple:
            return list(self._cat_tuple[cat_key])
        c_j = self._cat_index[cat_key]
        if 'category' in c_j:
            cat = self._recurse_category_list(c_j['category']['@id'])
        else:
            cat = []
        cat.append(c_j['name'])
        return cat

    def openlca_category(self, name_or_full_list):
        """
        returns an olca id for a name or a list of hierarchical names, if one exists
        :param name_or_full_list:
        :return: either a known context or raise KeyError
        """
        try:
            return self._cat_lookup[tuple(name_or_full_list)]
        except KeyError:
            cx = self.tm[name_or_full_list]
            if cx is None:
                raise KeyError
            return self._cat_lookup[tuple(cx.as_list())]

    @property
    def product_systems(self):
        return {k: self._create_object('product_systems', k).get('name')
                for k, v in self._type_index.items() if v == 'product_systems'}

    @property
    def openlca_categories(self):
        for k in sorted(self._cat_lookup.keys()):
            yield k

    def _create_unit(self, unit_id):
        try:
            u_j = self._create_object('unit_groups', unit_id)
        except FileNotFoundError:
            return LcUnit(unit_id), None
        unitconv = dict()
        unit = None

        for conv in u_j['units']:
            is_ref = conv.pop(self._get_v_field('Unit', 'referenceUnit'), False)
            name = conv.pop('name')
            if conv['@id'] in self._unit_dict:
                if self._unit_dict[conv['@id']] != name:
                    raise ValueError('Unit ID collision! %s: %s X %s' % (conv['@id'], self._unit_dict[conv['@id']], name))
            else:
                self._unit_dict[conv['@id']] = name
            cf_i = conv.pop('conversionFactor')
            unitconv[name] = 1.0 / cf_i

            if is_ref:
                assert cf_i == 1, 'non-unit reference unit found! %s' % unit_id
                unit = LcUnit(name)

        if unit is None:
            raise OpenLcaException('No reference unit found for id %s' % unit_id)

        return unit, unitconv

    def _create_quantity(self, q_id):
        q = self[q_id]
        if q is not None:
            return q

        q_j, name, cat = self._clean_object('flow_properties', q_id)
        ug = q_j.pop('unitGroup')
        unit, unitconv = self._create_unit(ug['@id'])

        q = LcQuantity(q_id, Name=name, ReferenceUnit=unit, UnitConversion=unitconv, Category=cat, **q_j)

        self.add(q)
        return q

    def _create_allocation_quantity(self, process, alloc_type):
        key = '%s_%s' % (process.name, alloc_type)
        name = '%s (%s)' % (alloc_type, process.name.strip())
        u = self._ref_to_nsuuid(key)
        q = self[u]
        if q is not None:
            return q

        unit, _ = self._create_unit('alloc')
        q = LcQuantity(key, Name=name, ReferenceUnit=unit)
        self.add(q)
        assert q.uuid == u
        return q

    def _create_flow(self, f_id):
        q = self[f_id]
        if q is not None:
            return q

        f_j, name, comp = self._clean_object('flows', f_id)
        cas = f_j.pop('cas', '')
        loc = f_j.pop('location', {'name': 'GLO'})['name']

        fps = f_j.pop('flowProperties')

        qs = []
        facs = []
        ref_q = None

        for fp in fps:
            q = self.retrieve_or_fetch_entity(fp['flowProperty']['@id'], typ='flow_properties')
            ref = fp.pop(self._get_v_field('FlowPropertyFactor', 'referenceFlowProperty'), False)
            fac = fp.pop('conversionFactor')
            if ref:
                assert fac == 1.0, 'Non-unit reference flow property found! %s' % f_id
                ref_q = q
            else:
                if q not in qs:
                    qs.append(q)
                    facs.append(fac)
        if ref_q is None:
            raise OpenLcaException('No reference flow property found: %s' % f_id)
        if not comp:
            logging.warning('Flow %s with Null context' % f_id)

        # context gets set by _catch_context()
        f = LcFlow(f_id, Name=name, Compartment=comp, CasNumber=cas, ReferenceQuantity=ref_q, **f_j)

        self.add(f)  # context gets matched inside tm.add_flow().
        '''# NONSPECIFIC entries are automatically prepended with parent name in CompartmentManager.new_entry()'''

        for i, q in enumerate(qs):
            self.tm.add_characterization(f.link, ref_q, q, facs[i], context=f.context, location=loc)

        return f

    def _add_exchange(self, p, ex, value):
        is_ref = ex.pop(self._get_v_field('Exchange', 'quantitativeReference'), False)

        if self.defined and self._defined_ps.check_exchange_id(p.external_ref, ex['internalId']) and not is_ref:
            flow_id = self._defined_ps.get_target_flow(p.external_ref, ex['internalId'])
            target = self._defined_ps.get_target(p.external_ref, ex['internalId'])
            flow = self.retrieve_or_fetch_entity(flow_id, typ='flows')
        else:
            flow = self.retrieve_or_fetch_entity(ex['flow']['@id'], typ='flows')
            target = None

        # value = ex['amount']  # this comes from the parameter engine now
        dirn = 'Input' if ex[self._get_v_field('Exchange', 'input')] else 'Output'

        fp = self.retrieve_or_fetch_entity(ex['flowProperty']['@id'], typ='flow_properties')

        if 'unit' in ex:
            try:
                v_unit = self._unit_dict[ex['unit']['@id']]
            except KeyError:
                logging.warning('%s: %d: bad unit %s for flow property %s! using default' % (p.external_ref,
                                                                                             ex['internalId'],
                                                                                             ex['unit']['name'],
                                                                                             fp.name))
                v_unit = fp.unit
        else:
            v_unit = fp.unit

        if v_unit != fp.unit:
            oldval = value
            value *= fp.convert(from_unit=v_unit)  # ConversionErrors caught in add_process

            self._print('%s: Unit Conversion exch: %g %s to native: %g %s' % (p.uuid, oldval, v_unit, value, fp.unit))

        if fp != flow.reference_entity:
            try:
                value /= fp.cf(flow)  # is this even right?  ### yes  # TODO: account for locale?
            except (TypeError, ZeroDivisionError):
                print('%s:%s:%s flow reference quantity does not match\n%s exchange f.p. Conversion Required' %
                      (p.external_ref, dirn, flow.external_ref, flow.name))
                print('From %s to %g %s' % (flow.unit, value, fp.unit))
                val = parse_math(input('Enter conversion factor 1 %s = x %s [context %s]\nx: ' %
                                       (flow.unit, fp.unit, flow.context)))
                self.tm.add_characterization(flow.link, flow.reference_entity, fp, val, context=flow.context,
                                             origin=self.ref)
                value /= fp.cf(flow)

        if is_ref:
            term = None
        elif target:
            term = target
        else:
            cx = self.tm[flow.context]
            if cx is not None and cx.elementary:
                term = cx
            else:
                if 'defaultProvider' in ex:
                    term = ex['defaultProvider']['@id']
                else:
                    term = cx

        exch = p.add_exchange(flow, dirn, value=value, termination=term, add_dups=True)
        if is_ref:
            p.set_reference(flow, dirn)

        if 'amountFormula' in ex:
            exch.comment = ex['amountFormula'].strip()
            if 'description' in ex:
                logging.warning('%s:%d skipping description in favor of comment' % (p.external_ref, ex['internalId']))
        elif 'description' in ex:
            exch.comment = ex['description']

        return exch

    def _get_rx(self, p, flow_ref):
        """
        returns a reference exchange or candidate reference exchange
        :param p:
        :param flow_ref:
        :return:
        """
        rf = self.retrieve_or_fetch_entity(flow_ref)
        try:
            ft = rf['flowType']
            if ft == 'ELEMENTARY_FLOW':
                logging.info('%s: Skipping allocation factor for elementary rx %s' % (p.external_ref, rf.external_ref))
                raise _NotAnRx
        except KeyError:
            ft = 'PRODUCT_FLOW'  # more common ??
        try:
            rx = p.reference(rf)
        except NoReference:
            # implicit trickery with schema: reference flows MUST be outputs for products, inputs for wastes
            dr = {'PRODUCT_FLOW': 'Output',
                  'WASTE_FLOW': 'Input'}[ft]
            rx_cands = list(_x for _x in p.exchange_values(rf, direction=dr) if _x.type in ('context', 'cutoff'))
            if len(rx_cands) == 0:
                logging.error('%s: Unable to find allocatable exchange for %s' % (p.external_ref, rf.external_ref))
                raise _NotAnRx
            elif len(rx_cands) > 1:
                raise AmbiguousReferenceError('%s: Multiple flows with ID %s' % (p.external_ref, rf.external_ref))
            else:
                rx = rx_cands[0]
            # p.set_reference(rx.flow, rx.direction)

        return rx

    def _apply_olca_allocation(self, p, alloc=None):
        """
        For each allocation factor, we want to characterize the flow so that its exchange value times its
        characterization equals the stated factor.  Then we want to allocate the process by its default allocation
        property.

        A few notes: the allocation quantity is PROCESS-SPECIFIC (strictly, it's process.name-specific, so that is
        either a feature or a bug, TBD).  Because of this, NO allocation will be achievable UNLESS the process has
        alloationFactors defined.

        This whole thing needs to be sorted out with testing, probably in collaboration with GreenDelta.
        :param p: an LcProcess generated from the JSON-LD archive
        :return:
        """
        if alloc is None or len(alloc) == 0:
            return
        if p.has_property('defaultAllocationMethod'):
            dm = p['defaultAllocationMethod']
        else:
            dm = 'NO_ALLOCATION'
        _causal_msg = True
        stored_alloc = []
        for af in alloc:
            try:
                rx = self._get_rx(p, af['product']['@id'])
            except _NotAnRx:
                continue
            if rx.value == 0:
                continue  # can't allocate to a non-flow
            if af['allocationType'] == 'CAUSAL_ALLOCATION':
                if af['value'] == 0:
                    # Keep 0-allocation factors for non-causal
                    continue

                if dm != 'CAUSAL_ALLOCATION':
                    if _causal_msg:
                        logging.info('%s: Skipping Speculative CAUSAL_ALLOCATION' % p.external_ref)
                        _causal_msg = False
                    continue
                f = self.retrieve_or_fetch_entity(af['exchange']['flow']['@id'])

                xs = list(p.exchange_values(f))
                if len(xs) > 1:
                    raise AmbiguousReferenceError('%s: Multiple flows with ID %s' % (p.external_ref, f.external_ref))
                x = xs[0]

                val = af['value']
                stored_alloc.append(af)
                logging.warning('%s: Setting reference %s from CAUSAL alloc factor' % (p.uuid, rx.flow.uuid))
                p.set_reference(rx.flow, rx.direction)

                x[rx] = x.value * val
                if _causal_msg:
                    logging.warning('%s: Warning: causal allocation has not been tested' % p.external_ref)
                    _causal_msg = False
            else:
                q = self._create_allocation_quantity(p, af['allocationType'])

                v = af['value'] / rx.value
                stored_alloc.append(af)

                if not rx.is_reference:
                    logging.info('%s: Setting reference %s from allocation spec' % (p.uuid, rx.flow.uuid))
                    p.set_reference(rx.flow, rx.direction)

                self.tm.add_characterization(rx.flow.link, rx.flow.reference_entity, q, v,
                                             context=rx.flow.context, origin=self.ref)
                # f.add_characterization(q, value=v)

        if dm != 'NO_ALLOCATION':
            aq = self._create_allocation_quantity(p, p['defaultAllocationMethod'])
            try:
                p.allocate_by_quantity(aq)
            except ZeroAllocation:
                logging.warning('Process %s: Zero allocation found -- not allocating' % p.external_ref)

        p['allocationFactors'] = stored_alloc  # only keep factors we used

    def _create_process(self, p_id):
        q = self[p_id]
        if q is not None:
            return q

        p_j, name, cls = self._clean_object('processes', p_id)

        param_engine = OlcaParameterResolver(p_j, v2=self.schema_version == 2, process_ref=p_id)
        if self.defined:
            try:
                pv = self._defined_ps.get_param_values(p_id)
                param_engine.set_values(**pv)
            except KeyError:
                pass

        loc = p_j.pop('location', {'name': 'GLO'})
        try:
            ss = loc['name']
        except KeyError:
            ss = self._get_location(loc['@id'])['name']

        stt = dict()
        for key, tgt in (('validFrom', 'begin'), ('validUntil', 'end')):
            try:
                stt[tgt] = p_j['processDocumentation'][key]
            except KeyError:
                pass

        exch = p_j.pop('exchanges')

        alloc = p_j.pop('allocationFactors', None)

        p = LcProcess(p_id, Name=name, Classifications=cls, SpatialScope=ss, TemporalScope=stt, **p_j)

        self.add(p)

        broken_exch = []
        for ex in exch:
            val = param_engine.exchange_value(ex['internalId'])
            try:
                self._add_exchange(p, ex, val)
            except KeyError as e:
                ex_id = ex.get('internalId', -1)
                logging.error('%s: failed to add mal-formed exchange with ID %d: {%s}' % (p.uuid, ex_id, e.args[0]))
                broken_exch.append(ex)
            except ConversionError:
                ex_id = ex.get('internalId', -1)
                logging.info('%s: Unit Conversion Error for exchange with ID %d' % (p.uuid, ex_id))
                broken_exch.append(ex)

        if len(broken_exch) > 0:
            logging.warning('%s: %d broken exchanges' % (p.uuid, len(broken_exch)))
            p['brokenExchanges'] = broken_exch

        self._apply_olca_allocation(p, alloc)

        if param_engine.active:
            p['OlcaParameterEngine'] = param_engine

        return p

    def _create_lcia_quantity(self, l_j, method, **kwargs):
        q_id = l_j['@id']
        q = self[q_id]
        if q is not None:
            return q

        l_obj, l_name, cats = self._clean_object('lcia_categories', q_id)
        c_desc = l_obj.pop('description', None)
        ver = l_obj.pop('version', None)
        indicator = l_obj.pop(self._get_v_field('ImpactCategory', 'referenceUnitName'))
        unit = LcUnit(indicator)

        q_name = ', '.join([method, l_name])

        q = LcQuantity(q_id, Name=q_name, ReferenceUnit=unit, Method=method, Category=l_name, Indicator=indicator,
                       CategoryDescription=c_desc, Version=ver, **kwargs)

        self.add(q)
        for factor in l_obj.get('impactFactors', []):
            flow = self._create_flow(factor['flow']['@id'])
            loc = factor.get('location')
            ''' # this does not work
            if loc is None:
                try:
                    loc = geog_tail.search(flow.name).group()
                except AttributeError:
                    pass
            '''

            ref_qty = self._create_quantity(factor['flowProperty']['@id'])
            assert flow.reference_entity == ref_qty
            # value = factor['value']

            self.tm.add_characterization(flow.name, ref_qty, q, factor['value'], context=flow.context, location=loc,
                                         origin=self.ref)
        return q

    def _create_lcia_category(self, c_id):
        """
        In this case, the client has requested a specific lcia quantity, a.k.a. LCIA category.  This is tricky because
        we don't know which method the category is part of. So we modify our indexing process to track this.
        :param c_id:
        :return:
        """
        if c_id in self._lm_index:
            self._create_lcia_method(self._lm_index[c_id])
            q = self[c_id]
            if q is None:
                raise OpenLcaException('Specified LCIA category does not match the one found')
            return q
        raise KeyError('Specified key is not an LCIA Category: %s' % c_id)

    def _create_lcia_method(self, m_id):
        """
        Note: in OLCA archives, an "LCIA Method" is really a methodology with a collection of category indicators, which
        is what we colloquially call "methods". So every method includes zero or more distinct quantities.

        We create an LciaMethod "meta-quantity" which is NOT an lcia_method in antelope parlance (because it doesn't
        have an indicator) but which does contain pointers to its constituent lcia-methods (which are actually lcia
        categories, in OpenLCA parlance)
        :param m_id:
        :return:
        """
        lm = self[m_id]
        if lm is not None:
            return lm

        m_obj, method, cats = self._clean_object('lcia_methods', m_id)
        m_desc = m_obj.pop('description', None)

        sets = []
        norms = defaultdict(list)
        weights = defaultdict(list)

        if 'nwSets' in m_obj:
            for n in m_obj['nwSets']:
                if 'factors' in n:
                    norm_j = n
                else:
                    norm_j = self._create_object('nw_sets', n['@id'])
                sets.append(norm_j['name'])
                for fac in norm_j['factors']:
                    norms[fac['impactCategory']['@id']].append(fac.get('normalisationFactor', None))
                    weights[fac['impactCategory']['@id']].append(fac.get('weightingFactor', None))

        qs = []

        for imp in m_obj.pop('impactCategories', []):
            q = self._create_lcia_quantity(imp, method, MethodDescription=m_desc)
            norm = norms[q.external_ref]
            if len(norm) > 0:
                q['normalisationFactors'] = norm
                q['normSets'] = sets
                q['weightingFactors'] = weights[q.external_ref]
            qs.append(q.external_ref)

        m = LcQuantity(m_id, Name=method, ReferenceUnit=MetaQuantityUnit, Method=method, Description=m_desc,
                       ImpactCategories=qs)
        self.add(m)

    def _fetch(self, key, typ=None, **kwargs):
        if typ is None:
            if self._type_index is None:
                self._gen_index()
            typ = self._type_index[key]
        try:
            _ent_g = {'processes': self._create_process,
                      'flows': self._create_flow,
                      'flow_properties': self._create_quantity,
                      'lcia_methods': self._create_lcia_method,
                      'lcia_categories': self._create_lcia_category}[typ]
        except KeyError:
            logging.warning('Warning: generating generic object for unrecognized type %s' % typ)
            _ent_g = lambda x: self._create_object(typ, x)

        return _ent_g(key)

    def _load_all(self, **kwargs):
        self._print('Loading processes')
        if self.defined:
            for p_id in self._defined_ps.processes:
                self._create_process(p_id)
        else:
            for f in self._archive.listfiles(in_prefix='processes'):
                ff = f.split('/')
                fg = ff[1].split('.')
                self._create_process(fg[0])
        self._print('Loading LCIA methods')
        for f in self._archive.listfiles(in_prefix='lcia_methods'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_lcia_method(fg[0])
