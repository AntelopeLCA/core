import json
import os

from collections import defaultdict

# from antelope import NoFactorsFound

from ..exchanges import AmbiguousReferenceError

from ..entities import *
from ..entities.processes import NoExchangeFound
from ..archives import LcArchive
from .file_store import FileStore
from .ecospold import parse_math


valid_types = {'processes', 'flows', 'flow_properties'}


class OpenLcaException(Exception):
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
                self._type_index['root'] = ff[0]
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
        for cat_key in self._cat_index.keys():
            cat = self._cat_as_list(cat_key)
            lookup_key = tuple(cat)
            self._cat_tuple[cat_key] = lookup_key  # forward lookup key -> tuple
            self._cat_lookup[lookup_key] = cat_key  # reverse lookup of tuple -> key
            self.tm.add_context(lookup_key, cat_key)

    def __init__(self, source, prefix=None, skip_index=False, **kwargs):
        super(OpenLcaJsonLdArchive, self).__init__(source, **kwargs)

        self._drop_fields['process'].extend(['processDocumentation'])

        self._archive = FileStore(source, internal_prefix=prefix)

        self._type_index = None
        if not skip_index:
            self._gen_index()

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
        j.pop('@context')
        j.pop('@id')
        name = j.pop('name')

        if 'category' in j:
            c_j = j.pop('category')
            cat = self._get_category_list(c_j['@id'])
        else:
            cat = []
        return j, name, cat

    def _get_category_list(self, cat_key):
        if cat_key in self._cat_tuple:
            return list(self._cat_tuple[cat_key])
        c_j = self._cat_index[cat_key]
        if 'category' in c_j:
            cat = self._get_category_list(c_j['category']['@id'])
        else:
            cat = []
        cat.append(c_j['name'])
        return cat

    def openlca_category(self, name_or_full_list):
        """
        returns an olca id for a name or a list of hierarchical names, if one exists
        :param name_or_full_list:
        :return:
        """
        cx = self.tm[name_or_full_list]
        if cx is not None:
            # this will be a Context
            return self._cat_lookup[tuple(cx.as_list())]
        return self._cat_lookup[tuple(name_or_full_list)]

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
            is_ref = conv.pop('referenceUnit', False)
            name = conv.pop('name')
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
            ref = fp.pop('referenceFlowProperty', False)
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
        f = LcFlow(f_id, Name=name, Compartment=comp, CasNumber=cas, ReferenceQuantity=ref_q, **f_j)  # context gets set by _catch_context()

        self.add(f)  # context gets matched inside tm.add_flow().  NONSPECIFIC entries are automatically prepended with parent name in CompartmentManager.new_entry()

        for i, q in enumerate(qs):
            self.tm.add_characterization(f.link, ref_q, q, facs[i], context=f.context, location=loc)

        return f

    def _add_exchange(self, p, ex):
        flow = self.retrieve_or_fetch_entity(ex['flow']['@id'], typ='flows')
        value = ex['amount']
        dirn = 'Input' if ex['input'] else 'Output'

        fp = self.retrieve_or_fetch_entity(ex['flowProperty']['@id'], typ='flow_properties')

        try:
            v_unit = ex['unit']['name']
        except KeyError:
            print('%s: %d No unit! using default %s' % (p.external_ref, ex['internalId'], fp.unit))
            v_unit = fp.unit

        if v_unit != fp.unit:
            oldval = value
            value *= fp.convert(from_unit=v_unit)
            self._print('%s: Unit Conversion exch: %g %s to native: %g %s' % (p.uuid, oldval, v_unit, value, fp.unit))

        if fp != flow.reference_entity:
            try:
                value /= fp.cf(flow)  ## is this even right?  ### yes  # TODO: account for locale?
            except (TypeError, ZeroDivisionError):
                print('%s:%s:%s flow reference quantity does not match\n%s exchange f.p. Conversion Required' %
                      (p.external_ref, dirn, flow.external_ref, flow.name))
                print('From %s to %g %s' % (flow.unit, value, fp.unit))
                val = parse_math(input('Enter conversion factor 1 %s = x %s [context %s]\nx: ' %
                                       (flow.unit, fp.unit, flow.context)))
                self.tm.add_characterization(flow.link, flow.reference_entity, fp, val, context=flow.context,
                                             origin=self.ref)
                value /= fp.cf(flow)

        is_ref = ex.pop('quantitativeReference', False)
        if is_ref:
            term = None
        elif 'defaultProvider' in ex:
            term = ex['defaultProvider']['@id']
        else:
            term = self.tm[flow.context]

        exch = p.add_exchange(flow, dirn, value=value, termination=term, add_dups=True)
        if is_ref:
            p.set_reference(flow, dirn)

        if 'description' in ex:
            exch.comment = ex['description']

        return exch

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
        if alloc is None:
            return
        if p.has_property('defaultAllocationMethod'):
            dm = p['defaultAllocationMethod']
        else:
            dm = 'NO_ALLOCATION'
        _causal_msg = True
        stored_alloc = []
        for af in alloc:
            rf = self.retrieve_or_fetch_entity(af['product']['@id'])
            try:
                rx = p.reference(rf)
            except NoExchangeFound:
                try:
                    rx = next(_x for _x in p.exchange_values(rf) if _x.type in ('context', 'cutoff'))
                    p.set_reference(rf, rx.direction)
                    assert rx.is_reference
                except StopIteration:
                    print('%s: Unable to find allocatable exchange for %s' % (p.external_ref, rf.external_ref))
                    continue

            if af['allocationType'] == 'CAUSAL_ALLOCATION':
                if af['value'] == 0:
                    # Keep 0-allocation factors for non-causal
                    continue

                if dm != 'CAUSAL_ALLOCATION':
                    if _causal_msg:
                        print('%s: Skipping Speculative CAUSAL_ALLOCATION' % p.external_ref)
                        _causal_msg = False
                    continue
                f = self.retrieve_or_fetch_entity(af['exchange']['flow']['@id'])

                xs = list(p.exchange_values(f))
                if len(xs) > 1:
                    raise AmbiguousReferenceError('%s: Multiple flows with ID %s' % (p.external_ref, f.external_ref))
                x = xs[0]

                val = af['value']
                stored_alloc.append(af)

                x[rx] = x.value * val
                if _causal_msg:
                    print('%s: Warning: causal allocation has not been tested' % p.external_ref)
                    _causal_msg = False
            else:
                q = self._create_allocation_quantity(p, af['allocationType'])

                v = af['value'] / rx.value
                stored_alloc.append(af)

                self.tm.add_characterization(rf.link, rf.reference_entity, q, v, context=rf.context, origin=self.ref)
                #f.add_characterization(q, value=v)

        if dm != 'NO_ALLOCATION':
            aq = self._create_allocation_quantity(p, p['defaultAllocationMethod'])
            p.allocate_by_quantity(aq)
        p['allocationFactors'] = stored_alloc  # only keep factors we used

    def _create_process(self, p_id):
        q = self[p_id]
        if q is not None:
            return q

        p_j, name, cls = self._clean_object('processes', p_id)
        ss = p_j.pop('location', {'name': 'GLO'})['name']
        stt = dict()
        for key, tgt in (('validFrom', 'begin'), ('validUntil', 'end')):
            try:
                stt[tgt] = p_j['processDocumentation'][key]
            except KeyError:
                pass

        exch = p_j.pop('exchanges')

        if 'allocationFactors' in p_j:
            # process later-- do we keep or remove them???
            alloc = p_j.pop('allocationFactors')
        else:
            alloc = None


        p = LcProcess(p_id, Name=name, Classifications=cls, SpatialScope=ss, TemporalScope=stt, **p_j)

        self.add(p)

        for ex in exch:
            self._add_exchange(p, ex)

        self._apply_olca_allocation(p, alloc)

        return p

    def _create_lcia_quantity(self, l_j, method, **kwargs):
        q_id = l_j['@id']
        l = self[q_id]
        if l is not None:
            return l

        l_obj, l_name, cats = self._clean_object('lcia_categories', q_id)
        c_desc = l_obj.pop('description', None)
        ver = l_obj.pop('version', None)
        indicator = l_obj.pop('referenceUnitName')
        unit = LcUnit(indicator)

        q_name = ', '.join([method, l_name])

        q = LcQuantity(q_id, Name=q_name, ReferenceUnit=unit, Method=method, Category=l_name, Indicator=indicator,
                       CategoryDescription=c_desc, Version=ver, **kwargs)

        self.add(q)
        for factor in l_obj['impactFactors']:
            flow = self._create_flow(factor['flow']['@id'])
            ref_qty = self._create_quantity(factor['flowProperty']['@id'])
            assert flow.reference_entity == ref_qty
            self.tm.add_characterization(flow.link, ref_qty, q, factor['value'], context=flow.context, origin=self.ref)

        return q

    def _create_lcia_category(self, c_id):
        """
        In this case, the client has requested a specific lcia quantity, a.k.a. LCIA category.  This is tricky because
        we don't know which method the category is part of. So we modify our indexing process to track this.
        :param c_id:
        :return:
        """
        if c_id in self._lm_index:
            try:
                q = next(t for t in self._create_lcia_method(self._lm_index[c_id], c_id) if t.uuid == c_id)
            except StopIteration:
                raise OpenLcaException('Specified LCIA category does not match the one found')
            return q
        raise KeyError('Specified key is not an LCIA Category: %s' % c_id)

    def _create_lcia_method(self, m_id, *categories):
        """
        Note: in OLCA archives, an "LCIA Method" is really a methodology with a collection of category indicators, which
        is what we colloquially call "methods". So every method includes zero or more distinct quantities.
        :param m_id:
        :return:
        """
        m_obj, method, cats = self._clean_object('lcia_methods', m_id)
        m_desc = m_obj.pop('description', None)

        sets = []
        norms = defaultdict(list)
        weights = defaultdict(list)

        if 'nwSets' in m_obj:
            for n in m_obj['nwSets']:
                norm_j = self._create_object('nw_sets', n['@id'])
                sets.append(norm_j['name'])
                for fac in norm_j['factors']:
                    norms[fac['impactCategory']['@id']].append(fac.get('normalisationFactor', None))
                    weights[fac['impactCategory']['@id']].append(fac.get('weightingFactor', None))

        qs = []

        for imp in m_obj.pop('impactCategories', []):
            if len(categories) > 0:
                # if the user specifies certain categories to load, then skip the ones that aren't specified
                if imp['@id'] not in categories:
                    continue
            q = self._create_lcia_quantity(imp, method, MethodDescription=m_desc)
            norm = norms[q.external_ref]
            if len(norm) > 0:
                q['normalisationFactors'] = norm
                q['normSets'] = sets
                q['weightingFactors'] = weights[q.external_ref]
            qs.append(q)

        return qs

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
            print('Warning: generating generic object for unrecognized type %s' % typ)
            _ent_g = lambda x: self._create_object(typ, x)

        return _ent_g(key)

    def _load_all(self, **kwargs):
        self._print('Loading processes')
        for f in self._archive.listfiles(in_prefix='processes'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_process(fg[0])
        self._print('Loading LCIA methods')
        for f in self._archive.listfiles(in_prefix='lcia_methods'):
            ff = f.split('/')
            fg = ff[1].split('.')
            self._create_lcia_method(fg[0])
