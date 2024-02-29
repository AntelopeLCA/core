"""
supplies an Antelope archive that contains the ref data.
"""
import csv

from antelope_core.archives import BasicArchive
from antelope_core.entities import LcFlow, LcQuantity, LcUnit
from antelope import QuantityInterface, EntityNotFound
from antelope_core.implementations import BasicImplementation
from antelope_core.characterizations import Characterization, DuplicateCharacterizationError
from antelope_core.lcia_engine import LciaEngine

from antelope_core.entities import MetaQuantityUnit

from .olca_accessor import OlcaAccessorV2
import logging


class OlcaRefQuantityImplementation(BasicImplementation, QuantityInterface):
    def get_canonical(self, quantity, **kwargs):
        return self._archive.tm.get_canonical(quantity)

    def factors(self, quantity, flowable=None, context=None, **kwargs):
        """
        always retrieving these from file when asked allows the files to be updated
        but maybe it would be smrter just to add them locally
        what would be smrtest would be to just store them in a dumb rdbms instead of from file. but think of the
        *immediate* use case.
        that is all for qdb

        :param quantity:
        :param flowable:
        :param context:
        :param kwargs:
        :return: generate Characterization objects
        """
        qq = self.get_canonical(quantity)
        cfs = dict()
        l_count = 0

        if flowable is not None:
            flowable = self._archive.tm.get_flowable(flowable)
        if context is not None:
            context = self._archive.tm[context]

        for factor in self._archive.factors_for_quantity(qq):
            flow = self._archive[factor['Flow']]
            fb = self._archive.tm.get_flowable(flow.name)

            if flowable is not None:
                if flowable != fb:
                    continue

            cx = self._archive.tm[flow.context]  # OLCA flows are all bound to a distinct context
            if context is not None:
                if cx != context:
                    continue

            rq = self.get_canonical(factor['Flow property'])
            key = (fb, factor['Flow property'], cx)
            if key not in cfs:
                cfs[key] = Characterization(fb, rq, qq, cx)

            rq_unit = factor['Flow unit']
            # [Factor] [Indicator] / [Flow unit]  * [convert to] / [convert from] = value [Indicator] / rq.unit
            value = float(factor['Factor']) * rq.convert(to=rq_unit)
            locale = factor['Location']
            if locale:
                l_count += 1
            else:
                locale = flow.locale

            try:
                cfs[key][locale] = value
            except DuplicateCharacterizationError:
                logging.warning('Duplicate Characterization for locale %s\nkey: %s' % (locale, key))

        if l_count:
            print('method %s: %d non-null Locations' % (qq.uuid, l_count))

        for v in cfs.values():
            yield v


class OpenLcaRefData(BasicArchive):
    """
    Modern OLCA data format should be easy to use. We are ignoring their unit conversions (which, fair, should be
    incorporated) ... categories is gone ... and they hve no flow properties to speak of (that I can find)
    so we simply do what we did below, adapted to the simpler data format.

    first flow properties
    then we replicate the meta-quantities load sequence from json-LD
    and we do on-demand CF synthesis via a custom Quantity implementation
    and we're done
    """

    _ns_uuid_required = None  # don't bother with this- our entities all have UUIDs already

    def make_interface(self, iface):
        if iface == 'quantity':
            return OlcaRefQuantityImplementation(self)
        return super(OpenLcaRefData, self).make_interface(iface)

    def __init__(self, source=None, **kwargs):
        super(OpenLcaRefData, self).__init__(source, ns_uuid=None, **kwargs)  # term_manager=LciaEngine(),
        self._index = dict()
        self._a = OlcaAccessorV2(source)
        self._olca_unitgroups = self._a.read_unit_groups()
        self._olca_locales = self._a.read_locales()
        # self.load_all()  for profiling reasons, keep this separate

    def _fetch(self, entity, **kwargs):
        """
        :param entity:
        :param kwargs:
        :return:
        """
        ent = self[entity]
        if ent is None:
            raise KeyError(entity)
        else:
            return ent

    def _load_flow_properties(self):
        for fp in self._a.get_refdata('flow_properties'):
            if self[fp['ID']] is None:
                ug = self._olca_unitgroups[fp['Unit group']]

                q = LcQuantity(fp['ID'], Name=fp['Name'], ReferenceUnit=LcUnit(ug.ref_unit),
                               entity_uuid=fp['ID'], Category=fp['Category'], Type=fp['Property type'],
                               UnitConversion=ug.conversion)
                self.add(q)

    def _load_flows(self):
        for flow in self._a.get_refdata('flows'):
            f_id = flow.pop('ID')
            if self[f_id] is None:
                ref_prop = flow.pop('Reference flow property')
                try:
                    fp = self.tm.get_canonical(ref_prop)
                except EntityNotFound:
                    print('Flow %s: reference property not found: %s' % (f_id, ref_prop))
                    continue
                cx = tuple(flow.pop('Category').split('/'))
                flow['CasNumber'] = flow.pop('CAS number')

                # check for locale
                name = flow['Name']
                maybe_loc = name.split(',')[-1].strip()
                if maybe_loc in self._olca_locales:
                    flow['locale'] = maybe_loc

                f = LcFlow(f_id, ReferenceQuantity=fp, entity_uuid=f_id, context=cx, **flow)
                self.add(f)

    def _load_impact_categories(self):
        """
        lcia_categories: ID,Name,Description,Category,Reference unit
         it appears "category" is misnamed here- it actually is FK to lcia_methods.Name
         "description" is non-null for only a few.
        :return:
        """
        for lcia in self._a.get_refdata('lcia_categories'):
            l_id = lcia.pop('ID')
            if self[l_id] is None:
                method = lcia.pop('Category')
                name = lcia.pop('Name')
                indicator = lcia.pop('Reference unit')
                u = LcUnit(indicator)
                q = LcQuantity(l_id, ReferenceUnit=u, Name=name,
                               Method=method,
                               Category=name,
                               Indicator=indicator, normSets=[], normalisationFactors=[], weightingFactors=[])
                self.add(q)

    def _load_nw(self):
        """
        lcia_method_nw_sets: LCIA method,NW set - ID,NW set - name,LCIA category,Nomalisation factor,Weighting factor,Weighting score unit
         Weighting score unit appears null for all records, so we ignore it

        :return:
        """
        for nw in self._a.get_refdata('lcia_method_nw_sets'):
            q = self[nw['LCIA category']]
            if q['Method'] != nw['LCIA method']:
                print('!! Method disagreement %s: %s vs %s' % (q.uuid, q['Method'], nw['LCIA method']))

            # get the data
            set_name = nw['NW set - name']

            if nw['Weighting score unit']:
                print('%s: Weighting score unit found for %s [%s]' % (q.uuid, set_name, nw['Weighting score unit']))

            try:
                norm = float(nw['Nomalisation factor'])  # spelling!!!!
            except ValueError:
                norm = None
            try:
                wgt = float(nw['Weighting factor'])
            except ValueError:
                wgt = None
            if norm is None and wgt is None:
                print('%s: No values for weighting set %s' % (q.uuid, set_name))
                continue

            # store the data
            if set_name in q['normSets']:
                ix = q['normSets'].index(set_name)
                # the lists should always be the same length, but let's just pad them up
                while len(q['normalisationFactors']) < ix:
                    q['normalisationFactors'].append(0)
                while len(q['weightingFactors']) < ix:
                    q['weightingFactors'].append(0)
                q['normalisationFactors'][ix] = norm
                q['weightingFactors'][ix] = wgt
            else:
                q['normSets'].append(set_name)
                q['normalisationFactors'].append(norm)
                q['weightingFactors'].append(wgt)

    def _load_impact_methods(self):
        """
        the key question here is HOW to manage the normalization information. We had an ad hoc strategy for OpenLCA
        where each INDICATOR was given three properties: normSets, normalisationFactors, and weightingFactors,
        all lists, where the ith element of each list was in correspondence with one another.

        Then when we moved to pydantic, we created a Normalizations object which stored them as dicts.

        For the time being, let's just do what we said before and replicate the OpenLCA JsonLD approach.

        lcia_methods: ID,Name,Description,Category
         description is LONG STRING. Category is not interesting.
        lcia_method_categories: LCIA method,LCIA category
        :return:
        """
        categories = self._a.collate_lcia_categories()

        for im in self._a.get_refdata('lcia_methods'):
            if self[im['ID']] is None:
                method = im['Name']
                desc = im['Description']
                cat = im['Category']
                qs = categories.get(method, [])
                m = LcQuantity(method, Name=method, ReferenceUnit=MetaQuantityUnit, Method=method, Description=desc,
                               entity_uuid=im['ID'], Category=cat,
                               ImpactCategories=qs)
                self.add(m)
                if self[im['ID']] is not m:
                    raise ValueError

    def _load_all(self, flows=True, **kwargs):
        self._load_flow_properties()
        self._load_impact_categories()
        self._load_nw()
        self._load_impact_methods()
        if flows:
            self._load_flows()

    def factors_for_quantity(self, q):
        for cf in self._a.factors_for_method(q):
            yield cf

    def factors_to_csv(self, quantity, filepath):
        """
        Utility to output processed CFs to a file for easy analysis, debug unit conversions, duplicate CFs, etc
        :param quantity:
        :param filepath:
        :return:
        """
        qq = self.tm.get_canonical(quantity)

        fields = ('quantity',
                  'ref_quantity',
                  'flowable',
                  'context',
                  'locale',
                  'value',
                  'indicator',
                  'ref_unit')

        count = 0
        with open(filepath, 'w') as fp:
            cw = csv.DictWriter(fp, fieldnames=fields)
            for factor in self.factors_for_quantity(qq):
                flow = self[factor['Flow']]
                rq = flow.reference_entity
                fb = self.tm.get_flowable(flow.name)
                cx = self.tm[flow.context]
                locale = factor['Location']
                if not locale:
                    locale = flow.locale

                value = float(factor['Factor']) * rq.convert(to=factor['Flow unit'])

                row = {'quantity': qq.uuid,
                       'ref_quantity': rq.uuid,
                       'flowable': str(fb),
                       'context': '; '.join(cx.as_list()),
                       'locale': locale,
                       'value': value,
                       'indicator': qq['Indicator'],
                       'ref_unit': rq.unit}

                cw.writerow(row)
                count += 1
        print('Wrote %d rows to %s' % (count, filepath))
