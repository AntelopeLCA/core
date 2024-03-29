"""
Import ecospold2 files
"""

from collections import namedtuple
from time import time

from lxml import objectify
from lxml.etree import XMLSyntaxError

from ...characterizations import QRResult
from ...entities import LcQuantity, LcFlow, LcProcess
from ...entities.processes import AlreadyAReference
from ...exchanges import ExchangeValue, DirectionlessExchangeError
from ...lcia_results import LciaResult
from ..ecospold import tail
from ..file_store import FileStore
from ...archives import LcArchive, uuid_regex
from ..xml_widgets import *

from .ecospold2_index import EcoSpold2IndexImplementation
from .master_data import EcoSpoldMasterData


EcospoldExchange = namedtuple('EcospoldExchange', ('flow', 'direction', 'value', 'termination', 'is_ref', 'comment'))
EcospoldLciaResult = namedtuple('EcospoldLciaResult', ('Method', 'Category', 'Indicator', 'score'))


def spold_reference_flow(filename):
    """
    second UUID, first match should be reference flow uuid
    :param filename:
    :return: first uuid, second uuid
    """
    m = uuid_regex.findall(filename)
    try:
        return m[0][0], m[1][0]
    except IndexError:
        try:
            return m[0][0], None
        except IndexError:
            print('No UUID found in %s' % filename)
            raise


class EcospoldV2Error(Exception):
    pass


def _add_syn_if(syn, synset):
    g = syn.strip()
    if g != '' and g != 'PSM':
        synset.add(syn)


def _syn_set(exch):
    synonym_tag = list(find_tags(exch, 'synonym'))
    syns = set()
    if len(synonym_tag) == 1:
        # parse the comma-separated list
        if bool(re.search('etc\.', str(synonym_tag[0]))):
            syns.add(str(synonym_tag[0]).strip())
        else:
            for x in str(synonym_tag[0]).split(', '):
                _add_syn_if(x, syns)
    else:
        # multiple entries- allow embedded comma-space
        for syn in synonym_tag:
            _add_syn_if(str(syn), syns)
    return syns


class EcospoldV2Archive(LcArchive):
    """
    class for loading metadata from ecospold v2 files. Now I know ecoinvent supplies a whole ton of supplementary
    information in files that are *outside* the ecospold archives- and that information is going to be IGNORED.
    or loaded separately. But not handled here.
    """

    nsmap = 'http://www.EcoInvent.org/EcoSpold02'  # only valid for v1 ecospold files
    spold_version = tail.search(nsmap).groups()[0]

    def __init__(self, source, prefix=None, linked=True, **kwargs):
        """
        Just instantiates the parent class.
        :param source: physical data source
        :param prefix: relative path for datasets from the archive root
        :param linked: [True] whether the archive includes unlinked or linked datasets. Reference exchanges
        get detected differently in one case versus the other (see _create_process)
        :return:
        """
        super(EcospoldV2Archive, self).__init__(source, **kwargs)
        if prefix is not None:
            self._serialize_dict['prefix'] = prefix

        self._archive = FileStore(self.source, internal_prefix=prefix)
        self._master = EcoSpoldMasterData(self.source)

        ''' # not sure why I did this (preload all elem flows) but now I am taking it out
        for exch in self._master.elementary_exchanges.values():
            self._create_flow(exch)
        '''
        print('Warning: not preloading flows')

        self._linked = linked
        self._process_flow_map = defaultdict(set)
        self._terminations = defaultdict(set)
        self._map_datasets()

    @property
    def master_data(self):
        return self._master

    def load_flows(self):
        for exch in self._master.elementary_exchanges.values():
            self._create_flow(exch)

    '''
    def fg_proxy(self, proxy):
        for ds in self.list_datasets(proxy):
            self.retrieve_or_fetch_entity(ds)
        return self[proxy]

    def bg_proxy(self, proxy):
        return self.fg_proxy(proxy)
    '''
    def make_interface(self, iface, privacy=None):
        if iface == 'index':
            return EcoSpold2IndexImplementation(self)
        return super(EcospoldV2Archive, self).make_interface(iface)

    # no need for _ref_to_key - keys in ecospold are uuids
    def _fetch_filename(self, filename):
        st = self._archive.readfile(filename)
        if st is None:
            raise FileNotFoundError
        return st

    def _map_datasets(self):
        for f in self.list_datasets():
            p, r = spold_reference_flow(f)
            self._process_flow_map[p].add(r)
            self._terminations[r].add(p)

    @property
    def ti(self):
        return self._terminations

    def count_by_type(self, entity_type):
        if entity_type == 'process':
            return len(self._process_flow_map)
        return super(EcospoldV2Archive, self).count_by_type(entity_type)

    def processes(self, **kwargs):
        if len(kwargs) == 0:
            for p in self._process_flow_map.keys():
                yield self.retrieve_or_fetch_entity(p)
        else:
            for p in self.search('process', **kwargs):
                yield p

    def list_datasets(self, startswith=None):
        assert self._archive.remote is False, "Cannot list objects for remote archives"
        for x in self._archive.listfiles(in_prefix=startswith):
            yield x

    def _get_objectified_entity(self, filename):
        try:
            o = objectify.fromstring(self._fetch_filename(filename))
        except FileNotFoundError:
            raise
        except ValueError:
            print('failed on :%s:' % filename)
            return None
        if o.nsmap[None] != self.nsmap:
            raise EcospoldV2Error('This class is for EcoSpold v%s only!' % self.nsmap[-2:])
        return o

    def _get_objectified_entity_with_lt_gt(self, filename):
        try:
            f = self._fetch_filename(filename)
            f = re.sub(' < ', ' &lt; ', re.sub(' > ', ' &gt; ', f.decode()))
            o = objectify.fromstring(f)
        except ValueError:
            print('failed on :%s:' % filename)
            return None
        except TypeError:
            print('failed on :%s:' % filename)
            raise
        if o.nsmap[None] != self.nsmap:
            raise EcospoldV2Error('This class is for EcoSpold v%s only!' % self.nsmap[-2:])
        return o

    def _create_quantity(self, exchange):
        """
        In ecospold v2, quantities are defined by a Properties.xml master file-- but unfortunately Ecospold files
        do not USE them to describe exchanges-- only to characterize the flows.  Exchanges themselves are still defined
        using only a unit of measure, not a quantity.

        Moreover, there are widespread inconsistencies in the implementation of Ecoinvent's units and flow properties,
        which seem largely crafted for ad hoc convenience (e.g.: "net heating value" measured in MJ/kg and "heating
        value, net" measured in MJ).

        Thereby, quantities in Ecospold v2 are still only units, defined by string.  They do get their own uuids, but only
        as 'properties' of the flows- flows themselves are only measured by unit.
        this code is cc'd from ecospold1
        :param exchange:
        :return:
        """
        unitstring = exchange.unitName.text
        unit_uuid = exchange.attrib['unitId']
        try_q = self[unit_uuid]
        if try_q is None:
            ref_unit, _ = self._create_unit(unitstring)

            q = LcQuantity(unit_uuid, Name='EcoSpold Quantity %s' % unitstring, ReferenceUnit=ref_unit,
                           Comment=self.spold_version)
            self.add(q)
        else:
            q = try_q

        return q

    @staticmethod
    def _cls_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return ': '.join([i.classificationSystem.text, i.classificationValue.text])
        else:
            return ''

    @staticmethod
    def _cat_to_text(i):
        if isinstance(i, objectify.ObjectifiedElement):
            return [i.compartment.text, i.subcompartment.text]
        else:
            return []

    def _create_flow(self, exchange):
        """
        makes a flow entity and adds to the db
        :param exchange:
        :return:
        """
        if 'intermediate' in exchange.tag:
            try:
                uid = exchange.attrib['intermediateExchangeId']
            except KeyError:
                uid = exchange.attrib['id']
            cat = [self._cls_to_text(exchange.classification)]
        elif 'elementary' in exchange.tag:
            try:
                uid = exchange.attrib['elementaryExchangeId']
            except KeyError:
                uid = exchange.attrib['id']
            cat = self._cat_to_text(exchange.compartment)
        else:
            raise AttributeError('No exchange type found for id %s' % exchange.attrib['id'])

        f = self[uid]
        if f is not None:
            return f

        if 'casNumber' in exchange.attrib:
            cas = exchange.attrib['casNumber']
        else:
            cas = ''

        q = self._create_quantity(exchange)

        n = exchange.name.text
        c = 'EcoSpold02 Flow'

        f = LcFlow(uid, Name=n, CasNumber=cas, Comment=c, Compartment=cat, ReferenceQuantity=q)
        # TODO: implement ecospold flow properties, only for reference products

        syns = _syn_set(exchange)
        syns.add(uid)
        f['Synonyms'] = syns

        self.add(f)

        return f

    @staticmethod
    def _get_process_comment(ad, ud):
        if 1:
            gc = find_tag(ad, 'generalComment')
            if gc == '':
                # print('activity ID %s: no comment' % ud)
                return 'no comment.'
            try:
                c = render_text_block(gc)
            except KeyError:
                print(ud)
                raise
            except XmlWidgetError:
                print(ud)
                raise
            return c
        try:
            c = find_tag(ad, 'generalComment')['text'].text
        except TypeError:
            c = 'no comment.'
        except AttributeError:
            print('activity ID %s: no comment' % ud)
            c = 'no comment.'
        return c

    def _create_process_entity(self, o):
        """
        Constructs the process without populating exchanges
        :param o:
        :return:
        """
        ad = find_tag(o, 'activityDescription')

        u = ad.activity.get('id')

        if self[u] is not None:
            return self[u]

        n = find_tag(ad, 'activityName').text

        c = self._get_process_comment(ad, u)

        g = find_tag(ad, 'geography').shortname.text

        tp = find_tag(ad, 'timePeriod')
        stt = {'begin': tp.get('startDate'), 'end': tp.get('endDate')}
        cls = [self._cls_to_text(i) for i in find_tags(ad, 'classification')]

        p = LcProcess(u, Name=n, Comment=c, SpatialScope=g, TemporalScope=stt,
                      Classifications=cls)

        parent = find_tag(o, 'activity').get('parentActivityId')
        if parent is not None:
            p['ParentActivityId'] = parent

        self.add(p)
        return p

    def _grab_reference_flow(self, o, rf_uuid):
        """
        Create a reference exchange from the flowdata
        :param o:
        :param rf_uuid:
        :return:
        """
        for x in find_tag(o, 'flowData').getchildren():
            if 'intermediate' in x.tag:
                if x.attrib['intermediateExchangeId'] == rf_uuid:
                    flow = self._create_flow(x)
                    value = float(x.get('amount'))  # or None if not found
                    return flow, value

        raise KeyError('Noted reference exchange %s not found!' % rf_uuid)

    def _collect_exchanges(self, o, ref_uuid):
        """

        :param o:
        :param ref_uuid: strictly for diagnostic purposes
        :return:
        """
        flowlist = []

        for exch in find_tag(o, 'flowData').getchildren():
            if 'parameter' in exch.tag:
                continue
            if 'impactIndicator' in exch.tag:
                continue

            f = self._create_flow(exch)
            is_ref = False
            if hasattr(exch, 'outputGroup'):
                d = 'Output'
                og = exch.outputGroup
                if og == 0:
                    is_ref = True
                elif og == 2:  # 1, 3 not used
                    for cls in find_tags(exch, 'classification'):
                        if cls.classificationSystem == 'By-product classification':
                            if str(cls.classificationValue).startswith('allocat'):
                                is_ref = True
            elif hasattr(exch, 'inputGroup'):
                d = 'Input'
            else:
                raise DirectionlessExchangeError
            v = float(exch.get('amount'))  # or None if not found
            t = exch.get('activityLinkId')  # or None if not found
            if t is None:
                if 'elementary' in exch.tag:
                    t = self.tm[f.context]
            try:
                c = '; '.join([str(c) for c in exch.iterchildren() if c.tag == '{%s}comment' % o.nsmap[None]])
            except ValueError:
                ad = find_tag(o, 'activityDescription')

                u = ad.activity.get('id')

                print('Failed reading comment %s_%s: %s' % (u, ref_uuid, exch.get('id')))
                c = None
            flowlist.append(EcospoldExchange(f, d, v, t, is_ref, c))
        return flowlist

    @staticmethod
    def _collect_impact_scores(o):  # , process, flow):
        """
        the old "1115"
        :param o:
        :return:
        """

        scores = []
        # exch = ExchangeValue(process, flow, 'Output', value=1.0)

        for cf in find_tag(o, 'flowData').getchildren():
            if 'impactIndicator' in cf.tag:
                m = cf.impactMethodName.text
                c = cf.impactCategoryName.text
                x = cf.name.text
                v = float(cf.get('amount'))
                scores.append(EcospoldLciaResult(m, c, x, v))

        return scores

    def objectify(self, process_uuid, rf_uuid):
        filename = '%s_%s.spold' % (process_uuid.lower(), rf_uuid.lower())
        self._print('\nObjectifying %s' % filename)
        try:
            o = self._get_objectified_entity(filename)
        except FileNotFoundError:
            raise
        except XMLSyntaxError:
            print('  !!XMLSyntaxError-- trying to escape < and > signs')
            try:
                o = self._get_objectified_entity_with_lt_gt(filename)
            except XMLSyntaxError:
                print('  !!Failed loading %s' % filename)
                raise
        return o

    def _create_process_and_single_reference(self, process_uuid, ref_uuid, exchanges=True):
        """
        Extract dataset object from XML file.  Handles unlinked and linked archives differently:
         * for unlinked (unallocated) archives, there is exactly one spold file for each process.  Reference flows are
           determined contextually, based on outputGroup and By-product classification (see _collect_exchanges).

         * for linked (allocated) archives, there is a distinct spold file for each allocation. References are
           determined based on the filename, which specifies the reference exchange for which the allocation was
           performed.

        :param process_uuid: uuid of activity
        :param ref_uuid: uuid of reference flow
        :return:
        """
        p = self[process_uuid]
        if p is not None:
            if p.has_reference(ref_uuid):
                return p

        try:
            o = self.objectify(process_uuid, ref_uuid)
        except KeyError:
            raise FileNotFoundError

        p = self._create_process_entity(o)

        if p.has_reference(ref_uuid):
            self._print('Process %s already has reference %s' % (process_uuid, ref_uuid))
            return p

        '''
        if self._linked:
            rf, value = self._grab_reference_flow(o, ref_uuid)
            rx = p.add_exchange(rf, 'Output', value=value)  # this should get overwritten with an ExchangeValue later
            p.set_reference(rf, 'Output')
            self._print('# Identified reference exchange\n %s' % rx)
        else:
            rx = None
        '''
        if exchanges:
            rx = None
            exchs = self._collect_exchanges(o, ref_uuid)
            if self._linked:
                for exch in exchs:
                    if exch.is_ref and exch.value != 0:
                        try:
                            _rx = p.add_exchange(exch.flow, exch.direction, value=exch.value)
                        except AlreadyAReference:
                            raise EcospoldV2Error('Already a reference: %s | %s' % (process_uuid, ref_uuid))
                        p.set_reference(exch.flow, exch.direction)
                        self._print('# Identified reference exchange\n %s' % _rx)
                        if len(exch.comment) > 0:
                            _rx.comment = exch.comment
                        if exch.flow.external_ref == ref_uuid:
                            if rx is None:
                                rx = _rx
                            else:
                                raise EcospoldV2Error('Multiple rx found: %s | %s' % (process_uuid, ref_uuid))
                if rx is None:
                    raise EcospoldV2Error('No rx found: %s | %s' % (process_uuid, ref_uuid))
            for exch in exchs:
                """
                If the dataset is linked, all we do is load non-zero exchanges, ideally all with terminations.  Spurious
                 terminations in reference exchanges are dropped (deprecated EI linker feature)
                If the dataset is unlinked, we will err on the side of adding zero-valued exchanges.  The unlinked data
                 should include as much information as possible.  Spurious terminations in reference exchanges are
                  retained, but the reference status is dropped (in-use EI linker feature)

                We could simplify this function by placing those tests in _collect_exchanges, but I would rather not
                muck with the data at that stage.

                """
                if exch.value == 0 and self._linked:
                    continue
                self._print('## Exch %s [%s] (%g)' % (exch.flow, exch.direction, exch.value))

                term = exch.termination
                is_ref = exch.is_ref

                if is_ref:
                    if self._linked:  # in new regime, ref exch is already added
                        '''
                        if self._linked:
                            print('Squashing bad termination in linked reference exchange, %s\nFlow %s Term %s' % (
                                p.external_ref, exch.flow.external_ref, exch.termination))
                            term = None
                        '''
                        continue
                    else:
                        if exch.termination is not None:
                            print('Removing reference status from linked reference exchange, %s\nFlow %s Term %s' % (
                                p.external_ref, exch.flow.external_ref, exch.termination))
                            is_ref = False

                x = p.add_exchange(exch.flow, exch.direction, reference=rx, value=exch.value,
                                   termination=term)
                if len(exch.comment) > 0:
                    x.comment = exch.comment

                if not self._linked:
                    if is_ref:
                        self._print('## ## Exch is reference %s %s' % (exch.flow, exch.direction))
                        p.set_reference(exch.flow, exch.direction)
        return p

    def find_tag(self, process_uuid, rf_uuid, tag):
        return find_tag(self.objectify(process_uuid, rf_uuid), tag)

    '''
    def _fetch(self, uid, ref_flow=None):
        """
        ecospoldV2 files are named by activityId_referenceFlow - if none is supplied, take the first one found
        matching activityId
        :param uid:
        :param ref_flow:
        :return:
        """
        if ref_flow is not None:
            uid = '_'.join([uid, ref_flow])
        files = self.list_datasets(uid)
        if len(files) == 0:
            return None
        return self._create_process(files[0])
    '''

    def _fetch(self, ext_ref, **kwargs):
        """
        We want to handle two different kinds of external references: spold filenames (uuid_uuid.spold) and simple
        entity uuids.  In either case, we want to load *all* the files corresponding to the process, even if only
        one file is specified.  That means we want to truncate the key so that it is a maximum of one UUID long (but
        it would still be nice to allow the user to specify processes by partial uuid)
        :param ext_ref:
        :param kwargs:
        :return:
        """
        if ext_ref in self._master.elementary_exchanges:
            return self._create_flow(self._master.elementary_exchanges[ext_ref])

        if ext_ref in self._master.intermediate_exchanges:
            return self._create_flow(self._master.intermediate_exchanges[ext_ref])

        p_uuid, _ = spold_reference_flow(ext_ref)  # will error if at least one UUID is not present

        p = self[p_uuid]

        for r_uuid in self._process_flow_map[p_uuid]:
            p = self._create_process_and_single_reference(p_uuid, r_uuid, **kwargs)
        return p

    def retrieve_lcia_scores(self, process_uuid, rf_uuid, quantities=None):
        """
        This function retrieves LCIA scores from an Ecospold02 file and stores them as characterizations in
        an LcFlow entity corresponding to the *first* (and presumably, only) reference intermediate flow

        Only stores cfs for quantities that exist locally.
        :param process_uuid:
        :param rf_uuid:
        :param quantities: list of quantity entities to look for (defaults to all local lcia_methods)
        :return: a dict of quantity uuid to score
        """
        if quantities is None:
            quantities = [l for l in self.entities_by_type('quantity') if l.is_lcia_method]

        import time
        start_time = time.time()
        print('Loading LCIA results for %s_%s' % (process_uuid, rf_uuid))
        o = self.objectify(process_uuid, rf_uuid)

        self._print('%30.30s -- %5f' % ('Objectified', time.time() - start_time))
        p = self._create_process_entity(o)
        rf = self._grab_reference_flow(o, rf_uuid)

        exch = ExchangeValue(p, rf, 'Output', value=1.0)

        tags = dict()
        for q in quantities:
            if 'Method' in q.properties():
                if q['Name'] in tags:
                    raise KeyError('Name collision %s' % q['Name'])
                tags[q['Name']] = q

        results = LciaResults(p)

        for char in find_tag(o, 'flowData').getchildren():
            if 'impactIndicator' in char.tag:
                m = char.impactMethodName.text
                c = char.impactCategoryName.text
                i = char.name.text
                v = float(char.get('amount'))
                my_tag = ', '.join([m, c, i])
                if my_tag in tags:
                    q = tags[my_tag]
                    result = LciaResult(q)
                    cf = QRResult(rf['Name'], rf.reference_entity, q, rf.context, p['SpatialScope'], self.ref, v)
                    result.add_score(p.external_ref, exch, cf)
                    results[q.link] = result

        self._print('%30.30s -- %5f' % ('Impact scores collected', time.time() - start_time))

        return results

    def _load_all(self, exchanges=True, bailout=None):
        now = time()
        count = 0
        for p_u, r_set in self._process_flow_map.items():
            for r_u in r_set:
                self._create_process_and_single_reference(p_u, r_u, exchanges=exchanges)
            count += 1
            if count % 100 == 0:
                print(' Loaded %d processes (t=%.2f s)' % (count, time()-now))
            if bailout is not None:
                if count > bailout:
                    print('Bailing out...')
                    break

        print(' Loaded %d processes (t=%.2f s)' % (count, time() - now))
        self.check_counter()
