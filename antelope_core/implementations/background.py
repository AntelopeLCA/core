import re
from itertools import chain

from .basic import BasicImplementation
from antelope import BackgroundInterface, EntityNotFound, comp_dir  # , ProductFlow
from antelope.models import ExteriorFlow
from antelope_core.contexts import Context


class NonStaticBackground(Exception):
    pass


def search_skip(entity, search):
    if search is None:
        return False
    return not bool(re.search(search, str(entity), flags=re.IGNORECASE))


class BackgroundImplementation(BasicImplementation, BackgroundInterface):
    """
    The default Background Implementation exposes an ordinary inventory database as a collection of LCI results.
    Because it does not perform any ordering, there is no way to distinguish between foreground and background
    elements in a database using the proxy. It is thus inconsistent for the same resource to implement both
    inventory and [proxy] background interfaces.
    """
    def __init__(self, *args, **kwargs):
        super(BackgroundImplementation, self).__init__(*args, **kwargs)

        self._index = None

    def check_bg(self, **kwargs):
        self.setup_bm(**kwargs)
        return bool(self._index is not None)

    def setup_bm(self, index=None):
        """
        Requires an index interface or catalog query <-- preferred
        This must provide .get() and .get_context() (so really it should maybe be 'basic'
        The trivial implementation uses .flows() and .targets() to mock up an exterior flows method
        :param index:
        :return:
        """
        if self._index is None:
            if index is None:
                # print('%%%%%% Setting up Background Impl for %s from archive %s' % (self.origin, self._archive))
                self._index = self._archive.make_interface('index')
            else:
                # print('%%%%%% Setting up Background Impl for %s from index %s' % (self.origin, index))
                self._index = index

    def _ensure_ref_flow(self, ref_flow):
        if ref_flow is not None:
            if isinstance(ref_flow, str) or isinstance(ref_flow, int):
                ref_flow = self._archive.retrieve_or_fetch_entity(ref_flow)
        return ref_flow

    def foreground_flows(self, search=None, **kwargs):
        """
        No foreground flows in the proxy background
        :param search:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def background_flows(self, search=None, **kwargs):
        """
        all process reference flows are background flows
        :param search:
        :param kwargs:
        :return:
        """
        self.check_bg()
        for p in self._archive.entities_by_type('process'):
            for rx in p.references():
                if search_skip(p, search):
                    continue
                yield rx  # ProductFlow(self._archive.ref, rx.flow, rx.direction, p, None) don't need this

    def exterior_flows(self, direction=None, search=None, **kwargs):
        """
        Exterior flows are all flows that do not have interior terminations (i.e. not found in the index targets)
        Elementary contexts have a sense, but intermediate contexts do not [necessarily]- so we need some way to
        determine their directionality.  This whole implementation is just a stand-in anyway- the important thing
        is that this is handled correctly in tarjan
        :param direction:
        :param search:
        :param kwargs:
        :return:
        """
        self.check_bg()
        for f in self._index.flows():
            if search_skip(f, search):
                continue
            try:
                next(self._index.targets(f.external_ref, direction=direction))
            except StopIteration:
                cx = self._index.get_context(f.context)
                dirn = comp_dir(cx.sense)  # this is already w.r.t. interior
                '''
                if self.is_elementary(f):
                    yield ExteriorFlow(self._archive.ref, f, 'Output', f['Compartment'])
                else:
                    yield ExteriorFlow(self._archive.ref, f, 'Output', None)
                '''
                yield ExteriorFlow.from_background(f, dirn, cx)

    def consumers(self, process, ref_flow=None, **kwargs):
        """
        Not supported for trivial backgrounds
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def dependencies(self, process, ref_flow=None, **kwargs):
        """
        All processes are LCI, so they have no dependencies
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in []:
            yield i

    def emissions(self, process, ref_flow=None, **kwargs):
        """
        All processes are LCI, so they have only exterior flows. Emissions are the exterior flows with elementary
        contexts
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in self.lci(process, ref_flow=ref_flow, **kwargs):
            if isinstance(i.termination, Context):
                if i.termination.elementary:
                    yield i

    def cutoffs(self, process, ref_flow=None, **kwargs):
        """
        All processes are LCI, so they have only exterior flows. Emissions are the exterior flows with non-elementary
        contexts
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        for i in self.lci(process, ref_flow=ref_flow, **kwargs):
            if isinstance(i.termination, Context):
                if i.termination.elementary:
                    continue
            yield i

    def foreground(self, process, ref_flow=None, **kwargs):
        self.check_bg()
        ref_flow = self._ensure_ref_flow(ref_flow)
        p = self._index.get(process)
        yield p.reference(ref_flow)  # should be just one exchange

    def is_in_scc(self, process, ref_flow=None, **kwargs):
        """
        Distinction between is_in_background and is_in_scc will reveal the proxy nature of the interface
        :param process:
        :param ref_flow:
        :param kwargs:
        :return:
        """
        return False  # proxy has no knowledge of SCCs

    def is_in_background(self, process, ref_flow=None, **kwargs):
        self.check_bg()
        try:
            self._archive.retrieve_or_fetch_entity(process)
        except EntityNotFound:
            return False
        return True

    def ad(self, process, ref_flow=None, **kwargs):
        for i in []:
            yield i

    def bf(self, process, ref_flow=None, **kwargs):
        for i in []:
            yield i

    def lci(self, process, ref_flow=None, **kwargs):
        self.check_bg()
        ref_flow = self._ensure_ref_flow(ref_flow)
        p = self._archive.retrieve_or_fetch_entity(process)
        for x in p.inventory(ref_flow=ref_flow):
            yield x

    def sys_lci(self, demand, **kwargs):
        """
        For LCI, we simply yield process direct exchanges as LCI.
        For sys_lci, we should just do the same. yield the supplied demand as a degenerate LCI.
        :param demand:
        :param kwargs:
        :return:
        """
        for y in demand:
            yield y

    def sys_lcia(self, process, query_qty, observed=None, ref_flow=None, **kwargs):
        """
        returns an LciaResult object, aggregated as appropriate depending on the interface's privacy level.
        This is an ensemble function that stitches together bg functions with quantity access.

        :param process:
        :param query_qty: must be an operable quantity_ref. the process must have exchange access
        :param observed: iterable of DirectedFlows (flow: FlowSpec, direction: str)
        :param ref_flow:
        :param kwargs:
        :return:
        """
        p_ref = self.get(process)  # a ref-- unless this was a basic impl. hrm.
        if p_ref.is_entity:
            raise NotImplementedError  # we can't proceed
        if observed is None:
            observed = ()
        lci = p_ref.unobserved_lci(observed, ref_flow=ref_flow)
        '''
        obs = set()
        for k in observed:
            if k.value < 0:
                obs.add((k.flow.external_ref, comp_dir(k.direction)))
            else:
                obs.add((k.flow.external_ref, k.direction))
        # obs = set((k.flow.external_ref, k.direction) for k in observed)
        if len(obs) > 0:
            exts = chain(p_ref.emissions(ref_flow=ref_flow),
                         p_ref.cutoffs(ref_flow=ref_flow))
            incl = (k for k in p_ref.dependencies(ref_flow=ref_flow) if (k.flow.external_ref, k.direction) not in obs)
            ext = (k for k in exts if (k.flow.external_ref, k.direction) not in obs)
            lci = chain(self.sys_lci(incl), ext)
        else:
            lci = p_ref.lci(ref_flow=ref_flow)
        '''  # aggregation
        return query_qty.do_lcia(lci, **kwargs)
