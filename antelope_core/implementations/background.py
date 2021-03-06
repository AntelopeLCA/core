import re

from .basic import BasicImplementation
from antelope import BackgroundInterface, ProductFlow, ExteriorFlow, EntityNotFound, comp_dir
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
        return True

    def setup_bm(self, index=None):
        """
        Requires an index interface or catalog query <-- preferred
        :param index:
        :return:
        """
        if self._index is None:
            if index is None:
                self._index = self._archive.make_interface('index')
            else:
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
        for p in self._index.processes():
            for rx in p.references():
                if search_skip(p, search):
                    continue
                yield ProductFlow(self._archive.ref, rx.flow, rx.direction, p, None)

    def exterior_flows(self, direction=None, search=None, **kwargs):
        """
        Exterior flows are all flows that do not have interior terminations (i.e. not found in the index targets)
        Since contexts are still in limbo, we need a default directionality (or some way to establish directionality
        for compartments..) but for now let's just use default 'output' for all exterior flows
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
                dir = comp_dir(cx.sense)
                '''
                if self.is_elementary(f):
                    yield ExteriorFlow(self._archive.ref, f, 'Output', f['Compartment'])
                else:
                    yield ExteriorFlow(self._archive.ref, f, 'Output', None)
                '''
                yield ExteriorFlow(self._archive.ref, f, dir, cx)

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
            self._index.get(process)
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
        p = self._index.get(process)
        for x in p.inventory(ref_flow=ref_flow):
            yield x

    def sys_lci(self, node, demand, **kwargs):
        raise NotImplementedError

    def bg_lcia(self, process, query_qty, ref_flow=None, **kwargs):
        p = self._archive.retrieve_or_fetch_entity(process)
        lci = self.lci(p, ref_flow=ref_flow)
        res = query_qty.do_lcia(lci, locale=p['SpatialScope'], **kwargs)
        """
        if self.privacy > 0:
            return res.aggregate('*', entity_id=p.link)
        """
        return res
