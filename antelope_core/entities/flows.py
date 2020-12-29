from __future__ import print_function, unicode_literals
import uuid

from antelope import Flow, CatalogRef, CONTEXT_STATUS_
from .entities import LcEntity
# from lcatools.entities.quantities import LcQuantity


class RefQuantityError(Exception):
    pass


def new_flow(name, ref_quantity, cas_number='', comment='', context=None, compartment=None, external_ref=None, **kwargs):
    if CONTEXT_STATUS_ == 'compat' and compartment is None:
        if context is None:
            compartment = []
        else:
            compartment = context.as_list()

    kwargs['CasNumber'] = kwargs.pop('CasNumber', cas_number)
    kwargs['Comment'] = kwargs.pop('Comment', comment)
    kwargs['Compartment'] = kwargs.pop('Compartment', compartment)

    if external_ref is None:
        return LcFlow.new(name, ref_quantity, **kwargs)
    return LcFlow(external_ref, Name=name, ReferenceQuantity=ref_quantity, **kwargs)


class LcFlow(LcEntity, Flow):

    _ref_field = 'referenceQuantity'
    _new_fields = ['CasNumber']  # finally abolishing the obligation for the flow to have a Compartment

    @classmethod
    def new(cls, name, ref_qty, **kwargs):
        """
        :param name: the name of the flow
        :param ref_qty: the reference quantity
        :return:
        """
        u = uuid.uuid4()
        return cls(str(u), Name=name, entity_uuid=u, ReferenceQuantity=ref_qty, **kwargs)

    def __setitem__(self, key, value):
        self._catch_context(key, value)
        self._catch_flowable(key.lower(), value)
        super(LcFlow, self).__setitem__(key, value)

    @LcEntity.origin.setter
    def origin(self, value):  # pycharm lint is documented bug: https://youtrack.jetbrains.com/issue/PY-12803
        LcEntity.origin.fset(self, value)
        self._flowable.add_term(self.link)

    def __init__(self, external_ref, **kwargs):
        super(LcFlow, self).__init__('flow', external_ref, **kwargs)

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''
        self._chars_seen = dict()

    def make_ref(self, query):
        if self._query_ref is None:
            d = dict()
            for k in self.signature_fields():
                if k == self._ref_field:
                    continue
                if k in self._d:
                    d[k] = self._d[k]
            self._query_ref = CatalogRef.from_query(self.external_ref, query, self.entity_type,
                                                    uuid=self.uuid, **d)
            self._query_ref.context = self.context
            for k, v in self._chars_seen.items():
                self._query_ref.see_char(*k, v)

        return self._query_ref


    def __str__(self):
        cas = self.get('CasNumber')
        if cas is None:
            cas = ''
        if len(cas) > 0:
            cas = ' (CAS ' + cas + ')'
        context = '[%s]' % ';'.join(self.context)
        return '%s%s %s' % (self.get('Name'), cas, context)

    def characterize(self, quantity, value, context=None, **kwargs):
        if context is None:
            context = self.context
        flowable = self.name
        return quantity.characterize(flowable, self.reference_entity, value, context=context, origin=self.origin,
                                     **kwargs)

    def cf(self, quantity, **kwargs):
        return quantity.cf(self, **kwargs)

    def see_char(self, qq, cx, loc, qrr):
        self._chars_seen[qq, cx, loc] = qrr
        if self._query_ref is not None:
            self._query_ref.see_char(qq, cx, loc, qrr)

    def chk_char(self, qq, cx, loc):
        return self._chars_seen[qq, cx, loc]
