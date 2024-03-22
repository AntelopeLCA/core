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
    return LcFlow(external_ref, Name=name, ReferenceQuantity=ref_quantity, context=context, **kwargs)


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
        self._catch_flowable(key.lower(), value) or self._catch_context(key, value)
        super(LcFlow, self).__setitem__(key, value)

    @LcEntity.origin.setter
    def origin(self, value):  # pycharm lint is documented bug: https://youtrack.jetbrains.com/issue/PY-12803
        LcEntity.origin.fset(self, value)
        self._flowable.add_term(self.link)

    def __init__(self, external_ref, is_co2=None, **kwargs):
        if is_co2:
            self.is_co2 = True
        super(LcFlow, self).__init__('flow', external_ref, **kwargs)

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''

        if self.reference_entity is None:
            print('Warning: no reference quantity for flow %s' % external_ref)

    def _make_ref(self, query):
        query_ref = super(LcFlow, self)._make_ref(query)
        query_ref.context = self.context
        for k, v in self._chars_seen.items():
            query_ref._chars_seen[k] = v  # this is hacky obv

        return query_ref

    def __str__(self):
        cas = self.get('CasNumber')
        if cas is None:
            cas = ''
        if len(cas) > 0:
            cas = ' (CAS ' + cas + ')'
        context = '[%s]' % ';'.join(self.context)
        return '%s%s %s' % (self.get('Name'), cas, context)

    def characterize(self, quantity, value, context=None, origin=None, location='GLO', **kwargs):
        if context is None:
            context = self.context
        flowable = self.name
        if origin is None:
            origin = self.origin
        self.pop_char(quantity, context, location)
        return quantity.characterize(flowable, self.reference_entity, value, context=context, origin=origin,
                                     location=location, **kwargs)

    def get_context(self):
        return self.context

    def cf(self, quantity, **kwargs):
        if quantity.entity_type == 'quantity':
            return quantity.cf(self, **kwargs)
        elif quantity.entity_type == 'flow':
            return quantity.reference_entity.cf(self, **kwargs)
        else:
            raise TypeError('Invalid argument %s' % quantity)

    '''
    This is now done in Flow interface
    def see_char(self, qq, cx, loc, qrr):
        self._chars_seen[qq, cx, loc] = qrr
        if self._query_ref is not None:
            self._query_ref.see_char(qq, cx, loc, qrr)

    def chk_char(self, qq, cx, loc):
        return self._chars_seen[qq, cx, loc]

    def pop_char(self, qq, cx, loc):
        return self._chars_seen.pop((qq, cx, loc), None)
    '''
