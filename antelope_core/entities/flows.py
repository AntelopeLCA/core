from __future__ import print_function, unicode_literals
import uuid

from antelope_interface import Flow, CONTEXT_STATUS_
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

    def chk_char(self, *args):
        raise KeyError

    '''
    def profile(self):
        print('%s' % self)
        out = []
        for cf in self._characterizations.values():
            print('%2d %s' % (len(out), cf.q_view()))
            out.append(cf)
        return out

    def add_characterization(self, quantity, reference=False, value=None, overwrite=False, **kwargs):
        """

        :param quantity: entity or catalog ref
        :param reference: [False] should this be made the flow's reference quantity
        :param value:
        :param kwargs: location, origin
        value + location + optional origin make a data tuple
        :param overwrite: [False] if True, allow values to replace existing characterizations
        :return:
        """
        'x'x'x # we no longer want to be able to add literal characterizations. Just do it explicitly.
        if isinstance(quantity, Characterization):
            if quantity.flow.reference_entity != self.reference_entity:
                adj = self.cf(quantity.flow.reference_entity)
                if adj == 0:
                    raise MissingFactor('%s' % quantity.flow.reference_entity)
            else:
                adj = 1.0

            for l in quantity.locations():
                self.add_characterization(quantity.quantity, reference=reference,
                                          value=quantity[l] / adj, location=l, origin=quantity.origin[l])
            return
        'x'x'x
        if reference:
            if value is not None and value != 1.0:
                raise ValueError('Reference quantity always has unit value')
            value = 1.0
            self._set_reference(quantity)

        q = quantity.uuid
        if q in self._characterizations.keys():
            if value is None:
                return
            c = self._characterizations[q]
        else:
            c = Characterization(self, quantity)
            self._characterizations[q] = c
        if value is not None:
            if isinstance(value, dict):
                c.update_values(**value)
            else:
                c.add_value(value=value, overwrite=overwrite, **kwargs)
        try:
            quantity.register_cf(c)
        except FlowWithoutContext:
            pass  # add when the flow is contextualized
        return c

    def has_characterization(self, quantity, location='GLO'):
        """
        A flow keeps track of characterizations by link
        :param quantity:
        :param location:
        :return:
        """
        if quantity.uuid in self._characterizations.keys():
            if location == 'GLO' or location is None:
                return True
            if location in self._characterizations[quantity.uuid].locations():
                return True
        return False

    def del_characterization(self, quantity):
        if quantity is self.reference_entity:
            raise RefQuantityError('Cannot delete reference quantity')
        c = self._characterizations.pop(quantity.uuid)
        c.quantity.deregister_cf(c)

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    def factor(self, quantity):
        if quantity.uuid in self._characterizations:
            return self._characterizations[quantity.uuid]
        return Characterization(self, quantity)

    def cf(self, quantity, locale='GLO'):
        """
        These are backwards.  cf should return the Characterization ; factor should return the value.  instead, it's
        the other way around.
        :param quantity:
        :param locale: ['GLO']
        :return: value of quantity per unit of reference, or 0.0
        """
        if quantity.uuid in self._characterizations:
            try:
                return self._characterizations[quantity.uuid][locale]
            except KeyError:
                return self._characterizations[quantity.uuid].value
        return 0.0

    def convert(self, val, to=None, fr=None, locale='GLO'):
        """
        converts the value (in
        :param val:
        :param to: to quantity
        :param fr: from quantity
        :param locale: cfs are localized to unrestricted strings babee
        the flow's reference quantity is used if either is unspecified
        :return: value * self.char(to)[loc] / self.char(fr)[loc]
        """
        out = self.cf(to or self.reference_entity, locale=locale)
        inn = self.cf(fr or self.reference_entity, locale=locale)
        return val * out / inn
    '''
