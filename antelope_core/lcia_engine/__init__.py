from ..archives import Qdb, REF_QTYS
from .lcia_engine import LciaEngine, DEFAULT_CONTEXTS, DEFAULT_FLOWABLES
from antelope import QuantityRef, FlowInterface

import os

IPCC_2007_GWP = os.path.join(os.path.dirname(__file__), 'data', 'ipcc_2007_gwp.json')


class LciaDb(Qdb):
    """
    Augments the Qdb with an LciaEngine instead of a TermManager
    """
    @classmethod
    def new(cls, source=REF_QTYS, **kwargs):
        lcia = LciaEngine(**kwargs)
        qdb = cls.from_file(source, term_manager=lcia, quiet=True)
        # long overdue
        lcia.add_characterization('Water', lcia.get_canonical('volume'), lcia.get_canonical('mass'), value=1000.0)
        return qdb

    def _add_char(self, flow, q, v):
        """
        For LciaDb, all factors loaded from JSON become local-- let's see if this causes any problems
        :param flow:
        :param q:
        :param v:
        :return:
        """
        self.tm.add_characterization(flow.link, flow.reference_entity, q, v, context=flow.context,
                                     origin=self.ref)

    '''
    def _ref_to_key(self, key):
        """
        LciaDb uses links as keys so as to store different-sourced versions of the same quantity. But we also want
        to find local entities by external ref- so if they come up empty we try prepending local origin.
        of course that won't work basically ever, since none of the canonical quantities have local origin.
        so this may require some tuning.
        :param key:
        :return:
        """
        key = super(LciaDb, self)._ref_to_key(key)
        if key is None:
            key = super(LciaDb, self)._ref_to_key('%s/%s' % (self.ref, key))
        return key
    '''

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)
        (note that _get_entity does not get contexts)

        :param item:
        :return:
        """
        if hasattr(item, 'link'):
            item = item.link
        return super(LciaDb, self).__getitem__(item)

    def _ensure_valid_refs(self, entity):
        if entity.origin is None:
            entity.origin = self.ref
            # raise AttributeError('Origin not set! %s' % entity)
        super(LciaDb, self)._ensure_valid_refs(entity)

    def add(self, entity):
        """
        Add entity to archive, by link instead of external ref. If the entity has a uuid and uuid does not already
        exist, add it.  If the UUID does already exist, warn.
        :param entity:
        :return:
        """
        self._add(entity, entity.link)
        self._add_to_tm(entity)

    def _add_to_tm(self, entity, merge_strategy=None):
        if entity.entity_type == 'quantity':
            if entity.is_lcia_method:
                ind = entity['Indicator']
            else:
                ind = None
            if entity.is_entity and not entity.configured:  # local db is authentic source - do not masquerade
                # print('LciaDb: Adding real entity %s' % entity.link)
                q_masq = QuantityRef(entity.external_ref, self.query, entity.reference_entity,
                                     Name=entity['Name'], Indicator=ind)  # WHY am I not using entity.make_ref() ??
                entity.set_qi(self.make_interface('quantity'))
            else:
                if entity.has_lcia_engine():  # ready to go
                    """
                    These quantities will not be managed by the local LciaDb-- neither will access the other's value.
                    It seems like we may still want to override whether a particular quantity gets masqueraded
                    it's easy enough to do by giving the entity a property, but that is obviously sloppy. TBD.
                    """
                    self.tm.add_quantity(entity)
                    return

                else: # ref -- masquerade
                    # print('LciaDb: Adding qty ref %s' % entity)
                    q_masq = QuantityRef(entity.external_ref, self.query, entity.reference_entity,
                                         masquerade=entity.origin,
                                         Name=entity['Name'], Indicator=ind)

            for k in entity.properties():  # local only for ref
                q_masq[k] = entity[k]

            # print('LciaDb: Adding masquerade %s' % q_masq)
            self.tm.add_quantity(q_masq)
            cnncl = self.tm.get_canonical(q_masq)
            # assert self.tm.get_canonical(q_masq) is q_masq, 'impostor:%s\noriginal:%s' % (self.tm.get_canonical(q_masq),
            #                                                                               q_masq)
            self.tm.add_quantity(entity)  # should turn up as a child
            assert self.tm.get_canonical(entity) is cnncl, 'child:%s\n masq:%s[%s]' % (self.tm.get_canonical(entity),
                                                                                       cnncl, cnncl.link)

            if not entity.is_entity:  # not local -- local ones were already imported
                # print('LciaDb: Importing factors')
                self.tm.save_for_later(entity)

        elif isinstance(entity, FlowInterface):
            self.tm.add_flow(entity, merge_strategy=merge_strategy)

    def _serialize_quantities(self, domesticate=False):
        """
        Do not save masqueraded quantities
        :param domesticate:
        :return:
        """
        return sorted([q.serialize(domesticate=domesticate, drop_fields=self._drop_fields['quantity'])
                       for q in self.entities_by_type('quantity') if q.is_entity],
                      key=lambda x: x['externalId'])
