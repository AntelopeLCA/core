import unittest
from synonym_dict.compartments.test_compartments import CompartmentContainer
from ..contexts import Context, ContextManager, InconsistentSense, ImmutableContextName, ProtectedTerm
from antelope.interfaces.iindex import InvalidSense
from ..lcia_engine.lcia_engine import DEFAULT_CONTEXTS, NUM_DEFAULT_CONTEXTS


class ContextTest(CompartmentContainer.CompartmentTest):
    def test_sense(self):
        c = Context('emissions to air', sense='sink')
        self.assertEqual(c.sense, 'Sink')
        with self.assertRaises(InvalidSense):
            Context('emissions to Mars', sense='extraterrestrial')
        with self.assertRaises(InconsistentSense):
            Context('resources from urban air', parent=c, sense='source')

    def test_elementary(self):
        c = Context('eMissions', sense='sink')
        c1 = Context('emissions to boot', parent=c)
        d = Context('emulsions', sense='sink')
        self.assertTrue(c1.elementary)
        self.assertFalse(d.elementary)

    def test_parent(self):
        c = Context('emissions', sense='sink')
        d = Context('emissions to air', parent=c)
        e = Context('emissions to urban air', parent=d)
        f = Context('emissions to rural air', 'emissions from high stacks', parent=d)
        self.assertSetEqual(set(i for i in d.subcompartments), {e, f})
        self.assertListEqual([str(k) for k in c.self_and_subcompartments], ['emissions', 'emissions to air',
                                                                            'emissions to rural air',
                                                                            'emissions to urban air'])
        self.assertEqual(e.sense, 'Sink')


class ContextManagerTest(CompartmentContainer.CompartmentManagerTest):
    def test_pruned_set_name_closure(self):
        with self.assertRaises(ImmutableContextName):
            super(ContextManagerTest, self).test_pruned_set_name_closure()

    def test_remove_child(self):
        self.skipTest('cannot rename contexts')

    def test_no_lingering_subcompartments(self):
        pass

    def _test_class(self, ignore_case=True):
        if ignore_case is False:
            self.skipTest('skipping case sensitive test')
        else:
            return ContextManager()

    def setUp(self):
        self.cm = ContextManager()

    _water_dict_objects = 3  # one higher since we start with both emissions + resources

    def test_add_from_dict(self):
        self._add_water_dict()
        self.assertEqual(str(self.cm['to water']), 'water emissions')
        self.assertEqual(self.cm['to water'].sense, 'Sink')

    def test_retrieve_by_tuple(self):
        self._add_water_dict()
        w = self.cm['to water']
        self.assertIs(w, self.cm[('emissions', 'water emissions')])

    def test_relative_add(self):
        self._add_water_dict()
        uw = self.cm['to water']
        ud = self.cm.add_compartments(['to water', 'lake water'])
        self.assertIs(uw, ud.parent)
        self.assertListEqual(ud.as_list(), ['Emissions', 'water emissions', 'lake water'])

    def test_merge_inconsistent_sense(self):
        d = [
            {
                "name": "to water",
                "parent": "Emissions"
            },
            {
                "name": "fresh water",
                "synonyms": [
                    "freshwater"
                ],
                "parent": "to water"
            },
            {
                "name": "ground-",
                "parent": "fresh water"
            },
            {
                "name": "from ground",
                "synonyms": [
                    "ground-",
                ],
                "parent": "Resources"
            }
        ]
        for k in d[:3]:
            self.cm._add_from_dict(k)
        with self.assertRaises(InconsistentSense):
            self.cm._add_from_dict(d[3])

    def _add_water_context(self):
        self._add_water_dict()
        c = self.cm.add_compartments(('to water', 'to groundwater'))
        self.cm.add_synonym(c, 'ground-')

    def test_unspecified(self):
        c = self.cm.add_compartments(['emissions', 'water', 'unspecified'])
        self.assertEqual(c.name, 'to water')
        self.assertIs(self.cm['to water, unspecified'], c)

    def test_retrieve_nonspecific(self):
        self.assertIs(self.cm.__getitem__('undefined'), self.cm._null_entry)
        self.assertIs(self.cm[None], self.cm._null_entry)

    def test_distinct_lineage(self):
        fc = self.cm.add_compartments(['fuels', 'coal', 'bituminous'])
        hc = self.cm.add_compartments(['heat', 'coal', 'pulverized'])  #  conflict='attach' is default
        self.assertIs(fc.parent, hc.parent)  # in a context manager, ambiguous specification can screw you up; 'rename' would avoid this

    def test_inconsistent_lineage(self):
        """
        context manager uses attach, then rename to avoid inconsistent lineage problems
        :return:
        """
        self._add_water_context()
        with self.assertRaises(InconsistentSense):
            self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='attach')
        fix = self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='rename')
        self.assertEqual(fix.name, 'from water, ground-')

    def test_inconsistent_lineage_match(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self.skipTest('Too hard to replicate this case in ContextManager')

    def test_inconsistent_lineage_rename(self):
        self._add_water_context()
        c = self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='rename')
        self.assertEqual(c.name, 'from water, ground-')

    def test_inconsistent_lineage_skip(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self._add_water_context()
        rw = self.cm.add_compartments(('resources', 'from water'))
        fw = self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='skip')

        self.assertIs(fw, rw)

    def test_elementary(self):
        self.assertEqual(len(list(self.cm.objects)), 2)
        n = self.cm.add_compartments(['elementary flows', 'emissions', 'air', 'urban air'])
        self.assertListEqual(n.as_list(), ['elementary flows', 'Emissions', 'to air', 'urban air'])
        self.assertTrue(n.elementary)
        self.assertIn(self.cm['Emissions'], self.cm['Elementary flows'].subcompartments)
        self.assertEqual(len(list(self.cm.objects)), 5)

    def test_skip_nonspecific_spec(self):
        ew = self.cm.add_compartments(['household items', 'furniture'])
        dw = self.cm.add_compartments(['furniture', 'unspecified', 'droll'])
        self.assertIs(dw.parent, ew)
        fw = self.cm.add_compartments(['furniture', 'unspecified', 'serious'])
        self.assertIs(dw.parent, fw.parent)
        self.assertIs(self.cm['furniture, unspecified'], ew)
        self.assertIs(self.cm.__getitem__('unspecified'), self.cm._null_entry)

    def test_protected(self):
        """
        Protected terms are 'air', 'water', and 'ground'- if these are added as subcompartments to compartments with
        non-None sense, they are modified to e.g. 'to air' or 'from air' appropriate to the sense.

        We cannot actually enforce ProtectedTerm because it breaks all data sources- but we should not add protected
        synonyms to existing contexts. thus they will be directionless- unless a hint is supplied
        Really these are disallowed on *Public Query* so they should not be used but do not need to be prevented
        :return:
        """
        # with self.assertRaises(ProtectedTerm):
        #     self.cm.add_compartments(['air'])
        k = self.cm.add_compartments(('emissions', 'to air'))
        self.cm.add_synonym(k, 'secret.data:air')
        g = Context('air')
        with self.assertRaises(KeyError):  # or ProtectedTerm??
            self.cm.__getitem__(g)
        g.add_origin('secret.data')
        self.assertIs(self.cm.find_matching_context(g), k)
        self.assertIs(self.cm[g], k)
        with self.assertRaises(KeyError):
            self.cm.__getitem__('air')

    def test_as_list(self):
        r_l = ['elementary flows', 'resources', 'water', 'subterranean']
        e_l = ['elementary flows', 'emissions', 'water', 'subterranean']
        r = self.cm.add_compartments(r_l)
        e = self.cm.add_compartments(e_l)
        self.assertEqual(r.name, 'subterranean')
        self.assertEqual(e.name, 'to water, subterranean')
        self.assertListEqual(r.as_list(), ['elementary flows', 'Resources', 'from water', 'subterranean'])
        self.assertListEqual(e.as_list(), ['elementary flows', 'Emissions', 'to water', 'subterranean'])

    def test_remote_merge_error(self):
        sub_g = ['resources', 'ground', 'subterranean']
        sub_w = ['resources', 'water', 'subterranean']
        gs = self.cm.add_compartments(sub_g)
        ws = self.cm.add_compartments(sub_w)
        self.assertEqual(gs.name, 'subterranean')
        self.assertEqual(ws.name, 'from water, subterranean')
        cxx = Context('from water')
        cxxx = Context('subterranean', parent=cxx)
        cxxx.add_origin('far.away')
        self.assertIs(self.cm.find_matching_context(cxxx), ws)


class DefaultContextsTest(unittest.TestCase):
    def setUp(self):
        self.cm = ContextManager(source_file=DEFAULT_CONTEXTS)

    def test_load(self):
        self.assertEqual(len(self.cm), NUM_DEFAULT_CONTEXTS)
        self.assertSetEqual({k.name for k in self.cm.top_level_compartments}, {'Emissions', 'Resources'})

    def test_matching_compartment(self):
        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('resources', 'water', 'CA', 'CA-QC'))
        fx.add_origin('dummy.test')
        cx = self.cm.find_matching_context(fx)
        self.assertEqual(cx.sense, 'Source')
        self.assertIs(cx.top(), self.cm['Resources'])
        self.assertListEqual(cx.as_list(), ['Resources', 'from water', 'CA', 'CA-QC'])  # additional information added
        self.assertIs(self.cm['dummy.test:CA-QC'], cx)

    def test_matching_gap_compartment(self):
        """
        this should remove the superfluous "liquid resources" from the canonical context
        :return:
        """
        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('resources', 'liquid resources', 'water', 'CA', 'CA-QC'))
        fx.add_origin('dummy.other')
        cx = self.cm.find_matching_context(fx)
        self.assertEqual(cx.sense, 'Source')
        self.assertIs(cx.top(), self.cm['Resources'])
        self.assertListEqual(cx.as_list(), ['Resources', 'from water', 'CA', 'CA-QC'])  # superfluous information added
        self.assertIs(self.cm['dummy.other:CA-QC'], cx)

    def test_context_hint(self):
        self.cm.add_context_hint('dummy.test', 'air', 'to air')
        tgt = self.cm['to air']
        self.assertIs(self.cm['dummy.test:air'], tgt)

    def test_prepend_elementary(self):
        """
        By default, emissions and resources are top-level. but if client data uses 'elementary flows' category,
        it should be installed above via 'attach'
        :return:
        """
        tw = self.cm.add_compartments(['Emissions', 'to water', 'fresh water'])
        self.assertEqual(len(tw.seq), 3)
        ew = self.cm.add_compartments(['Elementary flows', 'fresh water'])
        self.assertIs(ew, tw)
        self.assertEqual(ew.top().name, 'Elementary flows')

    '''
    def test_matching_sublineage_alt(self):
        """
        This was originally going to test the disregarded functionality- only to realize that does not get triggered
        by find_matching_context.. without that, it is more or less the same as the test below it
        :return:
        """
        self.cm.add_context_hint('dummy.test', 'air', 'to air')
        tgt = self.cm['to air']
        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('Elementary Flows', 'NETL Coal Elementary Flows', 'air', 'unspecified'))
        fx.add_origin('dummy.test')
        self.assertEqual(fx.name, 'air, unspecified')  # just throwing this in there
        self.assertIs(self.cm.find_matching_context(fx), tgt)
        # self.assertIn('Elementary Flows', self.cm.disregarded_terms)
    '''

    def test_matching_sublineage_with_hint(self):
        tgt = self.cm['from ground']

        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('Elementary Flows', 'NETL Coal Elementary Flows', 'NETL Elementary Flows',
                                          ' [Resources] ', 'ground'))
        # context gets its origin added when used in a Characterization or an Exchange- for now we do it manually
        fx.add_origin('dummy.test')

        self.assertIs(self.cm.find_matching_context(fx), self.cm._null_entry)

        self.cm.add_context_hint('dummy.test', '[resources]', 'Resources')
        self.assertIs(self.cm['dummy.test:[resources]'], tgt.parent)

        self.assertIs(self.cm.find_matching_context(fx), tgt)
        self.assertIs(self.cm['dummy.test:ground'], tgt)

    def test_match_tuple(self):
        res = self.cm['Resources']
        ing = self.cm['in ground']  # the true name is 'from ground'; 'in ground' is a synonym
        self.assertIs(ing.parent, res)
        self.assertIs(self.cm['Resources', 'in ground'], ing)


if __name__ == '__main__':
    unittest.main()