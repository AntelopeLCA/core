"""
The purpose of this file is to enable dynamic updates to the flowables list.
This file contains two components:

 * discovered_synonyms a list of tuples, where each tuple is a set of synonymous terms

 * update_default_flowables(), a function that loads the default flowables file, applies discovered synonyms,
   and then saves the updated file.

"""
import os
from synonym_dict import MergeError, FlowablesDict


DEFAULT_FLOWABLES = os.path.join(os.path.dirname(__file__), 'flowables.json')

discovered_synonyms = [
    ('Zinc(ii)', 'Zinc, ion'),
    ('Copper(ii)', 'Copper (II)'),
    ('Vanadium(v)', 'Vanadium, ion'),
    ('Nickel(ii)', 'Nickel, ion'),
    ('Silver(i)', 'Silver, ion'),
    ('Tin(ii)', 'Tin, ion'),
    ('sulfur oxides', 'Sulphur oxides'),
    ('sulfur dioxide', 'sulphur dioxide'),
    ('sulfur trioxide', 'sulphur trioxide'),
    ('sulfuric acid', 'sulphuric acid'),
    ('hydrogen sulfide', 'hydrogen sulphide'),
    ('pm2.5', 'Particulate matter, ≤ 2.5μm', 'Dust (PM2.5)'),
    ('pm10', 'Particulate matter, ≤ 10μm'),
    # ('pm10', 'Particulate matter', 'Dust (> PM10)'),  # pm >10 should not be the same as pm10
    ('pm10', 'Particulate matter, > 2.5μm and ≤ 10μm', 'Dust (PM2.5 - PM10)', 'Particulates, > 2.5 um, and < 10um'),
    ('Ethane, 1,1,2-trichloro-1,2,2-trifluoro-, CFC-113', 'cfc-113'),
    ('Ethane, 1,2-dichloro-1,1,2,2-tetrafluoro-, CFC-114', 'cfc-114'),
    ('Methane, bromodifluoro-, Halon 1201', 'halon 1201'),
    ('Methane, bromochlorodifluoro-, Halon 1211', 'halon 1211'),
    ('Ethane, 1-chloro-1,1-difluoro-, HCFC-142b', 'hcfc-142b'),
    ('Ethane, chloropentafluoro-, CFC-115', 'cfc-115'),
    ('Ethane, 1,2-dibromotetrafluoro-, Halon 2402', 'halon 2402'),
    ('Methane, bromotrifluoro-, Halon 1301', 'halon 1301'),
    ('Methane, dibromodifluoro-, Halon 1202', 'halon 1202'),
    ('Ethane, 1,1-dichloro-1-fluoro-, HCFC-141b', 'hcfc-141b'),
    ('Ethane, 1-chloro-1,1-difluoro-, HCFC-142b', 'hcfc-142b'),
    ('Ethane, 2,2-dichloro-1,1,1-trifluoro-, HCFC-123', 'hcfc-123'),
    ('Ethane, 2-chloro-1,1,1,2-tetrafluoro-, HCFC-124', 'hcfc-124', 'chlorotetrafluoroethane',
     'R 124 (chlorotetrafluoroethane)'),
    ('Methane, chlorodifluoro-, HCFC-22', 'hcfc-22'),
    ('460-73-1', 'R 245fa (1,1,1,3,3-Pentafluoropropane)'),
    ('354-33-6', 'R 125 (pentafluoroethane)'),
    ('430-66-0', 'R 143 (trifluoroethane)'),
    ('76-16-4', 'R 116 (hexafluoroethane)'),
    ('Methane, tetrachloro-, R-10', 'tetrachloromethane', 'Carbon tetrachloride (tetrachloromethane)'),
    ('Methane, tetrafluoro-, R-14', 'tetrafluoromethane'),
    ('Methane, trichlorofluoro-, CFC-11', 'CFC-11'),
    ('75-10-5', 'R 32 (difluoromethane)', 'difluoromethane'),
    ('75-46-7', 'trifluoromethane', 'R 23 (trifluoromethane)'),
    ('75-09-2', 'methylene chloride', 'Dichloromethane (methylene chloride)'),
    ('75-43-4', 'R 21 (Dichlorofluoromethane)', 'Dichlorofluoromethane'),
    ('74-87-3', 'Chloromethane (methyl chloride)'),
    ('811-97-2', 'tetrafluoroethane'),
    ('75-37-6', 'R 152a (difluoroethane)'),
    ('Propane, 1,3-dichloro-1,1,2,2,3-pentafluoro-, HCFC-225cb', 'hcfc-225cb', 'hcfc225cb', 'hcfc 225cb'),
    ('Propane, 3,3-dichloro-1,1,1,2,2-pentafluoro-, HCFC-225ca', 'hcfc-225ca', 'hcfc225ca', 'hcfc 225ca'),
    ('Cyclohexane, pentyl-', 'pentyl cyclohexane'),
    ('75-65-0', 't-Butyl alcohol'),
    ('622-96-8', 'para-Ethyltoluene'),
    ('106-42-3', 'para-xylene'),
    ('75-00-3', 'monochloroethane'),
    ('611-14-3', 'ortho-ethyltoluene'),
    ('Nitric oxide', 'nitrogen monoxide'),
    ('Biological oxygen demand', 'biological oxygen demand (BOD)'),
    ('Chemical oxygen demand', 'chemical oxygen demand (COD)'),
    ('Nitrous oxide', 'Nitrous oxide (laughing gas)'),
    ('chloroform', 'Trichloromethane (chloroform)')
]


def update_default_flowables(merge=True, save=True):
    fd = FlowablesDict()
    fd.load(DEFAULT_FLOWABLES)
    l = len(fd)
    for terms in discovered_synonyms:
        try:
            fd.new_entry(*terms)
        except MergeError:
            if merge:
                dom = next(t for t in terms if t in fd)
                for t in terms:
                    if t in fd:
                        fd.merge(dom, t)
                    else:
                        fd.add_synonym(dom, t)
            else:
                print('Unable to add: %s' % terms)
    ll = len(fd)
    if ll != l:
        print('Net change %+d terms' % (ll-l))
    if save:
        fd.save(DEFAULT_FLOWABLES)
        print('Saved to %s' % DEFAULT_FLOWABLES)


if __name__ == '__main__':
    update_default_flowables()
