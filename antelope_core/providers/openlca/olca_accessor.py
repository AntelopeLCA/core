import csv
import os

from pydantic import BaseModel
from typing import Dict, Optional

from zipfile import ZipFile
from io import TextIOWrapper


class UnitGroup(BaseModel):
    name: str
    ref_unit: str
    conversion: Dict[str, float]  # my funky inverted UnitConversion dictionaries
    # (where every entry corresponds to a unit of the reference quantity)


class Location(BaseModel):
    uuid: str
    name: str
    description: Optional[str]
    code: str
    latitude: float
    longitude: float

    @classmethod
    def from_csv_dict(cls, record):
        return cls(uuid=record['ID'], name=record['Name'], description=record['Description'], code=record['Code'],
                   latitude=float(record['Latitude']), longitude=float(record['Longitude']))


class OlcaAccessorV2(object):
    """
    To access the reformed GreenDelta/data repository
    """
    def __init__(self, olca_root):
        self._root = olca_root
        if os.path.isdir(olca_root):
            self._zip = None
        else:
            self._zip = ZipFile(olca_root)

    @property
    def refdata(self):
        if self._zip:
            return 'refdata'
        return os.path.join(self._root, 'refdata')

    @property
    def lcia_factors(self):
        return os.path.join(self.refdata, 'lcia_factors')

    def get_refdata(self, ref_file):
        f = os.path.join(self.refdata, ref_file)
        if not f.endswith('.csv'):
            f += '.csv'
        if self._zip:
            with self._zip.open(f) as fp:
                wrapper = TextIOWrapper(fp, encoding='utf-8', newline='')
                d_r = csv.DictReader(wrapper, delimiter=",")
                data = list(d_r)
        else:
            with open(f) as fp:
                d_r = csv.DictReader(fp, delimiter=",")
                data = list(d_r)
        return data

    def read_unit_groups(self):
        """
        This returns a DICT whose KEYS are the Unit Group Name, and whose VALUES are UnitGroup objects
        That dict is then accessed later on
        :return:
        """
        ugs = self.get_refdata('unit_groups')
        us = self.get_refdata('units')

        unit_groups = {ug['Name']: UnitGroup(name=ug['Name'], ref_unit=ug['Reference unit'], conversion=dict())
                       for ug in ugs}

        # first we construct the U/C dicts as they are specified
        for u in us:
            ug = unit_groups[u['Unit group']]
            factor = float(u['Conversion factor'])
            ug.conversion[u['Name']] = factor
            for syn in u['Synonyms'].split(';'):
                ug.conversion[syn] = factor

        # then we invert them to be funky like ours
        for ug in unit_groups.values():
            ref = ug.conversion[ug.ref_unit]
            for k in list(ug.conversion.keys()):
                ug.conversion[k] = ref / ug.conversion.pop(k)

        return unit_groups

    def read_locales(self):
        """
        locations.csv has ID,Name,Description,Category,Code,Latitude,Longitude columns, but:
        - "Category" is empty for all records
        - "ID" we're skipping
        - "Latitude" and "Longitude" we have no use for at the moment
        - and we don't need "Description" either. but let's just keep em all.
        :return:
        """
        ls = self.get_refdata('locations')
        return {l['Code']: Location.from_csv_dict(l) for l in ls}

    def collate_lcia_categories(self):
        dd = dict()
        for row in self.get_refdata('lcia_method_categories'):
            if row['LCIA method'] not in dd:
                dd[row['LCIA method']] = []
            dd[row['LCIA method']].append(row['LCIA category'])
        return dd

    def factors_for_method(self, q):
        """
        LCIA category,Flow,Flow property,Flow unit,Location,Factor
        :param q:
        :return:
        """
        f = None
        i = 0
        l_count = 0
        while f is None:
            i += 1
            cand = os.path.join(self.lcia_factors, q.uuid[:i] + '.csv')
            try:
                self._zip.getinfo(cand)
                f = cand
            except KeyError:
                pass
            if i > 10:
                raise FileNotFoundError(cand)

        if self._zip:
            with self._zip.open(f) as fp:
                wrapper = TextIOWrapper(fp, encoding='utf-8', newline='')
                d_r = csv.DictReader(wrapper, delimiter=",")
                for row in d_r:
                    if row['Location']:
                        l_count += 1
                    yield row
        else:
            with open(f) as fp:
                d_r = csv.DictReader(fp, delimiter=",")
                for row in d_r:
                    if row['Location']:
                        l_count += 1
                    yield row

        if l_count:
            print('method %s: %d non-null Locations' % (q.uuid, l_count))
