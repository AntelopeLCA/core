"""
autorange.py

Scales numerical values by powers of 1000 to present engineering notation. Also scales metric prefixes to match.
"""
from math import log10


metric_offsets = {
    'a': -6,
    'f': -5,
    'p': -4,
    'n': -3,
    'u': -2,
    'm': -1,
    'k': 1,
    'M': 2,
    'G': 3,
    'T': 4,
    'P': 5,
    'E': 6,
    'Z': 7
}


numerals = ['', 'trillion', 'billion', 'million', 'thousand']  # 0 = nothing -1 = thousand


metric_prefixes = dict([(v, k) for k, v in metric_offsets.items()])
metric_prefixes[0] = ''


DISALLOW = {'mol', 'MT', 'PM'}  # units that start with these phrases should NOT use inferred prefix


class AutoRange(object):
    def _set_shift(self):
        self._shift = 0
        r = log10(self._range)
        while True:
            if r < self.bias:
                r += 3
                self._shift += 1
            elif r > 3 + self.bias:
                r -= 3
                self._shift -= 1
            else:
                return

    def __init__(self, rng, bias=0, kg_to_t=True, disallow_prefix=None):
        """
        creates an object for computing auto-ranged values.  The input argument should be the largest value that
        is expected to appear in the context.  It will be ranged to fall between 0-1000 (absolute value); this
        can be adjusted with the 'bias' param: one order per integer value up (pos) or down (neg)
        :param rng: either a scalar (abs) or an iterable (max(abs))
        :param bias: [0] default range is 0-1000.  bias=1: 10-10,000; bias=-1: 0.1-100; etc; floats are operable
        :param kg_to_t: [True] correct kg to t in cases where the results are scaled up
        example: with kg_to_t = True, a unit of 'kg' autoranged to 'Mg' or larger will be converted to 't',
        'Gg' to 'kt', etc, but a unit of 'kg' or smaller will not
        with kg_to_t False, no alteration is performed
        :param disallow_prefix: preifx(es) to add to DISALLOW ('MT', 'mol', 'PM'). can be string or iterable.
        """
        self.bias = bias
        self.disallow_prefix = set(DISALLOW)
        if disallow_prefix:
            if isinstance(disallow_prefix, str):
                self.disallow_prefix.add(disallow_prefix)
            else:
                for d in disallow_prefix:
                    self.disallow_prefix.add(d)

        try:
            self._range = max(abs(k) for k in rng)
        except TypeError:
            self._range = abs(rng)
        if self._range == 0:
            print('Warning: autorange initialized with 0 range; using 1')
            self._range = 1
        self._shift = 0
        self._set_shift()
        self.kg_to_t = kg_to_t

    @property
    def kg_to_t(self):
        return self._kg_to_t

    @kg_to_t.setter
    def kg_to_t(self, value):
        self._kg_to_t = bool(value)

    @property
    def scale(self):
        return 10 ** (3 * self._shift)

    def adjust(self, value):
        return value * self.scale

    def disallow(self, prefix):
        self.disallow_prefix.add(prefix)
        return self.disallow_prefix

    @property
    def numeral(self):
        """
        report scaling factor as multiplicative numeral
        :return:
        """
        if self._shift < 0:
            return numerals[self._shift]  # counting from end
        return ''

    def adj_unit(self, unit):
        """
        This is the tricky, ad-hoc part.  Assume the first character in the unit string is a prefix IF it is found
        in the metric prefixes dictionary.  If it is not, assume it is unprefixed.
        :param unit: a unit string
        :return:
        """
        prefix = unit[0]

        if any([unit.startswith(x) for x in self.disallow_prefix]) or prefix not in metric_offsets:
            pre_shift = 0
            adj = unit
        else:
            pre_shift = metric_offsets[prefix]
            adj = unit[1:]

        post_shift = pre_shift - self._shift

        if unit.startswith('kg') and post_shift >= 2 and self.kg_to_t:
            post_shift -= 2
            adj = 't' + unit[2:]

        return metric_prefixes[post_shift] + adj
