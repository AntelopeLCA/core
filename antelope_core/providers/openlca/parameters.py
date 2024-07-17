import ast
from collections import deque
from synonym_dict import LowerDict  # apparently Andreas does not check case

import logging

from .schema_mapping import OLCA_MAPPING
import re


class NodeIdentifier(ast.NodeVisitor):
    """
    identifies all the parameters named in an expression
    """
    def __init__(self):
        self.names = list()

    def visit_Name(self, node: ast.Name):
        if node.id not in self.names:
            self.names.append(node.id)
        self.generic_visit(node)


class NodeCalculator(ast.NodeTransformer):
    """
    substitutes constant values in for parameter names, and evaluates the expression
    """
    def __init__(self, values):
        self.values = values

    def visit_Name(self, node: ast.Name):
        if node.id in self.values:
            new_node = ast.Constant(self.values[node.id])
            return ast.copy_location(new_node, node)
        return node


class _Param(object):
    """
    Creates a param from a dictionary spec.
    At this level, the only required dictionary key is 'name'. optional 'unit' can also be overwritten.
    """
    _mkr = '_#'
    _unit = None

    def __init__(self, param):
        self._param = param

    @property
    def name(self):
        return self._param['name'].strip()

    @property
    def unit(self):
        if self._unit:
            return self._unit
        return self._param.get('unit')

    @unit.setter
    def unit(self, value):
        if self._unit:
            if value is None:
                self._unit = None
            elif str(value) != self._unit:
                raise ValueError('Unit %s conflicts with existing value %s' % (value, self._unit))
        else:
            self._unit = str(value)

    @property
    def level(self):
        return 0

    @property
    def dependencies(self):
        yield self.name

    def __str__(self):
        return '%s[%02d] %s' % (self._mkr, self.level, self.name)


class ParamConstant(_Param):
    """
    An input.  'param' API:
    dict. keys: 'name', 'value' (a float), 'unit' (an optional string)
    """
    @property
    def _mkr(self):
        if self._set is None:
            return ' *'
        return ' $'

    _set = None

    def set(self, value):
        self._set = float(value)

    def unset(self):
        self._set = None

    @property
    def value(self):
        if self._set is not None:
            return self._set
        return self._param['value']


class ConditionalParam(object):
    """
    Evaluate an expression, and return a if true, b if false
    this is nuts--- they're even nested.
    ChatGPT tells me I could use simple regexes to change e.g. IF(expr1; expr2; expr3) to (expr2 if expr1 else expr3)
    but trying to handle nested expressions with regexp seems foolhardy.

    I mean, look at this:
    IF(auto_calc_wtr=1;IF(0.474454781149916-(pct_fresh_srf-0.167302119871893)*(0.474454781149916*1/(0.358243098978191+0.474454781149916))>0.0424528301886793;0.474454781149916-(pct_fresh_srf-0.167302119871893)*(0.474454781149916*1/(0.358243098978191+0.474454781149916));0.0424528301886793);usr_fresh_gnd)

    """
    def __init__(self, name, conditional, if_true, if_false):
        self._name = name
        self._cond = conditional
        self._if_true = if_true
        self._if_false = if_false


class FormulaParser(_Param):
    """
    give it a parameter spec; it will act as a dynamic calculator.
    'param' api: dict with keys 'name' (str), 'value' (float), 'formula' (str with elementary operations + groups)
    """
    _mkr = '  '
    _faulty = False

    def __init__(self, engine, param):
        super(FormulaParser, self).__init__(param)
        self._engine = engine
        self._ni = NodeIdentifier()
        try:
            self._ni.visit(ast.parse(self.formula, mode='eval'))
        except SyntaxError:
            logging.error('(%s) %s - Unsupported formula\n%s' % (self._engine.process_ref,
                                                                 self.name,
                                                                 self.formula))
            self._faulty = True
            self._mkr = 'XX'

    @property
    def formula(self):
        if self._faulty:
            return str(self._param['value'])
        form = self._param['formula']
        if form.find('^') > 0:
            pattern = r"(\s|\b)(\^)(\s|\b)"
            form = re.sub(pattern, r'\1**\3', form)
        return form

    @property
    def variables(self):
        for k in self._ni.names:
            yield k

    @property
    def dependencies(self):
        """
        Generates input variables that are used to calculate the parameter value
        :return:
        """
        ys = set()
        for k in self.variables:
            for dk in self._engine[k].dependencies:
                if dk not in ys:
                    yield dk
                    ys.add(dk)

    @property
    def value(self):
        d = {k: self._engine.value(k) for k in self.variables}
        nc = NodeCalculator(d)
        transformed = nc.visit(ast.parse(self.formula, mode='eval'))
        c = compile(transformed, filename="foo", mode='eval')
        result = eval(c)
        if self._engine.noisy:
            print('%.5g %s' % (result, self))
        return result

    @property
    def level(self):
        return max(self._engine[k].level for k in self.variables) + 1


class OlcaParameterResolver(object):
    """
    A class to handle and compute parameters in the OLCA framework.
    Ultimately we want to be able to represent computed parameters involving sums as balance flows
    (and products as child flows)
    """
    def __init__(self, process_json, noisy=False, v2=True, process_ref=None):
        """
        Supply the json object.  will:
         - extract input parameters
         - extract and order derived parameters
         - confirm parameter values

        :param process_json:
        :param noisy: [False] print extensive debugging info
        :param v2: [True] whether to use the v2 schema or not
        """
        _input_param = 'inputParameter'

        if v2:
            _input_param = OLCA_MAPPING['Parameter'][_input_param]

        self._noisy = bool(noisy)

        self._pj = process_json
        self._process_ref = process_ref
        self._params = process_json.get('parameters', ())
        self._inputs = [ParamConstant(p) for p in self._params if bool(p.get(_input_param, False))]
        self._formulas = [FormulaParser(self, p) for p in self._params if not bool(p.get(_input_param, False))]

        self._map = LowerDict((p.name, p) for p in self._inputs)
        for f in self._formulas:
            self._map[f.name] = f
        self._xp = {x['internalId']: x['amountFormula'] for x in process_json['exchanges'] if x.get('amountFormula')}
        self._xs = {x['internalId']: x['amount'] for x in process_json['exchanges']}

        # try and set units, noting that OLCA schema does not enforce consistency
        for x in process_json['exchanges']:
            formula = x.get('amountFormula')
            if formula:
                try:
                    p = self._map[formula]
                except KeyError:
                    adhoc = {'name': formula, 'value': x['amount'], 'formula': formula}
                    p = FormulaParser(self, adhoc)
                    self._formulas.append(p)
                    self._map[p.name] = p
                try:
                    p.unit = x['unit']['name']
                except ValueError:
                    logging.warning('Conflicting units for %s (set: %s, attempted: %s)' % (x['amountFormula'],
                                                                                           p.unit,
                                                                                           x['unit']['name']))

        self._ordered = []
        self._order_params()

        self._stack = []

    @property
    def process_ref(self):
        return str(self._process_ref)

    @property
    def active(self):
        return len(self._params) > 0

    @property
    def noisy(self):
        return self._noisy

    @noisy.setter
    def noisy(self, value):
        self._noisy = bool(value)

    def _order_params(self):
        # put the inputs at the front
        self._ordered = list(self.inputs)
        dq = deque(k for k in self.formulas)
        count = 0
        while dq:
            g = dq.popleft()
            fg = self._map[g]
            if all(k in self._ordered for k in fg.variables):
                self._ordered.append(g)
                count = 0
            else:
                dq.append(g)
                count += 1
                if count > len(dq):
                    raise RecursionError('too many tries %d')

    @property
    def variables(self):
        for k in self._ordered:
            yield k

    @property
    def params(self):
        for k in self._ordered:
            yield self[k]

    @property
    def inputs(self):
        for k in self._inputs:
            yield k.name

    @property
    def formulas(self):
        for k in self._formulas:
            yield k.name

    @property
    def outputs(self):
        """
        parameters that are linked to an exchange
``        :return:
        """
        for v in self._xp.values():
            yield v

    def __getitem__(self, item):
        return self._map[item.strip()]

    def __contains__(self, item):
        return self._map.__contains__(item)

    def set_value(self, key, value):
        param = self._map[key]
        if param in self._inputs:
            param.set(value)
        else:
            raise TypeError('Cannot set derived parameter %s' % key)

    def unset_value(self, key):
        param = self._map[key]
        if param in self._inputs:
            param.unset()
        else:
            raise TypeError('Cannot set derived parameter %s' % key)

    def set_values(self, **kwargs):
        for k, v in kwargs.items():
            self.set_value(k, v)

    def unset_values(self, *args):
        for k in args:
            self.unset_value(k)

    def value(self, key):
        p = self._map[key]
        if self._noisy:
            print(p)
        if key in self.inputs:
            return p.value
        else:
            if key in self._stack:
                raise RecursionError('key %s already visited' % key)
            self._stack.append(key)
            v = p.value  # this recurses
            self._stack.remove(key)
            return v

    def exchange_value(self, internal_id):
        try:
            return self.value(self._xp[internal_id])
        except KeyError:
            return self._xs[internal_id]

    def param_unit(self, param):
        """
        This is inaccurate because the same param can be used for multiple exchanges regardless of their flow
        property. This will only capture the units applied to ONE USE of the parameter name (nondeterministically)
        :param param:
        :return:
        """
        return self._map[param].unit
