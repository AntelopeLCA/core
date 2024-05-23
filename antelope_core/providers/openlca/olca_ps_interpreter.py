import logging
from collections import defaultdict


class FlowInconsistency(Exception):
    pass


class OlcaParameterSet(object):
    def __init__(self, param_set_json):
        self._psj = param_set_json
        self._params = defaultdict(dict)
        for i, p in enumerate(self._psj.get('parameters', [])):
            if p['@type'] != 'ParameterRedef':
                logging.warning('non-ParameterRedef ignored [%d] (%s)' % (i, self.name))
                continue
            cx = p['context']['@id']
            pname = p['name']
            pval = p['value']
            self._params[cx][pname] = pval

    @property
    def name(self):
        return self._psj['name']

    @property
    def is_baseline(self):
        return bool(self._psj['isBaseline'])

    @property
    def contexts(self):
        for k in self._params.keys():
            yield k

    def param_values(self, context):
        return {k: v for k, v in self._params[context].items()}


class OlcaProductSystemInterpreter(object):
    """
    A convenience layer for interacting with an OpenLCA product system object
    """
    def __init__(self, ps_json, parameter_set=None):
        """

        :param ps_json: supply the json object corresponding to the named product system
        """
        self._psj = ps_json
        self._targets = defaultdict(dict)
        self._flows = defaultdict(dict)  # this is for errorchecking only
        for k in self._psj['processLinks']:
            parent = k['process']['@id']
            flow = k['flow']['@id']
            internal_id = k['exchange']['internalId']
            target = k['provider']['@id']
            self._targets[parent][internal_id] = target
            self._flows[parent][internal_id] = flow
        self._param_sets = [OlcaParameterSet(pset) for pset in self._psj.get('parameterSets', [])]
        if len(self._param_sets) > 1:
            if parameter_set is None:
                logging.warning('Using first parameter set %s' % self._param_sets[0].name)
                parameter_set = 0
            elif isinstance(parameter_set, int):
                if parameter_set >= len(self._param_sets):
                    logging.warning('Invalid parameter set specified (%d); using %s' % (parameter_set,
                                                                                        self._param_sets[0].name))
                    parameter_set = 0
            else:
                try:
                    parameter_set = next(i for i, p in enumerate(self._param_sets) if p.name == parameter_set)
                except StopIteration:
                    logging.warning('Invalid parameter set specified (%s); using %s' % (parameter_set,
                                                                                        self._param_sets[0].name))
                    parameter_set = 0
        else:
            parameter_set = 0
        self._param_set = parameter_set

    def _get_param_set(self, key):
        if key is None:
            return self._param_sets[self._param_set]
        elif isinstance(key, int):
            return self._param_sets[key]
        elif isinstance(key, str):
            try:
                return next(p for p in self._param_sets if p.name == key)
            except StopIteration:
                pass
        raise KeyError(key)

    @property
    def uuid(self):
        return self._psj['@id']

    @property
    def name(self):
        return self._psj['name']

    @property
    def processes(self):
        for p in self._psj['processes']:
            yield p['@id']

    @property
    def ref_process(self):
        return self._psj['refProcess']['@id']

    @property
    def ref_exchange(self):
        return self._psj['refExchange']['internalId']

    @property
    def parameter_sets(self):
        for s in self._param_sets:
            yield s.name

    def get_param_values(self, context, param_set=None):
        p = self._get_param_set(param_set)
        return p.param_values(context)

    def check_exchange_id(self, context, exchange_id):
        try:
            _ = self._targets[context][exchange_id]
            _ = self._flows[context][exchange_id]
            return True
        except KeyError:
            return False

    def get_target(self, context, exchange_id):
        return self._targets[context][exchange_id]

    def get_target_flow(self, context, exchange_id):
        """
        ps links specify anchor_flow, which must be used to build our model because bg engine expects it
        :param context:
        :param exchange_id:
        :return:
        """
        return self._flows[context][exchange_id]
