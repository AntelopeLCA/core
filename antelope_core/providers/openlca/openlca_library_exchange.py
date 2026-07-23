from antelope.interfaces.iexchange import ExchangeInterface
from antelope.interfaces.ibackground import BackgroundInterface
from ...implementations import BasicImplementation
from ...exchanges import Exchange, ExchangeValue


class OpenlcaLibraryExchangeImplementation(BasicImplementation, ExchangeInterface, BackgroundInterface):

    def _exchange(self, process, exch, value=False):
        f = self._archive.get(exch['flow_id'])
        if exch['elementary']:
            termination = self._archive.tm[f.context]
        else:
            termination = exch['termination']
        if value:
            return ExchangeValue(process, f, exch['direction'], termination=termination, value=exch['value'])
        else:
            return Exchange(process, f, exch['direction'], termination=termination)

    def lci(self, process, ref_flow=None, **kwargs):
        p = self._archive.get(process)
        for k in self._archive.lci(process):
            yield self._exchange(p, k, value=False)

    def exchanges(self, process, **kwargs):
        p = self._archive.get(process)
        if self._archive.force_lci:
            for k in self._archive.lci(process):
                yield self._exchange(p, k, value=False)
        else:
            for k in self._archive.inventory(process):
                yield self._exchange(p, k, value=False)

    def inventory(self, process, ref_flow=None, scenario=None, **kwargs):
        """
        I don't know how the library deals with multi-output processes
        :param process:
        :param ref_flow:
        :param scenario:
        :param kwargs:
        :return:
        """
        p = self._archive.get(process)
        if self._archive.force_lci:
            for k in self._archive.lci(process):
                if ref_flow is None and k['is_reference']:
                    continue
                yield self._exchange(p, k, value=True)
        else:
            for k in self._archive.inventory(process):
                yield self._exchange(p, k, value=True)
