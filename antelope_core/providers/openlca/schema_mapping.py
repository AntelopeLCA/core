OLCA_MAPPING = {
    'Currency':	{'referenceCurrency': 'refCurrency'},
    'Exchange': {'avoidedProduct': 'isAvoidedProduct',
                 'input': 'isInput',
                 'quantitativeReference': 'isQuantitativeReference'},
    'Flow': {'infrastructureFlow': 'isInfrastructureFlow'},
    'FlowPropertyFactor': {'referenceFlowProperty': 'isRefFlowProperty'},
    'ImpactCategory': {'referenceUnitName': 'refUnit'},
    'Parameter': {'inputParameter': 'isInputParameter'},
    'Process': {'infrastructureProcess': 'isInfrastructureProcess'},
    'ProcessDocumentation': {'copyright': 'isCopyrightProtected'},
    'ProductSystem': {'referenceExchange': 'refExchange',
                      'referenceProcess': 'refProcess'},
    'Unit': {'referenceUnit': 'isRefUnit'}
}
