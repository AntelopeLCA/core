# Checking inventory terminations

In order to build an LCI matrix for an inventory database it's necessary to link intermediate exchanges with 
processes that provide the exchanged flows.  Antelope calls this linking target the "terminal" of the exchange
(as in "other end"), or "term" for short.

The `CheckTerms` class is provided to test all the exchanges in a database and report statistics about them.
Below shows some example output for a recent release of the USLCI Database (download from 
[here](https://github.com/FLCAC-admin/uslci-content/blob/dev/downloads/uslci_fy24_q1_01_olca2_1_1_json_ld.zip)).

```python
from antelope_core import LcCatalog
from antelope_core.archives import CheckTerms

cat = LcCatalog.make_tester()
res = cat.new_resource('uslci.fy24.q1', '/path/to/uslci_fy24_q1_01_olca2_1_1_json_ld.zip', 'OpenLcaJsonLdArchive',
                       interfaces=('basic', 'exchange'))
cat.index_ref(res.origin)
check = CheckTerms(cat.query(res.origin))

'''
666 processes
878 reference exchanges
110361 dependent exchanges
terminated: 4997 exchanges
cutoff: 6731 exchanges
elementary: 98568 exchanges
self: 28 exchanges

missing: 22 exchanges
ambiguous: 15 exchanges
'''

```

This output tells us that the dataset is not ready to be linked.

First, there are 666 processes containing a total of 878 reference exchanges and 110,361 dependent exchanges. Out of 
these, 
 - 4,997 are terminated to providers.
 - 6,731 are cut-off (no termination specified or found)
 - 98,568 are elementary (terminated to an elementary context)
 - 28 are self-terminated (terminated to their own origination) - these are self-consuming exchanges (most commonly
grid losses in electricity processes)

That leaves 37 not-quite-normal exchanges:
 - 22 missing exchanges. Those are terminated exchanges where the target (termination) does not supply the terminated
flow as a reference exchange.
 - 15 ambiguous exchanges. Those are un-terminated exchanges which have more than 1 possible 
provider, as told by the `query.targets(exchange.flow)` method.

During ordering, the missing exchanges will be treated as cut-offs. Thus, they do not block the ordering process
but they may represent information loss.  **IN USLCI, these are most commonly the result of endogenous errors in
the dataset**. 

Ambiguous flows, however, are showstoppers because they can surely be terminated- it is just a question of *to what*.

## Resolving ambiguous flows

Ambiguous flows must be resolved either by terminating them in the data (not within Antelope) and re-loading the
dataset, or by specifying *preferred providers* as arguments to the ordering process.

### Preferred providers during background generation

Using the query, this is done by:

```
query.check_bg(prefer=(prefer-spec))
```

where the `prefer-spec` argument can take either of the following forms (see 
[bm_static.py](https://github.com/AntelopeLCA/background/blob/master/antelope_background/providers/bm_static.py#L99)): 
 - As a `dict`, mapping flow refs to process refs.
 - within the `dict`, `{flow: None}` is interpreted to cut-off the designated flow
 - within the `dict`, `{None: [processes]}` is interpreted as a *list* of processes to prefer for any reference flow
   (processes can only terminate their own reference flows)
 - as an iterable of 2-tuples of (flow, process) or (None, \[processes])
 - as an iterable of processes (analogous to (None, \[processes]))

### Preferred providers during index-and-order

If you are running `IndexAndOrder` and you want to specify preferred providers in a way that persists in configuration,
use `IndexAndOrder.configure()`:

```python
ixo.run()
'''
...
Found 3 ambiguous flows
[lcacommons.uslci.fy24.q1.01] Tailings, stockpiled, on-site, for unspecified beneficial use [kg]
[lcacommons.uslci.fy24.q1.01] Waste, industrial [kg]
[lcacommons.uslci.fy24.q1.01] Electricity, at grid [MJ]
---------------------------------------------------------------------------
AmbiguousAnchors                          Traceback (most recent call last)
...
AmbiguousAnchors: ['b35f19b6-e7e8-3c7a-a11e-1e5704413261', '153cadce-ae16-3fa0-9741-0eb91f1c77eb', '06581fb2-1de0-3e78-8298-f37605dea142']
'''

ixo.query.get('b35f19b6-e7e8-3c7a-a11e-1e5704413261').show()
'''
FlowRef catalog reference (b35f19b6-e7e8-3c7a-a11e-1e5704413261)
origin: lcacommons.uslci.fy24.q1.01
Context: CUTOFF Flows
 Locale: GLO
   UUID: b35f19b6-e7e8-3c7a-a11e-1e5704413261
   Name: Tailings, stockpiled, on-site, for unspecified beneficial use
'''
z = enum(ixo.query.targets('b35f19b6-e7e8-3c7a-a11e-1e5704413261'))
'''
 [00] [lcacommons.uslci.fy24.q1.01] Steel; sections, at plant [Northern America]
 [01] [lcacommons.uslci.fy24.q1.01] Steel; hot rolled coil, at plant [Northern America]

'''
ixo.configure('prefer_provider', 'b35f19b6-e7e8-3c7a-a11e-1e5704413261', None)
```

In this first case, the "Tailings, stockpiled" flow is ambiguously terminated to two different steel production
processes -- these flows are improperly tagged as references due to ambiguities in the OpenLCA archive's allocation
specification.  The `configure` instruction specified that these flows are to be left un-terminated.  The same is 
done for `153cadce-ae16-3fa0-9741-0eb91f1c77eb`, which is "Waste, Industrial" and is similarly erroneous.

```python
ixo.query.get('06581fb2-1de0-3e78-8298-f37605dea142').show()
'''
FlowRef catalog reference (06581fb2-1de0-3e78-8298-f37605dea142)
origin: lcacommons.uslci.fy24.q1.01
Context: 2211: Electric Power Generation, Transmission and Distribution
 Locale: GLO
   UUID: 06581fb2-1de0-3e78-8298-f37605dea142
   Name: Electricity, at grid
'''
z = enum(ixo.query.targets('06581fb2-1de0-3e78-8298-f37605dea142'))
'''
 [00] [lcacommons.uslci.fy24.q1.01] Electricity, at Grid, MRO, 2010 [Northern America]
 [01] [lcacommons.uslci.fy24.q1.01] Electricity, at eGrid, ERCT, 2008 [Northern America]
...
 [79] [lcacommons.uslci.fy24.q1.01] Electricity, at Grid, SERC, 2008 [Northern America]
 [80] [lcacommons.uslci.fy24.q1.01] Electricity, at grid, Eastern US, 2000 [Northern America]
'''

ixo.configure('prefer_provider', '06581fb2-1de0-3e78-8298-f37605dea142', z[67].external_ref)
```

There are many possibilities. One is selected and applied.

After that, the ordering runs correctly.

```python
ixo.order_res()
'''
local.data.LCI.aws-data.lcacommons.uslci.fy24.q1.01.background.TarjanBackground.29cd6bcb4e184a5cd1d01cec13212d06a8c9a315.mat: /data/LCI/aws-data/lcacommons.uslci.fy24.q1.01/background/TarjanBackground/29cd6bcb4e184a5cd1d01cec13212d06a8c9a315.mat
local.data.LCI.aws-data.lcacommons.uslci.fy24.q1.01.background.TarjanBackground.29cd6bcb4e184a5cd1d01cec13212d06a8c9a315.mat: Setting NSUUID (False) None
Applying configuration to TarjanBackground with 0 entities at /data/LCI/aws-data/lcacommons.uslci.fy24.q1.01/background/TarjanBackground/29cd6bcb4e184a5cd1d01cec13212d06a8c9a315.mat
...
Completed in 4.18 sec

'''
ixo.write()
```
