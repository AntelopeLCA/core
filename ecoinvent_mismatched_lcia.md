# Ecoinvent LCI and LCIA

This is a writeup to demonstrate how to use Antelope software with ecoinvent and the ecoinvent LCIA factors.  I will go through the steps of installing the ecoinvent 3.7.1 database and the ecoinvent 3.8 LCIA factors, just because it's more complicated that way.

## 1. Prepare the source data

### 1.1 Create a data directory to store the ecoinvent data. Mine is in (home directory)/ecoinvent. 

    (home directort)/ecoinvent/3.7.1/

Then create a sub-directory with the ecoinvent version and download the desired database from the ecoinvent website. I am going to use ecoinvent 3.7.1 cutoff.

    (home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02.7z

###  1.2 Extract the 7z file into a folder of the same name. 

The directory structure should now look like:

    (home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02/datasets/....
    (home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02/MasterData/....
    (home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02.7z

### 1.3 create a new subdirectory called LCIA, put the ecoinvent LCIA implementation in that subdirectory, and extract it:

	(home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02/datasets/....
	(home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02/MasterData/....
	(home directory)/ecoinvent/3.7.1/ecoinvent 3.7.1_cutoff_ecoSpold02.7z
	(home directory)/ecoinvent/LCIA/LCIA Implementation v3.8.xlsx
	(home directory)/ecoinvent/LCIA/LCIA Implementation v3.8.pdf
	(home directory)/ecoinvent/LCIA/ecoinvent 3.8_LCIA_implementation.7z


## 2. Prepare the antelope software.

### 2.1 Create virtualenv. I am putting mine in (home directory)/virtualenvs/antelope_ecoinvent

	$ python3 -m venv (home directory)/virtualenvs/antelope_ecoinvent
	$ source (home directory)/virtualenvs/antelope_ecoinvent/bin/activate
	(antelope_ecoinvent)$ which pip
	(home directory)/virtualenvs/antelope_ecoinvent/bin/pip
	(antelope_ecoinvent)$ 


### 2.2 install antelope 

	(antelope_ecoinvent)$ pip install antelope_core antelope_background lxml
	....
	
	(antelope_ecoinvent)$ 

It should also install scipy, which is needed for sparse matrix work. 

lxml is needed to read the ecospold files.

## 3. Setup your catalog

For this you need to be running in the virtual environment that you just created. You will need another directory to store your catalog. I'm using (home directory)/work.  All the steps in this section only need to be done once.  After you have created the catalog, it will remember its information.

### 3.1 create catalog

	(antelope_ecoinvent)$ ipython
	>>> import antelope_core
	>>> cat = antelope_core.LcCatalog('(home directory)/work')
	merging Emissions into Emissions
	merging Resources into Resources
	Loading JSON data from /home/b/work/reference-quantities.json:
	local.qdb: /home/b/work/reference-quantities.json
	local.qdb: Setting NSUUID (False) 77833297-6780-49bf-a61a-0cb707dce700
	local.qdb: /data/GitHub/lca-tools/lcatools/qdb/data/elcd_reference_quantities.json
	25 new quantity entities added (25 total)
	6 new flow entities added (6 total)

	>>> cat.show_interfaces()
	local.qdb [basic, index, quantity]

	>>>

### 3.2 Install Ecoinvent LCI

	>>> from antelope_core.data_sources.ecoinvent import EcoinventConfig
	>>> ec = EcoinventConfig('(home directory)/ecoinvent')
	>>> ec.register_all_references(cat)
	>>> cat.show_interfaces()
	local.ecoinvent.3.7.1.cutoff [basic, exchange]
	local.qdb [basic, index, quantity]

	>>>

### 3.3 Construct the ecoinvent LCI background matrix

	>>> cat.query('local.ecoinvent.3.7.1.cutoff').check_bg()
	[takes about 3 minutes to load all the processes and construct A and B matrices. This only has to be
	done once]
	True
	
	>>> cat.show_interfaces()
	local.ecoinvent.3.7.1.cutoff [basic, exchange]
	local.ecoinvent.3.7.1.cutoff.index.20220407 [background, basic, index]
	local.qdb [basic, index, quantity]

	>>>


### 3.4 Install LCIA 3.8


	>>> from antelope_core.data_sources.ecoinvent_lcia import EcoinventLciaConfig
	>>> el = EcoinventLciaConfig('(home directory)/ecoinvent/LCIA', '3.8')
	>>> list(el.references)
	['local.lcia.ecoinvent.3.8']

	>>> el.register_all_resources(cat)
	>>> cat.show_interfaces()local.ecoinvent.3.7.1.cutoff [basic, exchange]
	local.ecoinvent.3.7.1.cutoff [basic, exchange]
	local.ecoinvent.3.7.1.cutoff.index.20220407 [background, basic, index]
	local.lcia.ecoinvent.3.8 [basic, index, quantity]
	local.qdb [basic, index, quantity]

	>>>

## 4. Perform LCIA

### 4.1 Link LCIA 3.8 to LCI

This is a challenge because the version 3.8 LCI is not installed. But it is easy enough to work around.  We just have to tell the ecoinvent 3.8 LCIA to use ecoinvent 3.7.1 as a reference.  Note: you must do this _every time_ you initialize the catalog BEFORE loading the LCIA methods.  The obvious solution is to use the 3.8 LCI, which would make this unnecessary

	>>> ar = cat.get_archive('local.ecoinvent.3.7.1.cutoff', 'exchange')
	>>> res = cat.get_resource('local.lcia.ecoinvent.3.8')
	>>> res.init_args['ei_archive'] = ar
	>>> res.check(cat)
	[this takes quite some time to load the spreadsheet]
	[you will see a list of flows that are present in 3.8 but absent in 3.7.1]
	True

	>>>

### 4.1 Choose a process.

	>>> q371 = cat.query('local.ecoinvent.3.7.1.cutoff')   # hold on to the query
	>>> from antelope import enum   # this is just a handy way to list processes
	>>> yogurt = enum(q371.processes(Name='yogurt'))
	 [00] [local.ecoinvent.3.7.1.cutoff] yogurt production, from cow milk [RoW]
	 [01] [local.ecoinvent.3.7.1.cutoff] market for yogurt, from cow milk [GLO]
	 [02] [local.ecoinvent.3.7.1.cutoff] yogurt production, from cow milk [CA-QC]

	>>> yogurt[2].show()
	ProcessRef catalog reference (ec7a59f5-270f-4ab4-876c-3c379a26694c)
	origin: local.ecoinvent.3.7.1.cutoff
	UUID: ec7a59f5-270f-4ab4-876c-3c379a26694c
	   Name: yogurt production, from cow milk...

	>>> rx = enum(yogurt[2].references())
	[00] [ yogurt production, from cow milk [CA-QC] ]*==>  1 (kg) yogurt, from cow milk 
	[01] [ yogurt production, from cow milk [CA-QC] ]*==>  1 (kg) cheese, from cow milk, fresh, unripened 
	[02] [ yogurt production, from cow milk [CA-QC] ]*==>  1 (kg) cream, from cow milk 

	>>> 

### 4.2 Choose an LCIA method

	>>> recipes_eu = enum(cat.query('local.lcia.ecoinvent.3.8').lcia_methods(method='recipe', category='eutrophication'))


### 4.3 Perform the computation

Note that for multi-output processes, you must specify a reference flow.  For single-output processes (e.g. yogurt[0]) you could omit the `rx` argument.

	>>> result = recipes_eu[0].do_lcia(yogurt[2].lci(rx[0]))

	>>> result.show_details()
	[local.lcia.ecoinvent.3.8] ReCiPe Midpoint (E) V1.13 no LT, freshwater eutrophication, FEP [kg P-Eq] [LCIA] kg P-Eq
	------------------------------------------------------------

	[local.ecoinvent.3.7.1.cutoff] yogurt production, from cow milk [CA-QC]:
    0.00044 =          1  x    0.00044 [GLO] Phosphorus, surface water
	0.000174 =       0.33  x   0.000528 [GLO] Phosphate, surface water
	4.12e-05 =       0.33  x   0.000125 [GLO] Phosphate, ground-
	1.46e-05 =       0.33  x   4.41e-05 [GLO] Phosphate, water
	4.72e-06 =       0.33  x   1.43e-05 [GLO] Phosphate, ocean
	1.34e-06 =          1  x   1.34e-06 [GLO] Phosphorus, agricultural
	8.11e-07 =          1  x   8.11e-07 [GLO] Phosphorus, industrial
	7.87e-07 =          1  x   7.87e-07 [GLO] Phosphorus, ground-
	1.11e-07 =          1  x   1.11e-07 [GLO] Phosphorus, soil
	9.45e-08 =          1  x   9.45e-08 [GLO] Phosphorus, water
	8.57e-11 =          1  x   8.57e-11 [GLO] Phosphorus, ocean
	0.000678 [local.lcia.ecoinvent.3.8] ReCiPe Midpoint (E) V1.13 no LT, freshwater eutrophication, FEP [kg P-Eq] [LCIA]

	>>> recipes_eu[0].do_lcia(yogurt[2].lci(rx[1])).show_details()
	completed 66 iterations
	[local.lcia.ecoinvent.3.8] ReCiPe Midpoint (E) V1.13 no LT, freshwater eutrophication, FEP [kg P-Eq] [LCIA] kg P-Eq
	------------------------------------------------------------

	[local.ecoinvent.3.7.1.cutoff] yogurt production, from cow milk [CA-QC]:
	0.000141 =          1  x   0.000141 [GLO] Phosphorus, surface water
	5.58e-05 =       0.33  x   0.000169 [GLO] Phosphate, surface water
	1.32e-05 =       0.33  x      4e-05 [GLO] Phosphate, ground-
	4.66e-06 =       0.33  x   1.41e-05 [GLO] Phosphate, water
	1.51e-06 =       0.33  x   4.58e-06 [GLO] Phosphate, ocean
	4.29e-07 =          1  x   4.29e-07 [GLO] Phosphorus, agricultural
    2.6e-07 =          1  x    2.6e-07 [GLO] Phosphorus, industrial
	2.52e-07 =          1  x   2.52e-07 [GLO] Phosphorus, ground-
	3.54e-08 =          1  x   3.54e-08 [GLO] Phosphorus, soil
	3.03e-08 =          1  x   3.03e-08 [GLO] Phosphorus, water
	2.75e-11 =          1  x   2.75e-11 [GLO] Phosphorus, ocean
	0.000217 [local.lcia.ecoinvent.3.8] ReCiPe Midpoint (E) V1.13 no LT, freshwater eutrophication, FEP [kg P-Eq] [LCIA]
	
	>>>

Note that the results are different for different reference flows.
