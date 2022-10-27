
# Sam to Metacat Conversion guide

This document includes examples of `sam` queries gathered from DUNE Dataset definitions and their `metacat` translations

### Get metacat started

First find the documentation:

https://metacat.readthedocs.io/en/latest/index.html

metacat is a ups product so you can get it by

`source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh`  
`setup python v3_9_2`  # this avoids system python which may be very old   
`setup metacat`  

but you can also do a local install using:

https://metacat.readthedocs.io/en/latest/ui.html#installation

Make certain you can point to the metacat server:  

> `export METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune`  
> `export METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_demo/app`  


Then authenticate to metacat:


>`metacat auth login -m password $USER`
>Password:
>User:    schellma
>Expires: Thu Oct 13 16:27:29 2022

*Note: you can also autheticate via other methods, for example*

> `kx509`  
> `export X509_USER_PROXY=/tmp/x509up_u1327`  
> `export X509_USER_KEY=$X509_USER_PROXY`  
> `metacat auth login -m x509 $USER`  

### Example: Get the raw data from given protodune-sp detector runs

##### samweb
>`samweb list-files "file_type detector and run_type 'protodune-sp' and data_tier raw and data_stream physics and run_number 5141,5143"`

*Note that you need to specify the file_type (detector/mc), which experiment it was (protodune-sp), what tier of data it was (raw0 and what kind of running (physics) it was.*

add `--summary` if you wish to know how many files there are.

##### metacat

> `metacat query "files from dune:all where core.file_type=detector and core.run_type='protodune-sp' and core.data_tier=raw and core.data_stream=physics and core.runs[any] in (5141,5413)"`

add `--summary` after query if you want just the # of files

*Notes:*
- *many of the metadata values are now in categories like `core`*
- *things run faster if you ask for files from a known dataset like `dune:all`*
- *runs[any] means check any of the runs associated with the file for being 5141*
- *you can ask for multiple runs by using the `in (X,Y)` syntax*

TODO  make the interface less dependent on exactly where the `--summary` is

### Example: Save a dataset or definition query

If you are interested in everything physics from protodune-sp, you might want to save a generic dataset or query which you can then reuse in further filtered queries.  Then as you narrow thing down you can build additional datasets.

##### samweb

in sam you save a definition, which is the query

> `samweb create-definition schellma-protodune-sp-physics-generic "file_type detector and run_type 'protodune-sp' and data_stream physics"`

You can then ask for:

> `samweb list-files "defname:schellma-protodune-sp-physics-generic and data_tier raw and run_number 5141" --summary`

*Note: a sam definition is a query, not a list of files and can change, for example if more data are added.  You need to make a `snapshot` to make a list that does not change.*

*Another note: sam also prepends the user name to the definition so that you can't mess up official queries.  This is handled in metacat by the introduction of namespaces.*

##### metacat

To run a MQL query and create a new dataset with the query results:

> `metacat dataset create -f "files from dune:all where ..." <dataset_namespace>:<dataset_name> <dataset description>`

> `metacat dataset create -f @file_with_mql_query.txt <dataset_namespace>:<dataset_name> <dataset description>`


To run a query and add matching files to an existing dataset:

> `metacat dataset add-files -q "files from dune:all where ..." <dataset_namespace>:<dataset_name>`

> `metacat dataset add-files -q @file_with_mql_query.txt <dataset_namespace>:<dataset_name>
> 
TODO - this times out if all runs are included - I just did 5141 for this test.

TODO -  a utility command that does the query, adds the files and logs the query in the dataset metadata, possibly not in the "description" field

check it by querying the files in the dataset

> `metacat query -s "files from schellma:protodune-sp-physics-generic"`

> `metacat dataset show schellma:protodune-sp-physics-generic`

```children                 :
created_timestamp        : 2022-10-08 11:41:54
creator                  : schellma
description              : files from dune:all where core.file_type=detector and core.run_type='protodune-sp' and core.data_stream=physics
file_count               : 772631
file_meta_requirements   : {}
frozen                   : False
metadata                 : {}
monotonic                : False
name                     : protodune-sp-physics-generic
namespace                : schellma
parents                  :
```

*Note: I have not saved the query in the metacat dataset but just added it as an optional description. I have saved the list of files.  In `metacat` datasets do not change (for example if another file passing the query requirements comes in from the DAQ) until you explicitly add the new file.*

You can then ask for the subset from a particular data tier and run number.

> `metacat query "files from schellma:protodune-sp-physics-generic where core.runs[all]=5141 and core.data_tier=raw"`

#### Find only the files not processed with a version of code.

##### samweb

> `samweb list-files "defname:schellma-protodune-sp-physics-generic and data_tier raw and run_number 5141 minus isparentof:(defname:schellma-protodune-sp-physics-generic and data_tier 'full-reconstructed'  and run_number 5141 and version v08_27_% )" --summary`

> File count:	12
> Total size:	95354212618
> Event count:	1241

TODO - get the file size as well?

##### metacat

> `metacat query -s "files from schellma:protodune-sp-physics-generic where core.data_tier=raw and 5141 in core.runs -  parents(files from schellma:protodune-sp-physics-generic where 5141 in core.runs and core.data_tier='full-reconstructed' and core.application.version~'v08_27_.*')"`

> 12 files

*Note: the syntax for a parameter matching is Regular Expressions, in particular '.\*' matches any string*
