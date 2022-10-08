
# Sam to Metacat Conversion guide

This document includes examples of `sam` queries gathered from DUNE Dataset definitions and their `metacat` translations

### Metacat get started

First log in:

>`metacat auth login -m password $USER`
>Password:
>User:    schellma
>Expires: Thu Oct 13 16:27:29 2022

for now make your own namespace

>`metacat namespace create schellma`


### Get the raw data from given protodune-sp detector runs

##### samweb
> `samweb list-files "file_type detector and run_type 'protodune-sp' and data_tier raw and data_stream physics and run_number 5141"`

Note that you need to say if it is (detector/mc), which experiment it was (protodune-sp), what tier of data it was (raw0 and what kind of running (physics) it was.

add `--summary` if you wish to know how many files there are.

##### metacat

> `metacat query "files from dune:all where core.file_type=detector and core.run_type='protodune-sp' and core.data_tier=raw and core.data_stream=physics and core.runs[any]=5141"`

add `--summary` after query if you want just the # of files

Notes:
* many of the metadata values are now in categories like `core`
* things run faster if you ask for files from a known dataset like `dune:all`
* runs[any] means check any of the runs associated with the file for being 5141

TODO  make the interface less dependent on exactly where the `--summary` is

TODO  check the runs syntax.

### Save a generic dataset or definition query

If you are interested in everything physics from protodune-sp you might want to save a generic dataset or query which you can then reuse in further filtered queries.

##### samweb

in sam you save a definition, which is the query

> `samweb create-definition schellma-protodune-sp-physics-generic "file_type detector and run_type 'protodune-sp' and data_stream physics"`

You can then ask for:

> `samweb list-files "defname:schellma-protodune-sp-physics-generic and data_tier raw and run_number 5141" --summary`

Note: a sam definition is a query, not a list of files.  You need to make a `snapshot` to make a list that does not change.

Another note: sam also prepends the user name to the definition so that you can't mess up official queries.  This is handled in metacat by the introduction of namespaces.

##### metacat

This may take a bit of setup as you will need a namespace and then make a dataset

First as a user I will set up my own `dataset` namespace.  I can get files from other file namespaces and use datasets from other dataset namespaces but I can't modify those other namespaces without permissions.

>  `metacat dataset create namespace $USER`

Here I gave it my user name which you should too.

TODO - enforce namespace name for individuals?

Now I make a dataset with a good defname

>  `metacat dataset create schellma:protodune-sp-physics-generic`

find the list of files matching your query

>   `metacat query -i  "files  from dune:all where core.file_type=detector and core.run_type='protodune-sp' and core.data_stream=physics" > physics_ids.txt`

Notes: `-i` means return a list of file id numbers, not names.

add them to your dataset

> `metacat file add -i @physics_ids.txt schellma:protodune-sp-physics-generic`

TODO - this times out if all runs are included - I just did 5141 for this test.

check it by querying the files in the dataset

> `metacat query -s "files from schellma:protodune-sp-physics-generic"`

Note: I have not saved the query in the metacat dataset. I have saved the list of files.  In `metacat` datasets do not change (for example if another file passing the query requirements comes in from the DAQ) until you explicitly add the new file.

You can then ask for the subset from a particular data tier and run number.

> `metacat query "files from schellma:protodune-sp-physics-generic where core.runs[all]=5141 and core.data_tier=raw"`

#### Find only the files not processed with a version of code.

##### samweb

> `samweb list-files "defname:schellma-protodune-sp-physics-generic and data_tier raw and run_number 5141 minus isparentof:(defname:schellma-protodune-sp-physics-generic and data_tier 'full-reconstructed'  and run_number 5141 and version v08_27_% )" --summary`

> File count:	12
> Total size:	95354212618
> Event count:	1241

##### Metacat

> `metacat query -s "files from schellma:protodune-sp-physics-generic where core.data_tier=raw and core.runs[any]=5141 -  parents(files from schellma:protodune-sp-physics-generic where core.runs[any]=5141 and core.data_tier='full-reconstructed' and core.application.version~'v08_27_*')"`

> 12 files

Note: the syntax for a parameter wildcard is `~` for match and then `*` for any string instead of `%`
