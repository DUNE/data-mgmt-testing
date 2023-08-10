# README for json_concatenate.py usage

## Purpose

json_concatenate.py concatenates the json files corresponding to the transfer data within the queried date range and produces an out.json file used by the frontend webpage visualizer. It reaches into to the samweb_es_cache and rucio_es_cache, which caches daily transfer events obtained by the Elasticsearch client. See the ESClient directory within this repository for more information on the backend Elasticsearch process.

## Usage
"python3 json_concatenate.py --help" returns:
```
usage: json_concatenate.py [-h] [-S START_DATE] [-E END_DATE]
                           [-T TRANSFERTYPE] [-U UUID]

optional arguments:
  -h, --help            show this help message and exit
  -S START_DATE, --start START_DATE
  -E END_DATE, --end END_DATE
  -T TRANSFERTYPE, --transferType TRANSFERTYPE
  -U UUID, --uuid UUID

```
Note that UUID is a pseudorandom generated string of characters used to uniquely name the output json file. This allows multiple users to use the search query simultaneously without overwriting the other.