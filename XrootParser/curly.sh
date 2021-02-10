curl -s -X GET https://fifemon-es.fnal.gov/sam-events-v1-2021.02/_search?q=experiment:dune%20and%20project\_id:$1\&size=10000 > $1.json
