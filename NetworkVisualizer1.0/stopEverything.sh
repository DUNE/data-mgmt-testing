#!/bin/bash

forever stopall

fuser -k -n tcp 3000
fuser -k -n tpc 3001
