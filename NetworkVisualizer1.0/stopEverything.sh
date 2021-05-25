#!/bin/bash
input="curPIDs"
while IFS= read -r line
do
    kill "$line"
done < "$input"

rm curPIDs
    

pkill -f node
