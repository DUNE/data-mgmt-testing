#!/bin/bash
if [[ $1 =~ ^[0-9]{4}/[0-9]{2}/[0-9]{2}$ ]]
  then echo "Valid Date of $1, updating python file to search that month"
        newValue="10s/.*/"\"
        newValue+=${1:0:4}
        newValue+="\/"
        newValue+=${1:5:2}
        newValue+="\/"
        newValue+=${1:8:6}
        newValue+="\",/"
        newValue+=" api/routes/test.js"
        sed -i $newValue 
  else echo "invalid date, should be YYYY/MM/DD, defaulting to 2020/12/01"
        sed -i '10s/.*/"2020\/12\/01",/' api/routes/test.js
fi
cd api
npm start & echo $! >> ../curPIDs &
cd ..
npm start & echo $! >> curPIDs &
echo ""
echo ""
echo "Ok a browser window should pop up shortly with the interface"
