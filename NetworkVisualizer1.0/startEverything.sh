#!/bin/bash
cd api
npm start & echo $! >> ../curPIDs &
cd ..
npm start & echo $! >> curPIDs &
echo ""
echo ""
echo "Ok a browser window should pop up shortly with the interface"
