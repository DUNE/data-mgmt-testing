#!/bin/bash
forever --sourceDir "api"  start -c "npm start" ./
forever start -c "npm start" ./
echo ""
echo ""
echo "Ok a browser window should pop up shortly with the interface"
