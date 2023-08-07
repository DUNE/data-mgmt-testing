#!/bin/bash
#forever --sourceDir "api"  start -c "npm start" ./
#forever start -c "npm start" ./
#export HTTPS=true
#export SSL_CRT_FILE=/etc/grid-security/hostcert.pem
#export SSL_KEY_FILE=/etc/grid-security/hostkey.pem
forever --sourceDir "api"  start -c "npm start" ./
#export HTTPS=true
#export SSL_CRT_FILE=/etc/grid-security/hostcert.pem
#export SSL_KEY_FILE=/etc/grid-security/hostkey.pem
forever start -c "npm start" ./

echo ""
echo ""
echo "Ok if that worked the server should be up in <10 minutes, if successful a browser window should pop up shortly with the interface, assuming you have X-window forward on, or or running in a GUI."
echo ""
echo ""

