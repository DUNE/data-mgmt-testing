#!/bin/bash

export NVM_DIR="$HOME/.nvm" 
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" 
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

forever --sourceDir "api"  start -c "npm start" ./
forever start -c "npm start" ./
echo ""
echo ""
echo "Ok if that worked the server should be up in <10 minutes, if successful a browser window should pop up shortly with the interface, assuming you have X-window forward on, or or running in a GUI."
echo ""
echo ""

