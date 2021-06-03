#!/bin/bash

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.35.3/install.sh | bash

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

nvm install node
node --version
npm --version


npm install --prefix
echo ""
echo ""
echo ""
cd api
npm install --prefix
cd ..
echo ""
echo ""
echo ""
echo "Installation finished, hopefully that all worked, now try running startEverything.sh"
