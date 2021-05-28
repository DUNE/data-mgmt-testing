# Dune data visualizer Alpha

##Step 0, ensure NPM is installed on the host machine:

On linux this would be something like "sudo apt-get install NPM" or "sudo dnf install NPM", whatever package manager you use.

**On mac use homebrew, type: "brew install node"**

<br>
<br>

##Next install the "forever" package for NPM

(from root or with SUDO)

Ex: **sudo npm install forever -g**

<br>
<br>

##First step if you haven't run the demo thus far is to run setupEverything.sh

To do first time setup run "sh setupEverything.sh"

*estimated time: ~5 minuets*

<br>
<br>

##After that run the included start and stop scripts as follows:

To start: run "sh startEverything.sh"

Ex: **sh startEverything.sh**

*estimated time: ~8 minuets*

After which a browser window should launch with the network visualization.

<br>
<br>

**At this point the software will stay running, even after logout, until stopped with the "stopEverything.sh" script is run or with the command "forever stopall"

To stop: run "sh stopEverything.sh"
