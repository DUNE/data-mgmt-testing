# Dune data visualizer Alpha

##Step 0, ensure NPM and Python3 are installed on the host machine:

On linux this would be something like "sudo apt-get install NPM", "sudo dnf install NPM", "yum install NPM", etc... Whatever package manager you use.
<br>
Similiar syntax for python "sudo apt-get install Python3", "sudo dnf install Python3", "yum install Python3", etc...

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

**At this point the software will stay running, even after logout, until stopped with the "stopEverything.sh" script is run or with the command "forever stopall"**

To stop: run "sh stopEverything.sh"
