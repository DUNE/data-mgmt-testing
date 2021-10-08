# Dune Network Monitor

<img src="/duneMain.png">

## Step 0, ensure GIT, NPM and Python3 are installed on the host machine:

On linux run something like (package manager dependant): "sudo apt install NPM", "sudo dnf install NPM", "yum install NPM", etc...
<br>
Similiar syntax for python "sudo apt install Python3", "sudo dnf install Python3", "yum install Python3", etc...
<br>
Something like "sudo apt install git", etc...

## Step 1, run GIT clone this repo onto the machine that will host the network visualizer tool:

In a terminal type "git clone https://github.com/DUNE/data-mgmt-testing.git"

## Next install the "forever" package for NPM

(from root or with SUDO)

Ex: **sudo npm install forever -g**

<br>
<br>

## Now CD into the data-mgmt-testing directory that was cloned earlier

## Running for the first time, use the script setupEverything.sh, located in /data-mgmt-testing/NetworkVisualizer1.0/

To do first time setup run "sh setupEverything.sh" in the aforementioned directory

*estimated time: ~5 minuets*

<br>
<br>

## After that run the included start and stop scripts as follows:

To start: run "sh startEverything.sh"

Ex: **sh startEverything.sh**

*estimated time: ~8 minuets*

After which a browser window should launch with the network visualization.

<br>
<br>

**At this point the software will stay running, even after logout, until stopped with the "stopEverything.sh" script is run**

To stop: run **"sh stopEverything.sh"**
