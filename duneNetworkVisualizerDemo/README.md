# Dune data visualizer Alpha

##Step 0, ensure NPM is installed on the host machine:

On linux this would be something like "sudo apt-get install NPM" or "sudo dnf install NPM", whatever package manager you use.

**On mac use homebrew, type: "brew install node"**

##First step if you haven't run the demo thus far is to run setupEverything.sh

To do first time setup run "sh setupEverything.sh"

##After that run the included start and stop scripts as follows:

To start: run "sh startEverything.sh"

*NOTE: you can supply paramters for which month you want to retreive statistics, in the form of YYYY/MM/DD.* 

If no paramters are supplied, or incorrect form it will default to 2020/12/01.

Ex: **sh startEverything.sh 2021/02/01**

Then within a minute or so a browser window should launch with the network visualization.



To stop: run "sh stopEverything.sh"
