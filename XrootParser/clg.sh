g++ -Wall -o $1.exe $1.C `root-config --cflags --glibs` -I./json -L/usr/local/Cellar/jsoncpp/1.9.4_1/   -ljsoncpp.dylib
