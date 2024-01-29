#!/bin/bash
mkdir ./bin/
mkdir ./output/
BOOST_HOME=/usr/include/boost_1_84_0/
set -x
gcc -I $BOOST_HOME ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor -lstdc++ -Ofast
gcc -I /usr/include/boost_1_84_0/ ./code/lod_binary_reader.cpp  -o ./bin/lod_binary_reader -lstdc++ -Ofast
./bin/lod_preprocessor
./bin/lod_binary_reader
