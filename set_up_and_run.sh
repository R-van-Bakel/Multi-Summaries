#!/bin/bash
mkdir ./bin/
mkdir ./output/
BOOST_HOME=/usr/include/boost_1_84_0/
set -x
g++ -static -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor
g++ -static -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME ./code/lod_binary_reader.cpp -o ./bin/lod_binary_reader
./bin/lod_preprocessor
./bin/lod_binary_reader
