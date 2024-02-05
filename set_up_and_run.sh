#!/bin/bash
mkdir ./bin/
mkdir ./output/
BOOST_HOME=/usr/include/boost_1_84_0/
set -x
# g++ -static -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor
#g++ -static -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME ./code/lod_binary_reader.cpp -o ./bin/lod_binary_reader
g++ -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor $BOOST_HOME/lib/libboost_program_options.a
 g++ -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/lod_binary_reader.cpp -o ./bin/lod_binary_reader $BOOST_HOME/lib/libboost_program_options.a
# ./bin/lod_preprocessor
#./bin/lod_binary_reader
./bin/lod_preprocessor ./data/Laurence_Fishburne_Custom_Shuffled.trig ./output/Laurence_Fishburne_Custom_Shuffled.bin
./bin/lod_binary_reader ./output/Laurence_Fishburne_Custom_Shuffled.bin