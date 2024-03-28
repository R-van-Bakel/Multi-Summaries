#!/bin/bash
mkdir -p ./bin/
mkdir -p ./output/
BOOST_HOME=/usr/include/boost_1_84_0
FILE_NAME=Laurence_Fishburne_Custom_Shuffled_Extra_Edges
FILE_EXTENSION=.trig
set -x
g++ -std=c++20 -Wall -Wpedantic -Ofast -march=native -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor $BOOST_HOME/lib/libboost_program_options.a
g++ -std=c++20 -Wall -Wpedantic -Ofast -march=native -fdiagnostics-color=always -I $BOOST_HOME/include/ ./code/full_bisimulation_from_binary.cpp -o ./bin/full_bisimulation_from_binary  $BOOST_HOME/lib/libboost_program_options.a
g++ -std=c++20 -Wall -Wpedantic -Ofast -march=native -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/condensed_post_hoc_metric_writer.cpp -o ./bin/condensed_post_hoc_metric_writer $BOOST_HOME/lib/libboost_program_options.a

mkdir ./output/$FILE_NAME
/usr/bin/time -v ./bin/lod_preprocessor ./data/$FILE_NAME$FILE_EXTENSION ./output/$FILE_NAME
/usr/bin/time -v ./bin/full_bisimulation_from_binary run_k_bisimulation_store_partition_condensed_timed ./output/$FILE_NAME/binary_encoding.bin --output=./output/$FILE_NAME/
/usr/bin/time -v ./bin/condensed_post_hoc_metric_writer ./output/$FILE_NAME/