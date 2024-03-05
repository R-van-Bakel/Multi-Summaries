#!/bin/bash
mkdir -p ./bin/
mkdir -p ./output/
BOOST_HOME=/usr/include/boost_1_84_0
FILE_NAME=Laurence_Fishburne_Custom_Shuffled
FILE_EXTENSION=.trig
set -x
-v g++ -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor $BOOST_HOME/lib/libboost_program_options.a
-v g++ -std=c++2a -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME/include/ ./code/full_bisimulation_from_binary.cpp -o ./bin/full_bisimulation_from_binary  $BOOST_HOME/lib/libboost_program_options.a
-v g++ -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I $BOOST_HOME/include/  ./code/condensed_post_hoc_metric_writer.cpp -o ./bin/condensed_post_hoc_metric_writer $BOOST_HOME/lib/libboost_program_options.a

/usr/bin/time -v ./bin/lod_preprocessor ./data/$FILE_NAME$FILE_EXTENSION ./output/
/usr/bin/time -v ./bin/full_bisimulation_from_binary run_k_bisimulation_store_partition_condensed_timed ./output/$FILE_NAME/binary_encoding.bin --output=./output/$FILE_NAME/
/usr/bin/time -v ./bin/condensed_post_hoc_metric_writer ./output/$FILE_NAME/