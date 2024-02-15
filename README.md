# lod_summarization
This project contains code for the summarizing a lot of linked open data.
Run `set_up_and_run.sh` to compile the C++ files and test them on the `Laurence_Fishburne_Custom_Shuffled.trig` data.
For this to work, the `BOOST_HOME` variable, in `set_up_and_run.sh` should be set to the location of your boost installation.
Some basic commands are the following:
- `g++ -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I <boost>/include/  ./code/lod_preprocessor.cpp -o ./bin/lod_preprocessor <boost>/lib/libboost_program_options.a`
  - This compiles the preprocessor code. The preprocessor takes a `.trig` formatted file and writes it to a binary file.  You should replace `<boost>` with the directory in which you have installed boost (e.g. `/usr/include/boost_1_84_0`).
- `./bin/lod_preprocessor ./data/Laurence_Fishburne_Custom_Shuffled.trig ./output/Laurence_Fishburne_Custom_Shuffled.bin`
  - Using the compiled preprocessor, this command reads a trig file (`./data/Laurence_Fishburne_Custom_Shuffled.trig`) and writes it to a custom binary file (`./output/Laurence_Fishburne_Custom_Shuffled.bin`).
- `g++ -static -std=c++20 -Wall -Wpedantic -Ofast -fdiagnostics-color=always -I <boost>/include/ ./code/full_bisimulation_from_binary.cpp -o ./bin/full_bisimulation_from_binary  <boost>/lib/libboost_program_options.a`
  - This compiles the bisimulation code.
- `./bin/full_bisimulation_from_binary run_timed ./output/Laurence_Fishburne_Custom_Shuffled.bin`
  - This runs a timed bisimulation, using the file `./output/Laurence_Fishburne_Custom_Shuffled.bin` as input.
- `./bin/full_bisimulation_from_binary run_k_bisimulation_store_partition_timed ./output/Laurence_Fishburne_Custom_Shuffled.bin --output=./output/outcome`
  - This runs a timed bisimulation, but also writes the mapping for each node to its respective block to a file (for each layer, e.g. `./output/outcome-1.bin`).