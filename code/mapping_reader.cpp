// This file is meant for meanually checking whether the binary bisimulation outcomes written by "full_bisimulation_from_binary.cpp" are correct
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <iostream>
#include <boost/program_options.hpp>
// #include <vector>

const int BYTES_PER_BLOCK = 4;

u_int64_t read_uint_BLOCK_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_BLOCK);
    u_int64_t result = u_int64_t(0);

    for (unsigned int i = 0; i < BYTES_PER_BLOCK; i++)
    {
        result |= (u_int64_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data << (i * 8);
    }
    return result;
}

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("input_file", po::value<std::string>(), "Input file, must contain n-triples");
    po::positional_options_description pos;
    pos.add("input_file", 1);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string input_file = vm["input_file"].as<std::string>();

    std::ifstream infile(input_file, std::ifstream::in);
    u_int64_t split_count = 0;
    while (true)
    {
        u_int64_t original_block = read_uint_BLOCK_little_endian(infile);
        if (infile.eof())
        {
            if (split_count == 0)
            {
                std::cout << "There were no splits: This is a fixed point of the bisimulation." << std::endl;
            }
            break;
        }
        split_count++;
        u_int64_t new_blocks_count = read_uint_BLOCK_little_endian(infile);
        std::cout << "Original Block: " << original_block << ", New Block Count: " << new_blocks_count << std::endl;
        for (u_int64_t i = 0; i < new_blocks_count; i++)
        {
            u_int64_t new_block = read_uint_BLOCK_little_endian(infile);
            std::cout << "New Block: " << new_block << std::endl;
        }
    }
}
