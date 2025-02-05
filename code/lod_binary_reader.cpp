// This file is meant for manually checking whether the binary data written by "lod_preprocessor.cpp" is correct
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <iostream>
#include <boost/program_options.hpp>
// #include <vector>

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;

u_int64_t read_uint_ENTITY_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_ENTITY);
    u_int64_t result = uint64_t(0);

    for (unsigned int i = 0; i < BYTES_PER_ENTITY; i++)
    {
        result |= (uint64_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data << (i * 8);
    }
    return result;
}

u_int32_t read_PREDICATE_little_endian(std::istream &inputstream)
{
    char data[4];
    inputstream.read(data, BYTES_PER_PREDICATE);
    u_int32_t result = uint32_t(0);

    for (unsigned int i = 0; i < BYTES_PER_PREDICATE; i++)
    {
        result |= (uint32_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data
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
    while (true)
    {
        u_int64_t subject = read_uint_ENTITY_little_endian(infile);
        u_int32_t predicate = read_PREDICATE_little_endian(infile);
        u_int64_t object = read_uint_ENTITY_little_endian(infile);
        if (infile.eof())
        {
            break;
        }
        std::cout << subject << "\t" << predicate << "\t" << object << std::endl;
    }
}
