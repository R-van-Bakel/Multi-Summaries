#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
// For getting and printing the current datetime
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <filesystem>

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_BLOCK = 4;

u_int64_t read_uint_ENTITY_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_ENTITY);
    if (inputstream.eof())
    {
        return UINT64_MAX;
    }
    if (inputstream.fail())
    {
        std::cout << "Read entity failed with code: " << inputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << inputstream.good() << std::endl;
        std::cout << "Eofbit:  " << inputstream.eof() << std::endl;
        std::cout << "Failbit: " << (inputstream.fail() && !inputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << inputstream.bad() << std::endl;
        exit(inputstream.rdstate());
    }
    u_int64_t result = u_int64_t(0);

    for (unsigned int i = 0; i < BYTES_PER_ENTITY; i++)
    {
        result |= (u_int64_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data << (i * 8);
    }
    return result;
}

u_int64_t read_uint_BLOCK_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_BLOCK);
    if (inputstream.eof())
    {
        return UINT64_MAX;
    }
    if (inputstream.fail())
    {
        std::cout << "Read block failed with code: " << inputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << inputstream.good() << std::endl;
        std::cout << "Eofbit:  " << inputstream.eof() << std::endl;
        std::cout << "Failbit: " << (inputstream.fail() && !inputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << inputstream.bad() << std::endl;
        exit(inputstream.rdstate());
    }
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
    global.add_options()("input_path", po::value<std::string>(), "Input file");
    po::positional_options_description pos;
    pos.add("input_path", 1);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string input_path = vm["input_path"].as<std::string>();
    std::filesystem::create_directory(input_path + "post_hoc_results/");

    uint32_t k = 1;
    bool last_file = false;
    u_int64_t old_block_count = 0;
    u_int64_t old_split_count = 0;

    while (!last_file)
    {
        auto t_read{boost::chrono::system_clock::now()};
        auto time_t_read{boost::chrono::system_clock::to_time_t(t_read)};
        std::tm *ptm_read{std::localtime(&time_t_read)};
        std::cout << std::put_time(ptm_read, "%Y/%m/%d %H:%M:%S") << " Reading outcome " << k << std::endl;

        std::ostringstream k_stringstream;
        k_stringstream << std::setw(4) << std::setfill('0') << k;
        std::string k_string(k_stringstream.str());

        std::ostringstream k_next_stringstream;
        k_next_stringstream << std::setw(4) << std::setfill('0') << k+1;
        std::string k_next_string(k_next_stringstream.str());

        std::string input_file = input_path + "bisimulation/outcome_condensed-" + k_string + ".bin";
        std::string output_file = input_path + "post_hoc_results/statistics_condensed-" + k_string + ".json";

        std::ifstream infile(input_file, std::ifstream::in);
        std::ofstream outfile(output_file, std::ios::trunc | std::ofstream::out);

        std::string mapping_file = input_path + "bisimulation/mapping-" + k_string + "to" + k_next_string + ".bin";
        std::ifstream mappingfile(mapping_file, std::ifstream::in);

        uint64_t disappeared_count = 0;
        uint64_t split_count = 0;
        uint64_t block_node_count = 0;
        while (true)
        {
            // Skip reading the block ID, since we do not need it
            mappingfile.seekg(BYTES_PER_BLOCK, std::ios_base::cur);
            u_int64_t new_block_count = read_uint_BLOCK_little_endian(mappingfile);
            if (mappingfile.eof())
            {
                if (split_count == 0)
                {
                    last_file = true;
                }
                break;
            }
            split_count++;
            // If the split count is 1 and the new block id is 0, then the block got split into only singletons and therefore dissapeared
            if (new_block_count == 1)
            {
                u_int64_t new_block = read_uint_BLOCK_little_endian(mappingfile);
                if (new_block == 0)
                {
                    disappeared_count++;
                }
            }
            // Jump to the next block
            else
            {
                mappingfile.seekg(new_block_count*BYTES_PER_BLOCK, std::ios_base::cur);
            }
        }

        u_int64_t block_count = 0;
        outfile << "{\n    \"New block sizes\": {";  // A mapping from each block to its size
        
        while (true)
        {
            u_int64_t block = read_uint_BLOCK_little_endian(infile);
            if (infile.eof())
            {
                if (block_count == 0)
                {
                    outfile << "{}";  // Only singleton blocks were created
                }
                break;
            }
            if (block_count > 0)
            {
                outfile << ",";
            }
            u_int64_t block_size = read_uint_ENTITY_little_endian(infile);
            block_node_count += block_size;
            outfile << "\"" << block  << "\"" << ":" << block_size;
            infile.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
            block_count++;
        }

        outfile << "},\n    \"New block count\": " << block_count;                                 // How many new blocks did we get?
        outfile << ",\n    \"New vertex count\": " << block_node_count;                            // How many vertices are in the new blocks?
        outfile << ",\n    \"Split count\": " << split_count;                                      // How many blocks will split?
        outfile << ",\n    \"Disappeared count\": " << disappeared_count;                          // How many blocks will split into only singletons?
        outfile << ",\n    \"Block count\": " << old_block_count - old_split_count + block_count;  // How many blocks in total?
        outfile << "\n}";
        old_block_count = old_block_count - old_split_count + block_count;
        old_split_count = split_count;
        k++;
    }
}
