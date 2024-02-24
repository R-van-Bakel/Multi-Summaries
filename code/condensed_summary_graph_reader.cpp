// This file is meant for meanually checking whether the binary bisimulation outcomes written by "full_bisimulation_from_binary.cpp" are correct
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <iostream>
#include <boost/program_options.hpp>
// #include <vector>

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_BLOCK = 4;

u_int64_t read_uint_ENTITY_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_ENTITY);
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
    global.add_options()("input_path", po::value<std::string>(), "Input file, must contain n-triples");
    po::positional_options_description pos;
    pos.add("input_path", 1);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string input_path = vm["input_path"].as<std::string>();

    bool final_file = false;
    uint32_t k = 1;
    std::map<uint32_t,std::map<u_int64_t,std::vector<u_int64_t>>> block_to_entity_maps;
    while (!final_file)
    {
        std::ifstream outcome_file(input_path + "_outcome_condensed-" + std::to_string(k) + ".bin", std::ifstream::in);
        u_int64_t block_count = 0;
        // block_to_entity_maps[k] = std::map<u_int64_t,std::vector<u_int64_t>>>;
        while (true)
        {
            u_int64_t block = read_uint_BLOCK_little_endian(outcome_file);
            if (outcome_file.eof())
            {
                if (block_count == 0)
                {
                    final_file = true;
                }
                break;
            }
            block_count++;
            // block_to_entity_maps[k][block] = std::vector<u_int64_t>;
            u_int64_t block_size = read_uint_ENTITY_little_endian(outcome_file);
            for (u_int64_t i = 0; i < block_size; i++)
            {
                u_int64_t entity = read_uint_ENTITY_little_endian(outcome_file);
                block_to_entity_maps[k][block].push_back(entity);
            }
        }
        k++;
    }

    final_file = false;
    k = 1;
    while (!final_file)
    {
        std::ifstream edge_file(input_path + "_mapping-" + std::to_string(k) + "to" + std::to_string(k+1) + ".bin", std::ifstream::in);
        u_int64_t split_count = 0;
        // std::cout << "K: " << std::to_string(k) << "-->" << std::to_string(k+1) << std::endl;
        std::string mapping_msg_lhs = "K" + std::to_string(k) + "-" + std::to_string(k+1) + ": ";
        std::string mapping_msg = mapping_msg_lhs;
        while (true)
        {
            bool singleton_found = false;
            u_int64_t original_block = read_uint_BLOCK_little_endian(edge_file);
            if (edge_file.eof())
            {
                if (split_count == 0)
                {
                    std::cout << "K" << std::to_string(k) << "-" << std::to_string(k+1) << ": Fixed Point" << std::endl;
                    final_file = true;
                    break;
                }
                std::cout << mapping_msg.substr(0,mapping_msg.size() - (mapping_msg_lhs.size() + 1)) << std::endl;
                break;
            }
            split_count++;
            u_int64_t new_blocks_count = read_uint_BLOCK_little_endian(edge_file);
            mapping_msg += "{";
            u_int32_t offset = 0;
            // The block we need to access might not be in the last outcome, in which case we check earlier outcomes until the block is found
            while (block_to_entity_maps[k-offset].count(original_block) == 0)
            {
                offset++;
            }
            for (auto entity: block_to_entity_maps[k-offset][original_block])
            {
                mapping_msg += std::to_string(entity) + ",";
            }
            mapping_msg.pop_back();
            mapping_msg += "} --> ";
            for (u_int64_t i = 0; i < new_blocks_count; i++)
            {
                u_int64_t new_block = read_uint_BLOCK_little_endian(edge_file);
                if (new_block == 0)
                {
                    singleton_found = true;
                    continue;
                }
                mapping_msg += "{";
                for (auto entity: block_to_entity_maps[k+1][new_block])
                {
                    mapping_msg += std::to_string(entity) + ",";
                }
                mapping_msg.pop_back();
                mapping_msg += "}, ";
            }
            if (singleton_found)
            {
                mapping_msg += "{SINGLETONS}";
            }
            else
            {
                mapping_msg.pop_back();
                mapping_msg.pop_back();
            }
            mapping_msg += "\n" + std::string(mapping_msg_lhs.size(), ' ');
        }
        k++;
    }

    // for (auto k_blocks: block_to_entity_maps)
    // {
    //     for (auto block_enities: k_blocks.second)
    //     {
    //         for (auto entity: block_enities.second)
    //         {
    //             std::cout << "(k=" << std::to_string(k_blocks.first) << ", block=" << std::to_string(block_enities.first) << "): " << std::to_string(entity) << std::endl;
    //         }
    //     }
    // }
}
