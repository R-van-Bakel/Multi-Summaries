#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <filesystem>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/program_options.hpp>

using edge_type = uint32_t;
using node_index = uint64_t;
using block_index = node_index;
using block_or_singleton_index = int64_t;
using k_type = uint16_t;
using interval_map_type = boost::unordered_flat_map<block_or_singleton_index,std::pair<k_type,k_type>>;
using local_refines_type = std::vector<std::pair<block_or_singleton_index,block_or_singleton_index>>;
using global_refines_type = boost::unordered_flat_map<block_or_singleton_index,block_or_singleton_index>;
using local_to_global_map_type = boost::unordered_flat_map<std::pair<k_type,block_or_singleton_index>,block_or_singleton_index>;
using triple_set = boost::unordered_flat_set<std::tuple<block_or_singleton_index,edge_type,block_or_singleton_index>>;
using block_map = boost::unordered_flat_map<block_or_singleton_index,std::pair<k_type,block_or_singleton_index>>;
using level_to_local_to_global_map = boost::unordered_flat_map<k_type,boost::unordered_flat_map<block_index,block_or_singleton_index>>;
using block_set = boost::unordered_flat_set<block_or_singleton_index>;
using id_entity_map = boost::unordered_flat_map<node_index,std::string>;
// using level_to_blocks_map = boost::unordered_flat_map<k_type,boost::unordered::unordered_flat_set<block_or_singleton_index>>;
const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;
const int BYTES_PER_BLOCK = 4;
const int BYTES_PER_BLOCK_OR_SINGLETON = 5;
const int BYTES_PER_K_TYPE = 2;
const int SUMMARY_NODE_INTERVAL_PAIR_SIZE = BYTES_PER_BLOCK_OR_SINGLETON + BYTES_PER_K_TYPE + BYTES_PER_K_TYPE;

class MyException : public std::exception
{
private:
    const std::string message;

public:
    MyException(const std::string &err) : message(err) {}

    const char *what() const noexcept override
    {
        return message.c_str();
    }
};

class Triple
{
public:
    block_or_singleton_index subject;
    edge_type predicate;
    block_or_singleton_index object;
    Triple(block_or_singleton_index s, edge_type p, block_or_singleton_index o)
    {
        subject = s;
        predicate = p;
        object = o;
    }
};

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
    u_int64_t result = uint64_t(0);

    for (unsigned int i = 0; i < BYTES_PER_ENTITY; i++)
    {
        result |= (uint64_t(data[i]) & 0x00000000000000FFull) << (i * 8); // `& 0x00000000000000FFull` makes sure that we only write one byte of data
    }
    return result;
}

block_or_singleton_index read_int_BLOCK_OR_SINGLETON_little_endian(std::istream &inputstream)
{
    char data[BYTES_PER_BLOCK_OR_SINGLETON];
    inputstream.read(data, BYTES_PER_BLOCK_OR_SINGLETON);
    if (inputstream.eof())
    {
        return INT64_MAX;
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
    int64_t result = 0;

    for (unsigned int i = 0; i < BYTES_PER_BLOCK_OR_SINGLETON; i++)
    {
        result |= (int64_t(data[i]) & 0x00000000000000FFl) << (i * 8);
    }
    // If this is true, then we are reading a negative number, meaning the high bit needs to be set to 1
    if (int8_t(data[BYTES_PER_BLOCK_OR_SINGLETON-1]) < 0)
    {
        result |= 0xFFFFFF0000000000l;  // We need this conversion due to two's complement
    }
    return result;
}

k_type read_uint_K_TYPE_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_K_TYPE);
    if (inputstream.eof())
    {
        return INT16_MAX;
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

    for (unsigned int i = 0; i < BYTES_PER_K_TYPE; i++)
    {
        result |= (u_int64_t(data[i]) & 0x00000000000000FFull) << (i * 8); // `& 0x00000000000000FFull` makes sure that we only write one byte of data << (i * 8);
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
        result |= (u_int64_t(data[i]) & 0x00000000000000FFull) << (i * 8); // `& 0x00000000000000FFull` makes sure that we only write one byte of data << (i * 8);
    }
    return result;
}

u_int32_t read_uint_PREDICATE_little_endian(std::istream &inputstream)
{
    char data[4];
    inputstream.read(data, BYTES_PER_PREDICATE);
    if (inputstream.eof())
    {
        return UINT32_MAX;
    }
    if (inputstream.fail())
    {
        std::cout << "Read predicate failed with code: " << inputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << inputstream.good() << std::endl;
        std::cout << "Eofbit:  " << inputstream.eof() << std::endl;
        std::cout << "Failbit: " << (inputstream.fail() && !inputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << inputstream.bad() << std::endl;
        exit(inputstream.rdstate());
    }
    u_int32_t result = uint32_t(0);

    for (unsigned int i = 0; i < BYTES_PER_PREDICATE; i++)
    {
        result |= (uint32_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data
    }
    return result;
}

void write_int_BLOCK_OR_SINGLETON_little_endian(std::ostream &outputstream, block_or_singleton_index value)
{
    char data[BYTES_PER_BLOCK_OR_SINGLETON];
    for (unsigned int i = 0; i < BYTES_PER_BLOCK_OR_SINGLETON; i++)
    {
        data[i] = char(value);  // TODO check if removing & 0x00000000000000FFull fixed it
        value = value >> 8;
    }
    outputstream.write(data, BYTES_PER_BLOCK_OR_SINGLETON);
    if (outputstream.fail())
    {
        std::cout << "Write block failed with code: " << outputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << outputstream.good() << std::endl;
        std::cout << "Eofbit:  " << outputstream.eof() << std::endl;
        std::cout << "Failbit: " << (outputstream.fail() && !outputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << outputstream.bad() << std::endl;
        exit(outputstream.rdstate());
    }
}

void write_uint_PREDICATE_little_endian(std::ostream &outputstream, edge_type value)
{
    char data[BYTES_PER_PREDICATE];
    for (unsigned int i = 0; i < BYTES_PER_PREDICATE; i++)
    {
        data[i] = char(value & 0x00000000000000FFull);
        value = value >> 8;
    }
    outputstream.write(data, BYTES_PER_PREDICATE);
    if (outputstream.fail())
    {
        std::cout << "Write block failed with code: " << outputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << outputstream.good() << std::endl;
        std::cout << "Eofbit:  " << outputstream.eof() << std::endl;
        std::cout << "Failbit: " << (outputstream.fail() && !outputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << outputstream.bad() << std::endl;
        exit(outputstream.rdstate());
    }
}

void read_intervals_from_stream_timed(std::istream &inputstream, interval_map_type &interval_map, k_type level)
{
    k_type lower_level = level;
    k_type upper_level = level + 1;
    // Read a mapping file
    while (true)
    {
        block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        if (inputstream.eof())
        {
            break;
        }
        k_type start_time = read_uint_K_TYPE_little_endian(inputstream);
        if (start_time > upper_level)
        {
            inputstream.seekg(BYTES_PER_K_TYPE, std::ios_base::cur);  // No need to read the end time in this case
            // k_type end_time = read_uint_K_TYPE_little_endian(inputstream);
            // std::cout << "DEBUG skipped because start: " << global_block << ": [" << start_time << "," << end_time << "]" << std::endl;
            continue;
        }
        k_type end_time = read_uint_K_TYPE_little_endian(inputstream);
        if (end_time < lower_level)
        {
            // std::cout << "DEBUG skipped because end: " << global_block << ": [" << start_time << "," << end_time << "]" << std::endl;
            continue;
        }
        // Assuming start_time <= end_time, we are not only left with summary nodes that are alive during lower_level and/or upper_level
        interval_map[global_block] = {start_time,end_time};
    }
}

void read_intervals_timed(const std::string &filename, interval_map_type &interval_map, k_type level)
{
    std::ifstream infile(filename, std::ifstream::in);
    read_intervals_from_stream_timed(infile, interval_map, level);
}

void read_mapping_from_stream_timed(std::istream &inputstream, local_refines_type &refines_map)
{
    // Read a mapping file
    while (true)
    {
        block_or_singleton_index old_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(inputstream));
        if (inputstream.eof())
        {
            break;
        }

        block_index new_block_count = read_uint_BLOCK_little_endian(inputstream);
        for (block_index j = 0; j < new_block_count; j++)
        {
            block_or_singleton_index new_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(inputstream));
            if (new_block == 0)
            {
                continue;
            }
            refines_map.push_back(std::make_pair(new_block,old_block));
        }
    }
}

void read_mapping_timed(const std::string &filename, local_refines_type &refines_map)
{
    std::ifstream infile(filename, std::ifstream::in);
    read_mapping_from_stream_timed(infile, refines_map);
}

void read_singleton_mapping_from_stream_timed(std::istream &inputstream, local_refines_type &refines_map)
{
    // Read a mapping file
    while (true)
    {
        block_index possible_old_block = read_uint_BLOCK_little_endian(inputstream);
        // std::cout << "DEBUG sing refines: " << possible_old_block << std::endl;
        if (inputstream.eof())
        {
            // std::cout << "DEBUG IT BROKE" << std::endl;
            break;
        }
        block_or_singleton_index old_block = static_cast<block_or_singleton_index>(possible_old_block);
        // std::cout << "DEBUG sing refines: " << possible_old_block << std::endl;

        block_or_singleton_index new_block_count = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        for (block_or_singleton_index j = 0; j < new_block_count; j++)
        {
            block_or_singleton_index new_block = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
            // std::cout << "DEBUG sing refines: " << new_block << "," << old_block << std::endl;
            refines_map.push_back(std::make_pair(new_block,old_block));
        }
    }
}

void read_singleton_mapping_timed(const std::string &filename, local_refines_type &refines_map)
{
    std::ifstream infile(filename, std::ifstream::in);
    read_singleton_mapping_from_stream_timed(infile, refines_map);
}

void read_local_global_map_from_stream_timed(std::istream &inputstream, local_to_global_map_type &local_to_global_map, k_type level)
{
    k_type lower_level = level;
    k_type upper_level = level + 1;
    // Read a mapping file
    while (true)
    {
        k_type local_level = read_uint_K_TYPE_little_endian(inputstream);
        if (inputstream.eof())
        {
            break;
        }
        if (local_level < lower_level || local_level > upper_level)
        {
            inputstream.seekg(2*BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the ids in this case
            continue;
        }
        block_or_singleton_index local_block = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        local_to_global_map[std::make_pair(local_level,local_block)] = global_block;
    }
}

void read_local_global_map_timed(const std::string &filename, local_to_global_map_type &local_to_global_map, k_type level)
{
    std::ifstream infile(filename, std::ifstream::in);
    read_local_global_map_from_stream_timed(infile, local_to_global_map, level);
}

void read_data_edges_from_stream_timed(std::istream &inputstream, triple_set &quotient_graph_triples, global_refines_type &refines_edges, interval_map_type &interval_map, k_type level)
{
    k_type lower_level = level;
    k_type upper_level = level + 1;

    auto refines_end_it = refines_edges.cend();
    auto interval_end_it = interval_map.cend();
    // Read a mapping file
    while (true)
    {
        k_type subject = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        if (inputstream.eof())
        {
            break;
        }
        auto subject_interval_it = interval_map.find(subject);
        if (subject_interval_it == interval_end_it)
        {
            inputstream.seekg(BYTES_PER_PREDICATE+BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the predicate and object in this case
            continue;
        }
        k_type subject_end_time = (subject_interval_it->second).second;
        if (subject_end_time<upper_level)
        {
            inputstream.seekg(BYTES_PER_PREDICATE+BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the predicate and object in this case
            continue;
        }

        block_or_singleton_index predicate = read_uint_PREDICATE_little_endian(inputstream);

        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        auto object_interval_it = interval_map.find(subject);
        if (object_interval_it == interval_end_it)
        {
            continue;
        }
        k_type object_start_time = (object_interval_it->second).first;
        if (object_start_time>lower_level)
        {
            continue;
        }

        // Vertices that don't get mapped explicitly just stay the same
        auto subject_it = refines_edges.find(subject);
        if (subject_it != refines_end_it)
        {
            subject = subject_it->second;
        }

        quotient_graph_triples.insert({subject,predicate,object});
    }
}

void read_data_edges_timed(const std::string &infilename, triple_set &quotient_graph_triples, global_refines_type &refines_edges, interval_map_type &interval_map, k_type level)
{
    std::ifstream infile(infilename, std::ifstream::in);
    read_data_edges_from_stream_timed(infile, quotient_graph_triples, refines_edges, interval_map, level);
}

void read_data_edges_from_stream_timed_early(std::istream &inputstream, triple_set &quotient_graph_triples, interval_map_type &interval_map, k_type level)
{
    auto interval_end_it = interval_map.cend();
    // Read a mapping file
    while (true)
    {
        block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        if (inputstream.eof())
        {
            break;
        }
        auto subject_interval_it = interval_map.find(subject);
        if (subject_interval_it == interval_end_it)
        {
            // std::cout << "DEBUG: could not find subj: " << subject << std::endl;
            inputstream.seekg(BYTES_PER_PREDICATE+BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the predicate and object in this case
            continue;
        }

        block_or_singleton_index predicate = read_uint_PREDICATE_little_endian(inputstream);

        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(inputstream);
        auto object_interval_it = interval_map.find(object);
        if (object_interval_it == interval_end_it)
        {
            // std::cout << "DEBUG: could not find obj: " << object << std::endl;
            continue;
        }
        k_type object_end_time = (object_interval_it->second).second;
        if (object_end_time!=level)
        {
            // std::cout << "DEBUG: wrong level obj: " << object << ": [" << (object_interval_it->second).first << "," << (object_interval_it->second).second << "]" <<  std::endl;
            continue;
        }

        quotient_graph_triples.insert({subject,predicate,object});
    }
}

void read_data_edges_timed_early(const std::string &infilename, triple_set &quotient_graph_triples, interval_map_type &interval_map, k_type level)
{
    std::ifstream infile(infilename, std::ifstream::in);
    read_data_edges_from_stream_timed_early(infile, quotient_graph_triples, interval_map, level);
}

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("experiment_directory", po::value<std::string>(), "The directory for the experiment of interest");
    global.add_options()("level", po::value<int32_t>(), "Which level the generate the quotient graph for. Use -1 as an alias for the fixed point.");

    po::positional_options_description pos;
    pos.add("experiment_directory", 1).add("level", 2);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string experiment_directory = vm["experiment_directory"].as<std::string>();
    int32_t input_level = vm["level"].as<int32_t>();

    std::string graph_stats_file = experiment_directory + "ad_hoc_results/graph_stats.json";
    std::ifstream graph_stats_file_stream(graph_stats_file);

    std::string graph_stats_line;
    std::string final_depth_string = "\"Final depth\"";
    std::string fixed_point_string = "\"Fixed point\"";
    
    k_type final_depth;
    bool fixed_point_reached;
    
    bool k_found = false;
    bool fixed_point_found = false;

    while (std::getline(graph_stats_file_stream, graph_stats_line))
    {
        boost::trim(graph_stats_line);
        boost::erase_all(graph_stats_line, ",");
        std::vector<std::string> result;
        boost::split(result, graph_stats_line, boost::is_any_of(":"));
        if (result[0] == final_depth_string)
        {
            std::stringstream sstream(result[1]);
            sstream >> final_depth;
            k_found = true;
        }
        else if (result[0] == fixed_point_string)
        {
            std::stringstream sstream(result[1]);
            std::string fixed_point_string = sstream.str();
            boost::trim(fixed_point_string);
            if (fixed_point_string == "true")
            {
                fixed_point_reached = true;
            }
            else if (fixed_point_string != "false")
            {
                throw MyException("The \"Fixed point\" value in the graph_stats.json file has not been set to one of \"true\"/\"false\"");
            }
            fixed_point_found = true;
        }
        if (k_found && fixed_point_found)
        {
            break;
        }
    }
    graph_stats_file_stream.close();

    k_type level;
    if (input_level == -1)
    {
        if (not fixed_point_reached)
        {
            throw MyException("The level was specified as -1, but the bisimulation has not reached a fixed point. Use an absolute (non-negative) level instead.");
        }
        level = final_depth;  // TODO add some logic to for this case, since it is slighly different
    }
    else
    {
        if (input_level > final_depth)
        {
            throw MyException("The specified level goes beyond the final depth reached by the bisimulation.");
        }
        if (level == final_depth and not fixed_point_reached)
        {
            throw MyException("The specified level is the last computed level, but it is NOT the fixed point. To get the quotient graph for this level, please compute the bismulation for one more level.");
        }
        level = input_level;
    }

    std::ostringstream level_stringstream;
    level_stringstream << std::setw(4) << std::setfill('0') << level;
    std::string level_string(level_stringstream.str());

    std::string quotient_graphs_directory = experiment_directory + "quotient_graphs/";
    if (!std::filesystem::exists(quotient_graphs_directory))
    {
        std::filesystem::create_directory(quotient_graphs_directory);
    }

    // Read the entity to id map
    std::cout << "Reading the entity to id map" << std::endl;
    std::string entity_id_file = experiment_directory + "entity2ID.txt";
    std::ifstream entity_id_file_stream(entity_id_file, std::ifstream::in);

    id_entity_map entity_names;
    std::string line;
    std::string delimiter = " ";
    while (std::getline(entity_id_file_stream, line))
    {
        size_t delimiter_pos = line.find(delimiter);
        std::string entity_string = line.substr(0, delimiter_pos);
        std::string id_string = line.substr(delimiter_pos + delimiter.size());
        entity_names[std::stoull(id_string)] = entity_string;
    }
    entity_id_file_stream.close();

    std::string outcome_contains_file = quotient_graphs_directory + "quotient_graph_contains-" + level_string +".txt";
    std::ofstream outcome_contains_file_stream(outcome_contains_file, std::ios::trunc);

    if (level == final_depth)  // This is the fixed point, so we don't have explicit refines edges
    {
        // Read the node intervals
        std::cout << "Reading data edges" << std::endl;
        std::string intervals_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin";
        std::ifstream intervals_file_stream(intervals_file, std::ifstream::in);

        block_set living_blocks;
        block_set used_living_blocks;  // There might be disconnected nodes in the living blocks, used_living_blocks is used to filter those out

        while (true)
        {
            block_or_singleton_index global_block_id = read_int_BLOCK_OR_SINGLETON_little_endian(intervals_file_stream);
            if (intervals_file_stream.eof())
            {
                break;
            }
            block_or_singleton_index start_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
            block_or_singleton_index end_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
            
            if (end_time == level)
            {
                living_blocks.emplace(global_block_id);
            }
        }
        used_living_blocks.reserve(living_blocks.size());  // Prevent possbile rehasing later

        // Read the local to global ids
        std::cout << "Reading the global ids" << std::endl;
        std::string local_to_global_file = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin";
        std::ifstream local_to_global_file_stream(local_to_global_file, std::ifstream::in);

        level_to_local_to_global_map local_to_global_map;

        auto living_blocks_end_it = living_blocks.cend();

        while (true)
        {
            k_type local_level = read_uint_K_TYPE_little_endian(local_to_global_file_stream);
            if (local_to_global_file_stream.eof())
            {
                break;
            }
            block_or_singleton_index local_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_file_stream);
            block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_file_stream);
            if (living_blocks.find(global_block) != living_blocks_end_it)
            {
                local_to_global_map[local_level].emplace(local_block,global_block);
            }
        }

        for (auto level_map_pair: local_to_global_map)
        {
            auto level_end_it = level_map_pair.second.cend();
            k_type current_level = level_map_pair.first;

            std::ostringstream local_level_stringstream;
            local_level_stringstream << std::setw(4) << std::setfill('0') << current_level;
            std::string local_level_string(local_level_stringstream.str());

            std::string outcome_file = experiment_directory + "bisimulation/outcome_condensed-" + local_level_string + ".bin";
            std::ifstream outcome_file_stream(outcome_file, std::ifstream::in);

            while (true)
            {
                block_or_singleton_index block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(outcome_file_stream));
                if (outcome_file_stream.eof())
                {
                    break;
                }

                node_index block_size = read_uint_ENTITY_little_endian(outcome_file_stream);

                auto block_it = level_map_pair.second.find(block);
                if (block_it == level_end_it)
                {
                    outcome_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
                    continue;
                }
                // k_type stored_local_level = block_it->first;
                // if (stored_local_level != current_level)
                // {
                //     outcome_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
                //     continue;
                // }
                block_or_singleton_index global_block = block_it->second;
                
                for (node_index i = 0; i < block_size; i++)
                {
                    node_index entity_id = read_uint_ENTITY_little_endian(outcome_file_stream);
                    std::string entity = entity_names[entity_id];

                    outcome_contains_file_stream << global_block << " " << entity << "\n";
                }
            }
        }
        outcome_contains_file_stream.flush();

        // Read the data edges
        std::cout << "Reading data edges" << std::endl;
        std::string data_edges_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
        std::ifstream data_edges_file_stream(data_edges_file, std::ifstream::in);

        triple_set data_edges;

        while (true)
        {
            block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
            if (data_edges_file_stream.eof())
            {
                break;
            }
            auto living_subject_it = living_blocks.find(subject);
            if (living_subject_it == living_blocks_end_it)
            {
                intervals_file_stream.seekg(BYTES_PER_PREDICATE+BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the predicate and object in this case
                continue;
            }

            edge_type predicate = read_uint_PREDICATE_little_endian(data_edges_file_stream);

            block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
            auto living_object_it = living_blocks.find(object);
            if (living_object_it == living_blocks_end_it)
            {
                continue;
            }

            std::tuple triple = std::make_tuple(subject,predicate,object);
            used_living_blocks.emplace(subject);
            used_living_blocks.emplace(object);
            data_edges.emplace(triple);
        }

        // Write the quotient graph stats
        std::cout << "Writing the quotient graph stats" << std::endl;
        std::string outcome_stats_file = quotient_graphs_directory + "quotient_graph_stats-" + level_string + ".json";
        std::ofstream outcome_stats_file_stream(outcome_stats_file, std::ios::trunc);

        outcome_stats_file_stream << "{\n";
        outcome_stats_file_stream << "    " << "\"Vertex count\": " << used_living_blocks.size() << ",\n";
        outcome_stats_file_stream << "    " << "\"Edge count\": " << data_edges.size() << "\n";
        outcome_stats_file_stream << "}";

        outcome_stats_file_stream.close();
        
        //TODO this currently ONLY saves the final quotient graph, NOT the associated entities
        
        // Write the quotient graph edges and types
        std::cout << "Writing the quotient graph edges and types" << std::endl;
        std::string outcome_edges_file = quotient_graphs_directory + "quotient_graph_edges-" + level_string + ".txt";
        std::ofstream outcome_edges_file_stream(outcome_edges_file, std::ios::trunc);

        std::string outcome_types_file = quotient_graphs_directory + "quotient_graph_types-" + level_string + ".txt";
        std::ofstream outcome_types_file_stream(outcome_types_file, std::ios::trunc);

        for (auto triple: data_edges)
        {
            block_or_singleton_index subject = std::get<0>(triple);
            block_or_singleton_index predicate = std::get<1>(triple);
            block_or_singleton_index object = std::get<2>(triple);

            outcome_edges_file_stream << subject << " " << object << "\n";
            outcome_types_file_stream << predicate << "\n";
        }
        outcome_edges_file_stream.close();
        outcome_types_file_stream.close();


        exit(0);  // Close the program
    }

    std::string outcome_zero_file = experiment_directory + "bisimulation/outcome_condensed-0000.bin";
    std::ifstream outcome_zero_file_stream(outcome_zero_file, std::ifstream::in);

    block_map living_blocks;  // local_id --> (level, global_id)

    const block_or_singleton_index GLOBAL_ID_PLACEHOLDER = 0;

    // Read the initial blocks
    std::cout << "Reading the initial blocks" << std::endl;
    while (true)
    {
        block_or_singleton_index block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(outcome_zero_file_stream));
        if (outcome_zero_file_stream.eof())
        {
            break;
        }

        living_blocks[block] = std::make_pair(0, GLOBAL_ID_PLACEHOLDER);
        node_index block_size = read_uint_ENTITY_little_endian(outcome_zero_file_stream);
        outcome_zero_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
    }
    outcome_zero_file_stream.close();

    // Find the living blocks at the specified level
    std::cout << "Finding the living blocks" << std::endl;

    bool singletons_found;
    block_set replaced_blocks;

    for (k_type i = 0; i < level; i++)
    {
        singletons_found = false;

        std::ostringstream current_level_stringstream;
        current_level_stringstream << std::setw(4) << std::setfill('0') << i;
        std::string current_level_string(current_level_stringstream.str());

        std::ostringstream next_level_stringstream;
        next_level_stringstream << std::setw(4) << std::setfill('0') << i+1;
        std::string next_level_string(next_level_stringstream.str());

        std::string mapping_file = experiment_directory + "bisimulation/mapping-" + current_level_string + "to" + next_level_string + ".bin";
        std::ifstream mapping_file_stream(mapping_file, std::ifstream::in);

        replaced_blocks.clear();

        while (true)
        {
            block_or_singleton_index merged_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(mapping_file_stream));
            if (mapping_file_stream.eof())
            {
                break;
            }
            if (replaced_blocks.find(merged_block) == replaced_blocks.cend())
            {
                living_blocks.erase(merged_block);
            }
            block_index split_block_count = read_uint_BLOCK_little_endian(mapping_file_stream);
            for (block_index j = 0; j < split_block_count; j++)
            {
                block_or_singleton_index split_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(mapping_file_stream));
                if (split_block == 0)
                {
                    singletons_found = true;
                    continue;
                }
                replaced_blocks.emplace(split_block);
                living_blocks[split_block] = std::make_pair(i+1, GLOBAL_ID_PLACEHOLDER);
            }

        }
        mapping_file_stream.close();

        if (singletons_found)
        {
            std::string singleton_mapping_file = experiment_directory + "bisimulation/singleton_mapping-" + current_level_string + "to" + next_level_string + ".bin";
            std::ifstream singleton_mapping_file_stream(singleton_mapping_file, std::ifstream::in);

            while (true)
            {
                read_uint_BLOCK_little_endian(singleton_mapping_file_stream);
                if (singleton_mapping_file_stream.eof())
                {
                    break;
                }
                block_or_singleton_index singleton_count = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_mapping_file_stream);
                for (block_or_singleton_index j = 0; j < singleton_count; j++)
                {
                    block_or_singleton_index singleton = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_mapping_file_stream);
                    // TODO We might not have to store the stingleton values???
                    living_blocks[singleton] = std::make_pair(i+1, singleton);  // Singletons have a unique local block ID be design, so it is reused for the global id
                    node_index singleton_entity = static_cast<node_index>(-(singleton+1));
                    outcome_contains_file_stream << singleton << " " << entity_names[singleton_entity] << "\n";
                }
            }
            singleton_mapping_file_stream.close();
        }
    }
    outcome_contains_file_stream.flush();  // Flush the intermediate results to the file

    // Read and store the global ids for the summary nodes
    std::cout << "Reading the global ids" << std::endl;
    std::string local_to_global_file = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin";
    std::ifstream local_to_global_file_stream(local_to_global_file, std::ifstream::in);

    block_map next_blocks;  // local_id --> (level, global_id)

    auto living_blocks_end_it = living_blocks.cend();

    while (true)
    {
        k_type local_level = read_uint_K_TYPE_little_endian(local_to_global_file_stream);
        if (local_to_global_file_stream.eof())
        {
            break;
        }
        block_or_singleton_index local_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_file_stream);

        if (local_level == level + 1)  // We will need these ids later when mapping data edge subjects over refines edges
        {
            block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_file_stream);
            next_blocks[local_block] = std::make_pair(local_level, global_block);
            continue;
        }

        auto local_block_it = living_blocks.find(local_block);
        if (local_block_it == living_blocks_end_it)  // No block by the local id found
        {
            local_to_global_file_stream.seekg(BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);
            continue;
        }
        k_type stored_local_level = (local_block_it->second).first;
        if (local_level != stored_local_level)  // The block found by the local id starts at another level
        {
            local_to_global_file_stream.seekg(BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);
            continue;
        }
        block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_file_stream);
        (local_block_it->second).second = global_block;
    }
    local_to_global_file_stream.close();

    std::ostringstream next_level_stringstream;
    next_level_stringstream << std::setw(4) << std::setfill('0') << level+1;
    std::string next_level_string(next_level_stringstream.str());

    std::string next_level_singleton_mapping_file = experiment_directory + "bisimulation/singleton_mapping-" + level_string + "to" + next_level_string + ".bin";

    if (std::filesystem::exists(next_level_singleton_mapping_file))
    {
        std::ifstream next_level_singleton_mapping_file_stream(next_level_singleton_mapping_file, std::ifstream::in);

        while (true)
        {
            read_uint_BLOCK_little_endian(next_level_singleton_mapping_file_stream);
            if (next_level_singleton_mapping_file_stream.eof())
            {
                break;
            }
            block_or_singleton_index singleton_count = read_int_BLOCK_OR_SINGLETON_little_endian(next_level_singleton_mapping_file_stream);
            for (block_or_singleton_index j = 0; j < singleton_count; j++)
            {
                block_or_singleton_index singleton = read_int_BLOCK_OR_SINGLETON_little_endian(next_level_singleton_mapping_file_stream);
                next_blocks[singleton] = std::make_pair(level+1, singleton);
            }
        }
        next_level_singleton_mapping_file_stream.close();
    }

    std::cout << "Reading and storing the contained entities" << std::endl;
    for (k_type i = 0; i <= level; i++)
    {
        std::ostringstream current_level_stringstream;
        current_level_stringstream << std::setw(4) << std::setfill('0') << i;
        std::string current_level_string(current_level_stringstream.str());
        
        std::string outcome_file = experiment_directory + "bisimulation/outcome_condensed-" + current_level_string +".bin";
        std::ifstream outcome_file_stream(outcome_file, std::ifstream::in);
        
        while (true)
        {
            block_or_singleton_index block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(outcome_file_stream));
            if (outcome_file_stream.eof())
            {
                break;
            }

            node_index block_size = read_uint_ENTITY_little_endian(outcome_file_stream);

            auto block_it = living_blocks.find(block);
            if (block_it == living_blocks_end_it)
            {
                outcome_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
                continue;
            }
            k_type stored_local_level = (block_it->second).first;
            if (i != stored_local_level)
            {
                outcome_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
                continue;
            }
            block_or_singleton_index global_block = (block_it->second).second;
            
            for (node_index j = 0; j < block_size; j++)
            {
                node_index entity_id = read_uint_ENTITY_little_endian(outcome_file_stream);
                std::string entity = entity_names[entity_id];

                outcome_contains_file_stream << global_block << " " << entity << "\n";
            }
        }
    }
    outcome_contains_file_stream.close();  // This should flush

    std::cout << "Reading " << level_string << "-->" << next_level_string << " mapping" << std::endl;
    std::string mapping_file = experiment_directory + "bisimulation/mapping-" + level_string + "to" + next_level_string + ".bin";
    std::ifstream mapping_file_stream(mapping_file, std::ifstream::in);

    global_refines_type refines_edges;

    bool new_singletons_found = false;
    while (true)
    {
        block_or_singleton_index merged_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(mapping_file_stream));
        if (mapping_file_stream.eof())
        {
            break;
        }
        block_or_singleton_index global_merged_block = living_blocks[merged_block].second;
        
        block_index split_block_count = read_uint_BLOCK_little_endian(mapping_file_stream);
        for (block_index i = 0; i < split_block_count; i++)
        {
            block_or_singleton_index split_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(mapping_file_stream));
            if (split_block == 0)
            {
                new_singletons_found = true;
                continue;
            }
            block_or_singleton_index global_split_block = next_blocks[split_block].second;
            refines_edges[global_split_block] = global_merged_block;
        }

    }
    mapping_file_stream.close();

    if (new_singletons_found)
    {
        std::cout << "Reading " << level_string << "-->" << next_level_string << " singleton mapping" << std::endl;
        std::string singleton_mapping_file = experiment_directory + "bisimulation/singleton_mapping-" + level_string + "to" + next_level_string + ".bin";
        std::ifstream singleton_mapping_file_stream(singleton_mapping_file, std::ifstream::in);

        while (true)
        {
            block_or_singleton_index merged_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(singleton_mapping_file_stream));
            if (singleton_mapping_file_stream.eof())
            {
                break;
            }
            block_or_singleton_index global_merged_block = living_blocks[merged_block].second;
            block_or_singleton_index singleton_count = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_mapping_file_stream);
            for (block_or_singleton_index i = 0; i < singleton_count; i++)
            {
                block_or_singleton_index singleton = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_mapping_file_stream);
                refines_edges[singleton] = global_merged_block;
            }
        }
        singleton_mapping_file_stream.close();
    }
    else
    {
        std::cout << "No singletons found" << std::endl;
    }

    // Create the global_living_blocks and global_next_blocks sets
    std::cout << "Creating global living blocks and global next blocks sets" << std::endl;
    block_set global_living_blocks;
    block_set global_next_blocks;

    for (auto local_data_pair: living_blocks)
    {
        block_or_singleton_index global_block = local_data_pair.second.second;
        global_living_blocks.emplace(global_block);
    }
    for (auto local_data_pair: next_blocks)
    {
        block_or_singleton_index global_block = local_data_pair.second.second;
        global_next_blocks.emplace(global_block);
    }

    // Read the data edges    
    std::cout << "Reading data edges" << std::endl;
    std::string data_edges_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
    std::ifstream data_edges_file_stream(data_edges_file, std::ifstream::in);

    auto global_next_blocks_end_it = global_next_blocks.cend();
    auto global_living_blocks_end_it = global_living_blocks.cend();
    triple_set data_edges;

    block_set used_living_blocks;  // There might be disconnected nodes in the living blocks, used_living_blocks is used to filter those out
    used_living_blocks.reserve(global_living_blocks.size() + global_next_blocks.size());  // We care about the size of the union, so taking the size of both maps provides an upper bound

    while (true)
    {
        block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
        if (data_edges_file_stream.eof())
        {
            break;
        }

        auto subject_global_next_block_it = global_next_blocks.find(subject);
        // We know next blocks only contains the newly created blocks, but these are not the only relevant ones
        if (subject_global_next_block_it == global_next_blocks_end_it)
        {
            auto subject_global_living_it = global_living_blocks.find(subject);
            if (subject_global_living_it == global_living_blocks_end_it)
            {
                data_edges_file_stream.seekg(BYTES_PER_PREDICATE+BYTES_PER_BLOCK_OR_SINGLETON, std::ios_base::cur);  // No need to read the predicate and object in this case
                continue;
            }
            edge_type predicate = read_uint_PREDICATE_little_endian(data_edges_file_stream);
            block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
            auto object_global_living_it = global_living_blocks.find(object);
            // If the object is alive at level then the subject is alive at level+1 (and we already established this same subject is alive at level)
            if (object_global_living_it != global_living_blocks_end_it)
            {
                std::tuple triple = std::make_tuple(subject,predicate,object);  // We don't have to map the subject, as it did not refine between level and level+1 (i.e. the id stays the same)
                used_living_blocks.emplace(subject);
                used_living_blocks.emplace(object);
                data_edges.emplace(triple);
            }
            continue;
        }
        // In this case we have a data edge from level+1 to level and we should map the subject over the refines edge (to get an edge from level to level)
        block_or_singleton_index mapped_subject = refines_edges[subject];  // Map the subject over the refines edge
        edge_type predicate = read_uint_PREDICATE_little_endian(data_edges_file_stream);
        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
        auto object_global_living_it = global_living_blocks.find(object);
        if (object_global_living_it == global_living_blocks_end_it)
        {
            continue;
        }
        std::tuple triple = std::make_tuple(mapped_subject,predicate,object);
        used_living_blocks.emplace(subject);
        used_living_blocks.emplace(object);
        data_edges.emplace(triple);
    }

    // Write the quotient graph stats
    std::cout << "Writing the quotient graph stats" << std::endl;
    std::string outcome_stats_file = quotient_graphs_directory + "quotient_graph_stats-" + level_string + ".json";
    std::ofstream outcome_stats_file_stream(outcome_stats_file, std::ios::trunc);

    outcome_stats_file_stream << "{\n";
    outcome_stats_file_stream << "    " << "\"Vertex count\": " << used_living_blocks.size() << ",\n";
    outcome_stats_file_stream << "    " << "\"Edge count\": " << data_edges.size() << "\n";
    outcome_stats_file_stream << "}";

    outcome_stats_file_stream.close();

    // Write the quotient graph edges and types
    std::cout << "Writing the quotient graph edges and types" << std::endl;
    std::string outcome_edges_file = quotient_graphs_directory + "quotient_graph_edges-" + level_string + ".txt";
    std::ofstream outcome_edges_file_stream(outcome_edges_file, std::ios::trunc);

    std::string outcome_types_file = quotient_graphs_directory + "quotient_graph_types-" + level_string + ".txt";
    std::ofstream outcome_types_file_stream(outcome_types_file, std::ios::trunc);

    for (auto triple: data_edges)
    {
        block_or_singleton_index subject = std::get<0>(triple);
        block_or_singleton_index predicate = std::get<1>(triple);
        block_or_singleton_index object = std::get<2>(triple);

        outcome_edges_file_stream << subject << " " << object << "\n";
        outcome_types_file_stream << predicate << "\n";
    }
    outcome_edges_file_stream.close();
    outcome_types_file_stream.close();
}