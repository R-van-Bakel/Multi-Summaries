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
using block_set = boost::unordered_flat_set<block_or_singleton_index>;
using id_entity_map = boost::unordered_flat_map<node_index,std::string>;
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
        level = final_depth;  // TODO add some logic to for this case, since it is slighlt different
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
        // std::cout << "DEBUG initial block count: " << block_size << std::endl;
        outcome_zero_file_stream.seekg(block_size*BYTES_PER_ENTITY, std::ios_base::cur);
    }
    outcome_zero_file_stream.close();
    
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

    // Find the living blocks at the specified level
    std::cout << "Finding the living blocks" << std::endl;

    std::ostringstream level_stringstream;
    level_stringstream << std::setw(4) << std::setfill('0') << level;
    std::string level_string(level_stringstream.str());

    std::string quotient_graphs_directory = experiment_directory + "quotient_graphs/";
    if (!std::filesystem::exists(quotient_graphs_directory))
    {
        std::filesystem::create_directory(quotient_graphs_directory);
    }

    std::string outcome_contains_file = quotient_graphs_directory + "quotient_graph_contains-" + level_string +".txt";
    std::ofstream outcome_contains_file_stream(outcome_contains_file, std::ios::trunc);

    bool singletons_found;
    // node_index singleton_counts = 0;
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
                    // singleton_counts += 1;
                }
            }
            singleton_mapping_file_stream.close();
        }
    }
    outcome_contains_file_stream.flush();  // Flush the intermediate results to the file

    // std::cout << "DEBUG singleton count: " << singleton_counts << std::endl;
    // std::cout << "DEBUG living block count: " << living_blocks.size() << std::endl; 

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



    // >>> DEBUG >>>
    for(auto thingy: living_blocks)
    {
        if (thingy.second.second == 0)
        {
            std::cout << "DEBUG non-updated block: (" << thingy.first << "," << thingy.second.first << ") --> 0" << std::endl;
        }
    }

    std::string next_level_mapping_file = experiment_directory + "bisimulation/mapping-" + level_string + "to" + next_level_string + ".bin";
    std::ifstream next_level_mapping_file_stream(next_level_mapping_file, std::ifstream::in);
    block_set alt_next;

    while (true)
    {
        read_uint_BLOCK_little_endian(next_level_mapping_file_stream);
        if (next_level_mapping_file_stream.eof())
        {
            break;
        }
        block_index split_block_count = read_uint_BLOCK_little_endian(next_level_mapping_file_stream);
        for (block_index i = 0; i < split_block_count; i++)
        {
            block_or_singleton_index split_block = static_cast<block_or_singleton_index>(read_uint_BLOCK_little_endian(next_level_mapping_file_stream));
            if (split_block == 0)
            {
                continue;
            }
            alt_next.emplace(split_block);
        }
    }
    next_level_mapping_file_stream.close();

    for (block_or_singleton_index alt: alt_next)
    {
        if (next_blocks.find(alt) == next_blocks.cend())
        {
            std::cout << "DEBUG alt next missing in next: " << alt << std::endl;
        }
    }
    for (auto block: next_blocks)
    {
        if (alt_next.find(block.first) == alt_next.cend())
        {
            std::cout << "DEBUG next missing in alt next: " << block.first << std::endl;
        }
    }
    // <<< DEBUG <<<



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

    // for (auto lb_pair: living_blocks)
    // {
    //     std::cout << "DEBUG living block: " << lb_pair.first << " (" << lb_pair.second.first << ", " <<  lb_pair.second.second << ")" << std::endl;
    // }

    // Read and store the entities contained in the living blocks
    // TODO ALSO READ SINGLETONS
    std::cout << "Reading and storing the contained entities" << std::endl;
    // node_index DEBUG_block_sizes = 0;
    for (k_type i = 0; i <= level; i++)
    {
        // std::cout << "DEBUG level: " << i << std::endl;
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
            // std::cout << "DEBUG block-size: " << block << " --- " << block_size << std::endl;

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
            // DEBUG_block_sizes += 1;
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
    // std::cout << "DEBUG block sizes: " << DEBUG_block_sizes << std::endl;

    if (level == final_depth)  // This is the fixed point, so we don't have explicit refines edges
    {
        // TODO do stuff
        exit(0);  // Close the program
    }

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
        // if (global_merged_block == 1021)
        // {
        //     std::cout << "DEBUG merged block: 1021" << std::endl;
        // }
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
                // std::cout << "DEBUG singleton refines edge: " << singleton << std::endl;
                refines_edges[singleton] = global_merged_block;
            }
        }
        singleton_mapping_file_stream.close();
    }
    else
    {
        std::cout << "No singletons found" << std::endl;
    }

    // for (auto s_o: refines_edges)
    // {
    //     std::cout << "DEBUG s-o: " << s_o.first << "," << s_o.second << std::endl;
    // }

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

    // for (auto s_o: refines_edges)
    // {
    //     // if (global_living_blocks.find(s_o.first) == global_living_blocks.cend())
    //     // {
    //     //     std::cout << "DEBUG: wrong refines subject: " << s_o.first << std::endl;
    //     // }
    //     if (global_living_blocks.find(s_o.second) == global_living_blocks.cend())
    //     {
    //         std::cout << "DEBUG: wrong refines object: " << s_o.second << std::endl;
    //     }
    // }

    // Read the data edges    
    std::cout << "Reading data edges" << std::endl;
    std::string data_edges_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
    std::ifstream data_edges_file_stream(data_edges_file, std::ifstream::in);

    auto global_next_blocks_end_it = global_next_blocks.cend();
    auto global_living_blocks_end_it = global_living_blocks.cend();
    triple_set data_edges;
    // bool DEBUG_subject_found;



    // >>> DEBUG >>>
    std::string intervals_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin";
    std::ifstream intervals_file_stream(intervals_file, std::ifstream::in);

    block_set interval_blocks;

    while (true)
    {
        block_or_singleton_index block = read_int_BLOCK_OR_SINGLETON_little_endian(intervals_file_stream);
        if (intervals_file_stream.eof())
        {
            break;
        }
        block_or_singleton_index start_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
        block_or_singleton_index end_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
        if (start_time <= level and end_time >= start_time)
        {
            interval_blocks.emplace(block);
        }
    }

    for (block_or_singleton_index glob: global_living_blocks)
    {
        if (global_living_blocks.find(glob) == global_living_blocks.cend())
        {
            std::cout << "DEBUG alt missing glob in inter: " << glob << std::endl;
        }
    }
    for (block_or_singleton_index inter: interval_blocks)
    {
        if (interval_blocks.find(inter) == interval_blocks.cend())
        {
            std::cout << "DEBUG next missing inter in glob: " << inter << std::endl;
        }
    }
    // <<< DEBUG <<<




    while (true)
    {
        block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
        if (data_edges_file_stream.eof())
        {
            break;
        }
        // DEBUG_subject_found = false;
        // if(subject == 1021)
        // {
        //     DEBUG_subject_found = true;
        // }
        auto subject_global_next_block_it = global_next_blocks.find(subject);
        // We next blocks only contains the newly created blocks, but these are not the only relevant ones
        if (subject_global_next_block_it == global_next_blocks_end_it)
        {
            auto subject_global_living_it = global_living_blocks.find(subject);
            if (subject_global_living_it == global_living_blocks_end_it)
            {
                // if (DEBUG_subject_found)
                // {
                //     std::cout << "DEBUG: not next, not living" << std::endl;
                // }
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
                data_edges.emplace(triple);
            }
            continue;
        }
        // In this case we have a data edge from level+1 to level and we should map the subject over the refines edge (to get an edge from level to level)
        block_or_singleton_index mapped_subject = refines_edges[subject];  // Map the subject over the refines edge
        // if (mapped_subject == 1021)
        // {
        //     std::cout << "DEBUG sub-->mapped_sub: " << subject << "," << mapped_subject << std::endl;
        // }
        edge_type predicate = read_uint_PREDICATE_little_endian(data_edges_file_stream);
        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
        auto object_global_living_it = global_living_blocks.find(object);
        if (object_global_living_it == global_living_blocks_end_it)
        {
            continue;
        }
        std::tuple triple = std::make_tuple(mapped_subject,predicate,object);
        data_edges.emplace(triple);
    }

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

    // block_set data_edge_subjects;
    // block_set data_edge_objects;

    // for (auto triple: data_edges)
    // {
    //     block_or_singleton_index subject = std::get<0>(triple);
    //     block_or_singleton_index object = std::get<2>(triple);
    //     data_edge_subjects.emplace(subject);
    //     data_edge_objects.emplace(object);
    // }

    // auto data_edge_subjects_end_it = data_edge_subjects.cend();
    // auto data_edge_objects_end_it = data_edge_objects.cend();

    // for (auto global_living_block: global_living_blocks)
    // {
    //     auto check_subject_it = data_edge_subjects.find(global_living_block);
    //     auto check_object_it = data_edge_objects.find(global_living_block);
    //     if (check_subject_it == data_edge_subjects_end_it and check_object_it == data_edge_objects_end_it)
    //     {
    //         std::cout << "DEBUG missing living block: " << global_living_block << std::endl;
    //     }
    // }

    // ##############################################################################################################

    // std::ostringstream level_stringstream;
    // level_stringstream << std::setw(4) << std::setfill('0') << level;
    // std::string level_string(level_stringstream.str());

    // std::ostringstream next_level_stringstream;
    // next_level_stringstream << std::setw(4) << std::setfill('0') << level+1;
    // std::string next_level_string(next_level_stringstream.str());

    // std::string data_edges_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
    // std::string output_file = experiment_directory + "bisimulation/quotient_graph_level-" + level_string + ".bin";

    // std::string interval_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin";
    // std::ifstream interval_file_stream(interval_file, std::ifstream::in);
    // block_or_singleton_index interval_count = static_cast<size_t>(interval_file_stream.tellg()/SUMMARY_NODE_INTERVAL_PAIR_SIZE);

    // interval_map_type interval_map;
    // interval_map.reserve(interval_count);  // We can reserve space to avoid frequent rehashing

    // read_intervals_timed(interval_file, interval_map, level);
    
    // if (level == final_depth)
    // {
    //     triple_set quotient_graph_triples;
    //     read_data_edges_timed_early(data_edges_file, quotient_graph_triples, interval_map, level);

        
    //     std::cout << "DEBUG intervals size: " << interval_map.size() << std::endl;
    //     std::cout << "DEBUG quotient size: " << quotient_graph_triples.size() << std::endl;
    //     for (auto node_interval: interval_map)
    //     {
    //         std::cout << "DEBUG: " << node_interval.first << ": [" << node_interval.second.first << "," << node_interval.second.second << "]" << std::endl;
    //     }

    //     for (auto trip: quotient_graph_triples)
    //     {
    //         std::cout << "DEBUG: " << std::get<0>(trip) << "," << std::get<1>(trip) << "," << std::get<2>(trip) << std::endl;
    //     }
    //     exit(0);  // Close the program
    // }

    // std::string mapping_file = experiment_directory + "bisimulation/mapping-" + level_string + "to" + next_level_string + ".bin";
    // std::string singleton_mapping_file = experiment_directory + "bisimulation/singleton_mapping-" + level_string + "to" + next_level_string + ".bin";

    // local_refines_type local_refines_edges;
    // read_mapping_timed(mapping_file, local_refines_edges);

    // // for (auto refi: local_refines_edges)
    // // {
    // //     std::cout << refi.first << "," << refi.second << std::endl;
    // // }

    // read_singleton_mapping_timed(singleton_mapping_file, local_refines_edges);

    // // for (auto refi: local_refines_edges)
    // // {
    // //     std::cout << refi.first << "," << refi.second << std::endl;
    // // }

    // std::string local_to_global_file = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin";

    // local_to_global_map_type local_to_global_map;
    // read_local_global_map_timed(local_to_global_file, local_to_global_map, level);

    // // for (auto loc_glob: local_to_global_map)
    // // {
    // //     std::cout << loc_glob.first.first << "," << loc_glob.first.second <<  ":" << loc_glob.second << std::endl;
    // // }

    // // std::cout << "DEBUG: size=" << local_refines_edges.size() << std::endl;

    // global_refines_type global_refines_edges;
    // block_index refines_count = static_cast<size_t>(local_refines_edges.size());
    // size_t it_count = 0;
    // for (block_index i = 0; i < refines_count; i++)
    // {
    //     it_count++;
    //     std::pair<block_or_singleton_index,block_or_singleton_index> local_refines_edge = std::move(local_refines_edges.back());
    //     local_refines_edges.pop_back();
    //     block_or_singleton_index global_split_block;
    //     assert(local_refines_edge.second > 0);  // It should not be possible of the zero block or negative blocks to split
    //     if (local_refines_edge.first < 0)  // Literals don't have an explicit map, since they are global by design
    //     {
    //         // std::cout << "DEBUG singleton" << std::endl;
    //         global_split_block = local_refines_edge.first;
    //     }
    //     else
    //     {
    //         global_split_block = local_to_global_map[std::make_pair(level+1,local_refines_edge.first)];
    //     }
    //     block_or_singleton_index global_merged_block = local_to_global_map[std::make_pair(level,local_refines_edge.second)];
    //     global_refines_edges[global_merged_block] = global_split_block;
    // }

    // // std::cout << "DEBUG: size=" << local_refines_edges.size() << std::endl;
    // // std::cout << "DEBUG it count:" << it_count << std::endl;

    // // for (auto global_refines_edge: global_refines_edges)
    // // {
    // //     std::cout << global_refines_edge.first << ", " << global_refines_edge.second << std::endl;
    // // }

    // triple_set quotient_graph_triples;
    // read_data_edges_timed(data_edges_file, quotient_graph_triples, global_refines_edges, interval_map, level);

    // for (auto trip: quotient_graph_triples)
    // {
    //     std::cout << std::get<0>(trip) << "," << std::get<1>(trip) << "," << std::get<2>(trip) << std::endl;
    // }

    // // Write the data edges
    // // std::ofstream outfile(outfilename, std::ios::trunc | std::ofstream::out);
    // // write_int_BLOCK_OR_SINGLETON_little_endian(outputstream, subject);
    // // write_uint_PREDICATE_little_endian(outputstream, predicate);
    // // write_int_BLOCK_OR_SINGLETON_little_endian(outputstream, object);
}