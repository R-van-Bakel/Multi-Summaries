#include <string>
#include <fstream>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/program_options.hpp>

using edge_type = uint32_t;
using block_or_singleton_index = int64_t;
using k_type = uint16_t;
using triple_index = int64_t;
using interval_map_type = boost::unordered_flat_map<block_or_singleton_index,std::pair<k_type,k_type>>;
using block_set = boost::unordered_flat_set<block_or_singleton_index>;
const int BYTES_PER_PREDICATE = 4;
const int BYTES_PER_BLOCK_OR_SINGLETON = 5;
const int BYTES_PER_K_TYPE = 2;

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

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("experiment_directory", po::value<std::string>(), "The directory for the experiment of interest");

    po::positional_options_description pos;
    pos.add("experiment_directory", 1);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string experiment_directory = vm["experiment_directory"].as<std::string>();

    // Get the fixed point (if present)
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

    // Read the intervals
    std::cout << "Reading intervals" << std::endl;
    std::string intervals_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin";
    std::ifstream intervals_file_stream(intervals_file, std::ifstream::in);

    interval_map_type interval_map;

    // Read a mapping file
    while (true)
    {
        block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(intervals_file_stream);
        if (intervals_file_stream.eof())
        {
            break;
        }
        k_type start_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
        k_type end_time = read_uint_K_TYPE_little_endian(intervals_file_stream);
        // Assuming start_time <= end_time, we are not only left with summary nodes that are alive during lower_level and/or upper_level
        interval_map[global_block] = {start_time,end_time};
    }

    // Read the data edges
    std::string data_edges_file = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
    std::ifstream data_edges_file_stream(data_edges_file, std::ifstream::in);

    triple_index condensed_data_edge_count = 0;
    triple_index uncondensed_data_edge_count = 0;

    block_set blocks;

    triple_index final_data_edge_count = 0;

    while (true)
    {
        block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);
        if (data_edges_file_stream.eof())
        {
            break;
        }
        read_uint_PREDICATE_little_endian(data_edges_file_stream);
        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file_stream);

        blocks.emplace(subject);
        blocks.emplace(object);
        if (fixed_point_reached)
        {
            if (interval_map[subject].second == final_depth and interval_map[object].second == final_depth)
            {
                final_data_edge_count += 1;
            }
        }

        k_type subject_start = interval_map[subject].first;
        k_type subject_end = interval_map[subject].second;
        k_type object_start = interval_map[object].first;
        k_type object_end = interval_map[object].second;

        k_type offset_subject_start = subject_start-1;
        k_type offset_object_end = object_end+1;

        k_type edge_start = std::max(offset_subject_start,object_start);
        k_type edge_end = std::min(subject_end,offset_object_end);

        condensed_data_edge_count += 1;
        uncondensed_data_edge_count += edge_end-edge_start;
        if (uncondensed_data_edge_count < 0)
        {
            std::cout << "DEBUG: WARNING uncondensed_data_edge_count is negative: " << uncondensed_data_edge_count << std::endl;
        }
    }



    // Count the (connected) vertices and edges
    triple_index condensed_vertex_count = 0;
    triple_index uncondensed_vertex_count = 0;
    triple_index final_vertex_count = 0;

    for (block_or_singleton_index block: blocks)
    {
        condensed_vertex_count += 1;
        k_type start_time = interval_map[block].first;
        k_type end_time = interval_map[block].second;
        uncondensed_vertex_count += end_time - start_time + 1;
        if(end_time == final_depth)
        {
            final_vertex_count++;
        }
    }

    triple_index condensed_refines_edge_count = condensed_vertex_count-1;  // TODO technically should add final_vertex_count since all final blocks refine themeselves
    triple_index uncondensed_refines_edge_count = uncondensed_vertex_count-1;  // TODO technically should add final_vertex_count since all final blocks refine themeselves



    // Update the graph stats file
    std::cout << "Updating graph stats" << std::endl;
    std::string start_line = "{";
    std::string end_line = "}";
    std::string indentation = "    ";
    std::string summay_graph_stats_line;
    std::deque<std::string> stats_lines;

    std::string summay_graph_stats_file = experiment_directory + "ad_hoc_results/summary_graph_stats.json";
    std::ifstream summay_graph_stats_file_istream(summay_graph_stats_file);

    while (std::getline(summay_graph_stats_file_istream, summay_graph_stats_line))
    {
        boost::trim(summay_graph_stats_line);
        if (summay_graph_stats_line == start_line or summay_graph_stats_line == end_line)
        {
            continue;
        }

        stats_lines.push_back(summay_graph_stats_line);
    }
    summay_graph_stats_file_istream.close();

    std::ofstream summay_graph_stats_file_ostream(summay_graph_stats_file, std::ios::trunc);

    summay_graph_stats_file_ostream << start_line << std::endl;
    while (true) {
        std::string current_line = stats_lines[0];
        stats_lines.pop_front();
        summay_graph_stats_file_ostream << indentation << current_line;
        if (stats_lines.empty())
        {
            summay_graph_stats_file_ostream << "," << std::endl;
            break;
        }
        summay_graph_stats_file_ostream << std::endl;
    }
    summay_graph_stats_file_ostream << indentation << "\"Vertex count (condensed)\": " << condensed_vertex_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Vertex count (uncondensed)\": " << uncondensed_vertex_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (condensed)\": " << condensed_refines_edge_count+condensed_data_edge_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (uncondensed)\": " << uncondensed_refines_edge_count+uncondensed_data_edge_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (data) (condensed)\": " << condensed_data_edge_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (data) (uncondensed)\": " << uncondensed_data_edge_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (refines) (condensed)\": " << condensed_refines_edge_count << ",\n";
    summay_graph_stats_file_ostream << indentation << "\"Edge count (refines) (uncondensed)\": " << uncondensed_refines_edge_count;
    if (fixed_point_reached)
    {
        summay_graph_stats_file_ostream << ",\n";
        summay_graph_stats_file_ostream << indentation << "\"Final vertex count\": " << final_vertex_count << ",\n";
        summay_graph_stats_file_ostream << indentation << "\"Final (data) edge count\": " << final_data_edge_count;
    }
    summay_graph_stats_file_ostream << "\n";
    summay_graph_stats_file_ostream << end_line << "\n";
    summay_graph_stats_file_ostream.close();
}