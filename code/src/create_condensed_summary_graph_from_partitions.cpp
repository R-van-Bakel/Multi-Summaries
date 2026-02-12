#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <iomanip>
// #define BOOST_USE_VALGRIND  // TODO disable this command in the final running version
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/program_options.hpp>
#include <regex>
#include <filesystem>
#include <boost/algorithm/algorithm.hpp>
#include <boost/algorithm/string.hpp>
#include <nlohmann/json.hpp>

#include "../include/my_exception.hpp"
#include "../include/stopwatch.hpp"
#include "../include/binary_io.hpp"

using json = nlohmann::json;

using node_to_block_map_type = boost::unordered_flat_map<node_index, block_or_singleton_index>;
using local_to_global_map = boost::unordered_flat_map<std::pair<k_type,block_or_singleton_index>,block_or_singleton_index>;
using predicate_object_pair_set = boost::unordered_flat_set<std::pair<edge_type,block_or_singleton_index>>;
using time_interval = std::pair<k_type, k_type>;
using interval_map = boost::unordered_flat_map<block_or_singleton_index, time_interval>;

const std::string outcome_file_regex_string = R"(^outcome_condensed-\d{4}\.bin$)";
const std::string mapping_file_regex_string = R"(^mapping-\d{4}to\d{4}\.bin$)";
std::regex outcome_file_regex_pattern(outcome_file_regex_string);
std::regex mapping_file_regex_pattern(mapping_file_regex_string);

#define CREATE_REVERSE_INDEX

class NodeSet
{
private:
    boost::unordered_flat_set<node_index> nodes;

public:
    NodeSet()
    {
    }
    void add_node(node_index node)
    {
        nodes.emplace(node);  // Should the be emplace or insert?
    }
    boost::unordered_flat_set<node_index>& get_nodes()
    {
        return nodes;
    }
    void remove_node(node_index node)
    {
        nodes.erase(node);
    }
};

class BlockMap
{
private:
    boost::unordered_flat_map<block_or_singleton_index,NodeSet> block_map;

public:
    BlockMap()
    {
    }
    boost::unordered_flat_map<block_or_singleton_index,NodeSet>& get_map()
    {
        return block_map;
    }
    NodeSet& get_node_set(block_or_singleton_index local_block)
    {
        auto key_val_iterator = this->get_map().find(local_block);
        assert(key_val_iterator != this->get_map().end());  // The queried block must exist as a key
        return (*key_val_iterator).second;
    }
    void add_node(block_or_singleton_index merged_block, node_index node)
    {
        if (this->get_map().find(merged_block) == this->get_map().end())
        {
            NodeSet empty_node_set = NodeSet();
            block_map[merged_block] = empty_node_set;
        }
        block_map[merged_block].add_node(node);
    }
};

class SingletonMapper
{
private:
    boost::unordered_flat_map<k_type,BlockMap> block_to_singletons;

public:
    SingletonMapper()
    {
    }
    boost::unordered_flat_map<k_type,BlockMap>& get_maps()
    {
        return block_to_singletons;
    }
    BlockMap& get_map(k_type k)
    {
        return block_to_singletons[k];
    }
    void add_level(k_type k)
    {
        assert(this->get_maps().count(k) == 0);  // The block map should not exist
        BlockMap empty_block_map = BlockMap();
        block_to_singletons[k] = empty_block_map;
    }
    void add_mapping(k_type k, block_or_singleton_index merged_block, node_index singleton)
    {
        block_to_singletons[k].add_node(merged_block, singleton);
    }
    void write_map_to_file_binary(std::string output_directory)
    {
        for (auto block_to_singleton_map: this->get_maps())
        {
            k_type current_level = block_to_singleton_map.first;

            std::ostringstream current_level_stringstream;
            current_level_stringstream << std::setw(4) << std::setfill('0') << current_level;
            std::string current_level_string(current_level_stringstream.str());

            std::ostringstream previous_level_stringstream;
            previous_level_stringstream << std::setw(4) << std::setfill('0') << current_level-1;
            std::string previous_level_string(previous_level_stringstream.str());

            std::string output_file_path = output_directory + "singleton_mapping-" + previous_level_string + "to" + current_level_string + ".bin";
            std::ofstream output_file_binary(output_file_path, std::ios::trunc | std::ofstream::out);

            for (auto merged_singletons_pair: block_to_singleton_map.second.get_map())
            {
                block_index merged_block = (block_index) merged_singletons_pair.first;
                block_or_singleton_index singleton_count = (block_or_singleton_index) merged_singletons_pair.second.get_nodes().size();
                write_uint_BLOCK_little_endian(output_file_binary,merged_block);
                write_int_BLOCK_OR_SINGLETON_little_endian(output_file_binary,singleton_count);
                for (auto singleton: merged_singletons_pair.second.get_nodes())
                {
                    block_or_singleton_index singleton_block = (block_or_singleton_index) -(singleton+1);
                    write_int_BLOCK_OR_SINGLETON_little_endian(output_file_binary,singleton_block);
                }
            }
            output_file_binary.flush();
        }
    }

    uint64_t get_singleton_refines_edge_count()
    {
        uint64_t refines_edge_count = 0;
        for (auto level_blockmap_pair: this->get_maps())
        {
            // For all bisimulation levels, we count the created singletons and increase the refines_edge_count accordingly
            for (auto block_nodeset_pair: level_blockmap_pair.second.get_map())
            {
                refines_edge_count += block_nodeset_pair.second.get_nodes().size();
            }
        }
        return refines_edge_count;
    }
};

class LocalBlockToGlobalBlockMap
{
private:
    local_to_global_map block_map;
    block_or_singleton_index next_block = 1;

public:
    LocalBlockToGlobalBlockMap()
    {
    }
    local_to_global_map& get_map()
    {
        return block_map;
    }
    block_or_singleton_index get_next_id()
    {
        return next_block;
    }
    block_or_singleton_index map_block(k_type k, block_or_singleton_index local_block)
    {
        // Negative indices always belong to singleton blocks, making them already unique between layers
        if (local_block >= 0)
        {
            auto block_map_iterator = this->get_map().find({k,local_block});
            assert(block_map_iterator != this->get_map().cend());
            return block_map_iterator->second;
        }
        else
        {
            return local_block;
        }
        return this->get_map()[{k,local_block}];
    }
    block_or_singleton_index add_block(k_type k, block_or_singleton_index local_block)
    {
        block_or_singleton_index global_block = next_block;
        this->get_map()[{k,local_block}] = global_block;
        next_block++;
        return global_block;
    }
    block_or_singleton_index add_block_non_restricted(k_type k, block_or_singleton_index local_block)
    {
        block_or_singleton_index global_block;
        auto block_map_iterator = this->get_map().find({k,local_block});
        if (block_map_iterator == this->get_map().cend())
        {
            global_block = this->add_block(k,local_block);
        }
        else
        {
            global_block = block_map_iterator->second;
        }
        return global_block;
    }
    void write_map_to_file_binary(std::ostream &outputstream, interval_map &block_to_interval_map)  // TODO this format is very inefficient (as the level is written for every block), instead store like: LEVEL,SIZE,{{LOCAL,GLOBAL}...}
    {
        for (auto local_global_pair: this->get_map())
        {
            // k_type level = local_global_pair.first.first;
            block_or_singleton_index local_block = local_global_pair.first.second;
            block_or_singleton_index global_block = local_global_pair.second;
            k_type level = block_to_interval_map[global_block].first;

            write_uint_K_TYPE_little_endian(outputstream,level);
            write_int_BLOCK_OR_SINGLETON_little_endian(outputstream,local_block);
            write_int_BLOCK_OR_SINGLETON_little_endian(outputstream,global_block);
        }
        outputstream.flush();
    }
};

class SplitToMergedMap
{
private:
    boost::unordered_flat_map<block_or_singleton_index, block_or_singleton_index> mapping;

public:
    SplitToMergedMap()
    {
    }
    void add_pair(block_or_singleton_index split_block, block_or_singleton_index merged_block)
    {
        mapping[split_block] = merged_block;
    }
    block_or_singleton_index map_block(block_or_singleton_index possibly_split_block)
    {
        auto key_val_iterator = mapping.find(possibly_split_block);
        if (key_val_iterator == mapping.end())
        {
            return possibly_split_block;
        }
        else
        {
            return (*key_val_iterator).second;
        }
    }
    boost::unordered_flat_map<block_or_singleton_index, block_or_singleton_index>& get_map()
    {
        return mapping;
    }
};

class SummaryPredicateObjectSet
{
    private:
    predicate_object_pair_set po_pairs;

    public:
    SummaryPredicateObjectSet()
    {
    }
    void add_pair(edge_type predicate, block_or_singleton_index object)
    {
        po_pairs.emplace(std::make_pair(predicate,object));  // Should the be emplace or insert?
    }
    predicate_object_pair_set& get_pairs()
    {
        return po_pairs;
    }
    void remove_pair(edge_type predicate, block_or_singleton_index object)
    {
        po_pairs.erase(std::make_pair(predicate,object));
    }
};
using s_to_po_map = boost::unordered_flat_map<block_or_singleton_index,SummaryPredicateObjectSet>;

class SummaryGraph
{
private:
    s_to_po_map nodes;

    #ifdef CREATE_REVERSE_INDEX
    s_to_po_map reverse_nodes;
    #endif

    SummaryGraph(SummaryGraph &)
    {
    }

public:
    SummaryGraph()
    {
    }
    s_to_po_map& get_nodes()
    {
        return nodes;
    }
    s_to_po_map& get_reverse_nodes()
    {
        return reverse_nodes;
    }
    // boost::unordered_flat_map<block_or_singleton_index, SummaryNode>& get_nodes()
    // {
    //     return block_nodes;
    // }
    // boost::unordered_flat_map<block_or_singleton_index, SummaryNode>& get_reverse_index()
    // {
    //     return reverse_block_nodes;
    // }
    void add_block_node(block_or_singleton_index block_node)
    {
        assert(this->get_nodes().count(block_node) == 0);  // The node should not already exist
        SummaryPredicateObjectSet empty_predicate_object_set = SummaryPredicateObjectSet();
        nodes[block_node] = empty_predicate_object_set;

        #ifdef CREATE_REVERSE_INDEX
        if (this->get_reverse_nodes().count(block_node) == 0)  // The node should not already exist
        {
            SummaryPredicateObjectSet empty_predicate_object_set_reverse = SummaryPredicateObjectSet();
            reverse_nodes[block_node] = empty_predicate_object_set_reverse;
        }
        #endif
    }
    void try_add_block_node(block_or_singleton_index block_node)
    {
        if (this->get_nodes().count(block_node) == 0)  // The node should not already exist
        {
            SummaryPredicateObjectSet empty_predicate_object_set = SummaryPredicateObjectSet();
            nodes[block_node] = empty_predicate_object_set;
        }

        #ifdef CREATE_REVERSE_INDEX
        if (this->get_reverse_nodes().count(block_node) == 0)  // The node should not already exist
        {
            SummaryPredicateObjectSet empty_predicate_object_set_reverse = SummaryPredicateObjectSet();
            reverse_nodes[block_node] = empty_predicate_object_set_reverse;
        }
        #endif
    }
    // void remove_block_node(block_or_singleton_index block_node)
    // {
    //     this->get_nodes().erase(block_node);
    // }
    // void add_reverse_block_node(block_or_singleton_index block_node)
    // {
    //     assert(reverse_block_nodes.count(block_node) == 0);  // The node should not already exist
    //     SummaryNode empty_node = SummaryNode();
    //     reverse_block_nodes[block_node] = empty_node;
    // }
    // void remove_reverse_block_node(block_or_singleton_index block_node)
    // {
    //     this->get_reverse_index().erase(block_node);
    // }
    void add_edge_to_node(block_or_singleton_index subject, edge_type predicate, block_or_singleton_index object)//, bool add_reverse=true)
    {
        assert(this->get_nodes().count(subject) > 0);  // The node should exist
        this->get_nodes()[subject].add_pair(predicate, object);

        #ifdef CREATE_REVERSE_INDEX
        auto reverse_node_it = this->get_reverse_nodes().find(object);
        if (reverse_node_it != this->get_reverse_nodes().cend())
        {
            reverse_node_it->second.add_pair(predicate,subject);
        }
        else
        {
            SummaryPredicateObjectSet empty_predicate_object_set_reverse = SummaryPredicateObjectSet();
            reverse_nodes[object] = empty_predicate_object_set_reverse;
            reverse_nodes[object].add_pair(predicate,subject);
        }
        #endif
        // if (add_reverse)
        // {
        //     if (reverse_block_nodes.count(object) == 0)
        //     {
        //         add_reverse_block_node(object);
        //     }
        //     this->get_reverse_index()[object].add_edge(predicate, subject);
        // }
    }
    // void ammend_object(block_or_singleton_index subject, edge_type predicate, block_or_singleton_index old_object, block_or_singleton_index new_object)//, bool ammend_reverse=true)
    // {
    //     assert(this->get_nodes().count(subject) > 0);  // The subject should exist
    //     assert(this->get_nodes()[subject].count_edge_key(predicate) > 0);  // The predicate should exist
    //     assert(this->get_nodes()[subject].get_edges()[predicate].get_objects().count(old_object) > 0);  // The predicate should exist

    //     this->get_nodes()[subject].add_edge(predicate, new_object);
    //     this->get_nodes()[subject].remove_edge_recursive(predicate, old_object);

    //     // if (ammend_reverse)
    //     // {
    //     //     if (reverse_block_nodes.count(new_object) == 0)
    //     //     {
    //     //         add_reverse_block_node(new_object);
    //     //     }
    //     //     this->get_reverse_index()[new_object].add_edge(predicate, subject);
    //     //     this->get_reverse_index()[old_object].remove_edge_recursive(predicate, subject);

    //     //     // Remove the node in the reverse 
    //     //     if (this->get_reverse_index()[old_object].get_edges().size() == 0)
    //     //     {
    //     //         this->remove_reverse_block_node(old_object);
    //     //     }
    //     // }
    // }
    // std::vector<Triple> remove_split_blocks_edges(boost::unordered_flat_set<block_index> split_blocks)
    // {
    //     std::vector<Triple> removed_edges;
    //     // Remove the forward edges from the index and reverse index
    //     for (block_or_singleton_index subject: split_blocks)
    //     {
    //         for (auto& block_node_key_val: this->get_nodes()[subject].get_edges())
    //         {
    //             edge_type predicate = block_node_key_val.first;
    //             for (block_or_singleton_index object: block_node_key_val.second.get_objects())
    //             {
    //                 reverse_block_nodes[object].remove_edge_recursive(predicate, subject);
    //                 removed_edges.emplace_back(Triple(subject, predicate, object));  // Keep track of the edges we have removed
    //             }
    //         }
    //         this->get_nodes()[subject] = SummaryNode();  // After having removed the forward edges from the reverse index, clear the edges for the index
    //     }

    //     // Remove the backward edges from the index and reverse index
    //     for (block_or_singleton_index subject: split_blocks)
    //     {
    //         for (auto& reverse_block_node_key_val: reverse_block_nodes[subject].get_edges())
    //         {
    //             edge_type predicate = reverse_block_node_key_val.first;
    //             for (block_or_singleton_index object: reverse_block_node_key_val.second.get_objects())
    //             {
    //                 this->get_nodes()[object].remove_edge_recursive(predicate, subject);
    //                 removed_edges.emplace_back(Triple(subject, predicate, object));  // Keep track of the edges we have removed
    //             }
    //         }
    //         reverse_block_nodes[subject] = SummaryNode();  // After having removed the backward edges from the index, clear the edges for the reverse index
    //     }
    //     return removed_edges;
    // }
    void write_graph_to_file_binary(std::ostream &graphoutputstream)
    {
        for (auto s_po_pair: this->get_nodes())
        {
            block_or_singleton_index subject = s_po_pair.first;
            for (auto po_pair: s_po_pair.second.get_pairs())
            {
                edge_type predicate = po_pair.first;
                block_or_singleton_index object = po_pair.second;
                write_int_BLOCK_OR_SINGLETON_little_endian(graphoutputstream, subject);
                write_uint_PREDICATE_little_endian(graphoutputstream, predicate);
                write_int_BLOCK_OR_SINGLETON_little_endian(graphoutputstream, object);
            }
        }
        graphoutputstream.flush();
    }
    uint64_t get_vertex_count()
    {
        return nodes.size();
    }
    uint64_t get_edge_count()
    {
        uint64_t edge_cout = 0;
        for (auto id_node_pair: nodes)
        {
            edge_cout += id_node_pair.second.get_pairs().size();
        }
        return edge_cout;
    }
    // void write_graph_to_file_json(std::ostream &outputstream)
    // {
    //     outputstream << "[";
    //     bool first_line = true;
    //     for (auto node_key_val: this->get_nodes())
    //     {
    //         block_or_singleton_index subject = node_key_val.first;
    //         for (auto edge_key_val: node_key_val.second.get_edges())
    //         {
    //             edge_type predicate = edge_key_val.first;
    //             for (block_or_singleton_index object: edge_key_val.second.get_objects())
    //             {
    //                 if (first_line)
    //                 {
    //                     first_line = false;
    //                 }
    //                 else
    //                 {
    //                     outputstream << ",";
    //                 }
    //                 outputstream << "\n    [" << subject << ", " << object << ", " << predicate << "]";
    //             }
    //         }
    //     }
    //     outputstream << "\n]";
    // }
};

void read_graph_into_summary_from_stream_timed(std::istream &inputstream, node_to_block_map_type &node_to_block_map, LocalBlockToGlobalBlockMap& block_map, SplitToMergedMap &split_to_merged_map, boost::unordered_flat_map<block_or_singleton_index, time_interval> &block_to_interval_map, k_type current_level, bool include_zero, bool fixed_point_reached, SummaryGraph &gs)
{
    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();

    const int BufferSize = 8 * 16184;

    char _buffer[BufferSize];

    inputstream.rdbuf()->pubsetbuf(_buffer, BufferSize);

    w.start_step("Reading graph");
    u_int64_t line_counter = 0;

    auto t_start{boost::chrono::system_clock::now()};
    auto time_t_start{boost::chrono::system_clock::to_time_t(t_start)};
    std::tm *ptm_start{std::localtime(&time_t_start)};

    std::cout << std::put_time(ptm_start, "%Y/%m/%d %H:%M:%S") << " Reading started" << std::endl;
    while (true)
    {
        // subject
        node_index subject_index = read_uint_ENTITY_little_endian(inputstream);

        // edge
        edge_type edge_label = read_uint_PREDICATE_little_endian(inputstream);

        // object
        node_index object_index = read_uint_ENTITY_little_endian(inputstream);

        // Break when the last valid values have been read
        if (inputstream.eof())
        {
            break;
        }

        block_or_singleton_index subject_block = block_map.map_block(current_level,node_to_block_map[subject_index]);

        gs.try_add_block_node(subject_block);
        if (block_to_interval_map.find(subject_block) == block_to_interval_map.cend())  // If the block has not been given an initial interval, then do it now
        {
            k_type first_level = 1;
            if (include_zero)
            {
                first_level = 0;
            }
            block_to_interval_map[subject_block] = {first_level, current_level};
        }

        block_or_singleton_index object_block_previous_level = split_to_merged_map.map_block(block_map.map_block(current_level,node_to_block_map[object_index]));  // Data edges from last level to second-to-last level
        gs.add_edge_to_node(subject_block, edge_label, object_block_previous_level);

        if (line_counter % 1000000 == 0)
        {
            auto now{boost::chrono::system_clock::to_time_t(boost::chrono::system_clock::now())};
            std::tm *ptm{std::localtime(&now)};
            std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S") << " done with " << line_counter << " triples" << std::endl;
        }
        line_counter++;
    }
    w.stop_step();

    auto t_reading_done{boost::chrono::system_clock::now()};
    auto time_t_reading_done{boost::chrono::system_clock::to_time_t(t_reading_done)};
    std::tm *ptm_reading_done{std::localtime(&time_t_reading_done)};

    std::cout << std::put_time(ptm_reading_done, "%Y/%m/%d %H:%M:%S")
              << " Time taken = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_reading_done - t_start).count()
              << " ms, memory = " << w.get_times()[0].memory_in_kb << " kB" << std::endl;
// #ifdef CREATE_REVERSE_INDEX
//     w.start_step("Creating reverse index");
//     g.compute_reverse_index();
//     w.stop_step();

//     auto t_reverse_index_done{boost::chrono::system_clock::now()};
//     auto time_t_reverse_index_done{boost::chrono::system_clock::to_time_t(t_reverse_index_done)};
//     std::tm *ptm_reverse_index_done{std::localtime(&time_t_reverse_index_done)};

//     std::cout << std::put_time(ptm_reverse_index_done, "%Y/%m/%d %H:%M:%S")
//               << " Time taken for creating reverse index = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_reverse_index_done - t_reading_done).count()
//               << " ms, memory = " << w.get_times()[1].memory_in_kb << " kB" << std::endl;
// #endif
}

void read_graph_into_summary_timed(const std::string &filename, node_to_block_map_type &node_to_block_map, LocalBlockToGlobalBlockMap &block_map, SplitToMergedMap &split_to_merged_map, boost::unordered_flat_map<block_or_singleton_index, time_interval> &block_to_interval_map, k_type current_level, bool include_zero, bool fixed_point_reached, SummaryGraph &gs)
{

    std::ifstream infile(filename, std::ifstream::in);
    read_graph_into_summary_from_stream_timed(infile, node_to_block_map, block_map, split_to_merged_map, block_to_interval_map, current_level, include_zero, fixed_point_reached, gs);
}

struct LocalBlock {
    block_or_singleton_index local_index;
    k_type terminal_level;
};

int main(int ac, char *av[])
{
    StopWatch<boost::chrono::process_cpu_clock> w_total = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();
    w_total.start_step("Start experiment", true);  // Set newline to true

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

    auto t_read{boost::chrono::system_clock::now()};
    auto time_t_read{boost::chrono::system_clock::to_time_t(t_read)};
    std::tm *ptm_read{std::localtime(&time_t_read)};
    std::cout << std::put_time(ptm_read, "%Y/%m/%d %H:%M:%S") << " Setting up " << std::endl;

    std::string experiment_directory = vm["experiment_directory"].as<std::string>();
    std::string graph_file = experiment_directory + "binary_encoding.bin";

    std::string graph_stats_file = experiment_directory + "ad_hoc_results/graph_stats.json";
    std::ifstream graph_stats_file_stream(graph_stats_file);

    std::string graph_stats_line;
    std::string final_depth_string = "\"Final depth\"";
    std::string vertex_count_string = "\"Vertex count\"";
    std::string fixed_point_string = "\"Fixed point\"";

    size_t k;
    node_index graph_size;
    bool fixed_point_reached;

    bool k_found = false;
    bool size_found = false;
    bool fixed_point_found = false;

    bool include_zero_outcome = false;
    std::filesystem::path path_to_zero_outcome = experiment_directory + "bisimulation/outcome_condensed-0000.bin";
    bool immediate_stop = false;
    k_type first_level = 1;
    if (std::filesystem::exists(path_to_zero_outcome))
    {
        include_zero_outcome = true;  // Mark the 0th outcome to be included
        first_level = 0;  // We need to check an extra outcome if we have the 0 outcome
    }

    uint64_t refines_edge_count = 0;

    while (std::getline(graph_stats_file_stream, graph_stats_line))
    {
        boost::trim(graph_stats_line);
        boost::erase_all(graph_stats_line, ",");
        std::vector<std::string> result;
        boost::split(result, graph_stats_line, boost::is_any_of(":"));
        if (result[0] == final_depth_string)
        {
            std::stringstream sstream(result[1]);
            sstream >> k;
            k_found = true;
        }
        else if (result[0] == vertex_count_string)
        {
            std::stringstream sstream(result[1]);
            sstream >> graph_size;
            size_found = true;
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
        if (k_found && size_found && fixed_point_found)
        {
            break;
        }
    }
    graph_stats_file_stream.close();

    // >>> LOAD THE FINAL SUMMARY GRAPH >>>
    auto t_start{boost::chrono::system_clock::now()};
    auto time_t_start{boost::chrono::system_clock::to_time_t(t_start)};
    std::tm *ptm_start{std::localtime(&time_t_start)};
    std::cout << std::put_time(ptm_start, "%Y/%m/%d %H:%M:%S") << " Reading outcomes started" << std::endl;

    std::string blocks_file = experiment_directory + "bisimulation/outcome_condensed-000" + std::to_string(first_level) + ".bin";

    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();
    w.start_step("Reading outcomes", true);  // Set newline to true

    std::ifstream blocksfile(blocks_file, std::ifstream::in);
    node_to_block_map_type node_to_block_map;
    boost::unordered_flat_map<block_index, boost::unordered_flat_set<node_index>> blocks;

    // Initialize all nodes as being singletons
    for (node_index node = 0; node < graph_size; node++)
    {
        node_to_block_map[node] = -block_or_singleton_index(node) - 1;
    }

    auto t_reverse_index_done{boost::chrono::system_clock::now()};
    auto time_t_reverse_index_done{boost::chrono::system_clock::to_time_t(t_reverse_index_done)};
    std::tm *ptm_reverse_index_done{std::localtime(&time_t_reverse_index_done)};

    std::cout << std::put_time(ptm_reverse_index_done, "%Y/%m/%d %H:%M:%S") << " Processing k=" + std::to_string(first_level) << std::endl;
    
    // Read the first outcome file
    while (true)
    {
        block_index block = read_uint_BLOCK_little_endian(blocksfile);
        if (blocksfile.eof())
        {
            break;
        }
        assert(block <= MAX_SIGNED_BLOCK_SIZE);  // Later, we are storing a block_index as a block_or_singleton_index, so we need to check if the cast is possible
        u_int64_t block_size = read_uint_ENTITY_little_endian(blocksfile);

        for (uint64_t i = 0; i < block_size; i++)
        {
            node_index node = read_uint_ENTITY_little_endian(blocksfile);
            node_to_block_map[node] = (block_or_singleton_index) block;
            blocks[block].emplace(node);
        }
    }

    SingletonMapper blocks_to_singletons = SingletonMapper();

    for (uint32_t i = first_level+1; i <= k; i++)  // We can ignore the last outcome (k), if its only purpose was to find the fixed point (i.e. its empty), otherwise include the last outcome
    {
        auto t_reverse_index_done{boost::chrono::system_clock::now()};
        auto time_t_reverse_index_done{boost::chrono::system_clock::to_time_t(t_reverse_index_done)};
        std::tm *ptm_reverse_index_done{std::localtime(&time_t_reverse_index_done)};

        std::cout << std::put_time(ptm_reverse_index_done, "%Y/%m/%d %H:%M:%S") << " Processing k=" << i << std::endl;

        std::ostringstream previous_i_stringstream;
        previous_i_stringstream << std::setw(4) << std::setfill('0') << i-1;
        std::string previous_i_string(previous_i_stringstream.str());

        std::ostringstream i_stringstream;
        i_stringstream << std::setw(4) << std::setfill('0') << i;
        std::string i_string(i_stringstream.str());

        std::string current_mapping = experiment_directory + "bisimulation/mapping-" + previous_i_string + "to" + i_string + ".bin";
        std::string current_outcome = experiment_directory + "bisimulation/outcome_condensed-" + i_string + ".bin";
        std::ifstream current_mapping_file(current_mapping, std::ifstream::in);
        std::ifstream current_outcome_file(current_outcome, std::ifstream::in);

        boost::unordered_flat_set<block_index> split_block_incides;
        boost::unordered_flat_set<block_index> new_block_indices;
        boost::unordered_flat_set<block_index> disappeared_block_indices;

        bool new_singletons_created = false;

        // Read a mapping file
        while (true)
        {
            block_index old_block = read_uint_BLOCK_little_endian(current_mapping_file);
            if (current_mapping_file.eof())
            {
                break;
            }
            split_block_incides.emplace(old_block);

            block_index new_block_count = read_uint_BLOCK_little_endian(current_mapping_file);
            for (block_index j = 0; j < new_block_count; j++)
            {
                block_index new_block = read_uint_BLOCK_little_endian(current_mapping_file);
                if (new_block == 0)
                {
                    new_singletons_created = true;
                    // If the block got split into only singletons, then mark its (block to nodes) map to be cleared
                    if (new_block_count == 1)
                    {
                        disappeared_block_indices.emplace(old_block);
                    }
                }
                else
                {
                    new_block_indices.emplace(new_block);
                    refines_edge_count++;  // We only count refines edge to non-singleton blocks. The singletons are accounted for in the singleton mapper.
                }
            }
        }

        boost::unordered_flat_set<node_index> new_singleton_nodes;
        boost::unordered_flat_set<node_index> old_nodes_in_split;
        boost::unordered_flat_set<node_index> new_nodes_in_split;

        if (new_singletons_created)
        {
            // Get all nodes that were part of blocks that got split
            for (block_or_singleton_index split_block: split_block_incides)
            {
                assert(split_block <= MAX_SIGNED_BLOCK_SIZE);  // We need to check if the cast to block_or_singleton_index is possible
                old_nodes_in_split.insert(blocks[split_block].begin(), blocks[(block_or_singleton_index) split_block].end());
            }
            // Clear all blocks that got turned into only singletons
            // All other split blocks will be cleared later
            for (block_or_singleton_index disappeared_block: disappeared_block_indices)
            {
                blocks[disappeared_block].clear();
            }
        }

        // Read an outcome file
        while (true)
        {
            block_index block = read_uint_BLOCK_little_endian(current_outcome_file);
            if (current_outcome_file.eof())
            {
                break;
            }
            assert(block <= MAX_SIGNED_BLOCK_SIZE);  // Later, we are storing a block_index as a block_or_singleton_index, so we need to check if the cast is possible
            u_int64_t block_size = read_uint_ENTITY_little_endian(current_outcome_file);
            blocks[(block_or_singleton_index) block].clear();  // Remove the old map
            for (uint64_t i = 0; i < block_size; i++) {
                node_index node = read_uint_ENTITY_little_endian(current_outcome_file);
                node_to_block_map[node] = (block_or_singleton_index) block;
                blocks[(block_or_singleton_index) block].emplace(node);
            }
        }

        if (new_singletons_created)
        {
            for (block_or_singleton_index new_block: new_block_indices)
            {
                new_nodes_in_split.insert(blocks[new_block].begin(), blocks[new_block].end());
            }
            for (auto node: new_nodes_in_split)
            {
                old_nodes_in_split.erase(node);
            }
            new_singleton_nodes = std::move(old_nodes_in_split);
            
            blocks_to_singletons.add_level(i);
            for (node_index node: new_singleton_nodes)
            {
                blocks_to_singletons.get_map(i).add_node(node_to_block_map[node], node);  // Add the block to singlton mapping to keep track (for later) of which merged blocks singletons refine
                block_or_singleton_index singleton_block = -((block_or_singleton_index)node)-1;
                node_to_block_map[node] = singleton_block;
            }
        }
    }
    
    w.stop_step();

    auto t_outcomes_end{boost::chrono::system_clock::now()};
    auto time_t_outcomes_end{boost::chrono::system_clock::to_time_t(t_outcomes_end)};
    std::tm *ptm_outcomes_end{std::localtime(&time_t_outcomes_end)};
    auto step_info = w.get_times().back();
    std::cout << std::put_time(ptm_outcomes_end, "%Y/%m/%d %H:%M:%S")
    << " Final outcome loaded (Time taken = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_outcomes_end - t_start).count()
    << " ms, memory = " << step_info.memory_in_kb << " kB)" << std::endl;
    // <<< LOAD THE FINAL SUMMARY GRAPH <<<

    // We have read the last outcome, now we will create a summary graph accordingly
    LocalBlockToGlobalBlockMap block_map = LocalBlockToGlobalBlockMap();  // First, because different blocks can have the same name at different layers, we need to map the current block ids to globally unique ones
    boost::unordered_flat_map<block_or_singleton_index,LocalBlock> old_living_blocks;  // The blocks that currently exist (at the subject level)
    boost::unordered_flat_map<block_or_singleton_index,LocalBlock> new_living_blocks;  // The blocks that currently exist (at the object level)
    boost::unordered_flat_map<block_or_singleton_index,block_or_singleton_index> new_local_to_global_living_blocks;  // A map from the local index of a living block to its global one (at the object level)
    boost::unordered_flat_map<block_or_singleton_index,LocalBlock> spawning_blocks;  // The blocks that will come into existance in the following level.
    boost::unordered_flat_set<block_or_singleton_index> dying_blocks;  // The blocks that will not exist anymore in the following level
    boost::unordered_flat_set<block_or_singleton_index> old_dying_blocks;  // The blocks that exist anymore in the current level

    k_type current_level = k;

    if (include_zero_outcome)
    {
        if (current_level == 0)
        {
            immediate_stop = true;  // The bisimulations stopped immediatly if current_level==0, in case of a non-trivial k=0 outcome
            current_level = 1;  // This is a corner case: we artificially add a layer at k=1, so we can have edges between k=1 and k=0
        }
    }
    else if (current_level == 1)
    {
        immediate_stop = true;  // The bisimulations stopped immediatly if current_level==1, in case of a trivial k=0 outcome
    }

    // Add all terminal non-empty non-singleton blocks as living nodes
    for (auto iter = blocks.cbegin(); iter != blocks.cend(); iter++)
    {
        block_index block_id = iter->first;
        size_t block_size = iter->second.size();
        if (block_size == 0)  // Empty blocks do not yield summary nodes
        {
            continue;
        }
        block_or_singleton_index global_block = block_map.add_block(current_level, block_id);
        old_living_blocks[global_block] = {(block_or_singleton_index) block_id, (k_type) current_level};  // Earlier (when loading the outcomes) we had already checked that this cast is possible
    }

    // All existing singleton blocks are alive at the last layer (since they can't disappear by splitting further)
    for (auto node_block_pair: node_to_block_map)
    {
        if (node_block_pair.second < 0)  // Blocks have a negative index iff they are singletons
        {
            old_living_blocks[node_block_pair.second] = {node_block_pair.second, (k_type) current_level};
        }
    }

    // Copy the old living blocks into new living blocks
    for (auto living_block_key_val: old_living_blocks)
    {
        // old_local_to_global_living_blocks[living_block_key_val.second.local_index] = living_block_key_val.first;
        new_living_blocks[living_block_key_val.first] = living_block_key_val.second;
    }

    // Update the new local to global living block mapping (it uses the old blocks for step the first step)
    for (auto living_block_key_val: old_living_blocks)
    {
        new_local_to_global_living_blocks[living_block_key_val.second.local_index] = living_block_key_val.first;
    }

    interval_map block_to_interval_map;

    // // This corresponds to the one block that has no outgoing edges.
    // // Since it never apears as a subject, it will not be added by our algorithm, therefore we will manually add it here
    // // We might not actually have such a block in our graph, but having it redundantly in block_to_interval_map is never a problem
    // block_or_singleton_index literal_block = -1;
    // block_to_interval_map[literal_block] = {first_level, current_level};

    // Declare our condensed multi summary graph
    SummaryGraph gs;

    SplitToMergedMap old_split_to_merged_map;

    
    auto t_loading_graph{boost::chrono::system_clock::now()};
    auto time_t_loading_graph{boost::chrono::system_clock::to_time_t(t_loading_graph)};
    std::tm *ptm_loading_graph{std::localtime(&time_t_loading_graph)};
    std::cout << std::put_time(ptm_loading_graph, "%Y/%m/%d %H:%M:%S") << " Load graph edges" << std::endl;

    if (immediate_stop)
    {        
        w.start_step("Read edges (final) into summary graph", true);
        // Add the edges between the level 1 and level 0
        k_type zero_level = 0;

        // TODO The code behaves slightly unexpected (due to the "literal" node always being present) if include_zero_outcome is false and old_living_blocks.size() is 2
        // TODO The solution involves either detecting disconnected vertices now or do so in a earlier phase of the experiment
        if (include_zero_outcome || old_living_blocks.size()==1)  // If we use a non-trivial zero outcome, or there is only one block, then every block has its own counterpart
        {
            for (auto index_block_pair: old_living_blocks)
            {
                old_split_to_merged_map.add_pair(index_block_pair.first, index_block_pair.first);  // Each summary node refines itself
            }
        }
        else  // Otherwise we create a separate universal block to be the parent of all blocks in level k=1
        {
            block_or_singleton_index universal_block = 0;
            block_or_singleton_index global_universal_block = block_map.add_block(zero_level, universal_block);
            gs.add_block_node(global_universal_block);
            block_to_interval_map[global_universal_block] = {zero_level, zero_level};
            for (auto node_block_pair: node_to_block_map)
            {
                old_split_to_merged_map.add_pair(node_block_pair.second, global_universal_block);  // The universal block is the only parent to all nodes in k=1
            }
        }

        SplitToMergedMap initial_map;

        if (fixed_point_reached)
        {
            SplitToMergedMap fixed_point_map;
            for (auto index_block_pair: old_living_blocks)
            {
                fixed_point_map.add_pair(index_block_pair.first, index_block_pair.first);  // At the fixed point, all blocks map to themselves
            }
            initial_map = fixed_point_map;
        }
        else
        {
            initial_map = old_split_to_merged_map;
        }

        auto t_first_edges{boost::chrono::system_clock::now()};
        auto time_t_first_edges{boost::chrono::system_clock::to_time_t(t_first_edges)};
        std::tm *ptm_first_edges{std::localtime(&time_t_first_edges)};
        std::cout << std::put_time(ptm_first_edges, "%Y/%m/%d %H:%M:%S") << " Loading initial/terminal condensed data edges (0001-->0000) " << std::flush;  // We don't end the line so we can add statistics later

        read_graph_into_summary_timed(graph_file, node_to_block_map, block_map, initial_map, block_to_interval_map, current_level, include_zero_outcome, fixed_point_reached, gs);

        // This corresponds to the one block that has no outgoing edges.
        // Since it never apears as a subject, it will normally not be added by our algorithm, therefore we will manually add it here if it exists
        for (auto living_block_key_val: old_living_blocks)
        {
            if (gs.get_nodes().find(living_block_key_val.first) == gs.get_nodes().cend())
            {
                block_to_interval_map[living_block_key_val.first] = {first_level, current_level};
            }
        }

        if (fixed_point_reached)  // In this case we loaded in the fixed point edges, but we still need to add the edges between k=1 and k=0
        {
            for (auto s_po_pair: gs.get_nodes())
            {
                block_or_singleton_index subject = s_po_pair.first;
                for (auto predicate_object_pair: s_po_pair.second.get_pairs())
                {
                    block_or_singleton_index predicate = predicate_object_pair.first;
                    block_or_singleton_index object = predicate_object_pair.second;

                    block_or_singleton_index object_image = old_split_to_merged_map.map_block(object);
                    gs.add_edge_to_node(subject, predicate, object_image);
                }
            }
        }
        w.stop_step();

        std::ofstream ad_hoc_output(experiment_directory + "ad_hoc_results/data_edges_statistics_condensed-0001to0000.json", std::ios::trunc);

        auto t_write_graph_instant{boost::chrono::system_clock::now()};
        auto time_t_write_graph_instant{boost::chrono::system_clock::to_time_t(t_write_graph_instant)};
        std::tm *ptm_write_graph_instant{std::localtime(&time_t_write_graph_instant)};
        auto t_edges_end{boost::chrono::system_clock::now()};
        auto step_info = w.get_times().back();
        auto step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(step_info.duration).count();
        ad_hoc_output << "{\n    \"Time taken (ms)\": " << step_duration
                        << ",\n    \"Memory footprint (kB)\": " << step_info.memory_in_kb << "\n}";
        ad_hoc_output.flush();
        std::cout << "(Time taken: " << boost::chrono::ceil<boost::chrono::milliseconds>(t_edges_end - t_loading_graph).count() << " ms, memory = " << step_info.memory_in_kb << " kB)" << std::endl;
        std::cout << std::put_time(ptm_write_graph_instant, "%Y/%m/%d %H:%M:%S") << " Writing condensed summary graph to disk" << std::endl;

        uint64_t edge_count = 0;
        boost::unordered_flat_set<block_or_singleton_index> summary_nodes;
        for (auto s_po_pair: gs.get_nodes())
        {
            block_or_singleton_index subject = s_po_pair.first;
            summary_nodes.emplace(subject);
            for (auto predicate_object_pair: s_po_pair.second.get_pairs())
            {
                edge_count++;
                block_or_singleton_index object = predicate_object_pair.second;
                summary_nodes.emplace(object);
            }
        }

        // Write the condensed summary graph to a file
        std::string output_directory = experiment_directory + "bisimulation/";
        std::string output_graph_file_path = output_directory + "condensed_multi_summary_graph.bin";
        std::ofstream output_graph_file_binary(output_graph_file_path, std::ios::trunc | std::ofstream::out);
        gs.write_graph_to_file_binary(output_graph_file_binary);

        auto t_write_intervals_instant{boost::chrono::system_clock::now()};
        auto time_t_write_intervals_instant{boost::chrono::system_clock::to_time_t(t_write_intervals_instant)};
        std::tm *ptm_write_intervals_instant{std::localtime(&time_t_write_intervals_instant)};
        std::cout << std::put_time(ptm_write_intervals_instant, "%Y/%m/%d %H:%M:%S") << " Writing node intervals to disk" << std::endl;

        // Write the merged block to singleton node map to a file
        blocks_to_singletons.write_map_to_file_binary(output_directory);

        // Write node intvervals to a file
        std::string output_interval_file_path = output_directory + "condensed_multi_summary_intervals.bin";
        std::ofstream output_interval_file_binary(output_interval_file_path, std::ios::trunc | std::ofstream::out);
        for (auto block_interval_pair: block_to_interval_map)  // We effectively write the following to disk: {block,start_time,end_time}
        {
            write_int_BLOCK_OR_SINGLETON_little_endian(output_interval_file_binary, block_interval_pair.first);
            write_uint_K_TYPE_little_endian(output_interval_file_binary, block_interval_pair.second.first);
            write_uint_K_TYPE_little_endian(output_interval_file_binary, block_interval_pair.second.second);
        }
        output_interval_file_binary.flush();

        auto t_write_map_instant{boost::chrono::system_clock::now()};
        auto time_t_write_map_instant{boost::chrono::system_clock::to_time_t(t_write_map_instant)};
        std::tm *ptm_write_map_instant{std::localtime(&time_t_write_map_instant)};
        std::cout << std::put_time(ptm_write_map_instant, "%Y/%m/%d %H:%M:%S") << " Writing local to global block map to disk" << std::endl;

        // Write the LocalBlockToGlobalBlockMap to a file
        std::string output_map_file_path = output_directory + "condensed_multi_summary_local_global_map.bin";
        std::ofstream output_map_file_binary(output_map_file_path, std::ios::trunc | std::ofstream::out);
        block_map.write_map_to_file_binary(output_map_file_binary, block_to_interval_map);

        auto t_early_counts{boost::chrono::system_clock::now()};
        auto time_t_early_counts{boost::chrono::system_clock::to_time_t(t_early_counts)};
        std::tm *ptm_early_counts{std::localtime(&time_t_early_counts)};
        std::cout << std::put_time(ptm_early_counts, "%Y/%m/%d %H:%M:%S") << " vertex count: " << summary_nodes.size() << std::endl;
        std::cout << std::put_time(ptm_early_counts, "%Y/%m/%d %H:%M:%S") << " edge count: " << edge_count << std::endl;

        w_total.stop_step();
        auto experiment_info = w.get_times().back();
        auto experiment_duration = boost::chrono::ceil<boost::chrono::milliseconds>(experiment_info.duration).count();
        int maximum_memory_footprint = 0;
        for (auto step: w.get_times())
        {
            maximum_memory_footprint = std::max(maximum_memory_footprint, step.memory_in_kb);
        }
        std::ofstream summary_graph_stats_output(experiment_directory + "ad_hoc_results/summary_graph_stats.json", std::ios::trunc);
        summary_graph_stats_output << "{\n    \"Vertex count\": " << summary_nodes.size()
                                   << ",\n    \"Edge count\": " << edge_count
                                   << ",\n    \"Total time taken (ms)\": " << experiment_duration
                                   << ",\n    \"Maximum memory footprint (kB)\": " << maximum_memory_footprint << "\n}";
        summary_graph_stats_output.flush();

        exit(0);  // Close the program
    }
    
    std::ostringstream current_level_stringstream;
    current_level_stringstream << std::setw(4) << std::setfill('0') << current_level;
    std::string current_level_string(current_level_stringstream.str());

    std::ostringstream previous_level_stringstream;
    previous_level_stringstream << std::setw(4) << std::setfill('0') << current_level-1;
    std::string previous_level_string(previous_level_stringstream.str());

    std::string current_mapping = experiment_directory + "bisimulation/mapping-" + previous_level_string + "to" + current_level_string + ".bin";
    std::ifstream current_mapping_file(current_mapping, std::ifstream::in);

    w.start_step("Read edges into summary graph", true);
    if (fixed_point_reached)
    {
        std::cout << std::put_time(ptm_loading_graph, "%Y/%m/%d %H:%M:%S") << " Creating initial condensed data edges (" + current_level_string + "-->" + current_level_string + ") " << std::endl;
        SplitToMergedMap fixed_point_map;
        for (auto index_block_pair: old_living_blocks)
        {
            fixed_point_map.add_pair(index_block_pair.first, index_block_pair.first);  // At the fixed point, all blocks map to themselves
        }
        // Create the final set of data edges (between k and k-1)
        read_graph_into_summary_timed(graph_file, node_to_block_map, block_map, fixed_point_map, block_to_interval_map, current_level, include_zero_outcome, fixed_point_reached, gs);
        old_split_to_merged_map = fixed_point_map;
        w.stop_step();

        std::ofstream ad_hoc_output(experiment_directory + "ad_hoc_results/data_edges_statistics_condensed-" + current_level_string + "to" + current_level_string + ".json", std::ios::trunc);
        auto step_info = w.get_times().back();
        auto step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(step_info.duration).count();
        ad_hoc_output << "{\n    \"Time taken (ms)\": " << step_duration
                        << ",\n    \"Memory footprint (kB)\": " << step_info.memory_in_kb << "\n}";
        ad_hoc_output.flush();
    }
    else
    {
        auto t_first_edges{boost::chrono::system_clock::now()};
        auto time_t_first_edges{boost::chrono::system_clock::to_time_t(t_first_edges)};
        std::tm *ptm_first_edges{std::localtime(&time_t_first_edges)};
        std::cout << std::put_time(ptm_first_edges, "%Y/%m/%d %H:%M:%S") << " Creating initial condensed data edges (" + current_level_string + "-->" + previous_level_string + ") " << std::endl;

        while (true)
        {
            block_index merged_block = read_uint_BLOCK_little_endian(current_mapping_file);
            if (current_mapping_file.eof())
            {
                break;
            }
            
            block_or_singleton_index global_block = block_map.add_block(current_level-1, merged_block);
            spawning_blocks[global_block] = {(block_or_singleton_index) merged_block, (k_type) (current_level-1)};  // Earlier (when loading the outcomes) we had already checked that this cast is possible

            gs.add_block_node(global_block);
            block_to_interval_map[global_block] = {first_level, current_level-1};

            block_index split_block_count = read_uint_BLOCK_little_endian(current_mapping_file);
            
            for (block_index j = 0; j < split_block_count; j++)
            {
                block_index split_block = read_uint_BLOCK_little_endian(current_mapping_file);
                if (split_block == 0)
                {
                    BlockMap& block_to_singletons = blocks_to_singletons.get_map(current_level);
                    for (node_index singleton: block_to_singletons.get_node_set(merged_block).get_nodes())
                    {
                        assert(singleton <= MAX_SIGNED_BLOCK_SIZE); // Check if the following cast is possible

                        block_or_singleton_index singlton_block = (block_or_singleton_index) singleton;
                        singlton_block = (-singlton_block)-1;

                        // The singleton blocks don't have to be mapped to global blocks, as they are already unique
                        old_split_to_merged_map.add_pair(singlton_block, global_block);
                        dying_blocks.emplace(singlton_block);
                        block_to_interval_map[singlton_block] = {current_level,current_level};  // The block immediately dies, therefore it only lived at the last level (current_level)
                    }
                }
                else
                {
                    auto global_split_block_iterator = new_local_to_global_living_blocks.find(split_block);
                    assert(global_split_block_iterator != new_local_to_global_living_blocks.cend());
                    block_or_singleton_index global_split_block = (*global_split_block_iterator).second;
                    old_split_to_merged_map.add_pair(global_split_block, global_block);  // We do not need to assert that these can be cast to block_or_singleton_index, since we have already done so when loading the outcomes earlier
                    dying_blocks.emplace(global_split_block);
                    block_to_interval_map[global_split_block] = {current_level,current_level};  // The block immediately dies, therefore it only lived at the last level (current_level)
                }
            }
        }

        // Update new_living_blocks by removing the dying blocks and adding the spawning blocks
        for (block_or_singleton_index dying_block: dying_blocks)
        {
            new_living_blocks.erase(dying_block);
        }
        new_living_blocks.merge(spawning_blocks);
        old_dying_blocks = std::move(dying_blocks);
        dying_blocks.clear();
        spawning_blocks.clear();

        // Update the new local to global living block mapping
        for (auto living_block_key_val: new_living_blocks)
        {
            new_local_to_global_living_blocks[living_block_key_val.second.local_index] = living_block_key_val.first;
        }

        // Create the final set of data edges (between k and k-1)
        read_graph_into_summary_timed(graph_file, node_to_block_map, block_map, old_split_to_merged_map, block_to_interval_map, current_level, include_zero_outcome, fixed_point_reached, gs);
        w.stop_step();

        std::ofstream ad_hoc_output(experiment_directory + "ad_hoc_results/data_edges_statistics_condensed-" + current_level_string + "to" + previous_level_string + ".json", std::ios::trunc);
        auto step_info = w.get_times().back();
        auto step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(step_info.duration).count();
        ad_hoc_output << "{\n    \"Time taken (ms)\": " << step_duration
                        << ",\n    \"Memory footprint (kB)\": " << step_info.memory_in_kb << "\n}";
        ad_hoc_output.flush();
    }

    // This corresponds to the one block that has no outgoing edges.
    // Since it never apears as a subject, it will normally not be added by our algorithm, therefore we will manually add it here if it exists
    for (auto living_block_key_val: old_living_blocks)
    {
        if (gs.get_nodes().find(living_block_key_val.first) == gs.get_nodes().cend())
        {
            block_to_interval_map[living_block_key_val.first] = {first_level, current_level};
        }
    }

    k_type smallest_level = 0;
    if (!include_zero_outcome)
    {
        smallest_level = 1;  // If we do not have an explicit outcome for k=0, we will add the k=1-->k=0 data edges separately later
    }

    k_type initial_level;
    if (fixed_point_reached)
    {
        initial_level = current_level;
    }
    else
    {
        initial_level = current_level-1;
    }

    // We decrement the current level and continue until the current level is either 2 or 1 (this would cover the data edges going to level 1 or 0 respecively)
    // In case we use the trivial universal block at k=0, we have separate code after this loop to cover the data edges from level 1 to level 0
    for (current_level=initial_level; current_level>smallest_level; current_level--)
    {
        SplitToMergedMap current_split_to_merged_map;

        std::ostringstream current_level_stringstream;
        current_level_stringstream << std::setw(4) << std::setfill('0') << current_level;
        std::string current_level_string(current_level_stringstream.str());

        std::ostringstream previous_level_stringstream;
        previous_level_stringstream << std::setw(4) << std::setfill('0') << current_level-1;
        std::string previous_level_string(previous_level_stringstream.str());

        std::string current_mapping = experiment_directory + "bisimulation/mapping-" + previous_level_string + "to" + current_level_string + ".bin";
        std::ifstream current_mapping_file(current_mapping, std::ifstream::in);
        
        const int BufferSize = 8 * 16184;

        char _buffer[BufferSize];
    
        current_mapping_file.rdbuf()->pubsetbuf(_buffer, BufferSize);
        
        auto t_edges{boost::chrono::system_clock::now()};
        auto time_t_edges{boost::chrono::system_clock::to_time_t(t_edges)};
        std::tm *ptm_edges{std::localtime(&time_t_edges)};
        std::cout << std::put_time(ptm_edges, "%Y/%m/%d %H:%M:%S") << " Creating condensed data edges (" + current_level_string + "-->" + previous_level_string + ") " << std::flush;  // We don't end the line so we can add statistics later
        w.start_step("Adding data edges (" + current_level_string + "-->" + previous_level_string + ")", true);  // Set newline to true

        // Read a mapping file
        while (true)
        {
            block_index merged_block = read_uint_BLOCK_little_endian(current_mapping_file);
            if (current_mapping_file.eof())
            {
                break;
            }
            block_or_singleton_index global_block = block_map.add_block(current_level-1, merged_block);
            gs.add_block_node(global_block);
            block_to_interval_map[global_block] = {first_level, current_level-1};

            spawning_blocks[global_block] = {(block_or_singleton_index) merged_block, (k_type) (current_level-1)};  // Earlier (when loading the outcomes) we had already checked that this cast is possible

            block_index split_block_count = read_uint_BLOCK_little_endian(current_mapping_file);
            
            for (block_index j = 0; j < split_block_count; j++)
            {
                block_index split_block = read_uint_BLOCK_little_endian(current_mapping_file);
                if (split_block == 0)
                {
                    BlockMap& block_to_singletons = blocks_to_singletons.get_map(current_level);
                    for (node_index singleton: block_to_singletons.get_node_set(merged_block).get_nodes())
                    {
                        assert(singleton <= MAX_SIGNED_BLOCK_SIZE); // Check if the following cast is possible

                        block_or_singleton_index singlton_block = (block_or_singleton_index) singleton;
                        singlton_block = (-singlton_block)-1;

                        // The singleton blocks don't have to be mapped to global blocks, as they are already unique
                        current_split_to_merged_map.add_pair(singlton_block, global_block);
                        dying_blocks.emplace(singlton_block);
                        block_to_interval_map[singlton_block].first = current_level;
                    }
                }
                else
                {
                    auto global_split_block_iterator = new_local_to_global_living_blocks.find(split_block);
                    assert(global_split_block_iterator != new_local_to_global_living_blocks.cend());
                    block_or_singleton_index global_split_block = (*global_split_block_iterator).second;
                    current_split_to_merged_map.add_pair(global_split_block, global_block);  // We do not need to assert that these can be cast to block_or_singleton_index, since we have already done so when loading the outcomes earlier
                    dying_blocks.emplace(global_split_block);
                    block_to_interval_map[global_split_block].first = current_level;
                }
            }
        }

        for (block_or_singleton_index dying_block: dying_blocks)
        {
            // outer_loop_count++;
            block_or_singleton_index object_image = current_split_to_merged_map.map_block(dying_block);
            for (auto predicate_subject_pair: gs.get_reverse_nodes()[dying_block].get_pairs())
            {
                // inner_loop_a_count++;
                block_or_singleton_index subject = predicate_subject_pair.second;

                if (old_living_blocks.find(subject) == old_living_blocks.cend())
                {
                    continue;
                }
                // inner_loop_b_count++;
                block_or_singleton_index subject_image = old_split_to_merged_map.map_block(subject);

                if (subject_image == subject && object_image == dying_block)
                {
                    continue;
                }
                // inner_loop_c_count++;
                
                edge_type predicate = predicate_subject_pair.first;

                gs.add_edge_to_node(subject_image, predicate, object_image);
            }
        }

        for (block_or_singleton_index old_dying_block: old_dying_blocks)
        {
            // outer_loop_count++;
            block_or_singleton_index subject_image = old_split_to_merged_map.map_block(old_dying_block);
            for (auto predicate_object_pair: gs.get_nodes()[old_dying_block].get_pairs())
            {
                // inner_loop_a_count++;
                block_or_singleton_index object = predicate_object_pair.second;

                if (new_living_blocks.find(object) == new_living_blocks.cend())
                {
                    continue;
                }
                // inner_loop_b_count++;

                block_or_singleton_index object_image = current_split_to_merged_map.map_block(object);

                if (subject_image == old_dying_block && object_image == object)
                {
                    continue;
                }
                // inner_loop_c_count++;

                edge_type predicate = predicate_object_pair.first;
                gs.add_edge_to_node(subject_image, predicate, object_image);
            }
        }
        old_living_blocks.clear();
        for (auto living_block_key_val: new_living_blocks)
        {
            old_living_blocks[living_block_key_val.first] = living_block_key_val.second;
        }

        // Update living_blocks by removing the dying blocks and adding the spawning blocks
        for (block_or_singleton_index dying_block: dying_blocks)
        {
            new_living_blocks.erase(dying_block);
        }
        new_living_blocks.merge(spawning_blocks);
        old_dying_blocks = std::move(dying_blocks);
        dying_blocks.clear();
        spawning_blocks.clear();

        // Update the new local to global living block mapping
        new_local_to_global_living_blocks.clear();
        for (auto living_block_key_val: new_living_blocks)
        {
            new_local_to_global_living_blocks[living_block_key_val.second.local_index] = living_block_key_val.first;
        }

        old_split_to_merged_map = std::move(current_split_to_merged_map);
        w.stop_step();

        std::ofstream ad_hoc_output(experiment_directory + "ad_hoc_results/data_edges_statistics_condensed-" + current_level_string + "to" + previous_level_string + ".json", std::ios::trunc);

        auto t_edges_end{boost::chrono::system_clock::now()};
        auto step_info = w.get_times().back();
        auto step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(step_info.duration).count();
        ad_hoc_output << "{\n    \"Time taken (ms)\": " << step_duration
                        << ",\n    \"Memory footprint (kB)\": " << step_info.memory_in_kb << "\n}";
        ad_hoc_output.flush();
        std::cout << "(Time taken: " << boost::chrono::ceil<boost::chrono::milliseconds>(t_edges_end - t_start).count() << " ms, memory = " << step_info.memory_in_kb << " kB)" << std::endl;
    }

    if (!include_zero_outcome)  // If we don't have an explicit outcome for k=0, then we will manually add those data edges now, otherwise they should have automatically been added
    {
        auto t_last_edges{boost::chrono::system_clock::now()};
        auto time_t_last_edges{boost::chrono::system_clock::to_time_t(t_last_edges)};
        std::tm *ptm_last_edges{std::localtime(&time_t_last_edges)};
        std::cout << std::put_time(ptm_last_edges, "%Y/%m/%d %H:%M:%S") << " Creating condensed data edges (0001-->0000) " << std::flush;  // We don't end the line so we can add statistics later
        w.start_step("Adding data edges (0001-->0000)", true);  // Set newline to true

        // Add the edges between the level 1 and level 0
        k_type zero_level = 0;
        block_or_singleton_index universal_block = 0;
        block_or_singleton_index global_universal_block = block_map.add_block(zero_level, universal_block);
        gs.add_block_node(global_universal_block);
        k_type universal_block_time = 0;
        block_to_interval_map[global_universal_block] = {universal_block_time, universal_block_time};
        for (auto living_block_key_val: old_living_blocks)
        {
            block_or_singleton_index subject = living_block_key_val.first;
            for (auto predicate_object_pair: gs.get_nodes()[subject].get_pairs())
            {
                edge_type predicate = predicate_object_pair.first;
                block_or_singleton_index subject_image = old_split_to_merged_map.map_block(subject);
                gs.add_edge_to_node(subject_image, predicate, global_universal_block);
            }
        }
        w.stop_step();

        std::ofstream ad_hoc_output(experiment_directory + "ad_hoc_results/data_edges_statistics_condensed-0001to0000.json", std::ios::trunc);

        auto t_last_edges_end{boost::chrono::system_clock::now()};
        auto step_info = w.get_times().back();
        auto step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(step_info.duration).count();
        ad_hoc_output << "{\n    \"Time taken (ms)\": " << step_duration
                        << ",\n    \"Memory footprint (kB)\": " << step_info.memory_in_kb << "\n}";
        ad_hoc_output.flush();
        std::cout << "(Time taken: " << boost::chrono::ceil<boost::chrono::milliseconds>(t_last_edges_end - t_last_edges).count() << " ms, memory = " << step_info.memory_in_kb << " kB)" << std::endl;
    }

    auto t_write_graph{boost::chrono::system_clock::now()};
    auto time_t_write_graph{boost::chrono::system_clock::to_time_t(t_write_graph)};
    std::tm *ptm_write_graph{std::localtime(&time_t_write_graph)};
    std::cout << std::put_time(ptm_write_graph, "%Y/%m/%d %H:%M:%S") << " Writing condensed summary graph to disk" << std::endl;

    uint64_t data_edge_count = 0;
    boost::unordered_flat_set<block_or_singleton_index> summary_nodes;
    for (auto s_po_pair: gs.get_nodes())
    {
        block_or_singleton_index subject = s_po_pair.first;
        summary_nodes.emplace(subject);
        for (auto predicate_object_pair: s_po_pair.second.get_pairs())
        {
            data_edge_count++;
            block_or_singleton_index object = predicate_object_pair.second;
            summary_nodes.emplace(object);
        }
    }

    refines_edge_count += blocks_to_singletons.get_singleton_refines_edge_count();  // We purposefully ommited counting singleton refines edges earlier, so now we add them

    // Write the condensed summary graph (along with the time intervals) to a file
    std::string output_directory = experiment_directory + "bisimulation/";
    std::string output_graph_file_path = output_directory + "condensed_multi_summary_graph.bin";
    std::ofstream output_graph_file_binary(output_graph_file_path, std::ios::trunc | std::ofstream::out);
    gs.write_graph_to_file_binary(output_graph_file_binary);

    // Write the merged block to singleton node map to a file
    blocks_to_singletons.write_map_to_file_binary(output_directory);

    auto t_write_intervals{boost::chrono::system_clock::now()};
    auto time_t_write_intervals{boost::chrono::system_clock::to_time_t(t_write_intervals)};
    std::tm *ptm_write_intervals{std::localtime(&time_t_write_intervals)};
    std::cout << std::put_time(ptm_write_intervals, "%Y/%m/%d %H:%M:%S") << " Writing node intervals to disk" << std::endl;

    // Write node intvervals to a file
    std::string output_interval_file_path = output_directory + "condensed_multi_summary_intervals.bin";
    std::ofstream output_interval_file_binary(output_interval_file_path, std::ios::trunc | std::ofstream::out);
    for (auto block_interval_pair: block_to_interval_map)  // We effectively write the following to disk: {block,start_time,end_time}
    {
        write_int_BLOCK_OR_SINGLETON_little_endian(output_interval_file_binary, block_interval_pair.first);
        write_uint_K_TYPE_little_endian(output_interval_file_binary, block_interval_pair.second.first);
        write_uint_K_TYPE_little_endian(output_interval_file_binary, block_interval_pair.second.second);
    }
    output_interval_file_binary.flush();

    auto t_write_map{boost::chrono::system_clock::now()};
    auto time_t_write_map{boost::chrono::system_clock::to_time_t(t_write_map)};
    std::tm *ptm_write_map{std::localtime(&time_t_write_map)};
    std::cout << std::put_time(ptm_write_map, "%Y/%m/%d %H:%M:%S") << " Writing local to global block map to disk" << std::endl;

    // Write the LocalBlockToGlobalBlockMap to a file
    std::string output_map_file_path = output_directory + "condensed_multi_summary_local_global_map.bin";
    std::ofstream output_map_file_binary(output_map_file_path, std::ios::trunc | std::ofstream::out);
    block_map.write_map_to_file_binary(output_map_file_binary, block_to_interval_map);

    // Report some statistics about the condensed multi-summary graph and time + memory instrumentation
    std::ostringstream first_level_stringstream;
    first_level_stringstream << std::setw(4) << std::setfill('0') << first_level;
    std::string first_level_string(first_level_stringstream.str());

    std::ifstream first_level_statistics_file(experiment_directory + "ad_hoc_results/statistics_condensed-" + first_level_string + ".json");
    json first_level_statistics;
    first_level_statistics_file >> first_level_statistics;

    block_index initial_partition_size = first_level_statistics["Block count"];

    std::ostringstream k_stringstream;
    k_stringstream << std::setw(4) << std::setfill('0') << k;
    std::string k_string(k_stringstream.str());

    std::ifstream k_statistics_file(experiment_directory + "ad_hoc_results/statistics_condensed-" + k_string + ".json");
    json k_statistics;
    k_statistics_file >> k_statistics;
    
    block_index singleton_count = k_statistics["Singleton count"];

    auto t_counts{boost::chrono::system_clock::now()};
    auto time_t_counts{boost::chrono::system_clock::to_time_t(t_counts)};
    std::tm *ptm_counts{std::localtime(&time_t_counts)};
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " vertex count: " << summary_nodes.size() << std::endl;
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " data edge count: " << data_edge_count << std::endl;
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " refines edge count: " << refines_edge_count << std::endl;
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " singleton count: " << singleton_count << std::endl;
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " initial partition size: " << initial_partition_size << std::endl;

    w_total.stop_step();
    auto experiment_info = w.get_times().back();
    auto experiment_duration = boost::chrono::ceil<boost::chrono::milliseconds>(experiment_info.duration).count();
    int maximum_memory_footprint = 0;
    for (auto step: w.get_times())
    {
        maximum_memory_footprint = std::max(maximum_memory_footprint, step.memory_in_kb);
    }
    std::ofstream summary_graph_stats_output(experiment_directory + "ad_hoc_results/summary_graph_stats.json", std::ios::trunc);
    summary_graph_stats_output << "{\n    \"Vertex count\": " << summary_nodes.size()
                               << ",\n    \"Data edge count\": " << data_edge_count
                               << ",\n    \"Refines edge count\": " << refines_edge_count
                               << ",\n    \"Singleton count\": " << singleton_count
                               << ",\n    \"Initial partition size\": " << initial_partition_size
                               << ",\n    \"Total time taken (ms)\": " << experiment_duration
                               << ",\n    \"Maximum memory footprint (kB)\": " << maximum_memory_footprint << "\n}";
    summary_graph_stats_output.flush();
}