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

using edge_type = uint32_t;
using node_index = uint64_t;
using block_index = node_index;
using block_or_singleton_index = int64_t;
using k_type = uint16_t;
using node_to_block_map_type = boost::unordered_flat_map<node_index, block_or_singleton_index>;
using local_to_global_map = boost::unordered_flat_map<std::pair<k_type,block_or_singleton_index>,block_or_singleton_index>;
using predicate_object_pair_set = boost::unordered_flat_set<std::pair<edge_type,block_or_singleton_index>>;
using time_interval = std::pair<k_type, k_type>;
using interval_map = boost::unordered_flat_map<block_or_singleton_index, time_interval>;

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;
const int BYTES_PER_BLOCK = 4;
const int BYTES_PER_BLOCK_OR_SINGLETON = 5;
const int BYTES_PER_K_TYPE = 2;
const int64_t MAX_SIGNED_BLOCK_SIZE = INT64_MAX;
// const k_type DEFAULT_TIME_VALUE = 1;

const std::string outcome_file_regex_string = R"(^outcome_condensed-\d{4}\.bin$)";
const std::string mapping_file_regex_string = R"(^mapping-\d{4}to\d{4}\.bin$)";
std::regex outcome_file_regex_pattern(outcome_file_regex_string);
std::regex mapping_file_regex_pattern(mapping_file_regex_string);

#define CREATE_REVERSE_INDEX

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

// class Node;

// struct Edge
// {
// public:
//     const edge_type label;
//     const node_index target;
// };

// class Node
// {
//     std::vector<Edge> edges;

// public:
//     Node()
//     {
//         edges.reserve(1);
//     }
//     std::vector<Edge>& get_outgoing_edges()
//     {
//         std::vector<Edge> & edges_ref = edges;
//         return edges_ref;
//     }

//     void add_edge(edge_type label, node_index target)
//     {
//         edges.emplace_back(label, target);
//     }
// };

// class Graph
// {
// private:
//     std::vector<Node> nodes;
// #ifdef CREATE_REVERSE_INDEX
//     std::vector<Node> reverse;
// #endif
//     Graph(Graph &)
//     {
//     }

// public:
//     Graph()
//     {
//     }
//     node_index add_vertex()
//     {
//         nodes.emplace_back();
//         return nodes.size() - 1;
//     }
//     void resize(node_index vertex_count)
//     {
//         nodes.resize(vertex_count);
//     }

//     std::vector<Node>& get_nodes()
//     {
//         std::vector<Node> & nodes_ref = nodes;
//         return nodes_ref;
//     }
//     inline node_index size()
//     {
//         return nodes.size();
//     }
// #ifdef CREATE_REVERSE_INDEX

//     void compute_reverse_index()
//     {
//         if (!this->reverse.empty())
//         {
//             throw MyException("computing the reverse while this has been computed before. Probably a programming error");
//         }
//         size_t number_of_nodes = this->nodes.size();
//         for (size_t node = 0;  node < number_of_nodes; node++)
//         {
//             reverse.emplace_back();
//         }
//         for (size_t node = 0;  node < number_of_nodes; node++)
//         {
//             for (auto edge: nodes[node].get_outgoing_edges())
//             {
//                 reverse[edge.target].add_edge(edge.label, node);
//             }
//         }
//     }

//     std::vector<Node>& get_reverse_nodes()
//     {
//         std::vector<Node> & reverse_nodes_ref = reverse;
//         return reverse_nodes_ref;
//     }
// #endif
// };

void write_uint_K_TYPE_little_endian(std::ostream &outputstream, k_type value)
{
    char data[BYTES_PER_K_TYPE];
    for (unsigned int i = 0; i < BYTES_PER_K_TYPE; i++)
    {
        data[i] = char(value);  // TODO check if removing & 0x00000000000000FFull fixed it
        value = value >> 8;
    }
    outputstream.write(data, BYTES_PER_K_TYPE);
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

void write_uint_BLOCK_little_endian(std::ostream &outputstream, block_index value)
{
    char data[BYTES_PER_BLOCK];
    for (unsigned int i = 0; i < BYTES_PER_BLOCK; i++)
    {
        data[i] = char(value & 0x00000000000000FFull);
        value = value >> 8;
    }
    outputstream.write(data, BYTES_PER_BLOCK);
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

u_int32_t read_PREDICATE_little_endian(std::istream &inputstream)
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
        result |= (uint32_t(data[i]) & 0x000000FFul) << (i * 8); // `& 0x000000FFul` makes sure that we only write one byte of data
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

int64_t read_int_BLOCK_OR_SINGLETON_little_endian(std::istream &inputstream)
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

template <typename clock>
class StopWatch
{
    struct Step
    {
        const std::string name;
        const clock::duration duration;
        const int memory_in_kb;
        const bool newline;
        Step(const std::string &name, const clock::duration &duration, const int &memory_in_kb, const bool &newline)
            : name(name), duration(duration), memory_in_kb(memory_in_kb), newline(newline)
        {
        }
        Step(const Step &step)
            : name(step.name), duration(step.duration), memory_in_kb(step.memory_in_kb), newline(step.newline)
        {
        }
    };

private:
    std::vector<StopWatch::Step> steps;
    bool started;
    bool paused;
    clock::time_point last_starting_time;
    std::string current_step_name;
    clock::duration stored_at_last_pause;
    bool newline;

    StopWatch()
    {
    }

    static int current_memory_use_in_kb()
    {
        std::ifstream procfile("/proc/self/status");
        std::string line;
        while (std::getline(procfile, line))
        {
            if (line.rfind("VmRSS:", 0) == 0)
            {
                // split in 3 pieces
                std::vector<std::string> parts;
                boost::split(parts, line, boost::is_any_of("\t "), boost::token_compress_on);
                // check that we have exactly thee parts
                if (parts.size() != 3)
                {
                    throw MyException("The line with VmRSS: did not split in 3 parts on whitespace");
                }
                if (parts[2] != "kB")
                {
                    throw MyException("The line with VmRSS: did not end in kB");
                }
                int size = std::stoi(parts[1]);
                procfile.close();
                return size;
            }
        }
        throw MyException("fail. could not find VmRSS");
    }

public:
    static StopWatch create_started()
    {
        StopWatch c = create_not_started();
        c.start_step("start");
        return c;
    }

    static StopWatch create_not_started()
    {
        StopWatch c;
        c.started = false;
        c.paused = false;
        c.newline = false;
        return c;
    }

    void pause()
    {
        if (!this->started)
        {
            throw MyException("Cannot pause not running StopWatch, start it first");
        }
        if (this->paused)
        {
            throw MyException("Cannot pause paused StopWatch, resume it first");
        }
        this->stored_at_last_pause += clock::now() - last_starting_time;
        this->paused = true;
    }

    void resume()
    {
        if (!this->started)
        {
            throw MyException("Cannot resume not running StopWatch, start it first");
        }
        if (!this->paused)
        {
            throw MyException("Cannot resume not paused StopWatch, pause it first");
        }
        this->last_starting_time = clock::now();
        this->paused = false;
    }

    void start_step(std::string name, bool newline = false)
    {
        if (this->started)
        {
            throw MyException("Cannot start on running StopWatch, stop it first");
        }
        if (this->paused)
        {
            throw MyException("This must never happen. Invariant is wrong. If stopped, there can be no pause active.");
        }
        this->last_starting_time = clock::now();
        this->current_step_name = name;
        this->started = true;
        this->newline = newline;
    }

    void stop_step()
    {
        if (!this->started)
        {
            throw MyException("Cannot stop not running StopWatch, start it first");
        }
        if (this->paused)
        {
            throw MyException("Cannot stop not paused StopWatch, unpause it first");
        }
        auto stop_time = clock::now();
        auto total_duration = (stop_time - this->last_starting_time) + this->stored_at_last_pause;
        // For measuring memory, we sleep 100ms to give the os time to reclaim memory
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int memory_use = current_memory_use_in_kb();

        this->steps.emplace_back(this->current_step_name, total_duration, memory_use, this->newline);
        this->started = false;
        this->newline = false;
    }

    std::string to_string()
    {
        if (this->started)
        {
            throw MyException("Cannot convert a running StopWatch to a string, stop it first");
        }
        std::stringstream out;
        for (auto step : this->get_times())
        {
            out << "Step: " << step.name << ", time = " << boost::chrono::ceil<boost::chrono::milliseconds>(step.duration).count() << " ms"
                << ", memory = " << step.memory_in_kb << " kB"
                << "\n";
            if (step.newline)
            {
                out << "\n";
            }
        }
        // Create the output string and remove the final superfluous '\n' characters
        std::string out_str = out.str();
        boost::trim(out_str);
        return out_str;
    }

    std::vector<StopWatch::Step>& get_times()
    {
        std::vector<StopWatch::Step> & steps_ref = this->steps;
        return steps_ref;
    }
};

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
            // std::cout << "DEBUG mapped to global block B: " << block_map_iterator->second << std::endl;
            return block_map_iterator->second;
        }
        else
        {
            // std::cout << "DEBUG mapped to global block S: " << local_block << std::endl;
            return local_block;
        }
        return this->get_map()[{k,local_block}];
    }
    block_or_singleton_index add_block(k_type k, block_or_singleton_index local_block)
    {
        block_or_singleton_index global_block = next_block;
        this->get_map()[{k,local_block}] = global_block;
        // auto block_map_iterator = this->get_map().find({k,local_block});
        // std::cout << "DEBUG added and found block: <<" << (*block_map_iterator).first.first << "," << (*block_map_iterator).first.second << ">," << (*block_map_iterator).second << ">" << std::endl;
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


// class SummaryObjectSet
// {
//     private:
//     boost::unordered_flat_set<block_or_singleton_index> objects;

//     public:
//     SummaryObjectSet()
//     {
//     }
//     void add_object(block_or_singleton_index object)
//     {
//         objects.emplace(object);  // Should the be emplace or insert?
//     }
//     boost::unordered_flat_set<block_or_singleton_index>& get_objects()
//     {
//         return objects;
//     }
//     void remove_object(block_or_singleton_index object)
//     {
//         objects.erase(object);
//     }
// };
// using sp_to_o_map = boost::unordered_flat_map<std::pair<block_or_singleton_index,edge_type>,SummaryObjectSet>;

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


// class SummaryNode
// {
//     private:
//     boost::unordered_flat_map<edge_type, SummaryObjectSet> node;

//     public:
//     SummaryNode()
//     {
//     }
//     boost::unordered_flat_map<edge_type, SummaryObjectSet>& get_edges()
//     {
//         return node;
//     }
//     size_t count_edge_key(edge_type edge)
//     {
//         return this->get_edges().count(edge);
//     }
//     void add_edge(edge_type edge, block_or_singleton_index object)
//     {
//         if (node.count(edge) == 0)
//         {
//             SummaryObjectSet empty_object_set = SummaryObjectSet();
//             this->get_edges()[edge] = empty_object_set;
//         }
//         this->get_edges()[edge].add_object(object);
//     }
//     void remove_edge_recursive(edge_type edge, block_or_singleton_index object)
//     {
//         if (this->get_edges()[edge].get_objects().size() > 1)
//         {
//             this->get_edges()[edge].remove_object(object);  // Remove just the object if we have more triples with the same edge type
//         }
//         else
//         {
//             this->get_edges().erase(edge);  // If there was just one triple with the given edge type, remove the whole edge type from the map
//         }
//     }
// };

// class Triple
// {
// public:
//     block_or_singleton_index subject;
//     edge_type predicate;
//     block_or_singleton_index object;
//     Triple(block_or_singleton_index s, edge_type p, block_or_singleton_index o)
//     {
//         subject = s;
//         predicate = p;
//         object = o;
//     }
// };

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
                // std::cout << "DEBUG wrote SPO: " << subject << " " << predicate << " " << object << std::endl;
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
        edge_type edge_label = read_PREDICATE_little_endian(inputstream);

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
        // std::cout << "DEBUG adding edge: " << subject_block << "," << edge_label << "," << object_block_previous_level << std::endl;
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
            // if (i == 1 and block == 1)
            // {
            //     std::cout << "DEBUG: found (1,1)" << std::endl;
            // }
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

    // for (auto node_block_pair: node_to_block_map)
    // {
    //     std::cout << "DEBUG node:block :: " << node_block_pair.first << ":" << node_block_pair.second << std::endl;
    // }

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
        // if (current_level == 1)
        // {
        //     std::cout << "DEBUG: map found level 1 #1" << std::endl;
        // }
        // if (current_level == 1 and block_id == 1)
        // {
        //     std::cout << "DEBUG: map found (1,1) #1";
        // }
        size_t block_size = iter->second.size();
        if (block_size == 0)  // Empty blocks do not yield summary nodes
        {
            continue;
        }
        // std::cout << "DEBUG 1 block in:  <" << current_level << "," << block_id << ">" << std::endl;
        block_or_singleton_index global_block = block_map.add_block(current_level, block_id);
        // std::cout << "DEBUG 1 block out: <" << block_id << ">\n" << std::endl;
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
            // std::cout << "DEBUG 2 block in:  <" << zero_level << "," << universal_block << ">" << std::endl;
            block_or_singleton_index global_universal_block = block_map.add_block(zero_level, universal_block);
            // std::cout << "DEBUG 2 block out: <" << global_universal_block << ">\n" << std::endl;   
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
            // std::cout << "DEBUG wrote block-interval: " << block_interval_pair.first << ":[" << block_interval_pair.second.first << "," << block_interval_pair.second.second << "]" << std::endl;
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
            // if (current_level == 1)
            // {
            //     std::cout << "DEBUG: map found level 1 #2" << std::endl;
            // }
            // if (current_level-1 == 1 and merged_block == 1)
            // {
            //     std::cout << "DEBUG: map found (1,1) #2";
            // }
            if (current_mapping_file.eof())
            {
                break;
            }
            
            // std::cout << "DEBUG 3 block in:  <" << current_level-1 << "," << merged_block << ">" << std::endl;
            block_or_singleton_index global_block = block_map.add_block(current_level-1, merged_block);
            // std::cout << "DEBUG 3 block out: <" << global_block << ">\n" << std::endl; 
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
            // if (current_level-1 == 1)
            // {
            //     std::cout << "DEBUG: map found level 1 #3" << std::endl;
            // }
            // if (current_level-1 == 1 and merged_block == 1)
            // {
            //     std::cout << "DEBUG: map found (1,1) #3";
            // }
            if (current_mapping_file.eof())
            {
                break;
            }
            // std::cout << "DEBUG 4 block in:  <" << current_level-1 << "," << merged_block << ">" << std::endl;
            block_or_singleton_index global_block = block_map.add_block(current_level-1, merged_block);
            // std::cout << "DEBUG 4 block out: <" << global_block << ">\n" << std::endl; 
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

        // for (auto living_block_key_val: old_living_blocks)
        // {
        //     block_or_singleton_index subject = living_block_key_val.first;
        //     for (auto predicate_object_pair: gs.get_nodes()[subject].get_pairs())
        //     {
        //         block_or_singleton_index object = predicate_object_pair.second;

        //         if (new_living_blocks.find(object) == new_living_blocks.cend())  // These are blocks that have died
        //         {
        //             continue;
        //         }

        //         block_or_singleton_index object_image = current_split_to_merged_map.map_block(object);
        //         block_or_singleton_index subject_image = old_split_to_merged_map.map_block(subject);
        //         // If neither the subject nor object changed, then the edge already exists and there is no need to try to add it to the graph again
        //         if (subject_image == subject && object_image == object)
        //         {
        //             continue;
        //         }

        //         edge_type predicate = predicate_object_pair.first;

        //         std::cout << "DEBUG original SPO: " << subject_image << "," << predicate << "," << object_image << std::endl;
        //         gs.add_edge_to_node(subject_image, predicate, object_image);
        //     }
        // }
        // std::cout << std::endl;
        // std::cout << "DEBUG current level: " << current_level << std::endl;
        // Add the incomming edges for the blocks that died in the current level
        // auto t_debug_2{boost::chrono::system_clock::now()};
        // auto time_t_debug_2{boost::chrono::system_clock::to_time_t(t_debug_2)};
        // std::tm *ptm_debug_2{std::localtime(&time_t_debug_2)};
        // std::cout << "\n" << std::put_time(ptm_debug_2, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before add current incomming" << std::endl;
        // u_int64_t outer_loop_count = 0;
        // u_int64_t inner_loop_a_count = 0;
        // u_int64_t inner_loop_b_count = 0;
        // u_int64_t inner_loop_c_count = 0;
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

                // if (subject_image == 195){
                //     std::cout << "DEBUG 195 mapped from sub: " << subject << std::endl;
                // }

                // if (object_image == 195){
                //     std::cout << "DEBUG 195 mapped from obj: " << dying_block << std::endl;
                // }

                // if (subject == -2059) {
                //     edge_type predicate = predicate_object_pair.first;
                //     std::cout << "DEBUG -2059 edge (before): " << subject << "," << predicate << "," << dying_block << std::endl;
                //     std::cout << "DEBUG -2059 edge (after): " << subject_image << "," << predicate << "," << object_image << std::endl;
                // }
                
                // if (dying_block == 111)
                // {
                //     edge_type predicate = predicate_object_pair.first;
                //     if (predicate == 78)
                //     {
                //         std::cout << "DEBUG 111 edge: " << subject_image << ", " << predicate << ", " << object_image << std::endl;
                //     }
                // }
                // std::cout << "DEBUG dying block: " << dying_block << std::endl;

                if (subject_image == subject && object_image == dying_block)
                {
                    // edge_type predicate = predicate_object_pair.first;
                    // std::cout << "DEBUG skipped edge: " << subject_image << ", " << predicate << ", " << object_image << std::endl;
                    continue;
                }
                // inner_loop_c_count++;
                
                edge_type predicate = predicate_subject_pair.first;

                // std::cout << "DEBUG new dying SPO: " << subject_image << "," << predicate << "," << object_image << std::endl;
                gs.add_edge_to_node(subject_image, predicate, object_image);
            }
        }
        // auto t_debug_loop1{boost::chrono::system_clock::now()};
        // auto time_t_debug_loop1{boost::chrono::system_clock::to_time_t(t_debug_loop1)};
        // std::tm *ptm_debug_loop1{std::localtime(&time_t_debug_loop1)};
        // std::cout << std::put_time(ptm_debug_loop1, "%Y/%m/%d %H:%M:%S") << " DEBUG: out:" << outer_loop_count << " in1:" << inner_loop_a_count << " in2:" << inner_loop_b_count << " in3:" << inner_loop_c_count << std::endl;

        // Add the outgoing edges for the blocks that died in the previous level
        // auto t_debug_1{boost::chrono::system_clock::now()};
        // auto time_t_debug_1{boost::chrono::system_clock::to_time_t(t_debug_1)};
        // std::tm *ptm_debug_1{std::localtime(&time_t_debug_1)};
        // std::cout << std::put_time(ptm_debug_1, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before add previous outgoing" << std::endl;
        // outer_loop_count = 0;
        // inner_loop_a_count = 0;
        // inner_loop_b_count = 0;
        // inner_loop_c_count = 0;
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
                    // edge_type predicate = predicate_object_pair.first;
                    // std::cout << "DEBUG skipped edge: " << subject_image << ", " << predicate << ", " << object_image << std::endl;
                    continue;
                }
                // inner_loop_c_count++;

                edge_type predicate = predicate_object_pair.first;
                gs.add_edge_to_node(subject_image, predicate, object_image);
            }
        }
        // auto t_debug_loop2{boost::chrono::system_clock::now()};
        // auto time_t_debug_loop2{boost::chrono::system_clock::to_time_t(t_debug_loop2)};
        // std::tm *ptm_debug_loop2{std::localtime(&time_t_debug_loop2)};
        // std::cout << std::put_time(ptm_debug_loop2, "%Y/%m/%d %H:%M:%S") << " DEBUG: out:" << outer_loop_count << " in1:" << inner_loop_a_count << " in2:" << inner_loop_b_count << " in3:" << inner_loop_c_count << std::endl;


        // std::cout << std::endl;
        // for (block_or_singleton_index old_dying_block: old_dying_blocks)
        // {
        //     block_or_singleton_index subject_image = current_split_to_merged_map.map_block(old_dying_block);
        //     for (auto predicate_object_pair: gs.get_nodes()[old_dying_block].get_pairs())
        //     {
        //         edge_type predicate = predicate_object_pair.first;
        //         block_or_singleton_index object = predicate_object_pair.second;

        //         block_or_singleton_index object_image = old_split_to_merged_map.map_block(object);

        //         std::cout << "DEBUG old dying SPO: " << subject_image << "," << predicate << "," << object_image << std::endl;
        //     }
        // }
        // std::cout << std::endl;

        // Copy the new living blocks into the old living blocks
        // auto t_debug1{boost::chrono::system_clock::now()};
        // auto time_t_debug1{boost::chrono::system_clock::to_time_t(t_debug1)};
        // std::tm *ptm_debug1{std::localtime(&time_t_debug1)};
        // std::cout << std::put_time(ptm_debug1, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before clear old living blocks" << std::endl;
        old_living_blocks.clear();
        // auto t_debug2{boost::chrono::system_clock::now()};
        // auto time_t_debug2{boost::chrono::system_clock::to_time_t(t_debug2)};
        // std::tm *ptm_debug2{std::localtime(&time_t_debug2)};
        // std::cout << std::put_time(ptm_debug2, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before update old living blocks" << std::endl;
        for (auto living_block_key_val: new_living_blocks)
        {
            old_living_blocks[living_block_key_val.first] = living_block_key_val.second;
        }

        // Update living_blocks by removing the dying blocks and adding the spawning blocks
        // auto t_debug3{boost::chrono::system_clock::now()};
        // auto time_t_debug3{boost::chrono::system_clock::to_time_t(t_debug3)};
        // std::tm *ptm_debug3{std::localtime(&time_t_debug3)};
        // std::cout << std::put_time(ptm_debug3, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before erase new living blocks" << std::endl;
        for (block_or_singleton_index dying_block: dying_blocks)
        {
            new_living_blocks.erase(dying_block);
        }
        // auto t_debug4{boost::chrono::system_clock::now()};
        // auto time_t_debug4{boost::chrono::system_clock::to_time_t(t_debug4)};
        // std::tm *ptm_debug4{std::localtime(&time_t_debug4)};
        // std::cout << std::put_time(ptm_debug4, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before merge new living blocks" << std::endl;
        new_living_blocks.merge(spawning_blocks);
        // auto t_debug5{boost::chrono::system_clock::now()};
        // auto time_t_debug5{boost::chrono::system_clock::to_time_t(t_debug5)};
        // std::tm *ptm_debug5{std::localtime(&time_t_debug5)};
        // std::cout << std::put_time(ptm_debug5, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before move into old dying blocks" << std::endl;
        old_dying_blocks = std::move(dying_blocks);
        // auto t_debug6{boost::chrono::system_clock::now()};
        // auto time_t_debug6{boost::chrono::system_clock::to_time_t(t_debug6)};
        // std::tm *ptm_debug6{std::localtime(&time_t_debug6)};
        // std::cout << std::put_time(ptm_debug6, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before clear dyings blocks" << std::endl;
        dying_blocks.clear();
        // auto t_debug7{boost::chrono::system_clock::now()};
        // auto time_t_debug7{boost::chrono::system_clock::to_time_t(t_debug7)};
        // std::tm *ptm_debug7{std::localtime(&time_t_debug7)};
        // std::cout << std::put_time(ptm_debug7, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before clear spawing blocks" << std::endl;
        spawning_blocks.clear();

        // Update the new local to global living block mapping
        // auto t_debug8{boost::chrono::system_clock::now()};
        // auto time_t_debug8{boost::chrono::system_clock::to_time_t(t_debug8)};
        // std::tm *ptm_debug8{std::localtime(&time_t_debug8)};
        // std::cout << std::put_time(ptm_debug8, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before clear new local to global living blocks" << std::endl;
        new_local_to_global_living_blocks.clear();
        // auto t_debug9{boost::chrono::system_clock::now()};
        // auto time_t_debug9{boost::chrono::system_clock::to_time_t(t_debug9)};
        // std::tm *ptm_debug9{std::localtime(&time_t_debug9)};
        // std::cout << std::put_time(ptm_debug9, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before update new local to global living blocks" << std::endl;
        for (auto living_block_key_val: new_living_blocks)
        {
            new_local_to_global_living_blocks[living_block_key_val.second.local_index] = living_block_key_val.first;
        }

        // auto t_debug10{boost::chrono::system_clock::now()};
        // auto time_t_debug10{boost::chrono::system_clock::to_time_t(t_debug10)};
        // std::tm *ptm_debug10{std::localtime(&time_t_debug10)};
        // std::cout << std::put_time(ptm_debug10, "%Y/%m/%d %H:%M:%S") << " DEBUG: Before move into old split to merged map" << std::endl;
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
        // std::cout << "DEBUG 5 block in:  <" << zero_level << "," << universal_block << ">" << std::endl;
        block_or_singleton_index global_universal_block = block_map.add_block(zero_level, universal_block); 
        // std::cout << "DEBUG 5 block out: <" << global_universal_block << ">\n" << std::endl;    
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
        // std::cout << "DEBUG wrote block-interval: " << block_interval_pair.first << ":[" << block_interval_pair.second.first << "," << block_interval_pair.second.second << "]" << std::endl;
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

    auto t_counts{boost::chrono::system_clock::now()};
    auto time_t_counts{boost::chrono::system_clock::to_time_t(t_counts)};
    std::tm *ptm_counts{std::localtime(&time_t_counts)};
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " vertex count: " << summary_nodes.size() << std::endl;
    std::cout << std::put_time(ptm_counts, "%Y/%m/%d %H:%M:%S") << " edge count: " << edge_count << std::endl;

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
}