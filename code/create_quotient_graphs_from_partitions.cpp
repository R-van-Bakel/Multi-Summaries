#include <cstdint>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <iomanip>
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

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;
const int BYTES_PER_BLOCK = 4;
const int BYTES_PER_BLOCK_OR_SINGLETON = 5;
const int64_t MAX_SIGNED_BLOCK_SIZE = INT64_MAX;

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

class Node;

struct Edge
{
public:
    const edge_type label;
    const node_index target;
};

class Node
{
    std::vector<Edge> edges;

public:
    Node()
    {
        edges.reserve(1);
    }
    std::vector<Edge>& get_outgoing_edges()
    {
        std::vector<Edge> & edges_ref = edges;
        return edges_ref;
    }

    void add_edge(edge_type label, node_index target)
    {
        edges.emplace_back(label, target);
    }
};

class Graph
{
private:
    std::vector<Node> nodes;
#ifdef CREATE_REVERSE_INDEX
    std::vector<Node> reverse;
#endif
    Graph(Graph &)
    {
    }

public:
    Graph()
    {
    }
    node_index add_vertex()
    {
        nodes.emplace_back();
        return nodes.size() - 1;
    }
    void resize(node_index vertex_count)
    {
        nodes.resize(vertex_count);
    }

    std::vector<Node>& get_nodes()
    {
        std::vector<Node> & nodes_ref = nodes;
        return nodes_ref;
    }
    inline node_index size()
    {
        return nodes.size();
    }
#ifdef CREATE_REVERSE_INDEX

    void compute_reverse_index()
    {
        if (!this->reverse.empty())
        {
            throw MyException("computing the reverse while this has been computed before. Probably a programming error");
        }
        size_t number_of_nodes = this->nodes.size();
        for (size_t node = 0;  node < number_of_nodes; node++)
        {
            reverse.emplace_back();
        }
        for (size_t node = 0;  node < number_of_nodes; node++)
        {
            for (auto edge: nodes[node].get_outgoing_edges())
            {
                reverse[edge.target].add_edge(edge.label, node);
            }
        }
    }

    std::vector<Node>& get_reverse_nodes()
    {
        std::vector<Node> & reverse_nodes_ref = reverse;
        return reverse_nodes_ref;
    }
#endif
};

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

void read_graph_from_stream_timed(std::istream &inputstream, Graph &g)
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
        // node_index subject_index = node_ID_Mapper.getID(parts[0]);
        node_index subject_index = read_uint_ENTITY_little_endian(inputstream);

        // edge
        // edge_type edge_label = edge_ID_Mapper.getID(parts[1]);
        edge_type edge_label = read_PREDICATE_little_endian(inputstream);

        // object
        // node_index object_index = node_ID_Mapper.getID(parts[2]);
        node_index object_index = read_uint_ENTITY_little_endian(inputstream);

        // Break when the last valid values have been read
        if (inputstream.eof())
        {
            break;
        }
        // std::cout << subject_index << " " << edge_label <<  " " << object_index << std::endl;

        // Add Nodes
        // while (subject_index >= g.get_nodes().size())
        // {
        //     g.add_vertex();
        // }
        // while (object_index >= g.get_nodes().size())
        // {
        //     g.add_vertex();
        // }
        node_index largest = std::max(subject_index, object_index);
        if (largest >= g.size())
        {
            g.resize(largest + 1);
        }

        g.get_nodes()[subject_index].add_edge(edge_label, object_index);
        // also add reverse
        // g.get_nodes()[object_index].add_edge(edge_label, subject_index);

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
              << " Time taken for reading = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_reading_done - t_start).count()
              << " ms, memory = " << w.get_times()[0].memory_in_kb << " kB" << std::endl;
#ifdef CREATE_REVERSE_INDEX
    w.start_step("Creating reverse index");
    g.compute_reverse_index();
    w.stop_step();

    auto t_reverse_index_done{boost::chrono::system_clock::now()};
    auto time_t_reverse_index_done{boost::chrono::system_clock::to_time_t(t_reverse_index_done)};
    std::tm *ptm_reverse_index_done{std::localtime(&time_t_reverse_index_done)};

    std::cout << std::put_time(ptm_reverse_index_done, "%Y/%m/%d %H:%M:%S")
              << " Time taken for creating reverse index = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_reverse_index_done - t_reading_done).count()
              << " ms, memory = " << w.get_times()[1].memory_in_kb << " kB" << std::endl;
#endif
}

void read_graph_timed(const std::string &filename, Graph &g)
{

    std::ifstream infile(filename, std::ifstream::in);
    read_graph_from_stream_timed(infile, g);
}

class SummaryObjectSet
{
    private:
    boost::unordered_flat_set<block_or_singleton_index> objects;

    public:
    SummaryObjectSet()
    {
    }
    void add_object(block_or_singleton_index object)
    {
        objects.emplace(object);  // Should the be emplace or insert?
    }
    boost::unordered_flat_set<block_or_singleton_index>& get_objects()
    {
        return objects;
    }
    void remove_object(block_or_singleton_index object)
    {
        objects.erase(object);
    }
};


class SummaryNode
{
    private:
    boost::unordered_flat_map<edge_type, SummaryObjectSet> node;

    public:
    SummaryNode()
    {
    }
    boost::unordered_flat_map<edge_type, SummaryObjectSet>& get_edges()
    {
        return node;
    }
    size_t count_edge_key(edge_type edge)
    {
        return this->get_edges().count(edge);
    }
    void add_edge(edge_type edge, block_or_singleton_index object)
    {
        if (node.count(edge) == 0)
        {
            SummaryObjectSet empty_object_set = SummaryObjectSet();
            this->get_edges()[edge] = empty_object_set;
        }
        this->get_edges()[edge].add_object(object);
    }
    void remove_edge_recursive(edge_type edge, block_or_singleton_index object)
    {
        if (this->get_edges()[edge].get_objects().size() > 1)
        {
            this->get_edges()[edge].remove_object(object);  // Remove just the object if we have more triples with the same edge type
        }
        else
        {
            this->get_edges().erase(edge);  // If there was just one triple with the given edge type, remove the whole edge type from the map
        }
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

class SummaryGraph
{
private:
    boost::unordered_flat_map<block_or_singleton_index, SummaryNode> block_nodes;
    boost::unordered_flat_map<block_or_singleton_index, SummaryNode> reverse_block_nodes;

    SummaryGraph(SummaryGraph &)
    {
    }

public:
    SummaryGraph()
    {
    }
    boost::unordered_flat_map<block_or_singleton_index, SummaryNode>& get_nodes()
    {
        return block_nodes;
    }
    boost::unordered_flat_map<block_or_singleton_index, SummaryNode>& get_reverse_index()
    {
        return reverse_block_nodes;
    }
    void add_block_node(block_or_singleton_index block_node)
    {
        assert(this->get_nodes().count(block_node) == 0);  // The node should not already exist
        SummaryNode empty_node = SummaryNode();
        this->get_nodes()[block_node] = empty_node;
    }
    void add_reverse_block_node(block_or_singleton_index block_node)
    {
        assert(reverse_block_nodes.count(block_node) == 0);  // The node should not already exist
        SummaryNode empty_node = SummaryNode();
        reverse_block_nodes[block_node] = empty_node;
    }
    void add_edge_to_node(block_or_singleton_index subject, edge_type predicate, block_or_singleton_index object, bool add_reverse=true)
    {
        assert(this->get_nodes().count(subject) > 0);  // The node should exist
        this->get_nodes()[subject].add_edge(predicate, object);
        if (add_reverse)
        {
            if (reverse_block_nodes.count(object) == 0)
            {
                add_reverse_block_node(object);
            }
            this->get_reverse_index()[object].add_edge(predicate, subject);
        }
    }
    std::vector<Triple> remove_split_blocks_edges(boost::unordered_flat_set<block_index> split_blocks)
    {
        std::vector<Triple> removed_edges;
        // Remove the forward edges from the index and reverse index
        for (block_or_singleton_index subject: split_blocks)
        {
            for (auto& block_node_key_val: this->get_nodes()[subject].get_edges())
            {
                edge_type predicate = block_node_key_val.first;
                for (block_or_singleton_index object: block_node_key_val.second.get_objects())
                {
                    reverse_block_nodes[object].remove_edge_recursive(predicate, subject);
                    removed_edges.emplace_back(Triple(subject, predicate, object));  // Keep track of the edges we have removed
                }
            }
            this->get_nodes()[subject] = SummaryNode();  // After having removed the forward edges from the reverse index, clear the edges for the index
        }

        // Remove the backward edges from the index and reverse index
        for (block_or_singleton_index subject: split_blocks)
        {
            for (auto& reverse_block_node_key_val: reverse_block_nodes[subject].get_edges())
            {
                edge_type predicate = reverse_block_node_key_val.first;
                for (block_or_singleton_index object: reverse_block_node_key_val.second.get_objects())
                {
                    this->get_nodes()[object].remove_edge_recursive(predicate, subject);
                    removed_edges.emplace_back(Triple(subject, predicate, object));  // Keep track of the edges we have removed
                }
            }
            reverse_block_nodes[subject] = SummaryNode();  // After having removed the backward edges from the index, clear the edges for the reverse index
        }
        return removed_edges;
    }
    void write_graph_to_file_binary(std::ostream &outputstream)
    {
        for (auto node_key_val: this->get_nodes())
        {
            block_or_singleton_index subject = node_key_val.first;
            for (auto edge_key_val: node_key_val.second.get_edges())
            {
                edge_type predicate = edge_key_val.first;
                for (block_or_singleton_index object: edge_key_val.second.get_objects())
                {
                    // std::cout << "DEBUG spo: " << subject << " " << predicate << " " << object << std::endl;
                    write_int_BLOCK_OR_SINGLETON_little_endian(outputstream, subject);
                    write_uint_PREDICATE_little_endian(outputstream, predicate);
                    write_int_BLOCK_OR_SINGLETON_little_endian(outputstream, object);
                }
            }
        }
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

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("experiment_directory", po::value<std::string>(), "The directory for the experiment of interest");
    // global.add_options()("mode", po::value<std::string>(), "Whether to output only condensed results or uncondensed results or both.");
    // global.add_options()("mapping_file", po::value<std::string>(), "Mapping file");

    po::positional_options_description pos;
    pos.add("experiment_directory", 1);
    // pos.add("blocks_file", 2);
    // pos.add("mapping_file", 3);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string experiment_directory = vm["experiment_directory"].as<std::string>();
    // std::string blocks_file = vm["blocks_file"].as<std::string>();
    // std::string mapping_file = vm["mapping_file"].as<std::string>();
    std::string graph_file = experiment_directory + "binary_encoding.bin";

    std::string graph_stats_file = experiment_directory + "ad_hoc_results/graph_stats.json";
    std::ifstream graph_stats_file_stream(graph_stats_file);

    std::string graph_stats_line;
    std::string final_depth_string = "\"Final depth\"";
    size_t k;
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
            break;
        }
    }
    graph_stats_file_stream.close();
    // std::cout << "DEBUG " << k << std::endl;

    // std::vector<std::string> outcome_files;
    // std::vector<std::string> mapping_files;

    std::string summary_graph_file_path_base = experiment_directory + "bisimulation/summary_graph-";

    // for (const auto& entry : std::filesystem::directory_iterator(experiment_directory + "bisimulation/")) {
    //     std::string file_string = entry.path().filename().string();
    //     if (std::regex_match(file_string, outcome_file_regex_pattern))
    //     {
    //         outcome_files.emplace_back(file_string);
    //     }
    //     else if (std::regex_match(file_string, mapping_file_regex_pattern))
    //     {
    //         mapping_files.emplace_back(file_string);
    //     }
    // }
    // std::sort(outcome_files.begin(), outcome_files.end());
    // std::sort(mapping_files.begin(), mapping_files.end());

    std::string blocks_file = experiment_directory + "bisimulation/outcome_condensed-0001.bin";
    std::string summary_graph_file_path_binary = summary_graph_file_path_base + "0001.bin";
    // std::string summary_graph_file_path_json = summary_graph_file_path_base + "0001.json";
    // std::string condensed_summary_graph_file_path = experiment_directory + "bisimulation/summary_graph_condensed.bin";

    std::ofstream summary_graph_file_binary(summary_graph_file_path_binary, std::ios::trunc | std::ofstream::out);
    // std::ofstream summary_graph_file_json(summary_graph_file_path_json, std::ios::trunc | std::ofstream::out);

    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();
    Graph g;
    w.start_step("Read graph", true);  // Set newline to true
    read_graph_timed(graph_file, g);
    w.stop_step();

    SummaryGraph gs;

    std::ifstream blocksfile(blocks_file, std::ifstream::in);
    boost::unordered_flat_map<node_index, block_or_singleton_index> node_to_block_map;
    boost::unordered_flat_map<block_index, boost::unordered_flat_set<node_index>> blocks;

    // Initialize all nodes as being singletons
    for (node_index node = 0; node < g.size(); node++)
    {
        // std::cout << "DEBUG singleton initialization: " << (-block_or_singleton_index(node) - 1) << std::endl;
        node_to_block_map[node] = -block_or_singleton_index(node) - 1;
    }

    // boost::unordered_flat_set<block_or_singleton_index> new_blocks;

    auto t_reverse_index_done{boost::chrono::system_clock::now()};
    auto time_t_reverse_index_done{boost::chrono::system_clock::to_time_t(t_reverse_index_done)};
    std::tm *ptm_reverse_index_done{std::localtime(&time_t_reverse_index_done)};

    std::cout << std::put_time(ptm_reverse_index_done, "%Y/%m/%d %H:%M:%S") << " Processing k=1" << std::endl;
    
    // Read the first outcome file
    while (true)
    {
        block_index block = read_uint_BLOCK_little_endian(blocksfile);
        // new_blocks.emplace(block);
        if (blocksfile.eof())
        {
            break;
        }
        // std::cout << "DEBUG block: " << block <<std::endl;
        assert(block <= MAX_SIGNED_BLOCK_SIZE);  // Later, we are storing a block_index as a block_or_singleton_index, so we need to check if the cast is possible
        gs.add_block_node(block);
        u_int64_t block_size = read_uint_ENTITY_little_endian(blocksfile);
        // std::cout << "DEBUG block size: " << block_size <<std::endl;
        for (uint64_t i = 0; i < block_size; i++) {
            node_index node = read_uint_ENTITY_little_endian(blocksfile);
            // std::cout << "DEBUG node: " << node <<std::endl;
            node_to_block_map[node] = (block_or_singleton_index) block;
            blocks[block].emplace(node);
        }
    }

    // Add the respective edges to the summary graph
    for (node_index node = 0; node < g.get_nodes().size(); node++)
    {
        auto subject_node_iterator = node_to_block_map.find(node);
        assert(subject_node_iterator != node_to_block_map.end());
        block_or_singleton_index subject_block;
        subject_block = (*subject_node_iterator).second;
        // std::cout << "DEBUG normal subject block: " << subject_block << std::endl;
        // else
        // {
        //     // We want to make sure the node can be turned into a negative 64 bit signed integer
        //     // We want to know: -node-1 >= INT64_MIN
        //     // This expression is equal to: node <= MAX_SIGNED_BLOCK_SIZE
        //     assert(node <= MAX_SIGNED_BLOCK_SIZE);
        //     subject_block = -((block_or_singleton_index) node)-1;  // We assign singleton blocks a uniqe negative number
        //     std::cout << "DEBUG subject added: " << subject_block << std::endl;
        //     gs.add_block_node(subject_block);  // Singleton blocks still need to be added to the graph
        // }
        if (subject_block < 0) {
            gs.add_block_node(subject_block);  // We still need to add the singleton blocks to the graph
        }
        for (Edge edge: g.get_nodes()[node].get_outgoing_edges())
        {
            edge_type predicate = edge.label;
            auto object_node_iterator = node_to_block_map.find(edge.target);
            block_or_singleton_index object_block;
            if (object_node_iterator != node_to_block_map.end())
            {
                object_block = (*object_node_iterator).second;
            }
            else
            {
                // We want to make sure the node can be turned into a negative 64 bit signed integer
                // We want to know: -edge.target-1 >= INT64_MIN
                // This expression is equal to: edge.target <= MAX_SIGNED_BLOCK_SIZE
                assert(edge.target <= MAX_SIGNED_BLOCK_SIZE);  // Make sure the conversion to block_or_singleton_index is possible
                object_block = (block_or_singleton_index) -((block_or_singleton_index) edge.target)-1;  // We assign singleton blocks a uniqe negative number
            }
            // std::cout << "DEBUG subject block to add: " << subject_block << std::endl;
            gs.add_edge_to_node(subject_block, predicate, object_block, true);
        }
    }
    gs.write_graph_to_file_binary(summary_graph_file_binary);
    // gs.write_graph_to_file_json(summary_graph_file_json);
    summary_graph_file_binary.close();
    // summary_graph_file_json.close();
    // std::ifstream summary_graph_file_input(summary_graph_file_path, std::ifstream::in);
    // const int BufferSize = 8 * 16184;
    // char _buffer[BufferSize];
    // summary_graph_file_input.rdbuf()->pubsetbuf(_buffer, BufferSize);
    // while (true)
    // {
    //     // subject
    //     block_or_singleton_index subject_index = read_int_BLOCK_OR_SINGLETON_little_endian(summary_graph_file_input);

    //     // edge
    //     edge_type edge_label = read_PREDICATE_little_endian(summary_graph_file_input);

    //     // object
    //     block_or_singleton_index object_index = read_int_BLOCK_OR_SINGLETON_little_endian(summary_graph_file_input);

    //     // Break when the last valid values have been read
    //     if (summary_graph_file_input.eof())
    //     {
    //         break;
    //     }
    //     std::cout << "DEBUG spo (g2): " << subject_index << " " << edge_label << " " << object_index << std::endl;
    // }
    for (uint32_t i = 2; i <= k; i++)  // We can ignore the last outcome, because its only purpose was to find the fixed point, otherwise include the last outcome
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

        // std::ostringstream k_next_stringstream;
        // k_next_stringstream << std::setw(4) << std::setfill('0') << i+2;
        // std::string k_next_string(k_next_stringstream.str());

        std::string summary_graph_file_path = summary_graph_file_path_base + i_string + ".bin";
        std::ofstream summary_graph_file_binary(summary_graph_file_path, std::ios::trunc | std::ofstream::out);
        // std::ofstream summary_graph_file_json(summary_graph_file_path_base + i_string + ".json", std::ios::trunc | std::ofstream::out);

        boost::unordered_flat_set<block_index> split_block_incides;
        boost::unordered_flat_set<block_index> new_block_indices;

        bool new_singletons_created = false;
        // std::cout << "\nDEBUG New k" << std::endl;
        // std::cout << "DEBUG new mapping: " << current_mapping << std::endl;

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
                // std::cout << "DEBUG old --> new: " << old_block << " " << new_block << std::endl;
                if (new_block == 0)
                {
                    new_singletons_created = true;
                }
                else
                {
                    new_block_indices.emplace(new_block);
                }

                // std::cout << "DEBUG " << old_block << " to " << new_block << std::endl;
            }
            // std::cout << "DEBUG\n" << std::endl;
        }

        boost::unordered_flat_set<node_index> new_singleton_nodes;
        boost::unordered_flat_set<node_index> old_nodes_in_split;
        boost::unordered_flat_set<node_index> new_nodes_in_split;

        gs.remove_split_blocks_edges(split_block_incides);
        // std::vector<Triple> removed_edges = gs.remove_split_blocks_edges(split_block_incides);
        if (new_singletons_created)
        {
            for (block_or_singleton_index split_block: split_block_incides)
            {
                old_nodes_in_split.insert(blocks[(block_index) split_block].begin(), blocks[split_block].end());
            }
        }

        // boost::unordered_flat_set<block_or_singleton_index> new_blocks;

        // Read an outcome file
        while (true)
        {
            block_index block = read_uint_BLOCK_little_endian(current_outcome_file);
            // new_blocks.emplace(block);
            if (current_outcome_file.eof())
            {
                break;
            }
            // std::cout << "DEBUG block: " << block <<std::endl;
            assert(block <= MAX_SIGNED_BLOCK_SIZE);  // Later, we are storing a block_index as a block_or_singleton_index, so we need to check if the cast is possible
            if (gs.get_nodes().count(block) == 0)
            {
                gs.add_block_node(block);
            }
            u_int64_t block_size = read_uint_ENTITY_little_endian(current_outcome_file);
            // std::cout << "DEBUG block size: " << block_size <<std::endl;
            blocks[(block_or_singleton_index) block].clear();  // Remove the old map
            for (uint64_t i = 0; i < block_size; i++) {
                node_index node = read_uint_ENTITY_little_endian(current_outcome_file);
                // std::cout << "DEBUG node: " << node <<std::endl;
                node_to_block_map[node] = (block_or_singleton_index) block;
                blocks[(block_or_singleton_index) block].emplace(node);
            }
        }

        if (new_singletons_created)
        {
            for (block_or_singleton_index new_block: new_block_indices)
            {
                // std::cout << "DEBUG split block: " << new_block << std::endl;
                new_nodes_in_split.insert(blocks[new_block].begin(), blocks[new_block].end());
            }
            // for (auto node: old_nodes_in_split)
            // {
            //     std::cout << "DEBUG old node: " << node << std::endl;
            // }
            // for (auto node: new_nodes_in_split)
            // {
            //     std::cout << "DEBUG new node: " << node << std::endl;
            // }
            for (auto node: new_nodes_in_split)
            {
                old_nodes_in_split.erase(node);
            }
            // for (auto node: old_nodes_in_split)
            // {
            //     std::cout << "DEBUG old node: " << node << std::endl;
            // }
            // for (auto node: new_singleton_nodes)
            // {
            //     std::cout << "DEBUG singleton node: " << node << std::endl;
            // }
            // std::cout << "DEBUG perform move" << std::endl;
            new_singleton_nodes = std::move(old_nodes_in_split);
            // for (auto node: old_nodes_in_split)
            // {
            //     std::cout << "DEBUG old node: " << node << std::endl;
            // }
            // for (auto node: new_singleton_nodes)
            // {
            //     std::cout << "DEBUG singleton node: " << node << "\n" << std::endl;
            // }
            for (node_index node: new_singleton_nodes)
            {
                block_or_singleton_index singleton_block = -((block_or_singleton_index)node)-1;
                gs.add_block_node(singleton_block);
                node_to_block_map[node] = singleton_block;
            }
        }
        // For all non-singleton blocks add the new edges to the summary graph
        for (block_index block: new_block_indices)
        {
            if (block == 0)
            {
                // 0 is a placeholder for singletons, we will add them later
                continue;
            }
            for (node_index node: blocks[block])
            {
                for (auto edge: g.get_nodes()[node].get_outgoing_edges())
                {
                    block_or_singleton_index subject = node_to_block_map[node];
                    block_or_singleton_index object = node_to_block_map[edge.target];
                    gs.add_edge_to_node(subject, edge.label, object, true);
                }
                for (auto edge: g.get_reverse_nodes()[node].get_outgoing_edges())
                {
                    block_or_singleton_index object = node_to_block_map[node];
                    block_or_singleton_index subject = node_to_block_map[edge.target];
                    gs.add_edge_to_node(subject, edge.label, object, true);
                }
            }
        }
        for (node_index node: new_singleton_nodes)
        {
            for (auto edge: g.get_nodes()[node].get_outgoing_edges())
            {
                block_or_singleton_index subject = node_to_block_map[node];
                block_or_singleton_index object = node_to_block_map[edge.target];
                gs.add_edge_to_node(subject, edge.label, object, true);
            }
            for (auto edge: g.get_reverse_nodes()[node].get_outgoing_edges())
            {
                block_or_singleton_index object = node_to_block_map[node];
                block_or_singleton_index subject = node_to_block_map[edge.target];
                gs.add_edge_to_node(subject, edge.label, object, true);
            }
        }
        gs.write_graph_to_file_binary(summary_graph_file_binary);
        // gs.write_graph_to_file_json(summary_graph_file_json);
    }
}