#include <cstdint>
#include <vector>
#include <map>
#include <stack>
// #include <span>
#include <fstream>
#include <string>
// #define BOOST_USE_VALGRIND  // TODO disable this command in the final running version
#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <sstream>
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <thread>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/find.hpp>

using edge_type = uint32_t;
using node_index = uint64_t;
using block_index = node_index;
using block_or_singleton_index = int64_t;
using set_of_types = boost::unordered_flat_set<edge_type>;

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;
const int BYTES_PER_BLOCK = 4;

const int64_t MAX_SIGNED_BLOCK_SIZE = INT64_MAX;


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
    std::vector<std::vector<node_index>> reverse;

    void compute_reverse_index()
    {
        if (!this->reverse.empty())
        {
            throw MyException("computing the reverse while this has been computed before. Probably a programming error");
        }
        size_t number_of_nodes = this->nodes.size();
        // we create it first with sets to remove duplicates
        std::vector<boost::unordered_flat_set<node_index>> unique_index(number_of_nodes);
        for (node_index sourceID = 0; sourceID < number_of_nodes; sourceID++)
        {
            Node &node = this->nodes[sourceID];
            for (const Edge edge : node.get_outgoing_edges())
            {
                node_index targetID = edge.target;
                unique_index[targetID].insert(sourceID);
            }
        }
        // now convert to the final index
        this->reverse.resize(number_of_nodes);
        for (node_index targetID = 0; targetID < number_of_nodes; targetID++)
        {
            for (const node_index &sourceID : unique_index[targetID])
            {
                this->reverse[targetID].push_back(sourceID);
            }
            // minimize memory usage
            this->reverse[targetID].shrink_to_fit();
        }
    }
#endif
};

template <typename T>
class IDMapper
{
    boost::unordered_flat_map<std::string, T> mapping;

public:
    IDMapper() : mapping(10000000)
    {
    }

    T getID(std::string &stringID)
    {
        T potentially_new = mapping.size();
        // std::pair<boost::unordered_flat_map<std::string, T>::const_iterator, bool>
        auto result = mapping.try_emplace(stringID, potentially_new);
        T the_id = (*result.first).second;
        return the_id;
    }

    // template <class Stream>
    void dump(std::ostream &out)
    {
        for (auto a = this->mapping.cbegin(); a != this->mapping.cend(); a++)
        {
            std::string str(a->first);
            T id = a->second;
            out << str << " " << id << '\n';
        }
        out.flush();
    }

    void dump_to_file(const std::string &filename)
    {
        std::ofstream mapping_out(filename, std::ios::trunc);
        if (!mapping_out.is_open())
        {
            throw MyException("Opening the file to dump to failed");
        }
        this->dump(mapping_out);
        mapping_out.close();
    }
};

void write_uint_BLOCK_little_endian(std::ostream &outputstream, int64_t value)
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

void write_uint_ENTITY_little_endian(std::ostream &outputstream, int64_t value)
{
    char data[BYTES_PER_ENTITY];
    for (unsigned int i = 0; i < BYTES_PER_ENTITY; i++)
    {
        data[i] = char(value & 0x00000000000000FFull);
        value = value >> 8;
    }
    outputstream.write(data, BYTES_PER_ENTITY);
    if (outputstream.fail())
    {
        std::cout << "Write entity failed with code: " << outputstream.rdstate() << std::endl;
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
        result |= (uint64_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data
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
        result |= (uint32_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data
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

u_int64_t read_graph_from_stream_timed(std::istream &inputstream, Graph &g)
{
    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();

    const int BufferSize = 8 * 16184;

    char _buffer[BufferSize];

    inputstream.rdbuf()->pubsetbuf(_buffer, BufferSize);

    w.start_step("Reading graph");
    u_int64_t edge_count = 0;

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

        if (edge_count % 1000000 == 0)
        {
            auto now{boost::chrono::system_clock::to_time_t(boost::chrono::system_clock::now())};
            std::tm *ptm{std::localtime(&now)};
            std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S") << " done with " << edge_count << " triples" << std::endl;
        }
        edge_count++;
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
    return edge_count;
}

u_int64_t read_graph_timed(const std::string &filename, Graph &g)
{

    std::ifstream infile(filename, std::ifstream::in);
    u_int64_t edge_count = read_graph_from_stream_timed(infile, g);
    return edge_count;
}

using Block = std::vector<node_index>;

using BlockPtr = std::shared_ptr<Block>;

// this is isolated because it might be faster to use a boost::dynamic_bitset<>
class DirtyBlockContainer
{
private:
    boost::unordered_flat_set<block_index> blocks;

public:
    DirtyBlockContainer() {}
    void clear()
    {
        this->blocks.clear();
        // deallocate the underlying memory as well
        this->blocks.rehash(0);
    }
    void set_dirty(block_index index)
    {
        blocks.emplace(index);
    }

    boost::unordered_flat_set<block_index>::const_iterator cbegin() const
    {
        return this->blocks.cbegin();
    }

    boost::unordered_flat_set<block_index>::const_iterator cend() const
    {
        return this->blocks.cend();
    }
};

class MappingNode2BlockMapper; // forward declaration

class Node2BlockMapper
{ // interface
public:
    virtual ~Node2BlockMapper()
    { /* releases Base's resources */
    }
    /**
     * get_block returns a positive numebr only in case the block really exists.
     * If it is a singleton, a (singleton specific) negative number will be returned.
     */
    virtual int64_t get_block(node_index) = 0;
    virtual void clear() = 0;
    virtual node_index singleton_count() = 0;
    virtual std::shared_ptr<MappingNode2BlockMapper> modifyable_copy() = 0;
    virtual size_t freeblock_count() = 0;
};

class AllToZeroNode2BlockMapper : public Node2BlockMapper
{
private:
    // The highest node index (exclusive)
    node_index max_node_index;

    AllToZeroNode2BlockMapper(AllToZeroNode2BlockMapper &) {} // no copies
public:
    /**
     * max_node_index is exclusive
     */
    AllToZeroNode2BlockMapper(node_index max_node_index) : max_node_index(max_node_index)
    {
        if (max_node_index < 2)
        {
            throw MyException("The graph has only one, or zero nodes, breaking the precondition for using the AllToZeroNode2BlockMapper. It assumes there will not be any singletons.");
        }
    }
    int64_t get_block(node_index n_index) override
    {
        if (n_index >= this->max_node_index)
        {
            throw MyException("requested an index higher than the max_node_index");
        }
        return 0;
    }
    void clear() override
    {
        // do nothing
    }
    std::shared_ptr<MappingNode2BlockMapper> modifyable_copy() override
    {
        std::vector<int64_t> node_to_block(this->max_node_index, 0);
        node_to_block.shrink_to_fit();
        std::stack<std::size_t> emptystack;
        return std::make_shared<MappingNode2BlockMapper>(node_to_block, emptystack, 0);
    }
    node_index singleton_count()
    {
        return 0;
    }
    size_t freeblock_count() override
    {
        return 0;
    }
};

class MappingNode2BlockMapper : public Node2BlockMapper
{
private:
    std::vector<int64_t> node_to_block;
    uint64_t singleton_counter;
    MappingNode2BlockMapper(MappingNode2BlockMapper &) {} // No copies

public:
    MappingNode2BlockMapper(std::vector<int64_t> &node_to_block, std::stack<std::size_t> &freeblock_indices, uint64_t singleton_count) : node_to_block(node_to_block), singleton_counter(singleton_count), freeblock_indices(freeblock_indices) {}

    std::stack<block_index> freeblock_indices;

    size_t freeblock_count() override
    {
        return this->freeblock_indices.size();
    }
    node_index singleton_count()
    {
        return this->singleton_counter;
    }

    void overwrite_mapping(node_index n_index, block_index b_index)
    {
        this->node_to_block.at(n_index) = b_index;
    }

    int64_t get_block(node_index n_index) override
    {
        return this->node_to_block.at(n_index);
    }

    void clear() override
    {
        this->node_to_block.clear();
        this->node_to_block.reserve(0);
    }

    std::shared_ptr<MappingNode2BlockMapper> modifyable_copy() override
    {
        std::vector<int64_t> new_node_to_block(this->node_to_block);
        std::stack<block_index> new_freeblock_indices(this->freeblock_indices);
        return std::make_shared<MappingNode2BlockMapper>(new_node_to_block, new_freeblock_indices, this->singleton_counter);
    }

    /**
     * Modifies this mapping. Changes the specified node into a singleton. Future calls to get_block for this node will return a negative number
     */
    void put_into_singleton(node_index node)
    {
        if (node_to_block[node] < 0)
        {
            throw MyException("Tried to create a singleton from a node which already was a singleton. This is nearly certainly a mistake in the code.");
        }
        this->singleton_counter++;
        assert(node <= MAX_SIGNED_BLOCK_SIZE);
        this->node_to_block[node] = -((block_or_singleton_index) node)-1;
    }
};

class Refines_Edge
{
public:
    block_index original_block;
    std::vector<block_index> split_blocks;

    Refines_Edge(block_index original_block, std::vector<block_index> split_blocks)
    {
        this->original_block = original_block;
        this->split_blocks = split_blocks;
    }
};

class Refines_Mapping
{
public:
    std::map<block_index, std::vector<block_index>> refines_edges;

    Refines_Mapping()
    {
    }

    void add_edge(Refines_Edge edge)
    {
        this->refines_edges[edge.original_block] = edge.split_blocks;
    }
};

class KBisumulationOutcome
{
public:
    const std::vector<BlockPtr> blocks;
    DirtyBlockContainer dirty_blocks;
    // If the block for the node is not a singleton, this contains the block index.
    // Otherwise, this will contain a negative number unique for that singleton
    std::shared_ptr<Node2BlockMapper> node_to_block; // can most probably also be an auto_ptr, I don't think these will be shared, but overhead is minimal
    Refines_Mapping k_minus_one_to_k_mapping;

public:
    KBisumulationOutcome(const std::vector<BlockPtr> &blocks,
                         const DirtyBlockContainer &dirty_blocks,
                         const std::shared_ptr<Node2BlockMapper> &node_to_block) : blocks(blocks),
                                                                                   dirty_blocks(dirty_blocks),
                                                                                   node_to_block(node_to_block)
    {
    }

    void clear_indices()
    {
        this->node_to_block->clear();
        this->dirty_blocks.clear();
    }

    int64_t get_block_ID_for_node(const node_index &node) const
    {
        return this->node_to_block->get_block(node);
    }

    int64_t singleton_block_count()
    {
        return this->node_to_block->singleton_count();
    }

    std::size_t non_singleton_block_count()
    {
        std::size_t blocks_allocated = this->blocks.size();
        std::size_t unused_blocks = this->node_to_block->freeblock_count();
        return blocks_allocated - unused_blocks;
    }

    std::size_t total_blocks()
    {
        return this->singleton_block_count() + this->non_singleton_block_count();
    }

    void add_mapping(Refines_Mapping mapping)
    {
        this->k_minus_one_to_k_mapping = mapping;
    }
};

KBisumulationOutcome get_0_bisimulation(Graph &g)
{

    std::vector<BlockPtr> new_blocks;

    BlockPtr block = std::make_shared<Block>();

    std::size_t amount = g.get_nodes().size();
    block->reserve(amount);
    for (unsigned int i = 0; i < amount; i++)
    {
        block->emplace_back(i);
    }
    new_blocks.emplace_back(block);

    std::shared_ptr<AllToZeroNode2BlockMapper> node_to_block = std::make_shared<AllToZeroNode2BlockMapper>(g.size());

    DirtyBlockContainer dirty;
    dirty.set_dirty(0);

    KBisumulationOutcome result(new_blocks, dirty, node_to_block);
    return result;
}
static BlockPtr global_empty_block = std::make_shared<Block>();

KBisumulationOutcome get_typed_0_bisimulation(Graph &g)
{
    // collect the signatures for nodes in the block
    boost::unordered_flat_map<set_of_types, BlockPtr> partition_map;
    auto nodes = g.get_nodes();

    for (uint64_t i = 0; i < nodes.size(); i++ ){
        set_of_types set_of_types_of_node;
        Node & node = nodes[i];
        for (auto edge : node.get_outgoing_edges()){
            if (edge.label == 0){ // assumes rdf:type is mapped on 0!!!
                set_of_types_of_node.emplace(edge.target);
            }
        }

        node_index i_as_node_index = i;
        auto partition_map_iterator = partition_map.find(set_of_types_of_node);
        if (partition_map_iterator == partition_map.cend())
        {
            BlockPtr block = std::make_shared<Block>();
            partition_map[set_of_types_of_node] = block;
            partition_map_iterator = partition_map.find(set_of_types_of_node);
        }
        auto the_set = partition_map_iterator->first;
        auto the_block = partition_map_iterator->second;
        the_block->emplace_back(i_as_node_index);
    }

    std::vector<int64_t> new_node_to_block;
    new_node_to_block.resize(g.size());
    std::vector<BlockPtr> new_blocks;
    new_blocks.reserve(partition_map.size());

    int64_t singleton_counter = 0;

    DirtyBlockContainer dirty;

    for (auto part : partition_map){
        auto & block = part.second;
        if ((*block).size() == 1){
            // no block, add singleton
            singleton_counter++;
            int singleton_index = -singleton_counter;
            new_node_to_block[(*block)[0]] = singleton_index;
        } else {
            //add the block
            new_blocks.emplace_back(block);
            int new_block_index = new_blocks.size() - 1;
            for(auto node: *block){
                new_node_to_block[node] = new_block_index;
            }
            dirty.set_dirty(new_block_index);
        }
    }
    new_node_to_block.shrink_to_fit();

    std::stack<block_index> new_freeblock_indices; // empty
    auto mapper = std::make_shared<MappingNode2BlockMapper>(new_node_to_block, new_freeblock_indices, singleton_counter);

    KBisumulationOutcome result(new_blocks, dirty, mapper);

    return result;
}

KBisumulationOutcome get_k_bisimulation(Graph &g, const KBisumulationOutcome &k_minus_one_outcome, std::size_t min_support = 1)
{
    // we make copies which we will modify
    std::vector<BlockPtr> k_blocks(k_minus_one_outcome.blocks);
    std::shared_ptr<MappingNode2BlockMapper> k_node_to_block = k_minus_one_outcome.node_to_block->modifyable_copy();

    // We collect all nodes from split blocks. In the end we mark all blocks which target these as dirty.
    boost::unordered_flat_set<node_index> nodes_from_split_blocks;

    // Define a mapping in which we can store the refines edges
    Refines_Mapping refines_edges;

    // we first do dirty blocks of size 2 because if they split, they cause two singletons and a gap (freeblock) in the list of blocks
    // These freeblocks can be filled if larger blocks are split.

    if (min_support < 2)
    {
        for (auto iter = k_minus_one_outcome.dirty_blocks.cbegin(); iter != k_minus_one_outcome.dirty_blocks.cend(); iter++)
        {
            block_index dirty_block_index = *iter;
            BlockPtr dirty_block = k_minus_one_outcome.blocks[dirty_block_index];
            size_t dirty_block_size = dirty_block->size();

            // define the vector to store the new block indices in
            std::vector<block_index> new_block_indices;

            if (dirty_block_size != 2)
            {
                // we deal with this below
                continue;
            }
            // else
            // we checked above that min_support < 2, so no need to check that here.

            // pair of edge type and target *block*, the block ID can be negative if it is a singleton
            using signature_t = boost::unordered_flat_set<std::pair<edge_type, int64_t>>; //[tuple[HashableEdgeLabel, int]]
            // collect the signatures for nodes in the block
            boost::unordered_flat_map<signature_t, Block> M;
            for (auto v_iter = dirty_block->begin(); v_iter != dirty_block->end(); v_iter++)
            {
                node_index v = *v_iter;
                signature_t signature;
                for (Edge edge_info : g.get_nodes()[v].get_outgoing_edges())
                {
                    size_t to_block = k_minus_one_outcome.get_block_ID_for_node(edge_info.target);
                    signature.emplace(edge_info.label, to_block);
                }
                // try_emplace returns an iterator to a new element if there was nothing yet, otherwise to the existing one
                auto empl_res = M.try_emplace(signature);
                (*(empl_res.first)).second.emplace_back(v);
            }
            // if the block is not refined
            if (M.size() == 1)
            {
                // no need to update anything in the blocks, nor in the index
                continue;
            }
            // else form two singletons and mark the block as free
            for (auto &signature_blocks : M)
            {
                if (signature_blocks.second.size() != 1)
                {
                    throw MyException("invariant violation");
                }
                node_index the_node = *(signature_blocks.second.cbegin());
                k_node_to_block->put_into_singleton(the_node);
                nodes_from_split_blocks.emplace(the_node);
            }
            k_blocks[dirty_block_index] = global_empty_block;
            k_node_to_block->freeblock_indices.push(dirty_block_index);
            // Add the split to the refines edges. Here 0 is used to indicates some (in this case all) nodes became singletons
            new_block_indices.push_back(0);
            // Add 1 to the dirty block index to be consistent with the new_block_indeces
            refines_edges.add_edge(Refines_Edge(dirty_block_index+1, new_block_indices));
        }
    }

    // now we deal with larger blocks. When they split, we first attempt to fill the gaps created in the previous loop.
    // if there is no free space, we append the blocks.
    // in the meantime, we also maintain the k_node_to_block index.
    for (auto iter = k_minus_one_outcome.dirty_blocks.cbegin(); iter != k_minus_one_outcome.dirty_blocks.cend(); iter++)
    {
        block_index dirty_block_index = *iter;
        BlockPtr dirty_block = k_minus_one_outcome.blocks[dirty_block_index];
        size_t dirty_block_size = dirty_block->size();

        // define the vector to store the new block indices in
        std::vector<block_index> new_block_indices;

        if (dirty_block_size == 2 || dirty_block_size <= min_support)
        {
            // if it is 2, we dealt with it above.
            // if it is less than min_support, no need to update anything in the blocks, nor in the index
            continue;
        }
        // else

        // pair of edge type and target *block*, the block ID can be negative if it is a singleton
        using signature_t = boost::unordered_flat_set<std::pair<edge_type, int64_t>>; //[tuple[HashableEdgeLabel, int]]
        // collect the signatures for nodes in the block
        boost::unordered_flat_map<signature_t, Block> M;
        for (auto v_iter = dirty_block->begin(); v_iter != dirty_block->end(); v_iter++)
        {
            node_index v = *v_iter;
            signature_t signature;
            for (Edge edge_info : g.get_nodes()[v].get_outgoing_edges())
            {
                size_t to_block = k_minus_one_outcome.get_block_ID_for_node(edge_info.target);
                signature.emplace(edge_info.label, to_block);
            }
            // try_emplace returns an iterator to a new element if there was nothing yet, otherwise to the existing one
            auto empl_res = M.try_emplace(signature);
            (*(empl_res.first)).second.emplace_back(v);
        }
        // if the block is not refined
        if (M.size() == 1)
        {
            // no need to update anythign in the blocks, nor in the index
            continue;
        }
        // else

        // we first make sure all nodes are added to the nodes_from_split_blocks
        for (auto v_iter = dirty_block->begin(); v_iter != dirty_block->end(); v_iter++)
        {
            node_index v = *v_iter;
            nodes_from_split_blocks.emplace(v);
        }

        // We mark the current block_index as a free one, and set it to the empty one
        k_node_to_block->freeblock_indices.push(dirty_block_index);
        k_blocks[dirty_block_index] = global_empty_block;

        // all indices for this block will be overwritten, so no need to do this now

        // This bool is used to make sure we only add 0 at most once to the refines edges
        bool found_singleton = false;

        // categorize the blocks
        for (auto &signature_blocks : M)
        {
            // if singleton, make it a singleton in the mapping
            if (signature_blocks.second.size() == 1)
            {
                k_node_to_block->put_into_singleton(*(signature_blocks.second.cbegin()));
                if (!found_singleton)
                {
                    found_singleton = true;
                    // Add the split to the refines edges. Here 0 is used to indicates some nodes became singletons
                    new_block_indices.push_back(0);
                }
                continue;
            }
            // else

            BlockPtr block = std::make_shared<Block>(signature_blocks.second);
            block->shrink_to_fit();
            // if there are still known empty blocks, write on them
            block_index new_block_index;  // changed from std::size_t to block_index
            if (k_node_to_block->freeblock_indices.size() > 0)
            {
                new_block_index = k_node_to_block->freeblock_indices.top();
                k_node_to_block->freeblock_indices.pop();
                k_blocks[new_block_index] = block;
            }
            else
            {
                new_block_index = k_blocks.size();
                k_blocks.push_back(block);
            }
            // We add 1 since the index 0 is reserved for singletons
            new_block_indices.push_back(new_block_index+1);
            // we still need to update the k_node_to_block index
            if (new_block_index != dirty_block_index)
            { // if new_block_index == dirty_block_index, then it is already set
                for (auto node_iter = block->cbegin(); node_iter != block->cend(); node_iter++)
                {
                    node_index node_iter_index = *node_iter;
                    k_node_to_block->overwrite_mapping(node_iter_index, new_block_index);
                }
            }
        }
        // Add 1 to the dirty block index to be consistent with the new_block_indeces
        refines_edges.add_edge(Refines_Edge(dirty_block_index+1, new_block_indices));
    }

    // for (node_index i = 0; i < g.size(); i++)
    // {
    //     std::cout << "from " << k_minus_one_outcome.node_to_block->get_block(i) << " to " << k_node_to_block->get_block(i) << std::endl;
    // }

    // we are now done with splitting all blocks. Also the indices are up to date. Time to mark the dirty blocks
    DirtyBlockContainer dirty;
#ifdef CREATE_REVERSE_INDEX
    // start marking
    for (node_index target : nodes_from_split_blocks)
    {
        if (target > g.size() || target < 0)
        {
            throw MyException("impossible: target index goes beyond graph size");
        }
        for (node_index source : g.reverse.at(target))
        {
            if (source > g.size() || target < 0)
            {
                throw MyException("impossible: source index goes beyond graph size");
            }
            const int64_t dirty_block_ID = k_node_to_block->get_block(source);
            if (dirty_block_ID < 0)
            {
                // it is a singleton, which can never split, so no need to mark
                continue;
            }
            // else
            if (k_blocks.at(dirty_block_ID)->size() < min_support)
            {
                // that block will never split anyway, no need to mark it
                continue;
            }
            // else
            // mark as dirty block
            dirty.set_dirty(dirty_block_ID);
        }
    }
#else
    std::vector<Node> nodes = g.get_nodes();
    for (node_index the_node_index = 0; the_node_index < nodes.size(); the_node_index++)
    {
        Node &node = nodes[the_node_index];
        int64_t source_block = k_node_to_block->get_block(the_node_index);
        if (source_block < 0)
        {
            // it is a singleton, which can never split, so no need to mark
            continue;
        }
        // else
        if (k_blocks[source_block]->size() < min_support)
        {
            // that block will never split anyway, no need to mark it
            continue;
        }
        // else

        for (auto edge : node.get_outgoing_edges())
        {
            if (nodes_from_split_blocks.contains(edge.target))
            {
                // mark as dirty block
                dirty.set_dirty(source_block);
                // now this block has been set dirty, no need to investigate the other edges.
                break;
            }
        }
    }

#endif
    KBisumulationOutcome outcome(k_blocks, dirty, k_node_to_block);
    outcome.add_mapping(refines_edges);
    return outcome;
}

void run_k_bisimulation_store_partition_condensed_timed(const std::string &input_path, uint support, const std::string &output_path, bool typed_start)
{

    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();
    Graph g;
    w.start_step("Read graph", true);  // Set newline to true
    uint64_t edge_count = read_graph_timed(input_path, g);
    w.stop_step();

    auto t_start_bisim{boost::chrono::system_clock::now()};
    auto time_t_start_bisim{boost::chrono::system_clock::to_time_t(t_start_bisim)};
    std::tm *ptm_start_bisim{std::localtime(&time_t_start_bisim)};

    std::ofstream graph_stats_output(output_path + "ad_hoc_results/graph_stats.json", std::ios::trunc);
    graph_stats_output << "{\n    \"Vertex count\": " << g.size();
    graph_stats_output << ",\n    \"Edge count\": " << edge_count;

    std::cout << std::put_time(ptm_start_bisim, "%Y/%m/%d %H:%M:%S") << " Graph read with " << g.size() << " nodes" << std::endl;
    std::vector<std::string> lines;
    w.start_step("0000-bisimulation", true);  // Set newline to true

    // We do some pointer trickery here to make sure res will accessible outside of the if-statement
    // Using a regular pointer here would lead to some issues after calling w.get_times() later
    std::unique_ptr<KBisumulationOutcome> res_ptr;
    if (!typed_start)
    {
        res_ptr = std::make_unique<KBisumulationOutcome>(get_0_bisimulation(g));
        // res_ptr = &trivial_res;
    }
    else
    {
        res_ptr = std::make_unique<KBisumulationOutcome>(get_typed_0_bisimulation(g));
    }
    KBisumulationOutcome res = *res_ptr;

    // w.pause();
    // std::cout << "initially one block with " << res.blocks.begin().operator*()->size() << " nodes" << std::endl;
    // w.resume();
    std::deque<KBisumulationOutcome> outcomes;
    outcomes.push_back(res);
    w.stop_step();

    int previous_total = 0;  // We overwrite this in case we do not start with the trivial/universal outcome for k=0

    // These are the default if we have no typed start (i.e. k=0 contains the global block)
    block_or_singleton_index pre_accumulated_block_count = 1;
    block_or_singleton_index accumulated_block_count;
    if (typed_start)
    {
        auto times = w.get_times();
        auto bisim_step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(times.back().duration).count();
        auto bisim_step_memory = times.back().memory_in_kb;
        std::ofstream ad_hoc_output(output_path + "ad_hoc_results/statistics_condensed-0000.json", std::ios::trunc);

        pre_accumulated_block_count = outcomes[0].total_blocks() - outcomes[0].singleton_block_count();
        accumulated_block_count = pre_accumulated_block_count + outcomes[0].singleton_block_count();

        ad_hoc_output << "{\n    \"Block count\": " << outcomes[0].total_blocks()
                      << ",\n    \"Singleton count\": " << outcomes[0].singleton_block_count()
                      << ",\n    \"Accumulated block count\": " << accumulated_block_count
                      << ",\n    \"Time taken (ms)\": " << bisim_step_duration
                      << ",\n    \"Memory footprint (kB)\": " << bisim_step_memory << "\n}";
        ad_hoc_output.flush();

        w.start_step("0000-bisimulation (condensed) writing outcome to disk", true);  // Set newline to true
        std::ofstream condensed_output(output_path + "bisimulation/outcome_condensed-0000.bin", std::ios::trunc);
        // bool found_singletons = false;
        for (block_index i = 0; i<outcomes[0].blocks.size(); i++)
        {
            BlockPtr new_block_ptr = outcomes[0].blocks[i];
            uint64_t block_size = new_block_ptr->end() - new_block_ptr->begin();
            write_uint_BLOCK_little_endian(condensed_output, i+1);  // We add 1, because we want to reserve 0 for the singleton blocks
            write_uint_ENTITY_little_endian(condensed_output, u_int64_t(block_size));  // The reader needs this size to decode the data
            for (auto v_iter = new_block_ptr->begin(); v_iter != new_block_ptr->end(); v_iter++)
            {
                node_index v = *v_iter;
                write_uint_ENTITY_little_endian(condensed_output, u_int64_t(v));  // We store each entity contained in the new block
            }
        }
        condensed_output.flush();
        w.stop_step();
        previous_total = outcomes[0].total_blocks();
    }

    int i = 0;
    for (i = 0;; i++)
    {
        std::ostringstream k_stringstream;
        k_stringstream << std::setw(4) << std::setfill('0') << i;
        std::string k_string(k_stringstream.str());

        std::ostringstream k_next_stringstream;
        k_next_stringstream << std::setw(4) << std::setfill('0') << i+1;
        std::string k_next_string(k_next_stringstream.str());

        w.start_step(k_next_string + "-bisimulation");
        auto res = get_k_bisimulation(g, outcomes[0], support);
        outcomes.pop_front();
        outcomes.push_back(res);
        w.stop_step();
        auto times = w.get_times();
        auto bisim_step_duration = boost::chrono::ceil<boost::chrono::milliseconds>(times.back().duration).count();
        auto bisim_step_memory = times.back().memory_in_kb;

        block_index new_block_count = 0;

        // We do not care for the first mapping from k=0 to k=1 if it is the trivial mapping.
        if (typed_start || i > 0)
        {
            w.start_step(k_next_string + "-bisimulation writing " + k_string + " to " + k_next_string + " refines edges to disk");
            std::ofstream mapping_output(output_path + "bisimulation/mapping-" + k_string + "to" + k_next_string + ".bin", std::ios::trunc);
            for (auto orig_new: outcomes[0].k_minus_one_to_k_mapping.refines_edges)
            {
                block_index split_block_count = u_int64_t(orig_new.second.size());
                new_block_count += split_block_count;
                write_uint_BLOCK_little_endian(mapping_output, u_int64_t(orig_new.first));
                write_uint_BLOCK_little_endian(mapping_output, split_block_count);  // Store in how many blocks the original block had split
                for (auto new_block: orig_new.second)
                {
                    // BlockPtr new_block_ptr = res.blocks[new_block];
                    write_uint_BLOCK_little_endian(mapping_output, u_int64_t(new_block));  // Write all the new blocks the old one got split into

                    // 0 corresponds to a special block for singletons and as such requires special care
                    if (new_block == 0)
                    {
                        new_block_count--;
                    }
                }
            }
            w.stop_step();
            mapping_output.flush();
        }

        pre_accumulated_block_count = pre_accumulated_block_count + new_block_count;
        accumulated_block_count = pre_accumulated_block_count + outcomes[0].singleton_block_count();

        std::ofstream ad_hoc_output(output_path + "ad_hoc_results/statistics_condensed-" + k_next_string + ".json", std::ios::trunc);
        ad_hoc_output << "{\n    \"Block count\": " << outcomes[0].total_blocks()
                      << ",\n    \"Singleton count\": " << outcomes[0].singleton_block_count()
                      << ",\n    \"Accumulated block count\": " << accumulated_block_count
                      << ",\n    \"Time taken (ms)\": " << bisim_step_duration
                      << ",\n    \"Memory footprint (kB)\": " << bisim_step_memory << "\n}";
        ad_hoc_output.flush();

        w.start_step(k_next_string + "-bisimulation (condensed) writing outcome to disk", true);  // Set newline to true
        std::ofstream condensed_output(output_path + "bisimulation/outcome_condensed-" + k_next_string + ".bin", std::ios::trunc);
        // bool found_singletons = false;
        for (auto orig_new: outcomes[0].k_minus_one_to_k_mapping.refines_edges)
        {
            for (auto new_block: orig_new.second)
            {
                // Skip the singleton block
                if (new_block == 0)
                {
                    // found_singletons = true;
                    continue;
                }
                // We have to subtract 1 because we added 1 earlier
                BlockPtr new_block_ptr = outcomes[0].blocks[new_block-1];
                uint64_t block_size = new_block_ptr->end() - new_block_ptr->begin();
                write_uint_BLOCK_little_endian(condensed_output, u_int64_t(new_block));
                write_uint_ENTITY_little_endian(condensed_output, u_int64_t(block_size));  // The reader needs this size to decode the data
                for (auto v_iter = new_block_ptr->begin(); v_iter != new_block_ptr->end(); v_iter++)
                {
                    node_index v = *v_iter;
                    write_uint_ENTITY_little_endian(condensed_output, u_int64_t(v));  // We store each entity contained in the new block
                }
            }
        }
        condensed_output.flush();
        // // For the first outcome write the singltons into block 0
        // if (i == 0 && found_singletons)
        // {
        //     block_index singletons_block = 0;
        //     write_uint_BLOCK_little_endian(condensed_output, u_int64_t(singletons_block));
        //     write_uint_ENTITY_little_endian(condensed_output, u_int64_t(outcomes[0].singleton_block_count()));  // The reader needs this size to decode the data
        //     for (node_index node = 0; node<outcomes[0].total_blocks(); node++)
        //     {
        //         block_or_singleton_index node_block = outcomes[0].get_block_ID_for_node(node);
        //         // If the node is in a singleton block, then write to the outcome
        //         if (node_block < 0)
        //         {
        //             write_uint_ENTITY_little_endian(condensed_output, u_int64_t(node));
        //         }
        //     }
        // }
        w.stop_step();
        
        int new_total = outcomes[0].total_blocks();

        auto now{boost::chrono::system_clock::to_time_t(boost::chrono::system_clock::now())};
        std::tm *ptm{std::localtime(&now)};
        std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S")
                  << " level " << i + 1
                  << ", blocks = " << outcomes[0].total_blocks()
                  << ", accumulated blocks = " << accumulated_block_count
                  << ", singletons = " << outcomes[0].singleton_block_count()
                  << ", total = " << new_total
                  << ", time = " << bisim_step_duration << " ms"
                  << ", memory = " << bisim_step_memory << " kB" << std::endl;
        if (new_total == previous_total)
        {
            break;
        }
        previous_total = new_total;
    }

    // Print the clock
    std::cout << "\n" << w.to_string() << "\n" << std::endl;

    // Print and store the total time and memory
    auto t_bisim_done{boost::chrono::system_clock::now()};
    auto time_t_bisim_done{boost::chrono::system_clock::to_time_t(t_bisim_done)};
    std::tm *ptm_bisim_done{std::localtime(&time_t_bisim_done)};
    auto max_memory = w.get_times()[0].memory_in_kb;
    for (auto step : w.get_times())
    {
        max_memory = std::max(max_memory, step.memory_in_kb);
    }
    std::cout << std::put_time(ptm_bisim_done, "%Y/%m/%d %H:%M:%S")
              << " Time taken for the bisimulation = " << boost::chrono::ceil<boost::chrono::milliseconds>(t_bisim_done - t_start_bisim).count() << " ms" << std::endl;
    std::cout << std::put_time(ptm_bisim_done, "%Y/%m/%d %H:%M:%S")
              << " Maximum memory footprint = " << max_memory << " kB" << std::endl;
    graph_stats_output << ",\n    \"Total time taken (ms)\": " << boost::chrono::ceil<boost::chrono::milliseconds>(t_bisim_done - t_start_bisim).count()
                       << ",\n    \"Maximum memory footprint (kB)\": " << max_memory
                       << ",\n    \"Final depth\": " << i //TODO make an implementation for a fixed k (instead of always doing the full bisimulation)
                       << ",\n    \"Fixed point\": true" << "\n}"; //TODO make an implementation for a fixed k (instead of always doing the full bisimulation)
    graph_stats_output.flush();
}

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("input_file", po::value<std::string>(), "Input file");
    global.add_options()("command", po::value<std::string>(), "command to execute");
    global.add_options()("commandargs", po::value<std::vector<std::string>>(), "Arguments for command");
    global.add_options()("strings", po::value<std::string>()->default_value("map_to_one_node"), "What to do with string values? Currently only map_to_one_node (which maps all strings to one node before applying the bisimulation).");
    po::positional_options_description pos;
    pos.add("command", 1).add("input_file", 2).add("commandargs", -1);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string cmd = vm["command"].as<std::string>();
    std::string input_file = vm["input_file"].as<std::string>();
    std::string string_treatment = vm["strings"].as<std::string>();

    if (string_treatment != "map_to_one_node")
    {
        throw MyException("Currently only map_to_one_node is supported as a string treatment. This is currently hard-coded.");
    }

    if (cmd == "run_k_bisimulation_store_partition_condensed_timed")
    {
        po::options_description run_timed_desc("run_k_bisimulation_store_partition_timed 0   options");
        run_timed_desc.add_options()("support", po::value<uint>()->default_value(1), "Specify the required size for a block to be considered splittable");
        run_timed_desc.add_options()("output,o", po::value<std::string>(), "output, the output path");
        // run_timed_desc.add_options()("skip_singletons", "flag indicating that singletons must be skipped in the output");
        run_timed_desc.add_options()("typed_start", "flag indicating that the nodes are initially (at k=0) partitioned acording to their RDF type set");

        // Collect all the unrecognized options from the first pass. This will include the
        // (positional) command name, so we need to erase that.
        std::vector<std::string> opts = po::collect_unrecognized(parsed.options, po::include_positional);
        opts.erase(opts.begin());
        // It also has the file name, so erase as well
        opts.erase(opts.begin());

        // Parse again...
        po::store(po::command_line_parser(opts).options(run_timed_desc).run(), vm);
        po::notify(vm);

        uint support = vm["support"].as<uint>();
        std::string output_path = vm["output"].as<std::string>();
        // bool skip_singletons = vm.count("skip_singletons");
        bool typed_start = vm.count("typed_start");

        std::filesystem::create_directory(output_path + "bisimulation/");
        std::filesystem::create_directory(output_path + "ad_hoc_results/");

        run_k_bisimulation_store_partition_condensed_timed(input_file, support, output_path, typed_start);

        return 0;
    }
    else
    {
        // unrecognised command
        throw po::invalid_option_value(cmd);
    }
}
