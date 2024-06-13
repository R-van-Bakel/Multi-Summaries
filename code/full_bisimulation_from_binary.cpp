#include <cstdint>
#include <vector>
#include <stack>
#include <span>
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/format.hpp>
#include <iostream>
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <chrono>
#include <iomanip>
#include <thread>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/find.hpp>

using edge_type = uint32_t;
using node_index = uint64_t;
using block_index = node_index;

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;

#undef CREATE_REVERSE_INDEX

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
    std::span<Edge> get_outgoing_edges()
    {
        return edges;
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

    std::span<Node> get_nodes()
    {
        return nodes;
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

u_int64_t read_uint_ENTITY_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_ENTITY);
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
    u_int32_t result = uint32_t(0);

    for (unsigned int i = 0; i < BYTES_PER_PREDICATE; i++)
    {
        result |= (uint32_t(data[i]) & 255) << (i * 8); // `& 255` makes sure that we only write one byte of data
    }
    return result;
}

void read_graph_from_stream(std::istream &inputstream, Graph &g)
{
    const int BufferSize = 8 * 16184;

    char _buffer[BufferSize];

    inputstream.rdbuf()->pubsetbuf(_buffer, BufferSize);

    unsigned int line_counter = 0;
    while (true)
    {

        line_counter++;

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
            std::tm* ptm{std::localtime(&now)};
            std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S") << " done with " << line_counter << " triples" << std::endl;
        }
    }
#ifdef CREATE_REVERSE_INDEX
    g.compute_reverse_index();
#endif
}

void read_graph(const std::string &filename, Graph &g)
{

    std::ifstream infile(filename, std::ifstream::in);
    read_graph_from_stream(infile, g);
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
        this->node_to_block[node] = -this->singleton_counter;
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

KBisumulationOutcome get_k_bisimulation(Graph &g, const KBisumulationOutcome &k_minus_one_outcome, std::size_t min_support = 1)
{
    // we make copies which we will modify
    std::vector<BlockPtr> k_blocks(k_minus_one_outcome.blocks);
    std::shared_ptr<MappingNode2BlockMapper> k_node_to_block = k_minus_one_outcome.node_to_block->modifyable_copy();

    // We collect all nodes from split blocks. In the end we mark all blocks which target these as dirty.
    boost::unordered_flat_set<node_index> nodes_from_split_blocks; //TODO: there seems to be no reason why this would have to be a set, since all the nodes on the split blocks can only be in one location anyway.

    // we first do dirty blocks of size 2 because if they split, they cause two singletons and a gap (freeblock) in the list of blocks
    // These freeblocks can be filled if larger blocks are split.

    if (min_support < 2)
    {
        for (auto iter = k_minus_one_outcome.dirty_blocks.cbegin(); iter != k_minus_one_outcome.dirty_blocks.cend(); iter++)
        {
            block_index dirty_block_index = *iter;
            BlockPtr dirty_block = k_blocks[dirty_block_index];
            size_t dirty_block_size = dirty_block->size();

            if (dirty_block_size != 2)
            {
                // we deal with this below
                continue;
            }
            // else
            // we checked above that min_support < 2, so no need to theck that here.

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
                // try_emplace returns an iterator to a new element if there was nothign yet, otherwise to the existing one
                auto empl_res = M.try_emplace(signature);
                (*(empl_res.first)).second.emplace_back(v);
            }
            // if the block is not refined
            if (M.size() == 1)
            {
                // no need to update anythign in the blocks, nor in the index
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
        }
    }

    // now we deal with larger blocks. When they split, we first attempt to fill the gaps created in the previous loop.
    // if there is no free space, we append the blocks.
    // in the meantime, we also maintain the k_node_to_block index.
    for (auto iter = k_minus_one_outcome.dirty_blocks.cbegin(); iter != k_minus_one_outcome.dirty_blocks.cend(); iter++)
    {
        block_index dirty_block_index = *iter;
        BlockPtr dirty_block = k_blocks[dirty_block_index];
        size_t dirty_block_size = dirty_block->size();

        if (dirty_block_size == 2 || dirty_block_size <= min_support)
        {
            // if it is 2, we dealt with it above.
            // if it is less tan min_support, no need to update anythign in the blocks, nor in the index
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
            // try_emplace returns an iterator to a new element if there was nothign yet, otherwise to the existing one
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

        // we first make sure all nodes are add to the nodes_from_split_blocks
        for (auto v_iter = dirty_block->begin(); v_iter != dirty_block->end(); v_iter++)
        {
            node_index v = *v_iter;
            nodes_from_split_blocks.emplace(v);
        }

        // We mark the current block_index as aa free one, and set it to the empty one
        k_node_to_block->freeblock_indices.push(dirty_block_index);
        k_blocks[dirty_block_index] = global_empty_block;

        // all indices for this block will be overwritten, so no need to do this now

        // categorize the blocks

        for (auto &signature_blocks : M)
        {
            // if singleton, make it a singleton in the mapping
            if (signature_blocks.second.size() == 1)
            {
                k_node_to_block->put_into_singleton(*(signature_blocks.second.cbegin()));
                continue;
            }
            // else

            BlockPtr block = std::make_shared<Block>(signature_blocks.second);
            block->shrink_to_fit();
            // if there are still known empty blocks, write on them
            std::size_t new_block_index;
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
    std::span<Node> nodes = g.get_nodes();
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
    return outcome;
}

template <typename clock>
class StopWatch
{
    struct Step
    {
        const std::string name;
        const clock::duration duration;
        const int memory_in_kb;
        Step(const std::string &name, const clock::duration &duration, const int &memory_in_kb)
            : name(name), duration(duration), memory_in_kb(memory_in_kb)
        {
        }
        Step(const Step &step)
            : name(step.name), duration(step.duration), memory_in_kb(step.memory_in_kb)
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

    void start_step(std::string name)
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

        this->steps.emplace_back(this->current_step_name, total_duration, memory_use);
        this->started = false;
    }

    std::string to_string()
    {
        if (this->started)
        {
            throw MyException("Cannot convert a running StopWatch to a string, stop it first");
        }
        std::stringstream out;
        typename clock::duration total = clock::duration::zero();
        for (auto step : this->get_times())
        {
            total += step.duration;

            out << "Step: " << step.name << ", time = " << boost::chrono::ceil<boost::chrono::milliseconds>(step.duration) << " ms"
                << ", memory = " << step.memory_in_kb << " kb"
                << "\n";
        }
        out << "Total time = " << total;
        return out.str();
    }

    std::span<Step> get_times()
    {
        return this->steps;
    }
};

// Run an experiment where each step of the bisumulation is timed
void run_timed(const std::string &path, uint support)
{

    StopWatch<boost::chrono::process_cpu_clock> w = StopWatch<boost::chrono::process_cpu_clock>::create_not_started();
    Graph g;
    w.start_step("Read graph");
    read_graph(path, g);
    w.stop_step();
    std::cout << "Graph read with " << g.size() << " nodes" << std::endl;
    w.start_step("bisimulation");
    KBisumulationOutcome res = get_0_bisimulation(g);
    // w.pause();
    // std::cout << "initially one block with " << res.blocks.begin().operator*()->size() << " nodes" << std::endl;
    // w.resume();
    std::deque<KBisumulationOutcome> outcomes;
    outcomes.push_back(res);
    w.stop_step();
    int previous_total = 0;
    for (auto i = 0;; i++)
    {
        w.start_step(std::to_string(i + 1) + "-bisimulation");
        auto res = get_k_bisimulation(g, outcomes[0], support);
        outcomes.push_back(res);
        outcomes.pop_front();
        w.stop_step();
        int new_total = outcomes[0].total_blocks();

        std::cout << "level " << i + 1 << " blocks = " << outcomes[0].non_singleton_block_count()
                  << ", singletons = " << outcomes[0].singleton_block_count() << ", total = "
                  << new_total << std::endl;
        if (new_total == previous_total)
        {
            break;
        }
        previous_total = new_total;
    }
    // pring timing
    // w.stop_step();
    std::cout << w.to_string() << std::endl;
}

void run_k_bisimulation_store_partition(const std::string &input_path, uint support, int k, const std::string &output_path, bool skip_singletons)
{
    Graph g;
    read_graph(input_path, g);
    std::cout << "Graph read with " << g.size() << " nodes" << std::endl;
    KBisumulationOutcome res = get_0_bisimulation(g);
    std::vector<KBisumulationOutcome> outcomes;
    outcomes.push_back(res);
    int previous_total = 0;
    for (auto i = 1; k == -1 || i <= k; i++)
    {
        auto res = get_k_bisimulation(g, outcomes[i - 1], support);
        outcomes.push_back(res);
        outcomes[i - 1].clear_indices();
        int new_total = outcomes[i].total_blocks();

        std::cout << "level " << i << " blocks = " << outcomes[i].non_singleton_block_count()
                  << ", singletons = " << outcomes[i].singleton_block_count() << ", total = "
                  << new_total << std::endl;
        if (new_total == previous_total)
        {
            break;
        }
        previous_total = new_total;
    }
    // write out partition

    std::ofstream output(output_path, std::ios::trunc);
    // we just write the final one
    const KBisumulationOutcome &final_partition = *(outcomes.cend() - 1);

    for (node_index node = 0; node < g.get_nodes().size(); node++)
    {
        int64_t blockID = final_partition.get_block_ID_for_node(node);
        if (skip_singletons && blockID < 0)
        {
            continue;
        }
        output << blockID << '\n';
    }

    output.flush();
}

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("input_file", po::value<std::string>(), "Input file, must contain n-triples");
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

    if (cmd == "run_timed")
    {
        po::options_description run_timed_desc("run_timed options");
        run_timed_desc.add_options()("support", po::value<uint>()->default_value(1), "Specify the required size for a block to be considered splittable");

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
        run_timed(input_file, support);
        return 0;
    }
    else if (cmd == "run_k_bisimulation_store_partition")
    {
        po::options_description run_timed_desc("run_k_bisimulation_store_partition options");
        run_timed_desc.add_options()("support", po::value<uint>()->default_value(1), "Specify the required size for a block to be considered splittable");
        run_timed_desc.add_options()("k", po::value<int>()->default_value(-1), "k, the depth of the bisimulation. Default is -1, i.e., infinite");
        run_timed_desc.add_options()("output,o", po::value<std::string>(), "output, the output path");
        run_timed_desc.add_options()("skip_singletons", "flag indicating that singletons must be skipped in the output");

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
        int k = vm["k"].as<int>();
        std::string output_path = vm["output"].as<std::string>();
        bool skip_singletons = vm.count("skip_singletons");

        run_k_bisimulation_store_partition(input_file, support, k, output_path, skip_singletons);

        return 0;
    }
    else
    {
        // unrecognised command
        throw po::invalid_option_value(cmd);
    }
}
// $ /usr/bin/time -v ./full_bisimulation run_timed ../mappingbased-objects_lang\=en.ttl
// done with 1000000 triples
// done with 2000000 triples
// done with 3000000 triples
// done with 4000000 triples
// done with 5000000 triples
// done with 6000000 triples
// done with 7000000 triples
// done with 8000000 triples
// done with 9000000 triples
// done with 10000000 triples
// done with 11000000 triples
// done with 12000000 triples
// done with 13000000 triples
// done with 14000000 triples
// done with 15000000 triples
// done with 16000000 triples
// done with 17000000 triples
// done with 18000000 triples
// done with 19000000 triples
// done with 20000000 triples
// done with 21000000 triples
// done with 22000000 triples
// Graph read with 7958883 nodes
// level 1 blocks = 47757, singletons = 61265, total = 109022
// level 2 blocks = 189418, singletons = 1786863, total = 1976281
// level 3 blocks = 227883, singletons = 2270231, total = 2498114
// level 4 blocks = 230881, singletons = 2309469, total = 2540350
// level 5 blocks = 230602, singletons = 2316214, total = 2546816
// level 6 blocks = 230480, singletons = 2317685, total = 2548165
// level 7 blocks = 230408, singletons = 2318160, total = 2548568
// level 8 blocks = 230371, singletons = 2318409, total = 2548780
// level 9 blocks = 230342, singletons = 2318570, total = 2548912
// level 10 blocks = 230325, singletons = 2318660, total = 2548985
// level 11 blocks = 230315, singletons = 2318725, total = 2549040
// level 12 blocks = 230308, singletons = 2318771, total = 2549079
// level 13 blocks = 230305, singletons = 2318804, total = 2549109
// level 14 blocks = 230300, singletons = 2318835, total = 2549135
// level 15 blocks = 230297, singletons = 2318861, total = 2549158
// level 16 blocks = 230295, singletons = 2318882, total = 2549177
// level 17 blocks = 230294, singletons = 2318897, total = 2549191
// level 18 blocks = 230291, singletons = 2318913, total = 2549204
// level 19 blocks = 230289, singletons = 2318925, total = 2549214
// level 20 blocks = 230288, singletons = 2318935, total = 2549223
// level 21 blocks = 230287, singletons = 2318945, total = 2549232
// level 22 blocks = 230285, singletons = 2318956, total = 2549241
// level 23 blocks = 230285, singletons = 2318962, total = 2549247
// level 24 blocks = 230286, singletons = 2318966, total = 2549252
// level 25 blocks = 230287, singletons = 2318970, total = 2549257
// level 26 blocks = 230288, singletons = 2318974, total = 2549262
// level 27 blocks = 230288, singletons = 2318979, total = 2549267
// level 28 blocks = 230287, singletons = 2318985, total = 2549272
// level 29 blocks = 230286, singletons = 2318991, total = 2549277
// level 30 blocks = 230285, singletons = 2318997, total = 2549282
// level 31 blocks = 230284, singletons = 2319003, total = 2549287
// level 32 blocks = 230282, singletons = 2319010, total = 2549292
// level 33 blocks = 230281, singletons = 2319014, total = 2549295
// level 34 blocks = 230280, singletons = 2319018, total = 2549298
// level 35 blocks = 230279, singletons = 2319022, total = 2549301
// level 36 blocks = 230278, singletons = 2319026, total = 2549304
// level 37 blocks = 230277, singletons = 2319030, total = 2549307
// level 38 blocks = 230276, singletons = 2319034, total = 2549310
// level 39 blocks = 230275, singletons = 2319038, total = 2549313
// level 40 blocks = 230274, singletons = 2319042, total = 2549316
// level 41 blocks = 230273, singletons = 2319046, total = 2549319
// level 42 blocks = 230272, singletons = 2319050, total = 2549322
// level 43 blocks = 230271, singletons = 2319054, total = 2549325
// level 44 blocks = 230270, singletons = 2319058, total = 2549328
// level 45 blocks = 230269, singletons = 2319062, total = 2549331
// level 46 blocks = 230268, singletons = 2319066, total = 2549334
// level 47 blocks = 230267, singletons = 2319070, total = 2549337
// level 48 blocks = 230266, singletons = 2319074, total = 2549340
// level 49 blocks = 230265, singletons = 2319078, total = 2549343
// level 50 blocks = 230264, singletons = 2319082, total = 2549346
// level 51 blocks = 230263, singletons = 2319086, total = 2549349
// level 52 blocks = 230262, singletons = 2319090, total = 2549352
// level 53 blocks = 230262, singletons = 2319092, total = 2549354
// level 54 blocks = 230262, singletons = 2319094, total = 2549356
// level 55 blocks = 230262, singletons = 2319096, total = 2549358
// level 56 blocks = 230262, singletons = 2319098, total = 2549360
// level 57 blocks = 230262, singletons = 2319100, total = 2549362
// level 58 blocks = 230262, singletons = 2319102, total = 2549364
// level 59 blocks = 230262, singletons = 2319104, total = 2549366
// level 60 blocks = 230262, singletons = 2319106, total = 2549368
// level 61 blocks = 230262, singletons = 2319108, total = 2549370
// level 62 blocks = 230262, singletons = 2319110, total = 2549372
// level 63 blocks = 230262, singletons = 2319112, total = 2549374
// level 64 blocks = 230262, singletons = 2319114, total = 2549376
// level 65 blocks = 230262, singletons = 2319116, total = 2549378
// level 66 blocks = 230262, singletons = 2319118, total = 2549380
// level 67 blocks = 230262, singletons = 2319120, total = 2549382
// level 68 blocks = 230262, singletons = 2319122, total = 2549384
// level 69 blocks = 230262, singletons = 2319124, total = 2549386
// level 70 blocks = 230262, singletons = 2319126, total = 2549388
// level 71 blocks = 230262, singletons = 2319128, total = 2549390
// level 72 blocks = 230262, singletons = 2319130, total = 2549392
// level 73 blocks = 230262, singletons = 2319132, total = 2549394
// level 74 blocks = 230262, singletons = 2319134, total = 2549396
// level 75 blocks = 230262, singletons = 2319136, total = 2549398
// level 76 blocks = 230262, singletons = 2319138, total = 2549400
// level 77 blocks = 230262, singletons = 2319140, total = 2549402
// level 78 blocks = 230262, singletons = 2319142, total = 2549404
// level 79 blocks = 230262, singletons = 2319144, total = 2549406
// level 80 blocks = 230262, singletons = 2319146, total = 2549408
// level 81 blocks = 230262, singletons = 2319148, total = 2549410
// level 82 blocks = 230262, singletons = 2319150, total = 2549412
// level 83 blocks = 230262, singletons = 2319152, total = 2549414
// level 84 blocks = 230262, singletons = 2319154, total = 2549416
// level 85 blocks = 230262, singletons = 2319156, total = 2549418
// level 86 blocks = 230262, singletons = 2319158, total = 2549420
// level 87 blocks = 230262, singletons = 2319160, total = 2549422
// level 88 blocks = 230262, singletons = 2319162, total = 2549424
// level 89 blocks = 230262, singletons = 2319164, total = 2549426
// level 90 blocks = 230262, singletons = 2319166, total = 2549428
// level 91 blocks = 230262, singletons = 2319168, total = 2549430
// level 92 blocks = 230262, singletons = 2319170, total = 2549432
// level 93 blocks = 230262, singletons = 2319172, total = 2549434
// level 94 blocks = 230262, singletons = 2319174, total = 2549436
// level 95 blocks = 230262, singletons = 2319176, total = 2549438
// level 96 blocks = 230262, singletons = 2319178, total = 2549440
// level 97 blocks = 230262, singletons = 2319180, total = 2549442
// level 98 blocks = 230262, singletons = 2319182, total = 2549444
// level 99 blocks = 230262, singletons = 2319184, total = 2549446
// level 100 blocks = 230262, singletons = 2319186, total = 2549448
// level 101 blocks = 230262, singletons = 2319188, total = 2549450
// level 102 blocks = 230262, singletons = 2319190, total = 2549452
// level 103 blocks = 230262, singletons = 2319192, total = 2549454
// level 104 blocks = 230262, singletons = 2319194, total = 2549456
// level 105 blocks = 230262, singletons = 2319196, total = 2549458
// level 106 blocks = 230262, singletons = 2319198, total = 2549460
// level 107 blocks = 230262, singletons = 2319200, total = 2549462
// level 108 blocks = 230262, singletons = 2319202, total = 2549464
// level 109 blocks = 230262, singletons = 2319204, total = 2549466
// level 110 blocks = 230262, singletons = 2319206, total = 2549468
// level 111 blocks = 230262, singletons = 2319208, total = 2549470
// level 112 blocks = 230262, singletons = 2319210, total = 2549472
// level 113 blocks = 230262, singletons = 2319212, total = 2549474
// level 114 blocks = 230262, singletons = 2319214, total = 2549476
// level 115 blocks = 230262, singletons = 2319216, total = 2549478
// level 116 blocks = 230262, singletons = 2319218, total = 2549480
// level 117 blocks = 230262, singletons = 2319220, total = 2549482
// level 118 blocks = 230262, singletons = 2319222, total = 2549484
// level 119 blocks = 230262, singletons = 2319224, total = 2549486
// level 120 blocks = 230262, singletons = 2319226, total = 2549488
// level 121 blocks = 230262, singletons = 2319228, total = 2549490
// level 122 blocks = 230262, singletons = 2319230, total = 2549492
// level 123 blocks = 230262, singletons = 2319232, total = 2549494
// level 124 blocks = 230262, singletons = 2319234, total = 2549496
// level 125 blocks = 230262, singletons = 2319236, total = 2549498
// level 126 blocks = 230262, singletons = 2319238, total = 2549500
// level 127 blocks = 230262, singletons = 2319240, total = 2549502
// level 128 blocks = 230262, singletons = 2319242, total = 2549504
// level 129 blocks = 230262, singletons = 2319244, total = 2549506
// level 130 blocks = 230262, singletons = 2319246, total = 2549508
// level 131 blocks = 230262, singletons = 2319248, total = 2549510
// level 132 blocks = 230262, singletons = 2319250, total = 2549512
// level 133 blocks = 230262, singletons = 2319252, total = 2549514
// level 134 blocks = 230262, singletons = 2319254, total = 2549516
// level 135 blocks = 230262, singletons = 2319256, total = 2549518
// level 136 blocks = 230262, singletons = 2319258, total = 2549520
// level 137 blocks = 230262, singletons = 2319260, total = 2549522
// level 138 blocks = 230262, singletons = 2319262, total = 2549524
// level 139 blocks = 230262, singletons = 2319264, total = 2549526
// level 140 blocks = 230262, singletons = 2319266, total = 2549528
// level 141 blocks = 230262, singletons = 2319268, total = 2549530
// level 142 blocks = 230262, singletons = 2319270, total = 2549532
// level 143 blocks = 230262, singletons = 2319272, total = 2549534
// level 144 blocks = 230261, singletons = 2319275, total = 2549536
// level 145 blocks = 230260, singletons = 2319277, total = 2549537
// level 146 blocks = 230260, singletons = 2319277, total = 2549537
// Step: Read graph, time = 19560 milliseconds ms, memory = 1360848 kb
// Step: bisimulation, time = 30 milliseconds ms, memory = 1422884 kb
// Step: 1-bisimulation, time = 2070 milliseconds ms, memory = 1684740 kb
// Step: 2-bisimulation, time = 2570 milliseconds ms, memory = 1756616 kb
// Step: 3-bisimulation, time = 1370 milliseconds ms, memory = 1818796 kb
// Step: 4-bisimulation, time = 550 milliseconds ms, memory = 1880976 kb
// Step: 5-bisimulation, time = 280 milliseconds ms, memory = 1943156 kb
// Step: 6-bisimulation, time = 230 milliseconds ms, memory = 2005336 kb
// Step: 7-bisimulation, time = 210 milliseconds ms, memory = 2067516 kb
// Step: 8-bisimulation, time = 230 milliseconds ms, memory = 2129696 kb
// Step: 9-bisimulation, time = 200 milliseconds ms, memory = 2191876 kb
// Step: 10-bisimulation, time = 210 milliseconds ms, memory = 2254056 kb
// Step: 11-bisimulation, time = 190 milliseconds ms, memory = 2316236 kb
// Step: 12-bisimulation, time = 210 milliseconds ms, memory = 2378416 kb
// Step: 13-bisimulation, time = 190 milliseconds ms, memory = 2440596 kb
// Step: 14-bisimulation, time = 190 milliseconds ms, memory = 2502776 kb
// Step: 15-bisimulation, time = 200 milliseconds ms, memory = 2564956 kb
// Step: 16-bisimulation, time = 240 milliseconds ms, memory = 2641388 kb
// Step: 17-bisimulation, time = 190 milliseconds ms, memory = 2703680 kb
// Step: 18-bisimulation, time = 190 milliseconds ms, memory = 2765860 kb
// Step: 19-bisimulation, time = 190 milliseconds ms, memory = 2828040 kb
// Step: 20-bisimulation, time = 200 milliseconds ms, memory = 2890220 kb
// Step: 21-bisimulation, time = 190 milliseconds ms, memory = 2952400 kb
// Step: 22-bisimulation, time = 200 milliseconds ms, memory = 3014580 kb
// Step: 23-bisimulation, time = 190 milliseconds ms, memory = 3076760 kb
// Step: 24-bisimulation, time = 190 milliseconds ms, memory = 3138940 kb
// Step: 25-bisimulation, time = 190 milliseconds ms, memory = 3201120 kb
// Step: 26-bisimulation, time = 190 milliseconds ms, memory = 3263300 kb
// Step: 27-bisimulation, time = 190 milliseconds ms, memory = 3325480 kb
// Step: 28-bisimulation, time = 200 milliseconds ms, memory = 3387660 kb
// Step: 29-bisimulation, time = 190 milliseconds ms, memory = 3453268 kb
// Step: 30-bisimulation, time = 190 milliseconds ms, memory = 3519056 kb
// Step: 31-bisimulation, time = 190 milliseconds ms, memory = 3584844 kb
// Step: 32-bisimulation, time = 290 milliseconds ms, memory = 3751744 kb
// Step: 33-bisimulation, time = 200 milliseconds ms, memory = 3814000 kb
// Step: 34-bisimulation, time = 190 milliseconds ms, memory = 3876180 kb
// Step: 35-bisimulation, time = 190 milliseconds ms, memory = 3938360 kb
// Step: 36-bisimulation, time = 200 milliseconds ms, memory = 4000540 kb
// Step: 37-bisimulation, time = 190 milliseconds ms, memory = 4062720 kb
// Step: 38-bisimulation, time = 190 milliseconds ms, memory = 4124900 kb
// Step: 39-bisimulation, time = 190 milliseconds ms, memory = 4187080 kb
// Step: 40-bisimulation, time = 180 milliseconds ms, memory = 4249260 kb
// Step: 41-bisimulation, time = 190 milliseconds ms, memory = 4311440 kb
// Step: 42-bisimulation, time = 190 milliseconds ms, memory = 4373620 kb
// Step: 43-bisimulation, time = 190 milliseconds ms, memory = 4435800 kb
// Step: 44-bisimulation, time = 190 milliseconds ms, memory = 4497980 kb
// Step: 45-bisimulation, time = 200 milliseconds ms, memory = 4560160 kb
// Step: 46-bisimulation, time = 200 milliseconds ms, memory = 4622340 kb
// Step: 47-bisimulation, time = 190 milliseconds ms, memory = 4684520 kb
// Step: 48-bisimulation, time = 190 milliseconds ms, memory = 4746700 kb
// Step: 49-bisimulation, time = 190 milliseconds ms, memory = 4808880 kb
// Step: 50-bisimulation, time = 190 milliseconds ms, memory = 4871060 kb
// Step: 51-bisimulation, time = 200 milliseconds ms, memory = 4933240 kb
// Step: 52-bisimulation, time = 190 milliseconds ms, memory = 4995420 kb
// Step: 53-bisimulation, time = 200 milliseconds ms, memory = 5057600 kb
// Step: 54-bisimulation, time = 180 milliseconds ms, memory = 5119780 kb
// Step: 55-bisimulation, time = 190 milliseconds ms, memory = 5181960 kb
// Step: 56-bisimulation, time = 190 milliseconds ms, memory = 5244140 kb
// Step: 57-bisimulation, time = 190 milliseconds ms, memory = 5306320 kb
// Step: 58-bisimulation, time = 200 milliseconds ms, memory = 5368500 kb
// Step: 59-bisimulation, time = 190 milliseconds ms, memory = 5430680 kb
// Step: 60-bisimulation, time = 190 milliseconds ms, memory = 5492860 kb
// Step: 61-bisimulation, time = 200 milliseconds ms, memory = 5555040 kb
// Step: 62-bisimulation, time = 200 milliseconds ms, memory = 5620648 kb
// Step: 63-bisimulation, time = 190 milliseconds ms, memory = 5686436 kb
// Step: 64-bisimulation, time = 410 milliseconds ms, memory = 5972400 kb
// Step: 65-bisimulation, time = 170 milliseconds ms, memory = 5972400 kb
// Step: 66-bisimulation, time = 190 milliseconds ms, memory = 6034440 kb
// Step: 67-bisimulation, time = 180 milliseconds ms, memory = 6096744 kb
// Step: 68-bisimulation, time = 180 milliseconds ms, memory = 6158784 kb
// Step: 69-bisimulation, time = 180 milliseconds ms, memory = 6221088 kb
// Step: 70-bisimulation, time = 180 milliseconds ms, memory = 6283128 kb
// Step: 71-bisimulation, time = 180 milliseconds ms, memory = 6345432 kb
// Step: 72-bisimulation, time = 180 milliseconds ms, memory = 6407472 kb
// Step: 73-bisimulation, time = 180 milliseconds ms, memory = 6469776 kb
// Step: 74-bisimulation, time = 180 milliseconds ms, memory = 6531816 kb
// Step: 75-bisimulation, time = 180 milliseconds ms, memory = 6594120 kb
// Step: 76-bisimulation, time = 180 milliseconds ms, memory = 6656160 kb
// Step: 77-bisimulation, time = 180 milliseconds ms, memory = 6718464 kb
// Step: 78-bisimulation, time = 180 milliseconds ms, memory = 6780504 kb
// Step: 79-bisimulation, time = 180 milliseconds ms, memory = 6842808 kb
// Step: 80-bisimulation, time = 180 milliseconds ms, memory = 6905112 kb
// Step: 81-bisimulation, time = 180 milliseconds ms, memory = 6967152 kb
// Step: 82-bisimulation, time = 180 milliseconds ms, memory = 7029456 kb
// Step: 83-bisimulation, time = 180 milliseconds ms, memory = 7091496 kb
// Step: 84-bisimulation, time = 180 milliseconds ms, memory = 7153800 kb
// Step: 85-bisimulation, time = 170 milliseconds ms, memory = 7215840 kb
// Step: 86-bisimulation, time = 180 milliseconds ms, memory = 7278144 kb
// Step: 87-bisimulation, time = 180 milliseconds ms, memory = 7340184 kb
// Step: 88-bisimulation, time = 180 milliseconds ms, memory = 7402488 kb
// Step: 89-bisimulation, time = 180 milliseconds ms, memory = 7464528 kb
// Step: 90-bisimulation, time = 180 milliseconds ms, memory = 7526832 kb
// Step: 91-bisimulation, time = 190 milliseconds ms, memory = 7589096 kb
// Step: 92-bisimulation, time = 180 milliseconds ms, memory = 7651276 kb
// Step: 93-bisimulation, time = 190 milliseconds ms, memory = 7713456 kb
// Step: 94-bisimulation, time = 200 milliseconds ms, memory = 7775636 kb
// Step: 95-bisimulation, time = 190 milliseconds ms, memory = 7837816 kb
// Step: 96-bisimulation, time = 200 milliseconds ms, memory = 7899996 kb
// Step: 97-bisimulation, time = 190 milliseconds ms, memory = 7962176 kb
// Step: 98-bisimulation, time = 190 milliseconds ms, memory = 8024356 kb
// Step: 99-bisimulation, time = 190 milliseconds ms, memory = 8086536 kb
// Step: 100-bisimulation, time = 190 milliseconds ms, memory = 8148716 kb
// Step: 101-bisimulation, time = 190 milliseconds ms, memory = 8210896 kb
// Step: 102-bisimulation, time = 190 milliseconds ms, memory = 8273076 kb
// Step: 103-bisimulation, time = 190 milliseconds ms, memory = 8335256 kb
// Step: 104-bisimulation, time = 200 milliseconds ms, memory = 8397436 kb
// Step: 105-bisimulation, time = 200 milliseconds ms, memory = 8459616 kb
// Step: 106-bisimulation, time = 200 milliseconds ms, memory = 8521796 kb
// Step: 107-bisimulation, time = 180 milliseconds ms, memory = 8587404 kb
// Step: 108-bisimulation, time = 190 milliseconds ms, memory = 8653192 kb
// Step: 109-bisimulation, time = 190 milliseconds ms, memory = 8718980 kb
// Step: 110-bisimulation, time = 190 milliseconds ms, memory = 8784768 kb
// Step: 111-bisimulation, time = 190 milliseconds ms, memory = 8850556 kb
// Step: 112-bisimulation, time = 180 milliseconds ms, memory = 8916344 kb
// Step: 113-bisimulation, time = 190 milliseconds ms, memory = 8982128 kb
// Step: 114-bisimulation, time = 180 milliseconds ms, memory = 9047916 kb
// Step: 115-bisimulation, time = 190 milliseconds ms, memory = 9113704 kb
// Step: 116-bisimulation, time = 190 milliseconds ms, memory = 9179492 kb
// Step: 117-bisimulation, time = 180 milliseconds ms, memory = 9245280 kb
// Step: 118-bisimulation, time = 190 milliseconds ms, memory = 9311068 kb
// Step: 119-bisimulation, time = 180 milliseconds ms, memory = 9376856 kb
// Step: 120-bisimulation, time = 190 milliseconds ms, memory = 9442644 kb
// Step: 121-bisimulation, time = 190 milliseconds ms, memory = 9508432 kb
// Step: 122-bisimulation, time = 200 milliseconds ms, memory = 9574216 kb
// Step: 123-bisimulation, time = 190 milliseconds ms, memory = 9640004 kb
// Step: 124-bisimulation, time = 190 milliseconds ms, memory = 9705792 kb
// Step: 125-bisimulation, time = 190 milliseconds ms, memory = 9771580 kb
// Step: 126-bisimulation, time = 200 milliseconds ms, memory = 9837368 kb
// Step: 127-bisimulation, time = 190 milliseconds ms, memory = 9903156 kb
// Step: 128-bisimulation, time = 630 milliseconds ms, memory = 10416424 kb
// Step: 129-bisimulation, time = 180 milliseconds ms, memory = 10416424 kb
// Step: 130-bisimulation, time = 170 milliseconds ms, memory = 10416424 kb
// Step: 131-bisimulation, time = 170 milliseconds ms, memory = 10416424 kb
// Step: 132-bisimulation, time = 180 milliseconds ms, memory = 10416424 kb
// Step: 133-bisimulation, time = 190 milliseconds ms, memory = 10478464 kb
// Step: 134-bisimulation, time = 180 milliseconds ms, memory = 10540768 kb
// Step: 135-bisimulation, time = 190 milliseconds ms, memory = 10602808 kb
// Step: 136-bisimulation, time = 180 milliseconds ms, memory = 10665112 kb
// Step: 137-bisimulation, time = 180 milliseconds ms, memory = 10727152 kb
// Step: 138-bisimulation, time = 180 milliseconds ms, memory = 10789456 kb
// Step: 139-bisimulation, time = 180 milliseconds ms, memory = 10851496 kb
// Step: 140-bisimulation, time = 180 milliseconds ms, memory = 10913800 kb
// Step: 141-bisimulation, time = 180 milliseconds ms, memory = 10975840 kb
// Step: 142-bisimulation, time = 180 milliseconds ms, memory = 11038144 kb
// Step: 143-bisimulation, time = 180 milliseconds ms, memory = 11100184 kb
// Step: 144-bisimulation, time = 180 milliseconds ms, memory = 11162488 kb
// Step: 145-bisimulation, time = 180 milliseconds ms, memory = 11224792 kb
// Step: 146-bisimulation, time = 190 milliseconds ms, memory = 11286832 kb
// Total time = {53860000000;49190000000;4670000000} nanoseconds
//         Command being timed: "./full_bisimulation run_timed ../mappingbased-objects_lang=en.ttl"
//         User time (seconds): 51.32
//         System time (seconds): 4.90
//         Percent of CPU this job got: 99%
//         Elapsed (wall clock) time (h:mm:ss or m:ss): 0:56.27
//         Average shared text size (kbytes): 0
//         Average unshared data size (kbytes): 0
//         Average stack size (kbytes): 0
//         Average total size (kbytes): 0
//         Maximum resident set size (kbytes): 11286832
//         Average resident set size (kbytes): 0
//         Major (requiring I/O) page faults: 0
//         Minor (reclaiming a frame) page faults: 4696395
//         Voluntary context switches: 18
//         Involuntary context switches: 454
//         Swaps: 0
//         File system inputs: 0
//         File system outputs: 989944
//         Socket messages sent: 0
//         Socket messages received: 0
//         Signals delivered: 0
//         Page size (bytes): 4096
//         Exit status: 0
