#include <string>
#include <fstream>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include "../include/my_exception.hpp"
#include "../include/binary_io.hpp"

using json = nlohmann::json;

using triple_index = int64_t;
using interval_map_type = boost::unordered_flat_map<block_or_singleton_index,std::pair<k_type,k_type>>;
using block_set = boost::unordered_flat_set<block_or_singleton_index>;
using block_map = boost::unordered_flat_map<block_or_singleton_index,block_or_singleton_index>;
using local_to_global_map_type = boost::unordered_flat_map<block_or_singleton_index,block_or_singleton_index>;

struct Triple {
    block_or_singleton_index s;
    edge_type p;
    block_or_singleton_index o;

    bool operator==(Triple const& other) const {
        return s == other.s &&
               p == other.p &&
               o == other.o;
    }
};

struct TripleHash {
    std::size_t operator()(Triple const& t) const noexcept {
        std::size_t seed = 0;
        boost::hash_combine(seed, t.s);
        boost::hash_combine(seed, t.p);
        boost::hash_combine(seed, t.o);
        return seed;
    }
};

using triple_set = boost::unordered_flat_set<Triple, TripleHash>;

// This class is meant to load and use the local to global maps
// At layers 1 and above some of the blocks might have disappeared (i.e. when a block turns into only singletons and is not reused)
// However, in the LocalToGlobalMaps their keys and last-valid values are still present
// This means that the for levels 1 and above the current_map_ or next_map_ may contain invalid keys, but all valid keys should still have correct values
class LocalToGlobalMaps
{
private:
    std::vector<local_to_global_map_type> local_to_global_maps_;
    k_type current_level_;
    local_to_global_map_type current_map_;
    local_to_global_map_type next_map_;

    void compute_next_map_()
    {
        next_map_ = current_map_;
        for (auto& kv : local_to_global_maps_[current_level_+1])
        {
            next_map_.insert_or_assign(kv.first, kv.second);
        }
    }
public:
    LocalToGlobalMaps(k_type final_depth)
    {
        if (final_depth < 1) throw std::invalid_argument("final_depth must be at least 1");
        local_to_global_maps_.resize(final_depth+1);
    }

    void load_local_to_global_maps(std::ifstream& local_to_global_maps_file)
    {
        while (true)
        {
            k_type level = read_uint_K_TYPE_little_endian(local_to_global_maps_file);
            if (!local_to_global_maps_file)
            {
                if (local_to_global_maps_file.eof()) break;  // clean EOF after a complete read
                throw std::ios_base::failure("Input error while reading file");
            }
            block_or_singleton_index local_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_maps_file);
            block_or_singleton_index global_block = read_int_BLOCK_OR_SINGLETON_little_endian(local_to_global_maps_file);
            local_to_global_maps_.at(level)[local_block] = global_block;
        }
        reset_current_map();  // Set the current map
    }

    void reset_current_map()
    {
        current_level_ = 0;
        current_map_ = local_to_global_map_type(local_to_global_maps_[current_level_]);
        compute_next_map_();
    }

    void increment_current_map()
    {
        if (current_level_+1 >= static_cast<k_type>(local_to_global_maps_.size()-1)) throw std::out_of_range("increment_current_map() cannot increment current_level_, as next_map_ would exceed the final depth");
        current_level_++;
        current_map_ = std::move(next_map_);
        compute_next_map_();
    }

    const local_to_global_map_type& get_current_map() const
    {
        return current_map_;
    }

    const local_to_global_map_type& get_next_map() const
    {
        return next_map_;
    }
};

class StratifiedRefinesMaps
{
private:
    k_type final_depth_;
    std::vector<block_map> refines_maps_;

    void load_refines_map_(std::ifstream& refines_map_file, k_type k, const LocalToGlobalMaps& local_to_global_maps)
    {
        while (true)
        {
            block_index block = read_uint_BLOCK_little_endian(refines_map_file);
            if (!refines_map_file)
            {
                if (refines_map_file.eof()) break;  // clean EOF after a complete read
                throw std::ios_base::failure("Input error while reading file");
            }
            block_or_singleton_index global_block = local_to_global_maps.get_current_map().at(static_cast<block_or_singleton_index>(block));
            block_index new_block_count = read_uint_BLOCK_little_endian(refines_map_file);
            for (block_index i = 0; i < new_block_count; i++)
            {
                block_index new_block = read_uint_BLOCK_little_endian(refines_map_file);
                if (new_block == 0)
                {
                    continue;
                }
                block_or_singleton_index global_new_block = local_to_global_maps.get_next_map().at(static_cast<block_or_singleton_index>(new_block));
                if (new_block != 0)
                {
                    refines_maps_[k][global_new_block] = global_block;
                }
            }
        }
    }

    void load_singleton_refines_map_(std::ifstream& singleton_refines_map_file, k_type k, const LocalToGlobalMaps& local_to_global_maps)
    {
        while (true)
        {
            block_index block = read_uint_BLOCK_little_endian(singleton_refines_map_file);
            if (!singleton_refines_map_file)
            {
                if (singleton_refines_map_file.eof()) break;  // clean EOF after a complete read
                throw std::ios_base::failure("Input error while reading file");
            }
            block_or_singleton_index global_block = local_to_global_maps.get_current_map().at(static_cast<block_or_singleton_index>(block));
            block_or_singleton_index new_block_count = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_refines_map_file);
            for (block_or_singleton_index i = 0; i < new_block_count; i++)
            {
                block_or_singleton_index new_singleton = read_int_BLOCK_OR_SINGLETON_little_endian(singleton_refines_map_file);
                refines_maps_[k][new_singleton] = global_block;
            }
        }
    }

    Triple map_triple_(const Triple& data_edge, k_type k) const
    {
        auto it_end = refines_maps_[k].cend();
        auto it_s = refines_maps_[k].find(data_edge.s);
        block_or_singleton_index mapped_s = (it_s != it_end) ? it_s->second : data_edge.s;  // Only map if the respective refines edge is found
        auto it_o = refines_maps_[k].find(data_edge.o);
        block_or_singleton_index mapped_o = (it_o != it_end) ? it_o->second : data_edge.o;  // Only map if the respective refines edge is found
        return Triple{mapped_s,data_edge.p,mapped_o};
    }

    Triple map_offset_triple_(const Triple& data_edge, k_type k) const
    {
        auto it_end = refines_maps_[k].cend();
        auto it_s = refines_maps_[k].find(data_edge.s);
        block_or_singleton_index mapped_s = (it_s != it_end) ? it_s->second : data_edge.s;  // Only map if the respective refines edge is found
        return Triple{mapped_s,data_edge.p,data_edge.o};
    }
    
    triple_set map_triple_set_(triple_set& data_edges, k_type k, bool obj_already_at_k) const
    {
        triple_set mapped_data_edges;
        for (auto it = data_edges.begin(); it != data_edges.end(); ) {
            Triple edge = *it;
            it = data_edges.erase(it);
            Triple mapped_edge = obj_already_at_k ? map_offset_triple_(edge, k) : map_triple_(edge, k);
            mapped_data_edges.insert(mapped_edge);
        }
        return mapped_data_edges;
    }
public:
    StratifiedRefinesMaps(k_type final_depth, bool fixed_point_reached)
        : final_depth_(final_depth),
          refines_maps_(fixed_point_reached ? final_depth + 1 : final_depth)
    {
    }

    void load_refines_maps(std::string path_to_refines_maps, LocalToGlobalMaps& local_to_global_maps)
    {
        for (k_type k = 1; k <= final_depth_; k++)
        {
            if (k > 1) local_to_global_maps.increment_current_map();  // Don't increment before the first iteration

            std::ostringstream k_previous_stringstream;
            k_previous_stringstream << std::setw(4) << std::setfill('0') << k-1;
            std::string k_previous_string(k_previous_stringstream.str());

            std::ostringstream k_current_stringstream;
            k_current_stringstream << std::setw(4) << std::setfill('0') << k;
            std::string k_current_string(k_current_stringstream.str());

            std::ifstream refines_map_file(path_to_refines_maps + "mapping-" + k_previous_string + "to" + k_current_string + ".bin");
            load_refines_map_(refines_map_file, k-1, local_to_global_maps);

            std::ifstream singleton_refines_map_file(path_to_refines_maps + "singleton_mapping-" + k_previous_string + "to" + k_current_string + ".bin");
            if (!singleton_refines_map_file) continue;  // If the file can't be read, assume it doesn't exist and continue
            load_singleton_refines_map_(singleton_refines_map_file, k-1, local_to_global_maps);
        }
    }

    const block_map& get_refines_map(k_type k) const
    {
        return refines_maps_[k];
    }

    std::vector<block_or_singleton_index> calculate_quotient_graph_data_edge_counts_destructive(triple_set&& data_edges) const
    {
        return calculate_quotient_graph_data_edge_counts_destructive(std::move(data_edges), final_depth_);
    }

    std::vector<block_or_singleton_index> calculate_quotient_graph_data_edge_counts_destructive(triple_set&& data_edges, k_type subjects_level) const
    {
        triple_set current_data_edges = std::move(data_edges);  // Consume the data edges
        std::vector<block_or_singleton_index> data_edge_counts(final_depth_+1,-1);
        bool object_already_mapped = true;
        for (block_or_singleton_index k = subjects_level; k >= 0; k--)
        {
            current_data_edges = map_triple_set_(current_data_edges, k, object_already_mapped);
            data_edge_counts[k] = static_cast<block_or_singleton_index>(current_data_edges.size());
            object_already_mapped = false;
        }
        return data_edge_counts;
    }
};

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
    std::string graph_stats_file_string = experiment_directory + "ad_hoc_results/graph_stats.json";
    std::ifstream graph_stats_file(graph_stats_file_string);

    json graph_stats;
    graph_stats_file >> graph_stats;

    k_type final_depth = graph_stats["Final depth"];
    bool fixed_point_reached = graph_stats["Fixed point"];

    // Read the intervals
    std::cout << "Reading intervals" << std::endl;
    std::string intervals_file = experiment_directory + "bisimulation/condensed_multi_summary_intervals.bin";
    std::ifstream intervals_file_stream(intervals_file, std::ifstream::in);

    interval_map_type interval_map;

    // Read an interval file
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
    intervals_file_stream.close();

    // Set up the local to global maps for creating the refines maps right after
    std::string local_to_global_file_string = experiment_directory + "bisimulation/condensed_multi_summary_local_global_map.bin";
    std::ifstream local_to_global_file(local_to_global_file_string);
    LocalToGlobalMaps local_to_global_maps(final_depth);
    local_to_global_maps.load_local_to_global_maps(local_to_global_file);
    local_to_global_file.close();

    // Set up the stratified refines map
    StratifiedRefinesMaps refines_maps{final_depth, fixed_point_reached};
    refines_maps.load_refines_maps(experiment_directory + "bisimulation/", local_to_global_maps);

    // Read the data edges
    std::string data_edges_file_string = experiment_directory + "bisimulation/condensed_multi_summary_graph.bin";
    std::ifstream data_edges_file(data_edges_file_string, std::ifstream::in);
    std::vector<uint64_t> condensed_data_edge_counters(final_depth+2, 0);  // [0,...,final_depth+1]
    std::vector<uint64_t> uncondensed_data_edge_counters(final_depth+2, 0);  // [0,...,final_depth+1]
    triple_set current_data_edges;
    while (true)
    {
        block_or_singleton_index subject = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file);
        if (data_edges_file.eof())
        {
            break;
        }
        edge_type predicate = read_uint_PREDICATE_little_endian(data_edges_file);
        block_or_singleton_index object = read_int_BLOCK_OR_SINGLETON_little_endian(data_edges_file);

        k_type subject_start = interval_map[subject].first;
        k_type subject_end = interval_map[subject].second;
        k_type object_start = interval_map[object].first;
        k_type object_end = interval_map[object].second;

        k_type offset_subject_start;
        if (subject_start == 0)  // We add this if-statement to prevent underflow errors
        {
            offset_subject_start = subject_start;
        }
        else
        {
            offset_subject_start = subject_start-1;
        }
        k_type offset_object_end = object_end+1;  // We could add an overflow check, but these are unlikely to occur

        k_type edge_start = std::max(offset_subject_start,object_start);
        k_type edge_end = std::min(subject_end,offset_object_end);

        // Increment the per-level condensed data edge counters
        // Because the same data edge might appear several times in the uncondensed multi-summary, we keep track of the increasing number of occurences at each level via `uncondensed_weight` 
        k_type uncondensed_weight = 0;
        for (k_type i = edge_start+1; i <= final_depth; i++)
        {
            if (i <= edge_end) uncondensed_weight++;  // Stop increasing the uncondensed_weight when `i == edge_end`, because `edge_end` is the last level at which a new copy of the edge is created in the uncondensed representation
            condensed_data_edge_counters[i]++;
            uncondensed_data_edge_counters[i] += uncondensed_weight;
        }
        // If the fixed point is reached, then there will be an extra edge from level k to level k.
        // This edge represents an edge that will be present between all layers beyond the fixed point (e.g. between k+1 and k).
        if (fixed_point_reached)
        {
            condensed_data_edge_counters[final_depth+1]++;

            // If both the subject and object exist at the final level, then the actual edge_end is infinity (instead of `final_depth`) and as such we should still increase `uncondensed_weight` for level k+1
            if (subject_end == final_depth && object_end == final_depth)
            {
                uncondensed_weight++;
                current_data_edges.emplace(Triple{subject,predicate,object});
            }
            uncondensed_data_edge_counters[final_depth+1] += uncondensed_weight;
        }
    }

    // Compute the number of data edges in all quotient graphs
    std::cout << "Computing quotient data edge counts (expensive)" << std::endl;
    std::vector<block_or_singleton_index> data_edge_counts = refines_maps.calculate_quotient_graph_data_edge_counts_destructive(std::move(current_data_edges));

    // Count condensed refines edges
    boost::unordered_flat_map<k_type,block_index> condensed_refines_counts;
    condensed_refines_counts[0] = 0;  // Because there is only a single layer at k=0, there are no refines edges this deep into the multi-summary
    for (k_type current_level = 1; current_level <= final_depth; current_level++)
    {
        condensed_refines_counts[current_level] = condensed_refines_counts[current_level-1];

        std::ostringstream k_previous_stringstream;
        k_previous_stringstream << std::setw(4) << std::setfill('0') << current_level-1;
        std::string k_previous_string(k_previous_stringstream.str());

        std::ostringstream k_current_stringstream;
        k_current_stringstream << std::setw(4) << std::setfill('0') << current_level;
        std::string k_current_string(k_current_stringstream.str());

        // Read a mapping file
        std::string refines_file_string = experiment_directory + "bisimulation/mapping-" + k_previous_string + "to" + k_current_string + ".bin";
        std::ifstream refines_file(refines_file_string);
        while (true)
        {
            read_uint_BLOCK_little_endian(refines_file);  // Read and ignore a block id. We need to perform a read to update the eof() flag, even if we ignore the contents of the read operation.
            if (refines_file.eof())
            {
                break;
            }
            
            // TODO This can be done much cleaner with seekg if singleton blocks are just part of the regular mapping files
            block_index new_block_count = read_uint_BLOCK_little_endian(refines_file);
            for (block_index i = 0; i < new_block_count; i++)
            {
                block_index new_block = read_uint_BLOCK_little_endian(refines_file);
                if (new_block != 0)
                {
                    condensed_refines_counts[current_level]++;
                }
            }
        }

        // Read a singleton mapping file
        // TODO this is not needed if singleton blocks are just part of the regular mapping files
        std::string refines_singletons_file_string = experiment_directory + "bisimulation/singleton_mapping-" + k_previous_string + "to" + k_current_string + ".bin";
        std::ifstream refines_singletons_file(refines_singletons_file_string);
        if (!refines_singletons_file) continue;  // If the file can't be read, assume it doesn't exist and continue

        while (true)
        {
            read_uint_BLOCK_little_endian(refines_singletons_file);  // Read and ignore a block id. We need to perform a read to update the eof() flag, even if we ignore the contents of the read operation.
            if (refines_singletons_file.eof())
            {
                break;
            }

            block_or_singleton_index new_block_count = read_int_BLOCK_OR_SINGLETON_little_endian(refines_singletons_file);
            condensed_refines_counts[current_level] += new_block_count;
            refines_singletons_file.seekg(new_block_count*BYTES_PER_BLOCK_OR_SINGLETON, std::ios::cur);  // Skip over the singleton block ids, as we only care about the count.
        }
    }

    // Create the full stats JSON object
    // TODO we are storing repeated data intentionally (e.g. how "Singleton count (condensed)" is always equal to Singleton count (quotient)). These constraints could be encoded with JSON schema with the Ajv validator extensions.
    std::cout << "Creating full stats file" << std::endl;
    json full_stats;

    // Load and store the outcome statistics (number of blocks and singletons per layer)
    block_index uncondensed_vertex_count = 0;
    block_index uncondensed_singleton_count = 0;
    block_index uncondensed_refines_count = 0;

    full_stats["Vertex count (condensed)"] = json::array();
    full_stats["Vertex count (uncondensed)"] = json::array();
    full_stats["Vertex count (quotient)"] = json::array();
    full_stats["Singleton count (condensed)"] = json::array();
    full_stats["Singleton count (quotient)"] = json::array();
    full_stats["Singleton count (uncondensed)"] = json::array();
    full_stats["Refines edge count (uncondensed)"] = json::array();

    full_stats["Refines edge count (uncondensed)"].push_back(0);
    for (k_type current_level = 0; current_level <= final_depth; current_level++)
    {
        std::ostringstream k_stringstream;
        k_stringstream << std::setw(4) << std::setfill('0') << current_level;
        std::string k_string(k_stringstream.str());
        
        std::string outcome_stats_file_string = experiment_directory + "ad_hoc_results/statistics_condensed-" + k_string + ".json";
        std::ifstream outcome_stats_file(outcome_stats_file_string);
        json outcome_stats;
        outcome_stats_file >> outcome_stats;
        outcome_stats_file.close();

        uncondensed_vertex_count += outcome_stats["Block count"].get<block_index>();
        uncondensed_singleton_count += outcome_stats["Singleton count"].get<block_index>();

        full_stats["Vertex count (condensed)"].push_back(outcome_stats["Accumulated block count"]);
        full_stats["Vertex count (uncondensed)"].push_back(uncondensed_vertex_count);
        full_stats["Vertex count (quotient)"].push_back(outcome_stats["Block count"]);
        full_stats["Singleton count (condensed)"].push_back(outcome_stats["Singleton count"]);
        full_stats["Singleton count (quotient)"].push_back(outcome_stats["Singleton count"]);
        full_stats["Singleton count (uncondensed)"].push_back(uncondensed_singleton_count);

        if (current_level > 0)
        {
            uncondensed_refines_count += outcome_stats["Block count"].get<block_index>();
            full_stats["Refines edge count (uncondensed)"].push_back(uncondensed_refines_count);
        }
    }

    // Store the earlier loaded and newly computed stats
    full_stats["Final depth"] = final_depth;
    full_stats["Fixed point reached"] = fixed_point_reached;
    full_stats["Data edge count (condensed)"] = json::array();
    full_stats["Data edge count (uncondensed)"] = json::array();
    full_stats["Data edge count (quotient)"] = json::array();
    full_stats["Refines edge count (condensed)"] = json::array();
    for (k_type current_level = 0; current_level <= final_depth; current_level++)
    {
        full_stats["Data edge count (condensed)"].push_back(condensed_data_edge_counters[current_level]);
        full_stats["Data edge count (uncondensed)"].push_back(uncondensed_data_edge_counters[current_level]);
        full_stats["Data edge count (quotient)"].push_back(data_edge_counts[current_level]);
        full_stats["Refines edge count (condensed)"].push_back(condensed_refines_counts[current_level]);
    }
    // For the (condensed and uncondensed) multi-summaries we track data edge counts for one level beyond the fixed point (i.e. the data edges going from final_depth+1 to final_depth)
    full_stats["Data edge count (condensed)"].push_back(condensed_data_edge_counters[final_depth+1]);
    full_stats["Data edge count (uncondensed)"].push_back(uncondensed_data_edge_counters[final_depth+1]);
    
    // Write the full stats to a JSON file
    std::string full_stats_file_string = experiment_directory + "ad_hoc_results/full_stats.json";
    std::ofstream full_stats_file(full_stats_file_string);
    full_stats_file << full_stats.dump(4);
    full_stats_file.close();
}