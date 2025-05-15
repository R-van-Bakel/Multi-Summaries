
#include <fstream>
#include <string>
// #define BOOST_USE_VALGRIND  // TODO disable this command in the final running version
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <boost/program_options.hpp>

using edge_type = uint32_t;
using node_index = uint64_t;

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;

// This variable should be set in main. If it is set to true, the code will assume the first line will look like "<.*> {" and the last line looks like "}".
// If the first line looks like expected and "}" indicates the last line (i.e. there are no later lines), then our code will just ignore these lines.
// This variable is set to true for the LOD laundromat dataset
bool trigfile;

// This variable should be set in main. It indicates whether or not we want to skip rdf-lists
bool skipRDFlists;

// This variable should be set in main. It indicates whether or not we want to skip literals.
bool skip_literals;

// This variable should be set in main. It indicates whether or not we want to put type information on the predicates instead of the object.
bool types_to_predicates;

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

std::vector<std::string::iterator> parse_tuple(std::string &line)
{
    std::vector<std::string::iterator> indices;
    char searching_char = '\0';
    bool search_for_qualifier = false;
    bool escaped;
    for (std::string::iterator i = line.begin(); i != line.end(); ++i)
    {
        // We have separate code for string qualifiers (e.g. @nl)
        if (!search_for_qualifier)
        {
            // In this case we do not know if the next part is an entity, literal, or a blank node
            if (!searching_char)
            {
                switch (*i)
                {
                    case '<':
                        indices.push_back(i);
                        searching_char = '>';
                        break;
                    case '"':
                        indices.push_back(i);
                        searching_char = '"';
                        break;
                    case '_':
                        indices.push_back(i);
                        searching_char = ' ';
                        break;
                }
            }
            // In this case we know exacly what the next character we are searching for is
            else if (*i == searching_char)
            {
                switch (searching_char)
                {
                case '>':
                    // If the next character is not a space, ignore this character
                    if (!(*(i+1) == ' '))
                    {
                        break;
                    }
                    // For entities and relations the end of contents iterator coincides with the end of qualifiers iterator
                    indices.push_back(i);
                    indices.push_back(i);
                    searching_char = '\0';
                    break;
                case ' ':
                    // For blank nodes the end of contents iterator coincides with the end of qualifiers iterator
                    indices.push_back(i-1);
                    indices.push_back(i-1);
                    searching_char = '\0';
                    break;
                case '"':
                    // Literals may contain escaped " characters
                    escaped = false;
                    std::string::iterator previous_index = i-1;
                    while (*previous_index == '\\')
                    {
                        escaped = !escaped;
                        previous_index--;
                    }
                    if (escaped)
                    {
                        break;
                    }
                    // else
                    searching_char = '\0';
                    // Push the end of contents iterator
                    indices.push_back(i);
                    if (*(i+1) == ' ')
                    {
                        // In this case the end of qualifiers iterator coincides with the end of contents iterator
                        indices.push_back(i);
                    }
                    else
                    {
                        // In this case there is literal qualifier information
                        search_for_qualifier = true;
                    }
                    break;
                }
            }
        }
        else if (*i == ' ')
        {
            // Push the end of qualifiers iterator (for literals)
            indices.push_back(i-1);
            search_for_qualifier = false;
        }
    }
    return indices;
}

template <typename T>
class IDMapper
{
    boost::unordered_flat_map<std::string, T> mapping;

public:
    IDMapper() : mapping(100000000)
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

// u_int64_t read_uint64_little_endian(std::istream &inputstream){
//     char data[8];
//     inputstream.read(data, 8);
//     u_int64_t result = uint64_t(0) ;

//     for (unsigned int i = 0; i < 8; i++){
//         result |= uint64_t(data[i]) << (i*8);
//     }
//     return result;
// }

void write_uint_ENTITY_little_endian(std::ostream &outputstream, u_int64_t value)
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

// u_int32_t read_uint32_little_endian(std::istream &inputstream){
//     char data[4];
//     inputstream.read(data, 4);
//     u_int32_t result = uint32_t(0) ;

//     for (unsigned int i = 0; i < 4; i++){
//         result |= uint32_t(data[i]) << (i*8);
//     }
//     return result;
// }

void write_uint_PREDICATE_little_endian(std::ostream &outputstream, u_int32_t value)
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
        std::cout << "Write predicate failed with code: " << outputstream.rdstate() << std::endl;
        std::cout << "Goodbit: " << outputstream.good() << std::endl;
        std::cout << "Eofbit:  " << outputstream.eof() << std::endl;
        std::cout << "Failbit: " << (outputstream.fail() && !outputstream.bad()) << std::endl;
        std::cout << "Badbit:  " << outputstream.bad() << std::endl;
        exit(outputstream.rdstate());
    }
}

void convert_graph(std::istream &inputstream,
                   std::ostream &outputstream,
                   const std::string &node_ID_file,
                   const std::string &edge_ID_file
)
{
    IDMapper<node_index> node_ID_Mapper;
    IDMapper<edge_type> edge_ID_Mapper;

    // We make sure that the _:literalNode is first in the node IDs. ie. maps to zero
    std::string literal_node_string = "_:literalNode";
    node_ID_Mapper.getID(literal_node_string);

    // We add the _:rdfTypeNode node in case types_to_predicates is set to true
    std::string rdf_type_node_string = "_:rdfTypeNode";
    node_index rdf_type_node_id = node_ID_Mapper.getID(rdf_type_node_string);

    // We make sure that the rdf:type is first in the edge IDs. ie. maps to zero
    std::string rdf_type_string = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    edge_type rdf_type_id = edge_ID_Mapper.getID(rdf_type_string);

    const int BufferSize = 8 * 16184;

    char _buffer[BufferSize];

    inputstream.rdbuf()->pubsetbuf(_buffer, BufferSize);

    std::string line;
    unsigned long line_counter = 0;

    if (trigfile)
    {
        std::getline(inputstream, line);
        boost::trim(line);
        std::string suffix = "> {";
        size_t suffix_len = suffix.size();
        if (line[0] == '<' && line.substr(line.size()-suffix_len, suffix_len) == "> {")
        {
            std::cout << "Skipping the first line, because a `trigfile` has been set to true. The fist line was: " << line << std::endl;
        }
        else
        {
            throw MyException("`trigfile` has been set to true, but the first line (without line break characters) did not have the form \"<.*> {\": " + line);
        }
    }

    bool must_end = false;

    while (std::getline(inputstream, line))
    {
        const std::string original_line(line);

        if (line_counter % 1000000 == 0)
        {
            auto now{boost::chrono::system_clock::to_time_t(boost::chrono::system_clock::now())};
            std::tm* ptm{std::localtime(&now)};
            std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S") << " done with " << line_counter << " triples" << std::endl;
        }
        line_counter++;

        boost::trim(line);
        if (line[0] == '#' || line == "")
        {
            // ignore comment line
            continue;
        }
        if (must_end)
        {
            throw MyException("The file must have ended here, but did not!");
        }
        if (trigfile)
        {
            if (line == "}")
            {
                must_end = true;
                continue;
            }
        }
        
        // Get a vector of indices to the start and end of each element in the tuple
        std::vector<std::string::iterator> string_indices = parse_tuple(line);

        // Check if we got the expected amount of iterators
        if (!(string_indices.size() == 9))
        {
            throw MyException("Wrong number of iterators returned by `parse_tuple`: expected 9 (3 each for subject, predicate and object), but got "
                              + std::to_string(string_indices.size()) + " instead");
        }

        // Get to subject, predicate and object from the provided indices
        // Note that we ignore the end of qualifiers iterators at positions 2, 5 and 8
        std::string subject = std::string(string_indices[0], string_indices[1]+1);
        std::string predicate = std::string(string_indices[3], string_indices[4]+1);
        std::string object = std::string(string_indices[6], string_indices[7]+1);

        // Remove the angle brackets for enitites and remove the underscores for blank nodes
        if (subject.front() == '<' && subject.back() == '>')
        {
            subject = subject.substr(1, subject.size()-2);
        }
        else if (subject.substr(0, 2) == "_:")
        {
            subject = subject.substr(2, subject.size()-2);
        }
        else
        {
            throw MyException("The subject on line " + std::to_string(line_counter) + " could not be identified as an entity or blank node");
        }

        // Remove the angle brackets and for relations
        if (predicate.front() == '<' && predicate.back() == '>')
        {
            predicate = predicate.substr(1, predicate.size()-2);
        }
        else
        {
            throw MyException("The predicate on line " + std::to_string(line_counter) + " could not be identified as a relation");
        }

        // Remove the angle brackets and for enitites, remove the underscores for blank nodes and remove the double quotes for literals
        if (object.front() == '<' && object.back() == '>')
        {
            object = object.substr(1, object.size()-2);
        }
        else if (object.substr(0, 2) == "_:")
        {
            object = object.substr(0, object.size()-2);
        }
        else if (object.front() == '"' && object.back() == '"')
        {
            if (skip_literals)
            {
                continue;
            }
            object = literal_node_string;
        }
        else
        {
            throw MyException("The object on line " + std::to_string(line_counter) + " could not be identified as an entity, blank node or literal");
        }

        // If we want to skip RDF lists
        if (skipRDFlists)
        {
            if (predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#first" ||
                predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest" ||
                object == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil" ||
                object == "http://www.w3.org/1999/02/22-rdf-syntax-ns#List" ||
                subject == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil" ||
                subject == "http://www.w3.org/1999/02/22-rdf-syntax-ns#List" ||
                predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil" ||  // This one and the ones after are less likely to cause problems, but we remove them just in case
                predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#List" ||
                object == "http://www.w3.org/1999/02/22-rdf-syntax-ns#first" ||
                object == "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest" ||
                subject == "http://www.w3.org/1999/02/22-rdf-syntax-ns#first" ||
                subject == "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
            {
                continue;
            }
        }
        
        // subject
        node_index subject_index = node_ID_Mapper.getID(subject);

        // edge
        edge_type edge_index = edge_ID_Mapper.getID(predicate);

        // object
        node_index object_index;

        if (types_to_predicates and (edge_index == rdf_type_id))
        {
            edge_index = edge_ID_Mapper.getID(object);  // TODO there is currently no check to see if this cast is possible (in practice it likely will be possible)
            object_index = rdf_type_node_id;
        }
        else
        {
            object_index = node_ID_Mapper.getID(object);
        }

        // Write the indices in our binary format
        // std::cout << "DEBUG wrote (" << subject << ", " << predicate << ", " << object << ") as (" << subject_index << ", " << edge_index << ", " << object_index << ")" << std::endl;
        write_uint_ENTITY_little_endian(outputstream, subject_index);
        write_uint_PREDICATE_little_endian(outputstream, edge_index);
        write_uint_ENTITY_little_endian(outputstream, object_index);
    }
    if (inputstream.bad())
    {
        perror("error happened while reading file");
    }
    node_ID_Mapper.dump_to_file(node_ID_file);
    edge_ID_Mapper.dump_to_file(edge_ID_file);
}

int main(int ac, char *av[])
{
    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("input_file", po::value<std::string>(), "Input file, must contain n-triples");
    global.add_options()("output_path", po::value<std::string>(), "Output path");
    global.add_options()("skipRDFlists", "Makes the code ignore RDF lists");
    global.add_options()("skip_literals", "Triples with literal objects will be ignored.");
    global.add_options()("laundromat", "Set this flag to run on the LOD laundromat dataset");
    global.add_options()("types_to_predicates", "Transforms triples of the form <subject> <rdf:type> <object> to <subject> <object> _:rdfTypeNode");
    po::positional_options_description pos;
    pos.add("input_file", 1).add("output_path", 2);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string input_file = vm["input_file"].as<std::string>();
    std::string output_path = vm["output_path"].as<std::string>();

    // Set the `skipRDFlists` global variable
    skipRDFlists = vm.count("skipRDFlists");

    // Set the `skip_literals` global variable
    skip_literals = vm.count("skip_literals");

    // Set the `trigfile` global variable
    trigfile = vm.count("laundromat");

    // Set the `types_to_predicates` global variable
    types_to_predicates = vm.count("types_to_predicates");
    
    std::ifstream infile(input_file);
    std::ofstream outfile(output_path + "/binary_encoding.bin", std::ifstream::out);

    if (!infile.is_open())
    {
        perror("error while opening file");
    }

    if (!outfile.is_open())
    {
        perror("error while opening file");
    }

    std::string node_ID_file = output_path + "/entity2ID.txt";
    std::string rel_ID_file = output_path + "/rel2ID.txt";

    convert_graph(infile, outfile, node_ID_file, rel_ID_file);
    outfile.flush();
}