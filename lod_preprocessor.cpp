
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#define BOOST_CHRONO_HEADER_ONLY
#include <boost/chrono.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>

using edge_type = uint32_t;
using node_index = uint64_t;

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;

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
}

void convert_graph(std::istream &inputstream,
                   std::ostream &outputstream,
                   const std::string &node_ID_file,
                   const std::string &edge_ID_file
)
{
    IDMapper<node_index> node_ID_Mapper;
    IDMapper<edge_type> edge_ID_Mapper;

    // We make sure that the bisimulation:string is first in the IDs. ie. maps to zero
    std::string bisimulation_string = "bisimulation:string";
    node_ID_Mapper.getID(bisimulation_string);

    const int BufferSize = 8 * 16184;

    char _buffer[BufferSize];

    inputstream.rdbuf()->pubsetbuf(_buffer, BufferSize);

    std::string line;
    unsigned long line_counter = 0;

    std::getline(inputstream, line);
    boost::trim(line);
    if (line != "<https://krr.triply.cc/krr/lod-a-lot/graphs/default> {")
    {
        throw MyException("This binary is specific for the full bisimulation of krr.triply.cc/krr/lod-a-lot/");
    }

    bool must_end = false;

    while (std::getline(inputstream, line))
    {
        const std::string original_line(line);
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
        if (line == "}")
        {
            must_end = true;
            continue;
        }
        if (!(*(line.cend() - 1) == '.'))
        {
            throw MyException("The line '" + original_line + "' did not end in a period(.)");
        }
        if (!(*(line.cend() - 2) == ' '))
        {
            throw MyException("The line '" + original_line + "' did not end in a space before the period(.)");
        }
        line = line.substr(0, line.length() - 2);
        boost::trim(line);

        // subject
        // split in 2 pieces
        size_t delimeter_start1 = line.find("> <");
        std::string subject = line.substr(0, delimeter_start1);
        std::string predicate_object = line.substr(delimeter_start1 + std::string("> <").size(), line.size());

        if (subject[0] != '<')
        {
            throw MyException("The subject '" + subject + "' did not start with a '<'");
        }
        // The closing bracket is already off
        subject = subject.substr(1, subject.size());

        // predicate
        // split in 2 pieces

        // std::vector<std::string> parts2;
        // boost::iter_split(parts2, predicate_object, boost::first_finder("> "));
        size_t delimeter_start2 = predicate_object.find("> ");
        std::string predicate = predicate_object.substr(0, delimeter_start2);
        std::string object_literal_or_entity = predicate_object.substr(delimeter_start2 + std::string("> ").size(), predicate_object.size());

        if (predicate[0] == '<' || *(predicate.cend() - 1) == '>')
        {
            throw MyException("The predicate '" + predicate + "' did start with a double '<' or end with a double '>'");
        }

        // object
        // final part is the object
        boost::trim(object_literal_or_entity);

        std::string object;
        if (object_literal_or_entity[0] == '"')
        {
            // it is a literal
            object = bisimulation_string;
        }
        else if (object_literal_or_entity[0] == '<')
        {
            // it is an entity
            if (!( *(object_literal_or_entity.cend() - 1) == '>'))
            {
                throw MyException("The object '" + object_literal_or_entity + "' started with a '<' , but did not end with a '>'");
            }
            object = object_literal_or_entity.substr(1, object_literal_or_entity.size() - 1);
        }
        else
        {
            throw MyException("The object '" + object_literal_or_entity + "' did not start with \" or <");
        }

        // subject
        node_index subject_index = node_ID_Mapper.getID(subject);
        // object
        node_index object_index = node_ID_Mapper.getID(object);
        // edge
        edge_type edge_index = edge_ID_Mapper.getID(predicate);

        // output the line
        // This should be binary writing instead.
        write_uint_ENTITY_little_endian(outputstream, subject_index);
        write_uint_PREDICATE_little_endian(outputstream, edge_index);
        write_uint_ENTITY_little_endian(outputstream, object_index);

        if (line_counter % 1000000 == 0)
        {
            
            auto now{boost::chrono::system_clock::to_time_t(boost::chrono::system_clock::now())};
            std::tm* ptm{std::localtime(&now)};
            std::cout << std::put_time(ptm, "%Y/%m/%d %H:%M:%S") << " done with " << line_counter << " triples" << std::endl;
        }
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
    std::ifstream infile("./Laurence_Fishburne_Custom_Shuffled.trig");
    if (!infile.is_open())
    {
        perror("error while opening file");
    }
    std::ofstream output_file("./Laurence_Fishburne_Custom_Shuffled.bin", std::ifstream::out);
    if (!output_file.is_open())
    {
        perror("error while opening file");
    }

    std::string node_ID_file = "./entity2ID.txt";
    std::string rel_ID_file = "./rel2ID.txt";

    convert_graph(infile, output_file, node_ID_file, rel_ID_file);
    output_file.flush();
}