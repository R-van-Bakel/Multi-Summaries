// This file is meant for meanually checking whether the binary data written by "lod_preprocessor.cpp" is correct
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <iostream>

const int BYTES_PER_ENTITY = 5;
const int BYTES_PER_PREDICATE = 4;

u_int64_t read_uint_ENTITY_little_endian(std::istream &inputstream)
{
    char data[8];
    inputstream.read(data, BYTES_PER_ENTITY);
    u_int64_t result = uint64_t(0);

    for (unsigned int i = 0; i < BYTES_PER_ENTITY; i++)
    {
        result |= uint64_t(data[i]) << (i * 8);
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
        result |= uint32_t(data[i]) << (i * 8);
    }
    return result;
}

int main()
{
    std::ifstream infile("./Laurence_Fishburne_Custom_Shuffled.bin", std::ifstream::in);
    for (int i = 0; i < 70; i++)
    {
        u_int64_t subject = read_uint_ENTITY_little_endian(infile);
        u_int32_t predicate = read_PREDICATE_little_endian(infile);
        u_int64_t object = read_uint_ENTITY_little_endian(infile);
        std::cout << subject << "\t" << predicate << "\t" << object << std::endl;
    }
}