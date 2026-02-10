#pragma once
#include <cstdint>
#include <limits>
#include <iosfwd>

using edge_type = uint32_t;
using node_index = uint64_t;
using block_index = node_index;
using block_or_singleton_index = int64_t;
using k_type = uint16_t;

inline constexpr int BYTES_PER_ENTITY = 5;
inline constexpr int BYTES_PER_PREDICATE = 4;
inline constexpr int BYTES_PER_BLOCK = 4;
inline constexpr int BYTES_PER_BLOCK_OR_SINGLETON = 5;
inline constexpr int BYTES_PER_K_TYPE = 2;

inline constexpr block_or_singleton_index MAX_SIGNED_BLOCK_SIZE = std::numeric_limits<block_or_singleton_index>::max();
inline constexpr edge_type MAX_EDGE_ID = std::numeric_limits<edge_type>::max();

node_index read_uint_ENTITY_little_endian(std::istream &inputstream);
edge_type read_uint_PREDICATE_little_endian(std::istream &inputstream);
block_index read_uint_BLOCK_little_endian(std::istream &inputstream);
block_or_singleton_index read_int_BLOCK_OR_SINGLETON_little_endian(std::istream &inputstream);
k_type read_uint_K_TYPE_little_endian(std::istream &inputstream);

void write_uint_ENTITY_little_endian(std::ostream &outputstream, node_index value);
void write_uint_PREDICATE_little_endian(std::ostream &outputstream, edge_type value);
void write_uint_BLOCK_little_endian(std::ostream &outputstream, block_index value);
void write_int_BLOCK_OR_SINGLETON_little_endian(std::ostream &outputstream, block_or_singleton_index value);
void write_uint_K_TYPE_little_endian(std::ostream &outputstream, k_type value);