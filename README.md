# Condensed Summaries
# Preface
This project contains ***Rust*** code for computing the condensed multi-level bisimulation of edge-labelled graphs. We also provide C++ code for preprocessing ntriples (`.nt`) data into a representation suitable for input to the Rust code. The main, Rust, code can be found in `code/rust`, whereas the C++ preprocessing code can be found in `code/cpp`.

## Important Notes
- The code is built for ***Linux*** systems and will likely not run on other systems as is.

## Installation instructions
### C++ Setup
1. Make sure g++ (>= 13.0.0) is installed
2. Move to `code/cpp/include/`. This is where we will install *boost* and *nlohmann*'s JSON for C++.
    - If you already have valid boost and nlohmann JSON versions installed, then you can overwrite `include_path` and `boost_path` in `setup/settings.config` and skip to step 9.
3. Download Boost:
```bash
wget https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.gz
```
4. Unpack boost:
```bash
tar -v --strip-components=1 -xf ./boost_1_90_0.tar.gz
```
5. Make a directory called `nlohmann/` and download nlohmann's json.hpp into it:
```bash
mkdir nlohmann
cd nlohmann
wget https://github.com/nlohmann/json/releases/download/v3.12.0/json.hpp
```
6. At this point, the file tree might look something like this:
```text
Condensed-Summaries/
├─ code/cpp/include/
│  ├─ boost_1_90_0/
│  ├─ nlohmann/
│  │  └─ json.hpp
│  └─ my_exception.hpp
├─ data/
├─ setup/
└─ README.md
```
7. In the unpacked boost directory (`code/cpp/include/boost_1_90_0/`) run:
```bash
./bootstrap.sh --prefix=./ && ./b2 install --with-filesystem --with-program_options
```
9. Finally, to compile the C++ preprocessor, run the following from the project root:
```bash
mkdir ./code/cpp/bin/
cd ./setup/
./compile.sh ../code/src/preprocessor.cpp ../code/cpp/src/my_exception.cpp ../code/cpp/src/binary_io.cpp -o ../code/cpp/bin/preprocessor
```

### Rust setup
1. Make sure Rust is installed. This can be done via:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
2. Move to `code/rust/`
3. Run:
```bash
RUSTFLAGS="-C lto=thin -C embed-bitcode=yes -Clinker-plugin-lto" cargo build --target x86_64-unknown-linux-gnu --release
```

## Test the code
1. Take any ntriples file (e.g. the provided `data/heterogeneous_hubs.nt`) as input and optionally create an output directory (e.g. `heterogeneous_hubs/`).
2. From the project root run the following to preprocess the data:
```bash
./code/cpp/bin/preprocessor <path/to/ntriples/file> <path/to/output/directory/>
```
3. At this point, the file tree might look something like this:
```text
Condensed-Summaries/
├─ code/
├─ data/
│  └─ heterogeneous_hubs.nt
├─ heterogeneous_hubs/
│  ├─ binary_encoding.bin/
│  ├─ entity2ID.meta.json/
│  ├─ entity2ID.txt/
│  ├─ rel2ID.meta.json/
│  └─ rel2ID.txt/
├─ setup/
└─ README.md
```
4. Finally, from the project root run the following to run the bisimulation:
```bash
./code/rust/target/x86_64-unknown-linux-gnu/release/multi_summaries --rel-to-id-file <path/to/output>/rel2ID.txt <path/to/output>/binary_encoding.bin <path/to/output>/rust_out
```
This should create a directory called `rust_out` in the specified output directory. The data edges are stored in `rust_out/data_edges` in a single file, whereas the refines edges are stored per bisimulation level in `rust_out/refines_edges/`. Both are stored in binary format. The output also contains statistics on the bisimulation in `rust_out/statistics.json`, as well as time and memory instrumentation in `rust_out/instrumentation.json`.
- Note that `./code/rust/src/main.rs` tries to allocate considerable memory via `Graph::new(1_000_000_000)`. If the program runs out of memory, try setting it to a lower number and recompile Rust.