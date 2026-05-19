# Installation instructions

## C++ Setup
1. Make sure g++ (>= 13.0.0) is installed
2. Choose a directory to install boost in
3. Download Boost: `https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.gz`
4. Unpack boost: `tar -v --strip-components=1 -xf ./boost_1_90_0.tar.gz`
5. Set `boost_path` in `./setup/settings.config` to the chosen directory (e.g. `boost_path=../external/boost_1_90_0/`)
6. In the chosen boost path run: `./bootstrap.sh --prefix=./ && ./b2 install --with-filesystem --with-program_options`
7. Choose a directory to install other dependencies in
8. Download json.hpp from nlohmann: https://github.com/nlohmann/json/releases/download/v3.12.0/json.hpp into the chosen directory
9. Set `include_path` in `./setup/settings.config` to the chosen directory (e.g. `include_path=../external/include/`)
10. From the project root run `(mkdir ./code/bin/ && cd ./setup/ && ./compile.sh ../code/src/preprocessor.cpp ../code/src/my_exception.cpp ../code/src/binary_io.cpp -o ../code/bin/preprocessor)`

## Rust setup
1. Install rust: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. Move to `./code/rust/`
3. Run `RUSTFLAGS="-C lto=thin -C embed-bitcode=yes -Clinker-plugin-lto" cargo build --target x86_64-unknown-linux-gnu --release`

## Test the code
1. Take any ntriples file (`.nt`) as input and optionally create an output directory
2. Move to the project root
3. Run `./code/bin/preprocessor <path/to/ntriples/file> <path/to/output>`
4. Run `./code/rust/target/x86_64-unknown-linux-gnu/release/multi_summaries --rel-to-id-file <path/to/output>/rel2ID.txt <path/to/output>/binary_encoding.bin <path/to/output>/rust_out`
    - Note that `./code/rust/src/main.rs` tries to allocate considerable memory via `Graph::new(1_000_000_000)`. If the program runs out of memory, try setting it to a lower number and recompile Rust.