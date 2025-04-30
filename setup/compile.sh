# Load in the settings
. ./settings.config

# Remove commas and add spaces
compiler_flags="${compiler_flags//,/ }"

# Compile the given source code file $1 and store it in the given binary file $2
g++ $compiler_flags -I ${boost_path}include/ $1 -o $2 ${boost_path}lib/libboost_program_options.a