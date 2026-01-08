# Load in the settings
. ./settings.config

# Remove commas and add spaces
compiler_flags="${compiler_flags//,/ }"

# Remove commas and add spaces
boost_flags="${boost_flags//,/ }"

# Compile the given source code file $1 and store it in the given binary file $2
g++ $compiler_flags -I ${boost_path}include/ -I ${include_path} $1 -L ${boost_path}lib/ $boost_flags -o $2 ${boost_path}lib/libboost_program_options.a
