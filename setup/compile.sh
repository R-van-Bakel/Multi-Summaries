# Load in the settings
. ./settings.config

boost_path=$(realpath "${boost_path}")/

# Remove commas and add spaces
compiler_flags="${compiler_flags//,/ }"

# Remove commas and add spaces
boost_flags="${boost_flags//,/ }"

# Parse arguments
output=""
sources=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -o)
            output="$2"
            shift 2
            ;;
        *)
            sources+=("$1")
            shift
            ;;
    esac
done

# Validate output
if [[ -z "$output" ]]; then
    echo "Error: You must specify an output file using -o <file>"
    exit 1
fi

# Validate sources
if [[ ${#sources[@]} -eq 0 ]]; then
    echo "Error: No source files provided"
    exit 1
fi

# Compile the given source code file $1 and store it in the given binary file $2
g++ \
    $compiler_flags \
    -I "${boost_path}include/" \
    -I "${include_path}" \
    "${sources[@]}" \
    -Wl,-rpath-link,"${boost_path}lib" \
    -Wl,-rpath,"${boost_path}lib" \
    -L "${boost_path}lib/" \
    $boost_flags \
    -o "$output" \
    "${boost_path}lib/libboost_program_options.a"
