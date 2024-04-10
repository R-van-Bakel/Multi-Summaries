#include <fstream>
#include <string>
#include <iostream>
#include <boost/program_options.hpp>

// Allocate space for a triple's worth of indices (6)
size_t* parse_triple(std::string line)
{
    size_t* indices = new size_t[6];
    char searching_char = '\0';
    size_t current_index = 0;
    bool search_for_qualifier = false;
    bool escaped;
    for (size_t i = 0; i < line.length(); ++i)
    {
        // We have separate code for string qualifiers (e.g. @nl)
        if (!search_for_qualifier)
        {
            // In this case we do not know if the next part is an entity, literal, or a blank node
            if (!searching_char)
            {
                switch (line[i])
                {
                    case '<':
                        indices[current_index] = i;
                        searching_char = '>';
                        current_index++;
                        break;
                    case '"':
                        indices[current_index] = i;
                        searching_char = '"';
                        current_index++;
                        break;
                    case '_':
                        indices[current_index] = i;
                        searching_char = ' ';
                        current_index++;
                        break;
                }
            }
            // In this case we know exacly what the next character we are searching for is
            else if (line[i] == searching_char)
            {
                switch (searching_char)
                {
                case '>':
                    // If the next character is not a space, ignore this character
                    if (!(line[i+1] == ' '))
                    {
                        break;
                    }
                    indices[current_index] = i;
                    searching_char = '\0';
                    current_index++;
                    break;
                case ' ':
                    indices[current_index] = i-1;
                    searching_char = '\0';
                    current_index++;
                    break;
                case '"':
                    // Literals may contain escaped " characters
                    escaped = false;
                    size_t previous_index = i-1;
                    while (line[previous_index] == '\\')
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
                    if (line[i+1] == ' ')
                    {
                        indices[current_index] = i;
                        current_index++;
                    }
                    else
                    {
                        search_for_qualifier = true;
                    }
                    break;
                }
            }
        }
        else if (line[i] == ' ')
        {
            indices[current_index] = i-1;
            search_for_qualifier = false;
            current_index++;
        }
    }
    return indices;
}

// Allocate space for a quad's worth of indices (8)
size_t* parse_quad(std::string line)
{
    size_t* indices = new size_t[8];
    char searching_char = '\0';
    size_t current_index = 0;
    bool search_for_qualifier = false;
    bool escaped;
    for (size_t i = 0; i < line.length(); ++i)
    {
        // We have separate code for string qualifiers (e.g. @nl)
        if (!search_for_qualifier)
        {
            // In this case we do not know if the next part is an entity, literal, or a blank node
            if (!searching_char)
            {
                switch (line[i])
                {
                    case '<':
                        indices[current_index] = i;
                        searching_char = '>';
                        current_index++;
                        break;
                    case '"':
                        indices[current_index] = i;
                        searching_char = '"';
                        current_index++;
                        break;
                    case '_':
                        indices[current_index] = i;
                        searching_char = ' ';
                        current_index++;
                        break;
                }
            }
            // In this case we know exacly what the next character we are searching for is
            else if (line[i] == searching_char)
            {
                switch (searching_char)
                {
                case '>':
                    // If the next character is not a space, ignore this character
                    if (!(line[i+1] == ' '))
                    {
                        break;
                    }
                    indices[current_index] = i;
                    searching_char = '\0';
                    current_index++;
                    break;
                case ' ':
                    indices[current_index] = i-1;
                    searching_char = '\0';
                    current_index++;
                    break;
                case '"':
                    // Literals may contain escaped " characters
                    escaped = false;
                    size_t previous_index = i-1;
                    while (line[previous_index] == '\\')
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
                    if (line[i+1] == ' ')
                    {
                        indices[current_index] = i;
                        current_index++;
                    }
                    else
                    {
                        search_for_qualifier = true;
                    }
                    break;
                }
            }
        }
        else if (line[i] == ' ')
        {
            indices[current_index] = i-1;
            search_for_qualifier = false;
            current_index++;
        }
    }
    return indices;
}

int main(int ac, char *av[])
{
    // std::ifstream infile("./data/quads.nq");
    // std::ofstream outfile("./data/quads_triples.nt", std::ifstream::out);

    // This structure was inspired by https://gist.github.com/randomphrase/10801888
    namespace po = boost::program_options;

    po::options_description global("Global options");
    global.add_options()("input_file", po::value<std::string>(), "Input file, must contain n-quads");
    global.add_options()("output_file", po::value<std::string>(), "Output file");
    po::positional_options_description pos;
    pos.add("input_file", 1).add("output_file", 2);

    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(ac, av).options(global).positional(pos).allow_unregistered().run();

    po::store(parsed, vm);
    po::notify(vm);

    std::string input_file = vm["input_file"].as<std::string>();
    std::string output_file = vm["output_file"].as<std::string>();

    std::ifstream infile(input_file);
    std::ofstream outfile(output_file, std::ifstream::out);

    if (!infile.is_open())
    {
        perror("error while opening file");
    }

    if (!outfile.is_open())
    {
        perror("error while opening file");
    }

    std::string line;
    size_t line_count = 1;
    try
    {
        while(std::getline(infile, line))
        {
            size_t* index = parse_quad(line);

            // Set this to 4 if you want to store the full quad
            // Set this to 3 if you only want to store the triple component
            size_t tuple_size = 3;

            for (size_t i = 0; i < tuple_size; i++)
            {
                outfile << line.substr(*(index+i*2), *(index+i*2+1)-*(index+i*2)+1) << " ";
            }
            outfile << "." << std::endl;
            // Free up the space from the heap
            delete[] index;
            line_count++;
        }
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        std::cout << "Line number: " << line_count << std::endl;
        std::cout << line << std::endl;
    }
}