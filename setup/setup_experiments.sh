#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=$?
  local error_line=$BASH_LINENO
  local error_command=$BASH_COMMAND

  # Log the error details
  echo "Error occurred on line $error_line: $error_command (exit code: $error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "$log_file" ]] && [[ $log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "$logging_process" ]]; then
        logging_process=default
      fi
      echo $(date) $(hostname) "${logging_process}.Err: Error occurred on line $error_line: $error_command (exit code: $error_code)" >> $log_file
      echo $(date) $(hostname) "${logging_process}.Err: Exiting with code: 1" >> $log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Remove carriage returns from the config file, in oder to convert potential Windows line endings to Unix line endings
sed -i 's/\r//g' ./settings.config

skip_user_read=false
for var in "$@"
do
  if [ "$var" = "-y" ]; then
    skip_user_read=true
  fi
done

# Load in the settings
. ./settings.config

# Turn relative paths absolute
boost_path=$(realpath $boost_path)/

# Handle the case if the directory does not exist
if [ ! -d "$boost_path" ]; then
  echo "${boost_path} does not exist"
  if ! $skip_user_read; then
    while true; do
      read -p $'Would you like to set up boost in the specified directory? [y/n]\n'
      if [ ${REPLY,,} == "y" ]; then
          break
      elif [ ${REPLY,,} == "n" ]; then
          echo $'Please change boost_path in settings.config to a valid boost installation\nAborting'
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  mkdir -p $boost_path
  (
    cd $boost_path;
    wget https://boostorg.jfrog.io/artifactory/main/release/1.84.0/source/boost_1_84_0.tar.bz2;
    tar -v --strip-components=1 --bzip2 -xf ./boost_1_84_0.tar.bz2;
    rm ./boost_1_84_0.tar.bz2
  )
fi

# # Code for the DAS6 HPC system. Get the gcc version and if too old (older than 11.0) try to activate a module with version 12
# gcc_up_to_date=$(gcc --version | awk '{if (/gcc/ && ($3+0)>=11.0) {print 1} else if (/gcc/) {print 0}}')
# if [ ! $gcc_up_to_date == "1" ]; then
#   scl enable gcc-toolset-12 bash
#   module del gnu9
#   gcc_up_to_date=$(gcc --version | awk '{if (/gcc/ && ($3+0)>=11.0) {print 1} else if (/gcc/) {print 0}}')
#   if [ ! $gcc_up_to_date == "1" ]; then
#     echo 'Could not activate a module with a version of gcc higher than 11.0'
#     echo 'Aborting'
#     exit 1
#   fi
# fi

# Ask the user about the g++ version
g++ --version
if ! $skip_user_read; then
  while true; do
    read -p $'Would you like to use the above g++ version? [y/n]\n'
    if [ ${REPLY,,} == "y" ]; then
        break
    elif [ ${REPLY,,} == "n" ]; then
        echo $'Please change the g++ version manually. Calling `scl enable gcc-toolset-[version] bash` followed by `module del gnu9` might work'
        exit 1
    else
        echo 'Unrecognized response'
    fi
  done
fi

# Check if the boost binaries are (properly) installed. If not, ask the user to either compile them or abort
if [ ! -d "${boost_path}bin.v2/" ] || [ ! -d "${boost_path}include/" ] || [ ! -d "${boost_path}lib/" ]; then
  echo 'The boost binaries do not seem (properly) installed'
  if ! $skip_user_read; then
    while true; do
      read -p $'Would you like to (re)install the boost binaries? [y/n]\n'
      if [ ${REPLY,,} == "y" ]; then
          break
      elif [ ${REPLY,,} == "n" ]; then
          echo "Please properly install the boost binaries in ${boost_path}"
          echo 'Aborting'
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  (
    cd $boost_path;
    ./bootstrap.sh --prefix=./;
    ./b2 install
  )
fi

# Activate anaconda
if command -v conda &> /dev/null; then
    using_conda=true
    source activate base
    conda activate base
else
    using_conda=false
fi

# If the expriment directory already exists, ask the user what to do
git_hash=$(git rev-parse HEAD)
echo Creating directory from git hash: $git_hash
if [ -d ../$git_hash/ ]; then
  echo Directory exists.
  if ! $skip_user_read; then
    while true; do
      read -p $'Would you like to delete the old experiment? [y/n]\n'
      if [ ${REPLY,,} == "y" ]; then
        break
      elif [ ${REPLY,,} == "n" ]; then
        echo Experiment cancelled, because $git_hash already exists
        echo Aborting
        exit 1
      else
        echo 'Unrecognized response'
      fi
    done
  fi
  # If the directory is over 1 GiB then ask an extra question as the whether the results should be deleted
  if [ $(cd ../$git_hash/;du -sb . | awk '{print $1}') -ge $((1024 ** 3)) ]; then  # 1024 ** 3 = 1 GiB
    if ! $skip_user_read; then
      while true; do
        read -p $'The directory is over 1 GiB. Are you sure you want to remove it? [y/n]\n'
        if [ ${REPLY,,} == "y" ]; then
          break
        elif [ ${REPLY,,} == "n" ]; then
          echo Aborting
          exit 1
        else
          echo 'Unrecognized response'
        fi
      done
    fi
  fi
  # Clear up the associated conda environment if needed
  if [ -d ../$git_hash/code/python/.conda/ ] && [ $using_conda ]; then
    conda_env=$(cd ../$git_hash/code/python/.conda/; pwd)
    if conda activate $conda_env; then
      conda activate base
      conda env remove -y -p $conda_env
    fi
  fi
  rm -r ../$git_hash/
fi

# Create the directory for the compiled files and experiments
mkdir ../$git_hash/
mkdir ../$git_hash/code/
mkdir ../$git_hash/code/src/
mkdir ../$git_hash/code/bin/

# Make a log file
log_file_name=setup
logging_process=$log_file_name
log_file="../$git_hash/code/${log_file_name}.log"
touch $log_file

# Log the settings
echo $(date) $(hostname) "${logging_process}.Info: Compiler version: $(g++ --version | head -n 1)" >> $log_file
echo $(date) $(hostname) "${logging_process}.Info: Compiler location: $(which g++)" >> $log_file
echo $(date) $(hostname) "${logging_process}.Info: Creating compilation with the following settings:" >> $log_file
echo $(date) $(hostname) "${logging_process}.Info: boost_path=${boost_path}" >> $log_file
echo $(date) $(hostname) "${logging_process}.Info: compiler_flags=${compiler_flags}" >> $log_file

# Remove carriage returns from the compiler script and make sure we can run it 
sed -i 's/\r//g' ./compile.sh
chmod +x ./compile.sh

# Compile the preprocessor
compiler_flags="${compiler_flags//,/ }"
echo Copying preprocessor.cpp
echo $(date) $(hostname) "${logging_process}.Info: Copying preprocessor.cpp" >> $log_file
cp ../code/preprocessor.cpp ../$git_hash/code/src/preprocessor.cpp
echo Compiling preprocessor.cpp
echo $(date) $(hostname) "${logging_process}.Info: Compiling preprocessor.cpp" >> $log_file
./compile.sh ../$git_hash/code/src/preprocessor.cpp ../$git_hash/code/bin/preprocessor

# Compile the bisimulator
echo Copying bisimulator.cpp
echo $(date) $(hostname) "${logging_process}.Info: Copying bisimulator.cpp" >> $log_file
cp ../code/bisimulator.cpp ../$git_hash/code/src/bisimulator.cpp
echo Compiling bisimulator.cpp
echo $(date) $(hostname) "${logging_process}.Info: Compiling bisimulator.cpp" >> $log_file
./compile.sh ../$git_hash/code/src/bisimulator.cpp ../$git_hash/code/bin/bisimulator

# Compile the condensed summary graph program
echo Copying create_condensed_summary_graph_from_partitions.cpp
echo $(date) $(hostname) "${logging_process}.Info: Copying create_condensed_summary_graph_from_partitions.cpp" >> $log_file
cp ../code/create_condensed_summary_graph_from_partitions.cpp ../$git_hash/code/src/create_condensed_summary_graph_from_partitions.cpp
echo Compiling create_condensed_summary_graph_from_partitions.cpp
echo $(date) $(hostname) "${logging_process}.Info: Compiling create_condensed_summary_graph_from_partitions.cpp" >> $log_file
./compile.sh ../$git_hash/code/src/create_condensed_summary_graph_from_partitions.cpp ../$git_hash/code/bin/create_condensed_summary_graph_from_partitions

# Echo that the compilation was successful
echo C++ cpoying and compiling successful
echo $(date) $(hostname) "${logging_process}.Info: C++ cpoying and compiling successful" >> $log_file

# Copy the python package for loading, testing, displaying and plotting results
echo Copying python codebase
echo $(date) $(hostname) "${logging_process}.Info: Copying python codebase" >> $log_file
cp -r ../code/python/ ../$git_hash/code/python/

if [ using_conda ]; then
    echo Setting up Anaconda enviroment
    echo $(date) $(hostname) "${logging_process}.Info: Setting up Anaconda enviroment" >> $log_file
    (cd ../$git_hash/code/python/;
    python_version=$(grep -i "requires-python" pyproject.toml | awk -F: '{ st = index($0,"=");print substr($0,st+1)}' | awk '{$1=$1};1' | awk '{print substr($0, 2, length($0) - 2)}');
    conda create -y --prefix ./.conda/ "python$python_version";
    conda activate ./.conda/;
    pip install -e .;
    conda activate base)
    echo Successfully set up Anaconda enviroment
    echo $(date) $(hostname) "${logging_process}.Info: Successfully set up Anaconda enviroment" >> $log_file
fi

# Echo that the copying was successful
echo Python copying successful
echo $(date) $(hostname) "${logging_process}.Info: Copying successful" >> $log_file

# Create the experiment scripts, along with their config files
mkdir ../$git_hash/scripts/

# Create the config for the preprocessor experiment
echo Creating preprocessor.config
echo $(date) $(hostname) "${logging_process}.Info: Creating preprocessor.config" >> $log_file
preprocessor_config=../$git_hash/scripts/preprocessor.config
touch $preprocessor_config
cat >$preprocessor_config << EOF
job_name=preprocessing
time=48:00:00
N=1
ntasks_per_node=1
partition=defq
output=slurm_preprocessor.out
nodelist=
skipRDFlists=false
laundromat=false
use_lz4=false
lz4_command=/usr/local/lz4
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $preprocessor_config

# Create the shell file for the preprocessor experiment
echo Creating preprocessor.sh
echo $(date) $(hostname) "${logging_process}.Info: Creating preprocessor.sh" >> $log_file
preprocessor=../$git_hash/scripts/preprocessor.sh
touch $preprocessor
cat >$preprocessor << EOF
#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=\$?
  local error_line=\$BASH_LINENO
  local error_command=\$BASH_COMMAND

  if [ "\$error_command" == "sbatch_command=\\\$(command -v sbatch)" ]; then
    echo "Ignored error on line \$error_line: \$error_command"
    echo "This command is expected to fail if \`sbatch\` is not available"
    return 0
  fi

  # Log the error details
  echo "Error occurred on line \$error_line: \$error_command (exit code: \$error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "\$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "\$log_file" ]] && [[ \$log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "\$logging_process" ]]; then
        logging_process=default
      fi
      echo \$(date) \$(hostname) "\${logging_process}.Err: Error occurred on line \$error_line: \$error_command (exit code: \$error_code)" >> \$log_file
      echo \$(date) \$(hostname) "\${logging_process}.Err: Exiting with code: 1" >> \$log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Check if a path to a dataset has been provided
if [ \$# -eq 0 ]; then
  echo 'Please provide a path to an .nt file as argument'
  exit 1
fi

skip_user_read=false
for var in "\$@"
do
  if [ "\$var" = "-y" ]; then
    skip_user_read=true
  fi
done

# Load in the settings
. ./preprocessor.config

# Exit if laundromat is true with use_lz4 false
if [ \$laundromat == "true" ] && [ \$use_lz4 == "false" ]; then
  echo $'The laundromat dataset only works with lz4 enabled. This can be changed in preprocessor.config\nAborting'
fi

# Set a boolean based on the value of skipRDFlists
case \$skipRDFlists in
  'true') skiplists=' --skipRDFlists' ;;
  'false') skiplists='' ;;
  *) echo "skipRDFlists has been set to \\"\$skipRDFlists\\" in preprocessor.config. Please change it to \\"true\\" or \\"false\\" instead"; exit 1 ;;
esac

# Set a boolean based on the value of laundromat
case \$laundromat in
  'true') laundromat_flag=' --laundromat' ;;
  'false') laundromat_flag='' ;;
  *) echo "laundromat has been set to \\"\$laundromat\\" in preprocessor.config. Please change it to \\"true\\" or \\"false\\" instead"; exit 1 ;;
esac

# Sanity check the value of use_lz4
case \$use_lz4 in
  'true');;
  'false');;
  *) echo "use_lz4 has been set to \\"\$use_lz4\\" in preprocessor.config. Please change it to \\"true\\" or \\"false\\" instead"; exit 1 ;;
esac

# Print the settings
echo Using the following settings:
echo job_name=\$job_name
echo time=\$time
echo N=\$N
echo ntasks_per_node=\$ntasks_per_node
echo partition=\$partition
echo output=\$output
echo nodelist=\$nodelist
echo skipRDFlists=\$skipRDFlists
echo laundromat=\$laundromat
echo use_lz4=\$use_lz4
echo lz4_command=\$lz4_command

if ! \$skip_user_read; then
  # Ask the user to run the experiment with the aforementioned settings
  while true; do
    read -p $'Would you like to run the experiment with the aforementioned settings? [y/n]\n'
    if [ \${REPLY,,} == "y" ]; then
        break
    elif [ \${REPLY,,} == "n" ]; then
        echo $'Please change the settings in preprocessor.config\nAborting'
        exit 1
    else
        echo 'Unrecognized response'
    fi
  done
fi

# Create a directory for the experiments
dataset_path="\${1}"
dataset_file="\${1##*/}"
# Remove the extra extension from the LOD Laundromat file (.trig.lz4)
dataset_name="\${dataset_file%.*}"
if [ \$use_lz4 == "true" ]; then
  dataset_name="\${dataset_name%.*}"
fi
dataset_path_absolute=\$(realpath \$dataset_path)/
output_dir=../\$dataset_name/
mkdir \$output_dir

# Use a different set of commands for the LOD Laundromat and semopenalex datasets, because they are compressed with lz4
if [ \$use_lz4 == "true" ]; then
  preprocessor_command=\$(cat << EOM
mkfifo ttl_buffer
/usr/bin/time -v \$lz4_command -d -c \$dataset_path -d -c > ttl_buffer &
../code/bin/preprocessor ./ttl_buffer ./\$skiplists\$laundromat_flag
rm ./ttl_buffer
EOM
  )
else
  preprocessor_command="/usr/bin/time -v ../code/bin/preprocessor \$dataset_path ./\$skiplists"
fi

# Create a log file for the experiments
log_file=\${output_dir}experiments.log
logging_process=Prepocessor
touch \$log_file

# Log the settings
echo \$(date) \$(hostname) "\${logging_process}.Info: Preprocessing the dataset: \${dataset_path_absolute}" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: Using the following settings:" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: job_name=\$job_name" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: time=\$time" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: N=\$N" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: ntasks_per_node=\$ntasks_per_node" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: partition=\$partition" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: output=\$output" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: nodelist=\$nodelist" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: skipRDFlists=\$skipRDFlists" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: laundromat=\$laundromat" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: use_lz4=\$use_lz4" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: lz4_command=\$lz4_command" >> \$log_file

# Create the slurm script
echo Creating slurm script
echo \$(date) \$(hostname) "\${logging_process}.Info: Creating slurm script" >> \$log_file
preprocessor_job=\${output_dir}slurm_preprocessor.sh
touch \$preprocessor_job
cat >\$preprocessor_job << EOF2
#!/bin/bash
#SBATCH --job-name=\$job_name
#SBATCH --time=\$time
#SBATCH -N \$N
#SBATCH --ntasks-per-node=\$ntasks_per_node
#SBATCH --partition=\$partition
#SBATCH --output=\$output
#SBATCH --nodelist=\$nodelist
\$preprocessor_command
EOF2

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' \$preprocessor_job

# Make sure the preprocessor job script can be executed (in case of local execution)
chmod +x \$preprocessor_job

# Queueing slurm script or ask to execute directly
sbatch_command=\$(command -v sbatch)
if [ ! \$sbatch_command == '' ]; then
  echo Queueing slurm script
  echo \$(date) \$(hostname) "\${logging_process}.Info: Queueing slurm script" >> \$log_file
  (cd \$output_dir; sbatch \$preprocessor_job)
  echo Preprocessor queued successfully, see \$output for the results
  echo \$(date) \$(hostname) "\${logging_process}.Info: Preprocessor queued successfully, see \$output for the results" >> \$log_file
else
  echo sbatch command not found
  echo \$(date) \$(hostname) "\${logging_process}.Info: sbatch command not found" >> \$log_file
  if ! \$skip_user_read; then
    while true; do
      read -p \$'Would you like to directly run the preprocessor locally instead? [y/n]\n'
      if [ \${REPLY,,} == "y" ]; then
          break
      elif [ \${REPLY,,} == "n" ]; then
          echo $'Direct execution declined\nAborting'
          echo \$(date) \$(hostname) "\${logging_process}.Info: User declined direct execution" >> \$log_file
          echo \$(date) \$(hostname) "\${logging_process}.Info: Exiting with code: 1" >> \$log_file
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  echo Running slurm script directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: User accepted direct execution" >> \$log_file
  echo \$(date) \$(hostname) "\${logging_process}.Info: Running slurm script directly" >> \$log_file
  (cd \$output_dir; \$preprocessor_job > slurm_preprocessor.out)
  echo Successfully ran preprocessor directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: Successfully ran preprocessor directly" >> \$log_file
fi
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $preprocessor

# Make sure the preprocessor can be executed
chmod +x $preprocessor

# Create the config for the bisimulator experiment
echo Creating bisimulator.config
echo $(date) $(hostname) "${logging_process}.Info: Creating bisimulator.config" >> $log_file
bisimulator_config=../$git_hash/scripts/bisimulator.config
touch $bisimulator_config
cat >$bisimulator_config << EOF
job_name=bisimulating
time=48:00:00
N=1
ntasks_per_node=1
partition=defq
output=slurm_bisimulator.out
nodelist=
bisimulation_mode=run_k_bisimulation_store_partition_condensed_timed
typed_start=true
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $bisimulator_config

# Create the shell file for the bisimulator experiment
echo Creating bisimulator.sh
echo $(date) $(hostname) "${logging_process}.Info: Creating bisimulator.sh" >> $log_file
bisimulator=../$git_hash/scripts/bisimulator.sh
touch $bisimulator
cat >$bisimulator << EOF
#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=\$?
  local error_line=\$BASH_LINENO
  local error_command=\$BASH_COMMAND

  if [ "\$error_command" == "sbatch_command=\\\$(command -v sbatch)" ]; then
    echo "Ignored error on line \$error_line: \$error_command"
    echo "This command is expected to fail if \`sbatch\` is not available"
    return 0
  fi

  # Log the error details
  echo "Error occurred on line \$error_line: \$error_command (exit code: \$error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "\$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "\$log_file" ]] && [[ \$log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "\$logging_process" ]]; then
        logging_process=default
      fi
      echo \$(date) \$(hostname) "\${logging_process}.Err: Error occurred on line \$error_line: \$error_command (exit code: \$error_code)" >> \$log_file
      echo \$(date) \$(hostname) "\${logging_process}.Err: Exiting with code: 1" >> \$log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Check if a path to a dataset has been provided
if [ \$# -eq 0 ]; then
  echo 'Please provide a path to a dataset directory. This directory should have been created by preprocessor.sh'
  exit 1
fi

skip_user_read=false
for var in "\$@"
do
  if [ "\$var" = "-y" ]; then
    skip_user_read=true
  fi
done

# Load in the settings
. ./bisimulator.config

# Print the settings
echo Using the following settings:
echo job_name=\$job_name
echo time=\$time
echo N=\$N
echo ntasks_per_node=\$ntasks_per_node
echo partition=\$partition
echo output=\$output
echo nodelist=\$nodelist
echo bisimulation_mode=\$bisimulation_mode
echo typed_start=\$typed_start

if ! \$skip_user_read; then
  # Ask the user to run the experiment with the aforementioned settings
  while true; do
    read -p $'Would you like to run the experiment with the aforementioned settings? [y/n]\n'
    if [ \${REPLY,,} == "y" ]; then
        break
    elif [ \${REPLY,,} == "n" ]; then
        echo $'Please change the settings in bisimulator.config\nAborting'
        exit 1
    else
        echo 'Unrecognized response'
    fi
  done
fi

# Select a directory for the experiments
output_dir="\${1}"
output_dir_absolute=\$(realpath \$output_dir)/

# The log file for the experiments
log_file=\${output_dir}experiments.log
logging_process=Bisimulator

# Log the settings
echo \$(date) \$(hostname) "\${logging_process}.Info: Performing bisimulation in: \${output_dir_absolute}" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: Using the following settings:" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: job_name=\$job_name" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: time=\$time" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: N=\$N" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: ntasks_per_node=\$ntasks_per_node" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: partition=\$partition" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: output=\$output" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: nodelist=\$nodelist" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: bisimulation_mode=\$bisimulation_mode" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: typed_start=\$typed_start" >> \$log_file

# Set a flag based on the value of typed_start
case \$typed_start in
  'true') typed_start_flag=' --typed_start' ;;
  'false') typed_start_flag='' ;;
  *) echo "typed_start has been set to \\"\$typed_start\\" in preprocessor.config. Please change it to \\"true\\" or \\"false\\" instead"; exit 1 ;;
esac

# Create the slurm script
echo Creating slurm script
echo \$(date) \$(hostname) "\${logging_process}.Info: Creating slurm script" >> \$log_file
bisimulator_job=\${output_dir}slurm_bisimulator.sh
touch \$bisimulator_job
cat >\$bisimulator_job << EOF2
#!/bin/bash
#SBATCH --job-name=\$job_name
#SBATCH --time=\$time
#SBATCH -N \$N
#SBATCH --ntasks-per-node=\$ntasks_per_node
#SBATCH --partition=\$partition
#SBATCH --output=\$output
#SBATCH --nodelist=\$nodelist
/usr/bin/time -v ../code/bin/bisimulator \$bisimulation_mode ./binary_encoding.bin --output=./\$typed_start_flag
EOF2

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' \$bisimulator_job

# Make sure the bisimulator job script can be executed (in case of local execution)
chmod +x \$bisimulator_job

# Queueing slurm script or ask to execute directly
sbatch_command=\$(command -v sbatch)
if [ ! \$sbatch_command == '' ]; then
  echo Queueing slurm script
  echo \$(date) \$(hostname) "\${logging_process}.Info: Queueing slurm script" >> \$log_file
  (cd \$output_dir; sbatch \$bisimulator_job)
  echo Bisimulator queued successfully, see \$output for the results
  echo \$(date) \$(hostname) "\${logging_process}.Info: Bisimulator queued successfully, see \$output for the results" >> \$log_file
else
  echo sbatch command not found
  echo \$(date) \$(hostname) "\${logging_process}.Info: sbatch command not found" >> \$log_file
  if ! \$skip_user_read; then
    while true; do
      read -p \$'Would you like to directly run the bisimulator locally instead? [y/n]\n'
      if [ \${REPLY,,} == "y" ]; then
          break
      elif [ \${REPLY,,} == "n" ]; then
          echo $'Direct execution declined\nAborting'
          echo \$(date) \$(hostname) "\${logging_process}.Info: User declined direct execution" >> \$log_file
          echo \$(date) \$(hostname) "\${logging_process}.Info: Exiting with code: 1" >> \$log_file
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  echo Running slurm script directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: User accepted direct execution" >> \$log_file
  echo \$(date) \$(hostname) "\${logging_process}.Info: Running slurm script directly" >> \$log_file
  (cd \$output_dir; \$bisimulator_job > slurm_bisimulator.out)
  echo Successfully ran bisimulator directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: Successfully ran bisimulator directly" >> \$log_file
fi
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $bisimulator

# Make sure the bisimulator can be executed
chmod +x $bisimulator

# Create the config for the summary graphs experiment
echo Creating summary_graphs_creator.config
echo $(date) $(hostname) "${logging_process}.Info: Creating summary_graphs.config" >> $log_file
summary_graphs_creator_config=../$git_hash/scripts/summary_graphs_creator.config
touch $summary_graphs_creator_config
cat >$summary_graphs_creator_config << EOF
job_name=summary_graphs_creation
time=01:00:00
N=1
ntasks_per_node=1
partition=defq
output=slurm_summary_graphs_creator.out
nodelist=
multi_summary=true
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $summary_graphs_creator_config

# Create the shell file for the summary graphs experiment
echo Creating summary_graphs_creator.sh
echo $(date) $(hostname) "${logging_process}.Info: Creating summary_graphs_creator.sh" >> $log_file
summary_graphs_creator=../$git_hash/scripts/summary_graphs_creator.sh
touch $summary_graphs_creator
cat >$summary_graphs_creator << EOF
#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=\$?
  local error_line=\$BASH_LINENO
  local error_command=\$BASH_COMMAND

  if [ "\$error_command" == "sbatch_command=\\\$(command -v sbatch)" ]; then
    echo "Ignored error on line \$error_line: \$error_command"
    echo "This command is expected to fail if \`sbatch\` is not available"
    return 0
  fi

  # Log the error details
  echo "Error occurred on line \$error_line: \$error_command (exit code: \$error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "\$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "\$log_file" ]] && [[ \$log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "\$logging_process" ]]; then
        logging_process=default
      fi
      echo \$(date) \$(hostname) "\${logging_process}.Err: Error occurred on line \$error_line: \$error_command (exit code: \$error_code)" >> \$log_file
      echo \$(date) \$(hostname) "\${logging_process}.Err: Exiting with code: 1" >> \$log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Check if a path to a dataset has been provided
if [ \$# -eq 0 ]; then
  echo 'Please provide a path to a dataset directory. This directory should have been created by preprocessor.sh'
  exit 1
fi

skip_user_read=false
for var in "\$@"
do
  if [ "\$var" = "-y" ]; then
    skip_user_read=true
  fi
done

# Load in the settings
. ./summary_graphs_creator.config

# Set a path based on the value of multi_summary
case \$multi_summary in
  'true') summary_graph_creator_executable='create_condensed_summary_graph_from_partitions' ;;
  'false') summary_graph_creator_executable='create_quotient_graphs_from_partitions' ;;
  *) echo "multi_summary has been set to \\"\$multi_summary\\" in summary_graphs_creator.config. Please change it to \\"true\\" or \\"false\\" instead"; exit 1 ;;
esac

# Print the settings
echo Using the following settings:
echo job_name=\$job_name
echo time=\$time
echo N=\$N
echo ntasks_per_node=\$ntasks_per_node
echo partition=\$partition
echo output=\$output
echo nodelist=\$nodelist
echo multi_summary=\$multi_summary

if ! \$skip_user_read; then
# Ask the user to run the experiment with the aforementioned settings
  while true; do
    read -p $'Would you like to run the experiment with the aforementioned settings? [y/n]\n'
    if [ \${REPLY,,} == "y" ]; then
        break
    elif [ \${REPLY,,} == "n" ]; then
        echo $'Please change the settings in summary_graphs_creator.config\nAborting'
        exit 1
    else
        echo 'Unrecognized response'
    fi
  done
fi

# Select a directory for the experiments
output_dir="\${1}"
output_dir_absolute=\$(realpath \$output_dir)/

# The log file for the experiments
log_file=\${output_dir}experiments.log
logging_process="Summary Graphs Creator"

# Log the settings
echo \$(date) \$(hostname) "\${logging_process}.Info: Creating summary graphs in: \${output_dir_absolute}" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: Using the following settings:" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: job_name=\$job_name" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: time=\$time" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: N=\$N" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: ntasks_per_node=\$ntasks_per_node" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: partition=\$partition" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: output=\$output" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: nodelist=\$nodelist" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: multi_summary=\$multi_summary" >> \$log_file

# Create the slurm script
echo Creating slurm script
echo \$(date) \$(hostname) "\${logging_process}.Info: Creating slurm script" >> \$log_file
summary_graphs_creator_job=\${output_dir}slurm_summary_graphs_creator.sh
touch \$summary_graphs_creator_job
cat >\$summary_graphs_creator_job << EOF2
#!/bin/bash
#SBATCH --job-name=\$job_name
#SBATCH --time=\$time
#SBATCH -N \$N
#SBATCH --ntasks-per-node=\$ntasks_per_node
#SBATCH --partition=\$partition
#SBATCH --output=\$output
#SBATCH --nodelist=\$nodelist
/usr/bin/time -v ../code/bin/\$summary_graph_creator_executable ./
EOF2

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' \$summary_graphs_creator_job

# Make sure the summary graphs creator job script can be executed (in case of local execution)
chmod +x \$summary_graphs_creator_job

# Queueing slurm script or ask to execute directly
sbatch_command=\$(command -v sbatch)
if [ ! \$sbatch_command == '' ]; then
  echo Queueing slurm script
  echo \$(date) \$(hostname) "\${logging_process}.Info: Queueing slurm script" >> \$log_file
  (cd \$output_dir; sbatch \$summary_graphs_creator_job)
  echo Summary graphs creator queued successfully, see \$output for the results
  echo \$(date) \$(hostname) "\${logging_process}.Info: Summary graphs creator queued successfully, see \$output for the results" >> \$log_file
else
  echo sbatch command not found
  echo \$(date) \$(hostname) "\${logging_process}.Info: sbatch command not found" >> \$log_file
  if ! \$skip_user_read; then
    while true; do
      read -p \$'Would you like to directly run the summary graphs creator locally instead? [y/n]\n'
      if [ \${REPLY,,} == "y" ]; then
          break
      elif [ \${REPLY,,} == "n" ]; then
          echo $'Direct execution declined\nAborting'
          echo \$(date) \$(hostname) "\${logging_process}.Info: User declined direct execution" >> \$log_file
          echo \$(date) \$(hostname) "\${logging_process}.Info: Exiting with code: 1" >> \$log_file
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  echo Running slurm script directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: User accepted direct execution" >> \$log_file
  echo \$(date) \$(hostname) "\${logging_process}.Info: Running slurm script directly" >> \$log_file
  (cd \$output_dir; \$summary_graphs_creator_job > slurm_summary_graphs_creator.out)
  echo Successfully ran summary graphs creator directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: Successfully ran summary graphs creator directly" >> \$log_file
fi
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $summary_graphs_creator

# Make sure the summary graphs creator can be executed
chmod +x $summary_graphs_creator

# Create the config for the python plotting scrips
echo Creating plot_results.config
echo $(date) $(hostname) "${logging_process}.Info: Creating plot_results.config" >> $log_file
results_plotter_config=../$git_hash/scripts/results_plotter.config
touch $results_plotter_config
cat >$results_plotter_config << EOF
job_name=plotting_results
time=01:00:00
N=1
ntasks_per_node=1
partition=defq
output=slurm_results_plotter.out
nodelist=
k=-1
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $results_plotter_config

# Create the shell file for the result plotter
echo Creating results_plotter.sh
echo $(date) $(hostname) "${logging_process}.Info: Creating results_plotter.sh" >> $log_file
results_plotter=../$git_hash/scripts/results_plotter.sh
touch $results_plotter
cat >$results_plotter << EOF
#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=\$?
  local error_line=\$BASH_LINENO
  local error_command=\$BASH_COMMAND

  if [ "\$error_command" == "sbatch_command=\\\$(command -v sbatch)" ]; then
    echo "Ignored error on line \$error_line: \$error_command"
    echo "This command is expected to fail if \`sbatch\` is not available"
    return 0
  fi

  # Log the error details
  echo "Error occurred on line \$error_line: \$error_command (exit code: \$error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "\$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "\$log_file" ]] && [[ \$log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "\$logging_process" ]]; then
        logging_process=default
      fi
      echo \$(date) \$(hostname) "\${logging_process}.Err: Error occurred on line \$error_line: \$error_command (exit code: \$error_code)" >> \$log_file
      echo \$(date) \$(hostname) "\${logging_process}.Err: Exiting with code: 1" >> \$log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Check if a path to a dataset has been provided
if [ \$# -eq 0 ]; then
  echo 'Please provide a path to a dataset directory. This directory should have been created by preprocessor.sh'
  exit 1
fi

skip_user_read=false
for var in "\$@"
do
  if [ "\$var" = "-y" ]; then
    skip_user_read=true
  fi
done

# Load in the settings
. ./results_plotter.config

# Print the settings
echo Using the following settings:
echo job_name=\$job_name
echo time=\$time
echo N=\$N
echo ntasks_per_node=\$ntasks_per_node
echo partition=\$partition
echo output=\$output
echo nodelist=\$nodelist
echo k=\$k

if ! \$skip_user_read; then
  # Ask the user to run the experiment with the aforementioned settings
  while true; do
    read -p $'Would you like to run the experiment with the aforementioned settings? [y/n]\n'
    if [ \${REPLY,,} == "y" ]; then
        break
    elif [ \${REPLY,,} == "n" ]; then
        echo $'Please change the settings in results_plotter.config\nAborting'
        exit 1
    else
        echo 'Unrecognized response'
    fi
  done
fi

# Select a directory for the experiments
output_dir="\${1}"
output_dir_absolute=\$(realpath \$output_dir)/

# The log file for the experiments
log_file=\${output_dir}experiments.log
logging_process="Results Plotter"

# Log the settings
echo \$(date) \$(hostname) "\${logging_process}.Info: Plotting results in: \${output_dir_absolute}" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: Using the following settings:" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: job_name=\$job_name" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: time=\$time" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: N=\$N" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: ntasks_per_node=\$ntasks_per_node" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: partition=\$partition" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: output=\$output" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: nodelist=\$nodelist" >> \$log_file
echo \$(date) \$(hostname) "\${logging_process}.Info: k=\$k" >> \$log_file

# Create the slurm script
echo Creating slurm script
echo \$(date) \$(hostname) "\${logging_process}.Info: Creating slurm script" >> \$log_file
results_plotter_job=\${output_dir}slurm_results_plotter.sh
touch \$results_plotter_job
cat >\$results_plotter_job << EOF2
#!/bin/bash
#SBATCH --job-name=\$job_name
#SBATCH --time=\$time
#SBATCH -N \$N
#SBATCH --ntasks-per-node=\$ntasks_per_node
#SBATCH --partition=\$partition
#SBATCH --output=\$output
#SBATCH --nodelist=\$nodelist
working_directory=\\\$(pwd)
source activate base
source \\\$HOME/.bashrc
cd \\\$working_directory  # We have to move back to the working directory, as .bashrc might contain code to change the directory
conda activate
conda activate \\\$working_directory/../code/python/.conda/
python \\\$working_directory/../code/python/summary_loader/graph_stats.py ./ -v
EOF2

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' \$results_plotter_job

# Make sure the results plotter job script can be executed (in case of local execution)
chmod +x \$results_plotter_job

# Queueing slurm script or ask to execute directly
sbatch_command=\$(command -v sbatch)
if [ ! \$sbatch_command == '' ]; then
  echo Queueing slurm script
  echo \$(date) \$(hostname) "\${logging_process}.Info: Queueing slurm script" >> \$log_file
  (cd \$output_dir; sbatch \$results_plotter_job)
  echo Results plotter queued successfully, see \$output for the results
  echo \$(date) \$(hostname) "\${logging_process}.Info: Results plotter queued successfully, see \$output for the results" >> \$log_file
else
  echo sbatch command not found
  echo \$(date) \$(hostname) "\${logging_process}.Info: sbatch command not found" >> \$log_file
  if ! \$skip_user_read; then
    while true; do
      read -p \$'Would you like to directly run the results plotter locally instead? [y/n]\n'
      if [ \${REPLY,,} == "y" ]; then
          break
      elif [ \${REPLY,,} == "n" ]; then
          echo $'Direct execution declined\nAborting'
          echo \$(date) \$(hostname) "\${logging_process}.Info: User declined direct execution" >> \$log_file
          echo \$(date) \$(hostname) "\${logging_process}.Info: Exiting with code: 1" >> \$log_file
          exit 1
      else
          echo 'Unrecognized response'
      fi
    done
  fi
  echo Running slurm script directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: User accepted direct execution" >> \$log_file
  echo \$(date) \$(hostname) "\${logging_process}.Info: Running slurm script directly" >> \$log_file
  (cd \$output_dir; \$results_plotter_job > slurm_results_plotter.out)
  echo Successfully ran results plotter directly
  echo \$(date) \$(hostname) "\${logging_process}.Info: Successfully ran results plotter directly" >> \$log_file
fi
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $results_plotter

# Make sure the summary graphs creator can be executed
chmod +x $results_plotter

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $results_plotter_config

# Create the shell file for running all experiments
echo Creating run_all.sh
echo $(date) $(hostname) "${logging_process}.Info: Creating run_all.sh" >> $log_file
run_all=../$git_hash/scripts/run_all.sh
touch $run_all
cat >$run_all << EOF
#!/bin/bash

# Using error handling code from https://linuxsimply.com/bash-scripting-tutorial/error-handling-and-debugging/error-handling/trap-err/
##################################################

# Define error handler function
function handle_error() {
  # Get information about the error
  local error_code=\$?
  local error_line=\$BASH_LINENO
  local error_command=\$BASH_COMMAND

  if [ "\$error_command" == "sbatch_command=\\\$(command -v sbatch)" ]; then
    echo "Ignored error on line \$error_line: \$error_command"
    echo "This command is expected to fail if \`sbatch\` is not available"
    return 0
  fi

  # Log the error details
  echo "Error occurred on line \$error_line: \$error_command (exit code: \$error_code)"
  echo "Exiting with code: 1"

  # Check if log_file has been set
  if [[ ! -z "\$log_file" ]]; then
    # Check log_file refers to an actual log file
    if [[ -f "\$log_file" ]] && [[ \$log_file == *.log ]]; then
      # If no process name has been set, then use "default"
      if [[ -z "\$logging_process" ]]; then
        logging_process=default
      fi
      echo \$(date) \$(hostname) "\${logging_process}.Err: Error occurred on line \$error_line:\ \$error_command (exit code: \$error_code)" >> \$log_file
      echo \$(date) \$(hostname) "\${logging_process}.Err: Exiting with code: 1" >> \$log_file
    fi
  fi

  # Optionally exit the script gracefully
  exit 1
}

# Set the trap for any error (non-zero exit code)
trap handle_error ERR

##################################################

# Check if a path to a dataset has been provided
if [ \$# -eq 0 ]; then
  echo 'Please provide a path to an .nt file as argument'
  exit 1
fi

y_flag=""
for var in "\$@"
do
  if [ "\$var" = "-y" ]; then
    y_flag=-y
  fi
done

# Load in the settings
. ./preprocessor.config

dataset_path="\${1}"
dataset_file="\${1##*/}"
# Remove the extra extension from the LOD Laundromat file (.trig.lz4)
dataset_name="\${dataset_file%.*}"
if [ \$use_lz4 == "true" ]; then
  dataset_name="\${dataset_name%.*}"
fi
dataset_path_absolute=\$(realpath \$dataset_path)/
output_dir=../\$dataset_name/

echo -e "\n##### SETTING UP PREPROCESSOR EXPERIMENT #####"
./preprocessor.sh \$dataset_path \$y_flag
echo -e "\n##### SETTING UP BISIMULATOR EXPERIMENT #####"
./bisimulator.sh \$output_dir \$y_flag
echo -e "\n##### SETTING UP SUMMARY GRAPH CREATOR EXPERIMENT #####"
./summary_graphs_creator.sh \$output_dir \$y_flag
echo -e "\n##### SETTING UP RESULT PLOTTER EXPERIMENT #####"
./results_plotter.sh \$output_dir \$y_flag
EOF

# Make sure the file will have Unix style line endings
sed -i 's/\r//g' $run_all

# Make sure the run all script can be executed
chmod +x $run_all

# Echo that the script ended successfully
(cd ../; working_directory=$(pwd); echo Everything set up in: $working_directory/$git_hash/)
echo Setup ended successfully
echo $(date) $(hostname) "${logging_process}.Info: Setup ended successfully" >> $log_file