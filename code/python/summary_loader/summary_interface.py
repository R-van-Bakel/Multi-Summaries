import ntpath
from typing import Any
import toml
import os
import subprocess

class SummaryInterface:
    def __init__(self, experiments_directory: str) -> None:
        self.__dataset_path = None
        self.__dataset_format = None
        self.__dataset_name = None
        self.__experiment_settings = None
        self.__experiment_state = None
        self.set_experiments_directory(experiments_directory)

    def set_experiments_directory(self, experiments_directory: str) -> None:
        self.__experiments_directory=os.path.abspath(experiments_directory) + "/"
        self.__experiment_settings = {"run_all": dict(), "preprocessor": dict(), "bisimulator": dict(), "summary_graphs_creator": dict(), "results_plotter": dict(), "serializer": dict()}
        self.__load_settings()

    def set_dataset(self, path_to_dataset: str) -> None:
        self.__dataset_path = os.path.abspath(path_to_dataset)
        dataset_file = ntpath.basename(path_to_dataset)
        dataset_file_components = dataset_file.split(".")
        if len(dataset_file_components) > 1:
            self.__dataset_format = dataset_file_components[-1]
            self.__dataset_name = dataset_file[:-(len(self.__dataset_format)+1)]
        else:
            self.__dataset_format = ""
            self.__dataset_name = dataset_file
        self.__load_state()

    def change_settings(self, new_settings: dict[str, dict]) -> None:
        scripts_directory = self.__experiments_directory + "scripts/"
        for script, settings in new_settings.items():
            with open(scripts_directory + script + ".config", "r+") as config_file:
                lines = config_file.readlines()
                config_file.seek(0)
                config_file.truncate()
                for line in lines:
                    setting = line.split("=")[0]
                    new_line = setting + "=" + settings[setting] + "\n"
                    config_file.write(new_line)
        self.__experiment_settings = new_settings
    
    def run_experiment(self) -> None:
        command = ["./run_all.sh", self.__dataset_path, "-y"]
        scripts_directory = self.__experiments_directory + "scripts/"
        subprocess.run(command, cwd=scripts_directory)
        self.__load_state()
    
    def merge_files(self, files_to_merge: str) -> None:
        rdf_directory = self.__experiments_directory + self.dataset_name + "/rdf_summary_graph/"
        if not self.__experiment_state["state"]["serialized"]:
            raise ValueError("Can not merge files if the serializer has not successfully been executed")
        files = []
        if "c" in files_to_merge:
            files.append("contains.nt")
        if "d" in files_to_merge:
            files.append("data.nt")
        if "i" in files_to_merge:
            files.append("intervals.nt")
        if "r" in files_to_merge:
            files.append("refines.nt")
        if "s" in files_to_merge:
            files.append("sizes.nt")
        if len(files) < 2:
            raise ValueError("`files_to_merge` should be a string containing at least two of \"cdirfs\" (in order two select at least two files to merge).")
        cat_command = "cat " + " ".join(files) + " > output_graph.nt"
        command = ["bash", "-c", cat_command]
        subprocess.run(command, cwd=rdf_directory)
        self.__load_state()
    
    @property
    def dataset_path(self) -> None | str:
        return self.__dataset_path
    
    @property
    def dataset_format(self) -> None | str:
        return self.__dataset_format
    
    @property
    def dataset_name(self) -> None | str:
        return self.__dataset_name
    
    @property
    def experiment_settings(self) -> None | dict[str, dict]:
        return self.__experiment_settings
    
    @property
    def experiment_state(self) -> None | dict[str, Any]:
        return self.__experiment_state
    
    def __load_settings(self) -> None:
        scripts_directory = self.__experiments_directory + "scripts/"
        for script, settings in self.__experiment_settings.items():
            with open(scripts_directory + script + ".config") as config_file:
                for line in config_file:
                    setting, value = line.split("=")
                    settings[setting] = value.strip()
    
    def __load_state(self) -> None:
        experiment_directory = self.__experiments_directory + self.__dataset_name + "/"
        experiment_directory_exists = os.path.exists(experiment_directory)
        if experiment_directory_exists:
            with open(experiment_directory + "state.toml") as toml_file:
                self.__experiment_state = toml.load(toml_file)
        else:
            self.__experiment_state = None

""""
# An example of how to work with the interface

# Load the interface
from summary_loader.summary_interface import SummaryInterface

# Set the experiment directory and create an interface
experiment_directory = "./"
my_interface = SummaryInterface(experiment_directory)

# Get the settings analyze them and change a setting
settings = my_interface.experiment_settings
import pprint
pprint.pp(settings)
settings["serializer"]["time"] = "01:00:00"
pprint.pp(settings)

# Change the settings. This changes the settings in the interface AND in the actual config files
my_interface.change_settings(settings)

# Set a dataset to be summarized
dataset_path = "../data/fb15k.nt"
my_interface.set_dataset(dataset_path)

# Run the summarization code on the given dataset with the given settings
my_interface.run_experiment()

# We can check on the state of the experiment
state = my_interface.experiment_state
pprint.pp(state)

# We can merge some of the RDF ntriples files together if we want one merged file
my_interface.merge_files("drs")  # Merges data.nt, refines.nt and sizes.nt
"""