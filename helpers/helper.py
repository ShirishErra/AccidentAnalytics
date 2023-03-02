import yaml
import pandas as pd

class Helper:

    def read_config(self, file_path):
        """
        Read Config file in YAML format

        :param file_path: file path to config.yaml
        :return: dictionary with config details
        """
        with open(file_path, "r") as f:
            return yaml.safe_load(f)

    def get_dataframe(self, file_path):
        """
        Read the input files and return the dadtaframe

        :param file_path: file path to input csv files
        :return: dataframe
        """
        df = pd.read_csv(file_path)
        return df

    def dataframe_to_output(self, dataframe, output_file):
        """
        Read the dataframe and ouput file and write the dadtaframe to that file

        :param dataframe: dataframe
        :param output_file: file path to output csv files
        :return: nothing
        """

        dataframe.to_csv(output_file, index=False)
