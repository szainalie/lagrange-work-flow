"""
A class for writing data to a csv file.
"""
from node.base import PTransform_Node


class WriteToCsvFile(PTransform_Node):
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, pandas_dataframe):
        return pandas_dataframe.to_csv(self.file_path, index=False)
