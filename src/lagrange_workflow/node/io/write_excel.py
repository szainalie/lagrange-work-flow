"""
A class to write panadas dataframes to an excel file.
"""
from node.base import PTransform_Node


class WriteToExcelFile(PTransform_Node):
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, pandas_dataframe):
        return pandas_dataframe.to_excel(self.file_path, index=False)
