"""
Class for reading Excel files and retun content as pandas dataframe. 
"""
import pandas as pd
from node.base import PTransform_Node


class ReadFromExcelFile(PTransform_Node):
    def __init__(self, file_path):
        self.file_path = file_path

    def expand(self, pcoll):
        df = pd.read_excel(self.file_path)
        return df
