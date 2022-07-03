"""
This class reads a csv file and and return its contains as a Pcollection.
"""
from node.base import PTransform_Node
from apache_beam.dataframe.io import read_csv


class ReadFromCsvFile(PTransform_Node):
    def __init__(self, file_path):
        self.file_path = file_path

    def expand(self, pcoll):
        try:
            return pcoll | "ReadFile" >> read_csv(self.file_path)
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)
