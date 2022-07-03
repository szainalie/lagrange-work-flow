from node.base import PTransform_Node
import apache_beam as beam


class FilterColumn(PTransform_Node):
    def __init__(self, column_name_list):
        self.column_name = column_name_list

    def expand(self, pcoll):
        try:
            return pcoll | "Map" >> beam.Map(lambda x: (x[i] for i in self.column_name))
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)


class PandasFilterColumn(PTransform_Node):
    def __init__(self, data, column_name_list):
        self.column_name = column_name_list
        self.data = data

    def filterByColumn(self):
        try:
            return self.data[self.column_name]
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)
