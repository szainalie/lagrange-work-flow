"""
3 classes for row filter:
2 class use Apache Beam
1 class use Pandas
"""
import apache_beam as beam
from node.base import PTransform_Node, DoFn_Node


class FilterByColumn(PTransform_Node):
    def __init__(self, column_name, value):
        self.column_name = column_name
        self.value = value

    def expand(self, pcoll):
        try:
            yield pcoll | "FilterByColumn" >> beam.Filter(
                lambda x: x[self.column_name] == self.value
            )
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)


class filterByColumn(DoFn_Node):
    def __init__(self, column_name, value):
        try:
            self.column_name = column_name
            self.value = value
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)


class PandasFilterByColumn(PTransform_Node):
    def __init__(self, data, column_name, value):
        self.column_name = column_name
        self.value = value
        self.data = data

    def process(self):
        try:
            return self.data[self.data[self.column_name] == self.value]
        except Exception as e:
            self.logger.error(e, name=e.__class__.__name__)
