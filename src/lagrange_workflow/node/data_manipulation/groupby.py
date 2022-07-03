"""
There are 2 classes to implement the groupby operation.
One is the Groping by Apache beam GroupByKey.
The other is the Grouping by Pandas groupby.
"""
from node.base import PTransform_Node
import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe


class GroupBy(PTransform_Node):
    def __init__(self, column_name):
        self.column_name = column_name

    def expand(self, pcoll):
        return pcoll | "GroupBy" >> beam.GroupByKey(lambda x: x[self.column_name])


class PandasGroupBy(PTransform_Node):
    def __init__(self, column_name):
        self.column_name = column_name

    def groupBy(self, data):

        return to_dataframe(data.groupby(self.column_name))
