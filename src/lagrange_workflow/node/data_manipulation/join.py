"""
Class for joining two dataframes.
"""
from node.base import PTransform_Node


class PandasJoin(PTransform_Node):
    def __init__(self, left_data, right_data, left_key, right_key):
        self.left_data = left_data
        self.right_data = right_data
        self.left_key = left_key
        self.right_key = right_key

    def join(self):
        return self.left_data.merge(
            self.right_data, left_on=self.left_key, right_on=self.right_key
        )
