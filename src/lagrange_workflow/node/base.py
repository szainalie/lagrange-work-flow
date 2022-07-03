import apache_beam as beam
import logging


class PTransform_Node(beam.PTransform):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.handler = logging.StreamHandler()
        self.logger.setLevel(logging.INFO)
        self.logger.info("Node initialized")


class DoFn_Node(beam.DoFn):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.handler = logging.StreamHandler()
        self.logger.setLevel(logging.INFO)
        self.logger.info("Node initialized")
