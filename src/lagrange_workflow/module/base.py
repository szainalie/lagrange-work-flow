"""
Base class for all Pipeline modules.
"""
import apache_beam as beam
import logging
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions


class module_base(Pipeline):
    def __init__(self, options=None):
        super(module_base, self).__init__(options)
        self.logger = logging.getLogger(__name__)
        self.logger.handler = logging.StreamHandler()
        self.logger.setLevel(logging.INFO)
        self.logger.info("Module initialized")
        self.options = options
        self.pipeline = Pipeline(options=self.options)

    def run(self):
        self.logger.info("Module running")
        self.pipeline.run()
        self.logger.info("Module finished")
