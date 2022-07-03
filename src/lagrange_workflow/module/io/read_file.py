"""
A read file module that read file data based on its file type.
"""
from module.base import module_base
from node.io.read_csv import ReadFromCsvFile
from node.io.read_excel import ReadFromExcelFile


class TypeCheck:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_type = None
        self.file_extension = None

    def get_file_type(self):
        self.file_extension = self.file_path.split(".")[-1]
        if self.file_extension == "xlsx" or self.file_extension == "xls":
            self.file_type = "excel"
        elif self.file_extension == "csv":
            self.file_type = "csv"
        else:
            self.file_type = "unknown"
        return self.file_type

    def get_file_extension(self):
        return self.file_extension


class ReadFromFile(module_base):
    def __init__(self, file_path):
        super(ReadFromFile, self).__init__()
        try:
            self.logger.info("Module initialized")
            self.file_type = TypeCheck(file_path).get_file_type()
            if self.file_type == "excel":
                self.read_data_from_file = (
                    self.pipeline
                    | "ReadFromExcelFile" >> ReadFromExcelFile(self.file_path)
                )

            elif self.file_type == "csv":
                self.read_data_from_file = (
                    self.pipeline | "ReadFromCsvFile" >> ReadFromCsvFile(self.file_path)
                )
            else:
                raise Exception("File type is not supported")

        except Exception as e:
            self.logger.error(e, name=__name__)

    def get_data(self):
        try:
            self.logger.info("Module running")
            self.run()
            return self.read_data_from_file
        except Exception as e:
            self.logger.error(e, name=__name__)
