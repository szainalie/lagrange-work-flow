"""
A module that write file data based on its file type.
"""
from module.base import module_base
from node.io.write_csv import WriteToCsvFile
from node.io.write_excel import WriteToExcelFile


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


class WriteToFile(module_base):
    def __init__(self, file_path):
        super(WriteToFile, self).__init__()
        try:
            self.logger.info("Module initialized")
            self.file_type = TypeCheck(file_path).get_file_type()
            if self.file_type == "excel":
                self.write_data_to_file = (
                    self.pipeline
                    | "WriteToExcelFile" >> WriteToExcelFile(self.file_path)
                )
            elif self.file_type == "csv":
                self.write_data_to_file = (
                    self.pipeline | "WriteToCsvFile" >> WriteToCsvFile(self.file_path)
                )
            else:
                raise Exception("File type is not supported")

        except Exception as e:
            self.logger.error(e, name=__name__)

    def get_data(self):
        try:
            self.logger.info("Module running")
            self.run()

        except Exception as e:
            self.logger.error(e, name=__name__)
