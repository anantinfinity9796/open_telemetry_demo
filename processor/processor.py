# processor.py


class Processor():
    """Class to consumer messages from the Kafka Topic and write to the file"""

    def __init__(self,):
        self._output_filename = "data_output.csv"