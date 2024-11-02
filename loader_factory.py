class DataSink:
    """
    Abstract class 
    """

    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params 


    def load_data_frame(self):
        """
        Abstract method, Function will be defined in sub classes
        """

        raise ValueError("Not Implemented")

