# %run "./loader_factory"
# importing loader_factory

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):

        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/apple_analysis/output/airpodsAfterIphone", 
            method = "overwrite"
        ).load_data_frame()

