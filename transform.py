from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains


class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """

        transcatioInputDF = inputDFs.get("transcatioInputDF")

        print("transcatioInputDF in transform")

        transcatioInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transcatioInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("JOINED DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

