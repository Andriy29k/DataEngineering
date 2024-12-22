from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType([
    StructField("house_id", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("square_meters", FloatType(), True)
])

df = spark.read.csv("/home/jovyan/work/data.csv", header=True, schema=schema)

df.show()

from abc import ABC, abstractmethod

class Expectation(ABC):
    def __init__(self, column, dimension, add_info={}):
        self.column = column
        self.dimension = dimension
        self.add_info = add_info

    @abstractmethod
    def test(self, ge_df):
        pass

class NotNullExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_not_be_null(
            column=self.column,
            meta={"dimension": self.dimension}
        )


from great_expectations.dataset.sparkdf_dataset import SparkDFDataset 
class GreaterThanExpectation(Expectation):
    def __init__(self, column, dimension, add_info, threshold=None, **kwargs):
        super().__init__(column, dimension, add_info)
        self.threshold = threshold

    def test(self, ge_df: SparkDFDataset):
        if self.threshold is None:
            raise ValueError("Threshold must be provided for 'greater_than' rule")
        
        if isinstance(self.threshold, (int, float)):
            ge_df.expect_column_values_to_be_between(
                column=self.column,
                min_value=self.threshold,
                meta={"dimension": self.dimension}
            )
        else:
            raise ValueError(f"Threshold must be a number, but got {type(self.threshold)}")

import json

class JSONFileReader:
    def __init__(self, filename):
        self.filename = filename

    def read(self):
        with open(self.filename, 'r') as f:
            return json.load(f)

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

class DataQuality:
    def __init__(self, pyspark_df, config_path):
        self.pyspark_df = pyspark_df
        self.config_path = config_path

    def rule_mapping(self, dq_rule):
        return {
            "check_if_not_null": NotNullExpectation,
            "check_if_greater_than": GreaterThanExpectation
        }[dq_rule]

    def convert_to_ge_df(self):
        return SparkDFDataset(self.pyspark_df)

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def run_test(self):
        ge_df = self.convert_to_ge_df()  
        config = self.read_config()  

        for rule in config:
            rule_name = rule["rule_name"]
            column_name = rule["column"]
            dimension = rule["dimension"]
            add_info = rule.get("add_info", {})

            expectation_class = self.rule_mapping(rule_name)

            expectation_instance = expectation_class(
                column=column_name,
                dimension=dimension,
                add_info=add_info
            )

            expectation_instance.test(ge_df)

        dq_results = ge_df.validate()
        return dq_results
    
class DataQuality:
    def __init__(self, pyspark_df, config_path):
        self.pyspark_df = pyspark_df
        self.config_path = config_path

    def rule_mapping(self, dq_rule):
        return {
            "check_if_not_null": NotNullExpectation,
            "check_if_greater_than": GreaterThanExpectation
        }[dq_rule]

    def convert_to_ge_df(self):
        return SparkDFDataset(self.pyspark_df)

    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()

    def run_test(self):
  
        ge_df = self.convert_to_ge_df()
        config = self.read_config()  
        
        for column in config["columns"]:  
            if column.get("dq_rule(s)") is None:
                continue
            
            for dq_rule in column["dq_rule(s)"]:
                expectation_obj = self.rule_mapping(dq_rule["rule_name"])
                
                threshold = dq_rule["add_info"].get('threshold', None)
                
                if dq_rule["rule_name"] == "check_if_not_null":
                    expectation_instance = expectation_obj(column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"])
                else:
                    expectation_instance = expectation_obj(
                        column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"], threshold
                    )
                
                expectation_instance.test(ge_df)
        
        dq_results = ge_df.validate() 
        
        return dq_results

dq = DataQuality(df, "/home/jovyan/work/config.json")

dq_results = dq.run_test()

dq_data = []

for result in dq_results["results"]:
    if result["success"] == True:
        status = 'PASSED'
    else:
        status = 'FAILED'
    
    dq_data.append((
        result["expectation_config"]["kwargs"]["column"],  
        result["expectation_config"]["meta"]["dimension"], 
        status, 
        result["expectation_config"]["expectation_type"],  
        result["result"]["unexpected_count"],  
        result["result"]["element_count"],  
        result["result"]["unexpected_percent"],  
        float(100 - result["result"]["unexpected_percent"])  
    ))

dq_columns = ["column", "dimension", "status", "expectation_type", "unexpected_count", 
              "element_count", "unexpected_percent", "percent"]

dq_df = spark.createDataFrame(data=dq_data, schema=dq_columns)

dq_df.show(truncate=False)
dq_df.write.format("delta").mode("overwrite").save("/home/jovyan/work/processed_data") 




