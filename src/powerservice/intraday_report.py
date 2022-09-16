import datetime
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

from powerservice import trading


def create_dataframe(trades_list: list) -> DataFrame:
    spark = SparkSession.builder.master("local[1]") \
        .appName('python-powerservice') \
        .getOrCreate()

    df_schema = StructType([StructField("date", StringType(), True),
                            StructField("time", ArrayType(StringType()), True),
                            StructField("volume", ArrayType(StringType()), True),
                            StructField("id", StringType(), True)])

    df = spark.createDataFrame(data=trades_list, schema=df_schema)
    logging.debug("Created dataframe size : " + str(df.count()))
    return df


def extract_values(trades_list: list) -> DataFrame:
    """
    Extract the aggregate values out of the trades list
    :param: trades_list
    :return: Dataframe with aggregated values
    """

    df = create_dataframe(trades_list)
    zipped_df = df.select("date",
                          F.explode(
                              F.zip_with(F.col("time"), F.col("volume"), lambda x, y: F.concat_ws("#", x, y))).alias(
                              "new_field"), "id")
    extracted_df = zipped_df.select("date", F.split(F.col("new_field"), "#").getItem(0).alias("time"),
                                    F.split(F.col("new_field"), "#").getItem(1).alias("volume"), "id")

    typed_df = extracted_df.select(F.to_date(F.col("date"), "dd/MM/yyyy").alias("date"),
                                   F.hour(F.col("time")).alias("time"),
                                   F.col("volume").cast(IntegerType()),
                                   F.col("id"))

    result_df = typed_df.groupBy("time").sum("volume").orderBy("time")
    logging.debug("Extracted dataframe count : " + str(result_df.count()))
    return result_df


def agg_values_per_hour(path: str, date: str, save_date: str):
    """
    Aggregates the values of traders
    :param: path, path where the csv to be saved
    :param: date, date in which the the extract to run
    """
    trades_today = trading.get_trades(date=date)
    trades_today_df = extract_values(trades_today)

    dt_obj = datetime.datetime.strptime("01/03/2022", "%d/%m/%Y")
    prev_date_obj = dt_obj - datetime.timedelta(days=1)
    prev_date = prev_date_obj.strftime("%d/%m/%Y")

    trades_yesterday = trading.get_trades(date=prev_date)
    trades_yesterday_df = extract_values(trades_yesterday)

    output_df = trades_yesterday_df.where(F.col("time") >= F.lit("23")).union(
        trades_today_df.where(F.col("time") < F.lit("23")))

    logging.debug("Generated aggregated output count : " + str(output_df.count()))
    output_df.show(n=100)

    logging.info("Aggregated output path : " + path + "/PowerPosition_" + save_date + ".csv")
    output_df.write.csv(path + "/PowerPosition_" + save_date + ".csv")

    # output_df.withColumn("date", F.lit(given_date))
    logging.info("Delta table save path : " + path + "/PowerPosition")
    # output_df.write.format("delta").mode("append").partitionBy("date").save(path + "/PowerPosition")


def quality_check(date: str, path: str, save_date: str):
    """
    quality check for the incoming data
    :param: path, path where the csv to be saved
    :param: date, date in which the the extract to run
    """
    trades_today = trading.get_trades(date=date)
    trades_today_df = create_dataframe(trades_today)
    zipped_df = trades_today_df.select("date",
                                       F.explode(
                                           F.zip_with(F.col("time"), F.col("volume"),
                                                      lambda x, y: F.concat_ws("#", x, y))).alias(
                                           "new_field"), "id")
    extracted_df = zipped_df.select("date", F.split(F.col("new_field"), "#").getItem(0).alias("time"),
                                    F.split(F.col("new_field"), "#").getItem(1).alias("volume"), "id")

    typed_df = extracted_df.select(F.to_date(F.col("date"), "dd/MM/yyyy").alias("date"),
                                   F.hour(F.col("time")).alias("time"),
                                   F.col("volume").cast(IntegerType()),
                                   F.col("id"))
    typed_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in ['volume', 'time']]).show()
    logging.info("Quality check output path : " + path + "/PowerPosition_" + save_date + "_data_quality.csv")
    typed_df.write.csv(path + "/PowerPosition_" + save_date + "_data_quality.csv")


def data_profiling(date: str, path: str, save_date: str):
    """
    Data profiling for the incoming data
    :param: path, path where the csv to be saved
    :param: date, date in which the the extract to run
    """
    trades_today = trading.get_trades(date=date)
    trades_today_df = create_dataframe(trades_today)
    zipped_df = trades_today_df.select("date",
                                       F.explode(
                                           F.zip_with(F.col("time"), F.col("volume"),
                                                      lambda x, y: F.concat_ws("#", x, y))).alias(
                                           "new_field"), "id")
    extracted_df = zipped_df.select("date", F.split(F.col("new_field"), "#").getItem(0).alias("time"),
                                    F.split(F.col("new_field"), "#").getItem(1).alias("volume"), "id")

    typed_df = extracted_df.select(F.to_date(F.col("date"), "dd/MM/yyyy").alias("date"),
                                   F.hour(F.col("time")).alias("time"),
                                   F.col("volume").cast(IntegerType()),
                                   F.col("id"))
    logging.info("Data Profiling output path : " + path + "/PowerPosition_" + save_date + "_data_quality.csv")
    typed_df.select(F.col("volume")).summary().write.csv(path + "/PowerPosition_" + save_date + "_data_profiling.csv")


if __name__ == '__main__':
    path = sys.argv[1]
    given_date = '01/03/2022'

    logging.info("Report generation date : " + given_date)
    logging.info("Report will be save on path : " + path)

    now = datetime.datetime.now()
    formatted_datetime = now.strftime("%Y%m%d_%H%M%S")

    agg_values_per_hour(path, given_date, formatted_datetime)
    data_profiling(given_date, path, formatted_datetime)
    quality_check(given_date, path, formatted_datetime)
