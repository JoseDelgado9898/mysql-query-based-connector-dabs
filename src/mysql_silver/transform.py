# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# The fully-qualified bronze table is injected via the pipeline `configuration`
# block (see resources/mysql_silver.pipeline.yml) so the catalog/schema are not
# hardcoded in this source file.
SOURCE_TABLE = spark.conf.get("source_table")
TIME_OFF_SOURCE_TABLE = spark.conf.get("time_off_source_table")


@dp.table(
    name="employees_clean",
    comment="Cleaned employees from MySQL bronze: typed salary/hire_date and a derived full_name.",
)
def employees_clean():
    return spark.read.table(SOURCE_TABLE).select(
        F.col("my_row_id").cast("long").alias("employee_id"),
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "email",
        "department",
        F.to_date("hire_date").alias("hire_date"),
        F.col("salary").cast("double").alias("salary"),
    )


@dp.table(
    name="department_summary",
    comment="Headcount and average salary per department.",
)
def department_summary():
    return (
        spark.read.table("employees_clean")
        .groupBy("department")
        .agg(
            F.count("*").alias("headcount"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
        )
    )


@dp.table(
    name="time_off_clean",
    comment="Cleaned time-off requests from MySQL bronze: typed dates and a derived day count.",
)
def time_off_clean():
    return spark.read.table(TIME_OFF_SOURCE_TABLE).select(
        F.col("my_row_id").cast("long").alias("time_off_id"),
        F.col("employee_id").cast("long").alias("employee_id"),
        "leave_type",
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        (F.datediff(F.to_date("end_date"), F.to_date("start_date")) + 1).alias("days"),
        "status",
    )


@dp.table(
    name="time_off_by_employee",
    comment="Approved and pending time-off days per employee.",
)
def time_off_by_employee():
    return (
        spark.read.table("time_off_clean")
        .join(spark.read.table("employees_clean"), "employee_id")
        .groupBy("employee_id", "full_name", "department")
        .agg(
            F.sum(F.when(F.col("status") == "APPROVED", F.col("days")).otherwise(0)).alias("approved_days"),
            F.sum(F.when(F.col("status") == "PENDING", F.col("days")).otherwise(0)).alias("pending_days"),
        )
    )
