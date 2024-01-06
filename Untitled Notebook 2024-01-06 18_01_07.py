# Databricks notebook source
#this is git

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("path")

