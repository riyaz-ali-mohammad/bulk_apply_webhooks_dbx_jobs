# Databricks notebook source
# Intentional failure so on_failure webhook fires.
# To exercise on_success instead, comment the raise line below and re-run.
raise Exception("intentional failure to exercise webhook on_failure")
print("ok")
