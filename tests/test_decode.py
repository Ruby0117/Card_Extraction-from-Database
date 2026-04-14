"""
test_decode.py
Unit tests for the decode module.

These tests use a lightweight local SparkSession so they can run
outside of a Databricks cluster (e.g. in CI / GitHub Actions).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from src.decode import (
    decode_dweltype,
    decode_q1_ownermanager,
    decode_q3,
    decode_q4,
    decode_status_fields,
    decode_q15,
    decode_q16,
)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("cmhc-card-extraction-tests")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# dweltype
# ---------------------------------------------------------------------------

def test_dweltype_apartment(spark):
    df = spark.createDataFrame([Row(dweltype=758280000)])
    result = decode_dweltype(df).collect()[0]["dweltype"]
    assert result == "Apartment"


def test_dweltype_row(spark):
    df = spark.createDataFrame([Row(dweltype=758280001)])
    result = decode_dweltype(df).collect()[0]["dweltype"]
    assert result == "Row"


def test_dweltype_unknown(spark):
    df = spark.createDataFrame([Row(dweltype=999)])
    result = decode_dweltype(df).collect()[0]["dweltype"]
    # Unknown value — should be treated as Apartment per original logic
    assert result == "Apartment"


# ---------------------------------------------------------------------------
# q1_ownermanager
# ---------------------------------------------------------------------------

def test_q1_manager(spark):
    df = spark.createDataFrame([Row(q1_ownermanager=758280000)])
    result = decode_q1_ownermanager(df).collect()[0]["q1_ownermanager"]
    assert result == "I am the manager"


def test_q1_refuse(spark):
    df = spark.createDataFrame([Row(q1_ownermanager=758280006)])
    result = decode_q1_ownermanager(df).collect()[0]["q1_ownermanager"]
    assert result == "Refuse"


# ---------------------------------------------------------------------------
# q3_sahconfirm
# ---------------------------------------------------------------------------

def test_q3_yes(spark):
    df = spark.createDataFrame([Row(q3_sahconfirm=758280002)])
    result = decode_q3(df).collect()[0]["q3_sahconfirm"]
    assert result == "Yes"


def test_q3_no(spark):
    df = spark.createDataFrame([Row(q3_sahconfirm=758280001)])
    result = decode_q3(df).collect()[0]["q3_sahconfirm"]
    assert result == "No"


# ---------------------------------------------------------------------------
# q4_projnameconfirmation
# ---------------------------------------------------------------------------

def test_q4_yes(spark):
    df = spark.createDataFrame([Row(q4_projnameconfirmation=758280000)])
    result = decode_q4(df).collect()[0]["q4_projnameconfirmation"]
    assert result == "Yes"


# ---------------------------------------------------------------------------
# status fields
# ---------------------------------------------------------------------------

def test_auto_status_complete(spark):
    df = spark.createDataFrame([Row(auto_status=758280003, manual_status=None)])
    result = decode_status_fields(df).collect()[0]["auto_status"]
    assert result == "Complete Data"


def test_manual_status_hard_refusal(spark):
    df = spark.createDataFrame([Row(auto_status=None, manual_status=758280001)])
    result = decode_status_fields(df).collect()[0]["manual_status"]
    assert result == "Hard Refusal"


# ---------------------------------------------------------------------------
# q15 — last renovation estimation
# ---------------------------------------------------------------------------

def test_q15_never(spark):
    df = spark.createDataFrame([Row(q15_buildinglastestimation=758280001)])
    result = decode_q15(df).collect()[0]["q15_buildinglastestimation"]
    assert result == "Never"


def test_q15_more_than_10(spark):
    df = spark.createDataFrame([Row(q15_buildinglastestimation=758280006)])
    result = decode_q15(df).collect()[0]["q15_buildinglastestimation"]
    assert result == "More than 10 years ago"


# ---------------------------------------------------------------------------
# q16 — building condition
# ---------------------------------------------------------------------------

def test_q16_excellent(spark):
    df = spark.createDataFrame([Row(q16_buildingcondition=758280000)])
    result = decode_q16(df).collect()[0]["q16_buildingcondition"]
    assert result == "Excellent: regular maintenance only"


def test_q16_poor(spark):
    df = spark.createDataFrame([Row(q16_buildingcondition=758280004)])
    result = decode_q16(df).collect()[0]["q16_buildingcondition"]
    assert result == "Poor: significant major repairs needed, significant deferred repairs/maintenance"
