import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def get_english_name(species):
    pattern = re.compile('(.*) (\(.*\))')
    x = pattern.match(species)

    return x.group(1)


@udf(returnType=StringType())
def get_start_year(period):
    pattern = re.compile('\((\d+)-(\d+)\)')
    x = pattern.match(period)
    return x.group(1)


@udf(returnType=StringType())
def get_trend(annual_percentage_change):
    if annual_percentage_change < -3.00:
        return 'strong decline'
    elif -3.00 <= annual_percentage_change <= -0.50:
        return 'weak decline'
    elif -0.50 < annual_percentage_change < 0.50:
        return 'no change'
    elif 0.50 <= annual_percentage_change <= 3.00:
        return 'weak increase'
    else:
        return 'strong increase'
