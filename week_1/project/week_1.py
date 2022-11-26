import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={'s3_key': String})
#use pythonic function connotations, relation to out
def get_s3_data(context) -> List[Stock]:
    s3_list_stock = list(csv_helper(context.op_config["s3_key"]))
    return s3_list_stock


@op
def process_data(context, stocks: List[Stock]):
    high_stock = max(stocks, key=lambda stock:stock.high)
    high_aggregation = Aggregation(date=high_stock.date, high=high_stock.high)
    return high_aggregation


@op
def put_redis_data(context, highest: Aggregation):
    pass


@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
