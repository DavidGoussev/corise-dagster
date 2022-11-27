import csv
from datetime import datetime
from heapq import nlargest
from random import randint
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
)
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


@op(
    config_schema={
        's3_key': String
        },
    out = {
        "stocks": Out(dagster_type=List[Stock], description="s3 list stock data", is_required=False),
        "empty_stocks": Out(Any, is_required=False)
        }
    )
def get_s3_data(context):
    stock_check = csv_helper(context.op_config["s3_key"])
    if not any(stock_check):
        yield Output(None, "empty_stocks")
    else:
        yield Output(list(stock_check), "stocks")


@op(    
    config_schema = {'nlargest': int},
    ins = {"stocks": In(dagster_type=List[Stock], description="s3 list stock data")},
    out = DynamicOut(Aggregation)
)
def process_data(context, stocks):
    stocks.sort(key=lambda stock: stock.high, reverse=True)
    n = context.op_config["nlargest"]
    for i in range(0,n):
        n_aggregation = Aggregation(date=stocks[i].date, high=stocks[i].high)
        yield DynamicOutput(n_aggregation, mapping_key=str(i))


@op
def put_redis_data(context, aggregates: Aggregation):
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks) -> Nothing:
    context.log.info("No stocks returned")


@job
def week_1_challenge():
    stocks, empty_stocks = get_s3_data()
    empty_stock_notify(empty_stocks)
    (process_data(stocks)).map(put_redis_data)
