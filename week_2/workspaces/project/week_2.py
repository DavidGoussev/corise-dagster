from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock

@op(
    config_schema={'s3_key': String},
    required_resource_keys={"s3"}
    )
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return list(csv_helper(context.op_config["s3_key"]))

@op(    
    ins = {"stocks": In(dagster_type=List[Stock], description="s3 list stock data")},
    out = {"high_aggregation": Out(dagster_type=Aggregation, description="highest stock price/date aggregation")}
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    high_stock = max(stocks, key=lambda stock:stock.high)
    return Aggregation(date=high_stock.date, high=high_stock.high)

@op (
    required_resource_keys={"redis"}
)
def put_redis_data(context, high_aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=str(high_aggregation.date), value=str(high_aggregation.high))

@op (
    required_resource_keys={"s3"}
)
def put_s3_data(context, high_aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key=high_aggregation.date, data=high_aggregation)


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    processed = process_data(stocks)
    put_redis_data(processed)
    put_s3_data(processed)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource()
    }
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    }
)
