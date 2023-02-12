import datetime
import json
import time
from typing import Union

import petl as etl

from adverity.pipeline.adverity_types import ProductionRepository, StagingRepository


class TransformService:
    @staticmethod
    def transform(repository: StagingRepository) -> ProductionRepository:
        people_table = etl.fromjson(repository.people_repository)
        with open(repository.planet_repository, "r") as f:
            planet_lookup = json.load(f)

        table = etl.addfield(
            people_table,
            "date",
            lambda row: datetime.date.fromisoformat(row["edited"][:10]),
        )
        table = etl.convert(table, "homeworld", lambda v: planet_lookup[v])
        table = etl.cutout(
            table,
            "films",
            "species",
            "vehicles",
            "starships",
            "created",
            "edited",
            "url",
        )
        timestamp = int(time.time())
        data_repository = f"data_{timestamp}.csv"
        etl.tocsv(table, data_repository)

        return ProductionRepository(
            data_repository=data_repository,
        )

    @staticmethod
    def aggregate_by(repository: Union[ProductionRepository, str], *args):
        """Aggregate by its attribute"""
        if isinstance(repository, ProductionRepository):
            data_repository = repository.data_repository
        else:
            data_repository = repository
        table = etl.fromcsv(data_repository)
        table = etl.aggregate(table, key=args, aggregation=len)
        return table

    @staticmethod
    def head(repository: Union[ProductionRepository, str], n: int = 10):
        if isinstance(repository, ProductionRepository):
            data_repository = repository.data_repository
        else:
            data_repository = repository

        table = etl.fromcsv(data_repository)
        table = etl.head(table, n)
        return table


if __name__ == "__main__":
    staging_repository = StagingRepository(
        people_repository="people_1676185299.json",
        planet_repository="planets_1676185299.json",
    )
    repository = TransformService.transform(repository=staging_repository)
    # repository = ProductionRepository(data_repository="data_1676113851.csv")
    # TransformService.aggregate_by(repository, "homeworld", "birth_year")
