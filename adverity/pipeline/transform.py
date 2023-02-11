import datetime
import time
import json

import petl as etl
from adverity.pipeline.adverity_types import StagingRepository, ProductionRepository


class TransformService:
    @staticmethod
    def transform(repository: StagingRepository) -> ProductionRepository:
        people_table = etl.fromjson(repository.people_repository)
        with open(repository.planet_repository, "r") as f:
            planet_lookup = json.load(f)

        table = etl.addfield(people_table, 'date', lambda row: datetime.date.fromisoformat(row["edited"][:10]))
        table = etl.convert(table, 'homeworld', lambda v: planet_lookup[v])
        table = etl.cutout(table, 'films', 'species', 'vehicles', 'starships', 'created', 'edited', 'url')
        timestamp = int(time.time())
        data_repository = f"data_{timestamp}.csv"
        etl.tocsv(table, data_repository)

        return ProductionRepository(
            data_repository=data_repository,
        )

    @staticmethod
    def aggregate_by(repository: ProductionRepository, *args):
        table = etl.fromcsv(repository.data_repository)
        table = etl.aggregate(table, key=args, aggregation=len)
        etl.tocsv(table, "result.csv")


if __name__ == '__main__':
    # staging_repository = StagingRepository(
    #     people_repository="people_1676071753.json",
    #     planet_repository="planets_1676071753.json"
    # )
    # repository = TransformService.transform(repository=staging_repository)
    repository = ProductionRepository(data_repository="data_1676113851.csv")
    TransformService.aggregate_by(repository, "homeworld", "birth_year")

