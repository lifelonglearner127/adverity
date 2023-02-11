from dataclasses import dataclass
from collections import namedtuple
from typing import List


@dataclass(frozen=True)
class PeopleAndPlanetAddress:
    people: List[dict]
    planet_addresses: set[str]


@dataclass(frozen=True)
class StagingRepository:
    people_repository: str
    planet_repository: str


@dataclass(frozen=True)
class ProductionRepository:
    data_repository: str


DataAndRepository = namedtuple('DataAndRepository', ['data', 'repository', ])
