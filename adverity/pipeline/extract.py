import asyncio
import json
import math
import time
from typing import Dict, Iterable, Optional

from aiohttp import ClientSession

from adverity.pipeline.adverity_types import PeopleAndPlanetAddress, StagingRepository

PEOPLE_PAGE_URL = "https://swapi.dev/api/people/"


class ExtractService:
    def __init__(self, session: Optional[ClientSession]):
        self._session = session
        self.end_page = 0

    async def __aenter__(self):
        if self._session is None:
            self._session = ClientSession()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    @property
    def session(self):
        return self._session

    async def get_people_on_single_page(self, page: int = 1) -> PeopleAndPlanetAddress:
        async with self.session.get(PEOPLE_PAGE_URL, params={"page": page}) as resp:
            response = await resp.json()
            people = response["results"]
            planet_addresses = set([person["homeworld"] for person in people])

            if page == 1:
                count = response["count"]
                page_size = len(response["results"])
                self.end_page = math.ceil(count / page_size)

            return PeopleAndPlanetAddress(
                people=response["results"],
                planet_addresses=planet_addresses,
            )

    async def get_people_from_pages(
        self, start_page: Optional[int] = None, end_page: Optional[int] = None
    ) -> PeopleAndPlanetAddress:
        if start_page is None:
            start_page = 1
        if end_page is None:
            end_page = self.end_page

        tasks = (self.get_people_on_single_page(page_number) for page_number in range(start_page, end_page + 1))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        people = []
        planet_addresses = set()

        for result in results:
            if isinstance(result, PeopleAndPlanetAddress):
                people.extend(result.people)
                planet_addresses.update(result.planet_addresses)

        return PeopleAndPlanetAddress(people=people, planet_addresses=planet_addresses)

    async def get_planet(self, url: str) -> Dict[str, str]:
        async with self.session.get(url) as resp:
            response = await resp.json()
            return {url: response["name"]}

    async def get_planets(self, urls: Iterable[str]) -> Dict[str, str]:
        tasks = (self.get_planet(url) for url in urls)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ret: Dict[str, str] = {}

        for result in results:
            if isinstance(result, dict):
                ret.update(**result)

        return ret

    async def download_data_async(self) -> StagingRepository:
        people = []
        planet_addresses = set()

        people_and_planet_address_from_first_page = await self.get_people_on_single_page(page=1)
        people.extend(people_and_planet_address_from_first_page.people)
        planet_addresses.update(people_and_planet_address_from_first_page.planet_addresses)

        if self.end_page >= 2:
            people_and_planet_address = await self.get_people_from_pages(start_page=2, end_page=self.end_page)
            people.extend(people_and_planet_address.people)
            planet_addresses.update(people_and_planet_address.planet_addresses)

        planets = await self.get_planets(planet_addresses)

        timestamp = int(time.time())
        people_data_repository = f"people_{timestamp}.json"
        planet_data_repository = f"planets_{timestamp}.json"

        with open(people_data_repository, "w") as f:
            json.dump(people, f)

        with open(planet_data_repository, "w") as f:
            json.dump(planets, f)

        return StagingRepository(
            people_repository=people_data_repository,
            planet_repository=planet_data_repository,
        )


async def download_meta_async() -> StagingRepository:
    async with ClientSession() as session:
        service = ExtractService(session)
        return await service.download_data_async()


def download_meta() -> StagingRepository:
    return asyncio.run(download_meta_async())


if __name__ == "__main__":
    download_meta()
