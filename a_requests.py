import asyncio
import aiohttp
import more_itertools
import datetime
from model import SessionDB, init_orm, SwapiStar

MAX_REQUESTS = 5


async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.json()


async def get_people(person_id, session):
    response = await session.get(f"http://swapi.dev/api/people/{person_id}/")
    json_data = await response.json()
    remove_keys = ['created', 'edited', 'url']
    for key in remove_keys:
        json_data.pop(key, None)
    return json_data


async def enrich_person_data(person):
    async with aiohttp.ClientSession() as session:
        if 'homeworld' in person:
            homeworld_data = await fetch_url(session, person['homeworld'])
            person['homeworld'] = homeworld_data.get('name', 'Unknown')
        else:
            person['homeworld'] = 'Unknown'

        if 'films' in person:
            films = []
            for film_url in person['films']:
                film_data = await fetch_url(session, film_url)
                films.append(film_data['title'])
            person['films'] = ', '.join(films)
        else:
            person['films'] = 'Unknown'

        if 'species' in person:
            species = []
            for species_url in person['species']:
                species_data = await fetch_url(session, species_url)
                species.append(species_data['name'])
            person['species'] = ', '.join(species)
        else:
            person['species'] = 'Unknown'

        if 'vehicles' in person:
            vehicles = []
            for vehicle_url in person['vehicles']:
                vehicle_data = await fetch_url(session, vehicle_url)
                vehicles.append(vehicle_data['name'])
            person['vehicles'] = ', '.join(vehicles)
        else:
            person['vehicles'] = 'Unknown'

        if 'starships' in person:
            starships = []
            for starship_url in person['starships']:
                starship_data = await fetch_url(session, starship_url)
                starships.append(starship_data['name'])
            person['starships'] = ', '.join(starships)
        else:
            person['starships'] = 'Unknown'

    return person


async def insert_people(enriched_people):
    async with SessionDB() as session:
        model_list = []
        for person_dict in enriched_people:
            new_obj = SwapiStar(json=person_dict)
            model_list.append(new_obj)
        session.add_all(model_list)
        await session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as session:
        coros = (get_people(i, session) for i in range(1, 101))
        for coro_chunk in more_itertools.chunked(coros, 5):
            result = await asyncio.gather(*coro_chunk)
            enriched_people = await asyncio.gather(*(enrich_person_data(person) for person in result))
            asyncio.create_task(insert_people(enriched_people))
        tasks = asyncio.all_tasks()
        main_task = asyncio.current_task()
        tasks.remove(main_task)
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
