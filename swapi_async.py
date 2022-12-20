import asyncio

from aiohttp import ClientSession
from more_itertools import chunked

from db import engine, Session, People, Base

CHUNK_SIZE = 10


async def get_data(url):
    session = ClientSession()
    response = await session.get(url)
    data_json = await response.json()
    await session.close()
    return data_json


async def get_person(person_id):
    session = ClientSession()
    response = await session.get(f'https://swapi.dev/api/people/{person_id}')
    person = await response.json()
    await session.close()

    if person.get('detail') == None:
        homeworld_data = await get_data(person['homeworld'])
        person['homeworld'] = homeworld_data['name']

        coroutines = [get_data(i) for i in person['films']]
        films = await asyncio.gather(*coroutines)
        person_films = []
        for film in films:
            person_films.append(film['title'])
        person['films'] = ', '.join(person_films)

        coroutines = [get_data(i) for i in person['species']]
        species = await asyncio.gather(*coroutines)
        person_species = []
        for specie in species:
            person_species.append(specie['name'])
        person['species'] = ', '.join(person_species)

        coroutines = [get_data(i) for i in person['vehicles']]
        vehicles = await asyncio.gather(*coroutines)
        person_vehicles = []
        for vehicle in vehicles:
            person_vehicles.append(vehicle['name'])
        person['vehicles'] = ', '.join(person_vehicles)

        coroutines = [get_data(i) for i in person['starships']]
        starships = await asyncio.gather(*coroutines)
        person_starships = []
        for starship in starships:
            person_starships.append(starship['name'])
        person['starships'] = ', '.join(person_starships)

    return person


async def get_people(start, end):
    for id_chunk in chunked(range(start, end), CHUNK_SIZE):
        coroutines = [get_person(i) for i in id_chunk]
        people = await asyncio.gather(*coroutines)
        for person in people:
            yield person


async def paste_people(people_jsons):
    async with Session() as session:
        for person_json in people_jsons:
            if person_json.get('detail') == None:
                people = People(birth_year=person_json['birth_year'],
                                eye_color=person_json['eye_color'],
                                films=person_json['films'],
                                gender=person_json['gender'],
                                hair_color=person_json['hair_color'],
                                height=person_json['height'],
                                homeworld=person_json['homeworld'],
                                mass=person_json['mass'],
                                name=person_json['name'],
                                skin_color=person_json['skin_color'],
                                species=person_json['species'],
                                starships=person_json['starships'],
                                vehicles=person_json['vehicles'])
                session.add(people)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    person_jsons_buffer = []

    person_count_json = await get_data('https://swapi.dev/api/people/')
    person_count = person_count_json['count']

    async for person_json in get_people(1, person_count + 1):
        person_jsons_buffer.append(person_json)
        if len(person_jsons_buffer) >= CHUNK_SIZE:
            asyncio.create_task(paste_people(person_jsons_buffer))
            person_jsons_buffer = []

    if person_jsons_buffer:
        await paste_people(person_jsons_buffer)

    tasks = set(asyncio.all_tasks())
    tasks = tasks - {asyncio.current_task()}
    for task in tasks:
        await task

    await engine.dispose()


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
