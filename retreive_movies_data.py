import json
import requests
import os
import asyncio
from dotenv import load_dotenv
import time

URL = "https://api.themoviedb.org/3"
# https://developer.themoviedb.org/reference/movie-details

MOVIES_IDS_FILE_NAME = "movies_ids.json"
CREDITS_FILE_NAME = "credits.json"
MOVIES_FILE_NAME = "movies.json"

load_dotenv()


def read_token() -> str:
    token: str | None = os.getenv("MOVIE_API_TOKEN")
    if not token:
        raise ValueError("missing movies api token")

    return token


def read_data(file_name: str, default: object):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", file_name)

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)

        return data

    return default


def save_data(file_name: str, data: list[any]) -> None:
    """
    Save the file inside the 'data' folder, overwriting the
    file is already exists.

    Args:
        file_name(str): name that the file will be saved
        data(list[any]): data that will be saved

    Returns: None
    """

    path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", file_name
    )

    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


async def retreive_movies_id(page: int, token: str) -> list[int]:
    """
    Retreive all movies ids in 'themoviedb' and save in a 'movies_ids' file
    in json format.

    Args:
        page (int): the current page to search the movies
        token (str): token to authenticate with the api

    Returns: None
    """
    try:
        response = requests.get(
            url=f"{URL}/discover/movie?include_adult=false&include_video=false&language=en-US&sort_by=popularity.desc",
            params={"page": page},
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )

        if response.status_code != 200:
            raise Exception("Error to retreive the movies data", response.json())

        movies = response.json()["results"]

        new_movies_id: list[int] = [movie["id"] for movie in movies]

        return new_movies_id
    except Exception as e:
        print(f"error to get movies from the page: {page}")
        print(e)
        return []


async def retreive_movies(movie_id: int, token: str):
    try:
        response = requests.get(
            f"{URL}/movie/{movie_id}",
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )

        if response.status_code != 200:
            raise Exception("Unable to get the response")

        movie_detail = response.json()

        return movie_detail
    except Exception as e:
        print(f"Error to retreive movie details from movie: {movie_id}")
        print(e)
        return {}


async def retreive_movies_crew(movie_id: int, token: str):
    """
    Retreive the movie crew based on the movie id provided as param.
    NOTE: the 'id' in the response is the movie_id

    Args:
        movie_id(int): movie that should be search for the crew
        token(str): to authenticate with the api

    Returns:
        Crew: the object containing the crew's for the provided movie
            *   **id** (str): the movie id
    """

    try:
        response = requests.get(
            url=f"{URL}/movie/{movie_id}/credits",
            headers={"accept": "application/json", "Authorization": f"Bearer {token}"},
        )

        if response.status_code != 200:
            raise Exception("Error to retreive the credits data", response.json())

        crew = response.json()

        return crew
    except Exception as e:
        print(f"error to get crew from the movie: {movie_id}")
        print(e)
        return {}


async def save_movies_ids(token: str):
    data_to_save = read_data(
        MOVIES_IDS_FILE_NAME,
        {"page": 1, "pages_per_run": 10, "max_pages": 500, "data": []},
    )

    pages: list[int] = list(
        range(
            data_to_save["page"], data_to_save["page"] + data_to_save["pages_per_run"]
        )
    )

    if data_to_save["page"] >= data_to_save["max_pages"]:
        raise ValueError("max pages to read exceeded")

    for page in pages:
        movie: list[int] = await retreive_movies_id(page, token)
        print(f"Saving movies from the page: {page}")
        data_to_save["data"].extend(movie)

    # update the page readed info
    data_to_save["page"] += data_to_save["pages_per_run"]

    save_data("movies_ids.json", data_to_save)


async def save_movies(movies_ids: list[int], token: str):
    movies = read_data(MOVIES_FILE_NAME, [])
    movies_ids_already_saved = [m["id"] for m in movies]

    for movie_id in movies_ids:
        if movie_id not in movies_ids_already_saved:
            movie = await retreive_movies(movie_id, token)
            print(f"Retreiving movie {movie_id} details")
            movies.append(movie)

    save_data("movies.json", movies)


async def save_credits_data(movies_ids: list[int], token: str):
    credits = read_data(CREDITS_FILE_NAME, [])
    # this variable save the movies id that already has the credit saved
    credits_already_saved = [c["id"] for c in credits]

    for movie_id in movies_ids:
        if movie_id not in credits_already_saved:
            credit = await retreive_movies_crew(movie_id, token)
            print(f"Reading credit: {credit["id"]}")
            credits.append(credit)

    save_data("credits.json", credits)


async def main() -> None:
    try:
        token: str = read_token()

        await save_movies_ids(token)

        # use 'set' to ensure that all 'ids' are read just one time
        movies_ids = set(
            read_data(MOVIES_IDS_FILE_NAME, [{"data": []}])["data"]
        )  # this time the movies ids is a default an empty list because we don't need the extra data

        await save_credits_data(movies_ids, token)
        await save_movies(movies_ids, token)
    except ValueError as e:
        print(e)


if __name__ == "__main__":
    qtds = list(range(10))
    for i in qtds:
        print(f"\n\nRunning {i}\n\n")
        asyncio.run(main())
        time.sleep(60*5)
