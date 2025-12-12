"""Kafka consumers for Hyperion ETL pipeline."""

from .films_consumer import FilmsConsumer
from .characters_consumer import CharactersConsumer
from .boxoffice_consumer import BoxOfficeConsumer
from .games_consumer import GamesConsumer
from .soundtracks_consumer import SoundtracksConsumer
from .awards_consumer import AwardsConsumer

__all__ = [
    "FilmsConsumer",
    "CharactersConsumer",
    "BoxOfficeConsumer",
    "GamesConsumer",
    "SoundtracksConsumer",
    "AwardsConsumer",
]
