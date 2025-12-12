"""Kafka producers for Hyperion ETL pipeline."""

from .films_producer import FilmsProducer
from .characters_producer import CharactersProducer
from .boxoffice_producer import BoxOfficeProducer
from .games_producer import GamesProducer
from .soundtracks_producer import SoundtracksProducer
from .awards_producer import AwardsProducer

__all__ = [
    "FilmsProducer",
    "CharactersProducer",
    "BoxOfficeProducer",
    "GamesProducer",
    "SoundtracksProducer",
    "AwardsProducer",
]
