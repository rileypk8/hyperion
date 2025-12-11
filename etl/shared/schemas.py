"""Avro schemas for Kafka messages."""

# Schema for film/media records
FILM_SCHEMA = {
    "type": "record",
    "name": "Film",
    "namespace": "com.hyperion.etl",
    "fields": [
        {"name": "title", "type": "string"},
        {"name": "year", "type": "int"},
        {"name": "studio", "type": "string"},
        {"name": "franchise", "type": "string"},
        {"name": "animation_type", "type": {"type": "enum", "name": "AnimationType", "symbols": ["fully_animated", "hybrid"]}},
        {"name": "source_file", "type": "string"},
    ]
}

# Schema for character records
CHARACTER_SCHEMA = {
    "type": "record",
    "name": "Character",
    "namespace": "com.hyperion.etl",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "franchise", "type": "string"},
        {"name": "role", "type": "string"},
        {"name": "species", "type": ["null", "string"], "default": None},
        {"name": "gender", "type": ["null", "string"], "default": None},
        {"name": "voice_actor", "type": ["null", "string"], "default": None},
        {"name": "film_title", "type": ["null", "string"], "default": None},
        {"name": "film_year", "type": ["null", "int"], "default": None},
        {"name": "notes", "type": ["null", "string"], "default": None},
        {"name": "source_file", "type": "string"},
    ]
}

# Schema for box office records
BOX_OFFICE_SCHEMA = {
    "type": "record",
    "name": "BoxOffice",
    "namespace": "com.hyperion.etl",
    "fields": [
        {"name": "film_title", "type": "string"},
        {"name": "film_year", "type": "int"},
        {"name": "date", "type": "string"},  # ISO format date
        {"name": "state_code", "type": "string"},
        {"name": "revenue", "type": "double"},
        {"name": "week_num", "type": "int"},
    ]
}

# Mapping of role names from source data to normalized values
ROLE_MAPPING = {
    "protagonist": "protagonist",
    "deuteragonist": "deuteragonist",
    "hero": "hero",
    "villain": "villain",
    "antagonist": "antagonist",
    "henchman": "henchman",
    "henchmen": "henchman",
    "ally": "ally",
    "sidekick": "sidekick",
    "love_interest": "love_interest",
    "love interest": "love_interest",
    "comic_relief": "comic_relief",
    "comic relief": "comic_relief",
    "supporting": "supporting",
    "minor": "minor",
}

# Mapping of gender values from source data to normalized values
GENDER_MAPPING = {
    "male": "male",
    "female": "female",
    "non-binary": "non-binary",
    "nonbinary": "non-binary",
    "n/a": "n/a",
    "na": "n/a",
    "unknown": "n/a",
    "various": "various",
    "mixed": "various",
}

# Species normalization - maps complex descriptions to base species
def normalize_species(species_str: str) -> str:
    """Normalize species string to a base category."""
    if not species_str:
        return None

    species_lower = species_str.lower()

    # Direct matches to lookup table values
    direct_matches = [
        "human", "animal", "toy", "monster", "robot", "emotion",
        "fairy", "fish", "car", "insect", "ghost", "alien",
        "mythical creature", "anthropomorphic animal",
        "heartless", "nobody", "dream eater",
    ]

    for match in direct_matches:
        if match in species_lower:
            return match

    # Special cases
    if "toy" in species_lower:
        return "toy"
    if any(x in species_lower for x in ["dog", "cat", "bird", "horse", "lion", "tiger", "bear"]):
        return "animal"
    if any(x in species_lower for x in ["mermaid", "dragon", "unicorn", "phoenix"]):
        return "mythical creature"
    if any(x in species_lower for x in ["droid", "android", "ai", "machine"]):
        return "robot"

    # Default to the original if no match
    return species_str


# Studio name mapping from various source formats
STUDIO_MAPPING = {
    "pixar": {"name": "Pixar Animation Studios", "start": 1986, "status": "active"},
    "pixar animation studios": {"name": "Pixar Animation Studios", "start": 1986, "status": "active"},
    "wdas": {"name": "Walt Disney Animation Studios", "start": 1937, "status": "active"},
    "walt disney animation studios": {"name": "Walt Disney Animation Studios", "start": 1937, "status": "active"},
    "disney": {"name": "Walt Disney Animation Studios", "start": 1937, "status": "active"},
    "blue sky": {"name": "Blue Sky Studios", "start": 1987, "end": 2021, "status": "closed"},
    "blue sky studios": {"name": "Blue Sky Studios", "start": 1987, "end": 2021, "status": "closed"},
    "disneytoon": {"name": "DisneyToon Studios", "start": 1988, "end": 2018, "status": "closed"},
    "disneytoon studios": {"name": "DisneyToon Studios", "start": 1988, "end": 2018, "status": "closed"},
    "20th century animation": {"name": "20th Century Animation", "start": 2020, "status": "active"},
    "marvel animation": {"name": "Marvel Animation", "start": 2008, "status": "active"},
}
