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

# Species normalization - maps complex descriptions to canonical species
# Returns (species_name, species_subtype, is_anthropomorphic)

# Canonical species from the schema (id -> name)
CANONICAL_SPECIES = {
    # Top-level
    "human", "toy", "monster", "robot", "emotion", "fairy", "car", "alien",
    "element", "mermaid",
    # Kingdom Hearts
    "heartless", "nobody", "dream eater",
    # Mythical/supernatural
    "god", "demigod", "genie", "ghost", "dragon", "enchanted object",
    # Mammals
    "dog", "cat", "mouse", "duck", "rabbit", "lion", "bear", "elephant",
    "gorilla", "deer", "fox", "horse", "pig", "wolf", "tiger", "hyena",
    "meerkat", "warthog", "raccoon", "skunk", "chipmunk", "squirrel", "rat",
    "monkey", "lemur", "sloth", "mammoth", "saber-toothed cat",
    # Birds
    "owl", "parrot", "macaw", "crow", "seagull", "hornbill", "penguin",
    "chicken", "vulture",
    # Aquatic
    "fish", "clownfish", "blue tang", "shark", "whale", "octopus", "crab",
    "sea turtle", "sea monster",
    # Insects
    "ant", "cricket", "grasshopper", "ladybug", "caterpillar", "firefly", "spider",
    # Reptiles/amphibians
    "dinosaur", "frog", "snake", "crocodile", "chameleon",
    # Fantasy humanoids
    "dwarf", "elf", "troll", "gargoyle", "satyr", "centaur",
    # Misc
    "soul", "mind worker", "who", "leafman",
}

# Maps raw species strings to (canonical_species, subtype, is_anthropomorphic)
SPECIES_MAPPING = {
    # Toy Story - toys with subtypes
    "toy (cowboy doll)": ("toy", "cowboy doll", False),
    "toy (action figure)": ("toy", "action figure", False),
    "toy (cowgirl doll)": ("toy", "cowgirl doll", False),
    "toy (horse)": ("toy", "horse", False),
    "toy (dinosaur)": ("toy", "dinosaur", False),
    "toy (piggy bank)": ("toy", "piggy bank", False),
    "toy (dog)": ("toy", "dog", False),
    "toy (potato)": ("toy", "potato", False),
    "toy (squeeze toy)": ("toy", "squeeze toy", False),
    "toy (porcelain lamp)": ("toy", "porcelain lamp", False),
    "toy (army man)": ("toy", "army man", False),
    "toy (fashion doll)": ("toy", "fashion doll", False),
    "toy (teddy bear)": ("toy", "teddy bear", False),
    "toy (spork craft)": ("toy", "spork craft", False),
    "toy (vintage doll)": ("toy", "vintage doll", False),

    # Anthropomorphic animals
    "anthropomorphic mouse": ("mouse", None, True),
    "anthropomorphic duck": ("duck", None, True),
    "anthropomorphic dog": ("dog", None, True),
    "anthropomorphic rabbit": ("rabbit", None, True),
    "anthropomorphic cat": ("cat", None, True),
    "anthropomorphic horse": ("horse", None, True),
    "anthropomorphic cow": ("cow", None, True),

    # Ice Age specific
    "woolly mammoth": ("mammoth", None, False),
    "ground sloth": ("sloth", None, False),
    "saber-toothed tiger": ("saber-toothed cat", None, False),
    "saber-toothed squirrel": ("squirrel", "saber-toothed", False),
    "possum": ("possum", None, False),
    "weasel": ("weasel", None, False),

    # Finding Nemo/Dory
    "clownfish": ("clownfish", None, False),
    "blue tang": ("blue tang", None, False),
    "sea turtle": ("sea turtle", None, False),

    # Simple mappings
    "human": ("human", None, False),
    "monster": ("monster", None, False),
    "robot": ("robot", None, False),
    "ghost": ("ghost", None, False),
    "fairy": ("fairy", None, False),
    "genie": ("genie", None, False),
    "dragon": ("dragon", None, False),
    "demigod": ("demigod", None, False),
    "god": ("god", None, False),
    "heartless": ("heartless", None, False),
    "nobody": ("nobody", None, False),
    "dream eater": ("dream eater", None, False),
}


def normalize_species(species_str: str) -> str | None:
    """
    Normalize species string to a canonical species name.

    Returns the canonical species name, or the original if no match found.
    For richer data extraction, use parse_species() instead.
    """
    if not species_str:
        return None

    species_lower = species_str.lower().strip()

    # Check explicit mapping first
    if species_lower in SPECIES_MAPPING:
        return SPECIES_MAPPING[species_lower][0]

    # Check for direct canonical match
    if species_lower in CANONICAL_SPECIES:
        return species_lower

    # Pattern matching for common formats

    # "toy (something)" -> toy
    if species_lower.startswith("toy"):
        return "toy"

    # "anthropomorphic X" -> X (marked anthropomorphic)
    if "anthropomorphic" in species_lower:
        for species in CANONICAL_SPECIES:
            if species in species_lower:
                return species
        return "animal"  # fallback for unknown anthropomorphic

    # Check if any canonical species is mentioned
    for species in CANONICAL_SPECIES:
        if species in species_lower:
            return species

    # Kingdom Hearts specific
    if "ink creature" in species_lower:
        return "heartless"
    if "animatronic" in species_lower:
        return "robot"
    if "gremlin" in species_lower:
        return "fairy"  # closest match

    # Fallback - return original (consumer will handle unknown)
    return species_str


def parse_species(species_str: str) -> tuple[str | None, str | None, bool]:
    """
    Parse species string into (canonical_species, subtype, is_anthropomorphic).

    More detailed than normalize_species() - extracts subtype and anthropomorphic flag.
    """
    if not species_str:
        return (None, None, False)

    species_lower = species_str.lower().strip()

    # Check explicit mapping first
    if species_lower in SPECIES_MAPPING:
        return SPECIES_MAPPING[species_lower]

    # Check for anthropomorphic
    is_anthro = "anthropomorphic" in species_lower

    # Extract subtype from parenthetical
    subtype = None
    if "(" in species_lower and ")" in species_lower:
        start = species_lower.index("(") + 1
        end = species_lower.index(")")
        subtype = species_lower[start:end].strip()

    # Get base species
    base_species = normalize_species(species_str)

    return (base_species, subtype, is_anthro)


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
