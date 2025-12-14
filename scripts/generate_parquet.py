#!/usr/bin/env python3
"""
Generate Parquet files from source JSON data files.

Reads the JSON files in data/ directory and exports normalized tables
as Parquet files for DuckDB-WASM consumption.
"""

import json
import re
from pathlib import Path
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "web" / "public" / "data"

# Directory to studio mapping
STUDIO_MAPPING = {
    "wdas_characters": "wdas",
    "pixar_characters": "pixar",
    "blue_sky_characters": "blue-sky",
    "disneytoon_characters": "disneytoon",
    "kingdom_hearts_characters": "kingdom-hearts",
    "marvel_animation_characters": "marvel",
    "disney_interactive_characters": "interactive",
    "20th_century_animation": "20th-century",
    "fox_disney_plus_characters": "fox-disney-plus",
}

STUDIO_NAMES = {
    "wdas": "Walt Disney Animation Studios",
    "pixar": "Pixar Animation Studios",
    "blue-sky": "Blue Sky Studios",
    "disneytoon": "DisneyToon Studios",
    "kingdom-hearts": "Kingdom Hearts (Square Enix)",
    "marvel": "Marvel Animation",
    "interactive": "Disney Interactive",
    "20th-century": "20th Century Animation",
    "fox-disney-plus": "Fox/Disney+ Animation",
}

STUDIO_SHORT_NAMES = {
    "wdas": "WDAS",
    "pixar": "Pixar",
    "blue-sky": "Blue Sky",
    "disneytoon": "DisneyToon",
    "kingdom-hearts": "KH",
    "marvel": "Marvel",
    "interactive": "Interactive",
    "20th-century": "20th Century",
    "fox-disney-plus": "Fox D+",
}


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def parse_film_title_year(film_str: str) -> tuple[str, int | None]:
    """Extract title and year from strings like 'Frozen (2013)'."""
    match = re.match(r'^(.+?)\s*\((\d{4})\)$', film_str)
    if match:
        return match.group(1).strip(), int(match.group(2))
    return film_str, None


def load_json_file(path: Path) -> dict | list | None:
    """Load JSON file, returning None on error."""
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Warning: Could not load {path}: {e}")
        return None


def process_all_data():
    """Process all JSON files and return normalized data structures."""
    studios = {}
    franchises = {}
    films = {}
    characters = []
    char_id_counter = 0

    # Process each studio directory
    for dir_name, studio_id in STUDIO_MAPPING.items():
        studio_dir = DATA_DIR / dir_name
        if not studio_dir.exists():
            print(f"Warning: Directory not found: {studio_dir}")
            continue

        print(f"Processing {dir_name} -> {studio_id}")

        # Initialize studio
        if studio_id not in studios:
            studios[studio_id] = {
                "id": studio_id,
                "name": STUDIO_NAMES.get(studio_id, studio_id),
                "short_name": STUDIO_SHORT_NAMES.get(studio_id, studio_id),
                "franchise_ids": [],
                "film_count": 0,
                "character_count": 0,
            }

        # Process each JSON file in the directory
        for json_file in sorted(studio_dir.glob("*.json")):
            data = load_json_file(json_file)
            if not data or not isinstance(data, dict):
                continue

            franchise_name = data.get("franchise") or data.get("game") or json_file.stem
            franchise_id = slugify(franchise_name)

            # Initialize franchise
            if franchise_id not in franchises:
                franchises[franchise_id] = {
                    "id": franchise_id,
                    "name": franchise_name,
                    "studio_id": studio_id,
                    "film_ids": [],
                }
                if franchise_id not in studios[studio_id]["franchise_ids"]:
                    studios[studio_id]["franchise_ids"].append(franchise_id)

            # Extract films from the data
            film_list = data.get("films", [])
            if data.get("game"):
                # Kingdom Hearts style: game is the "film"
                game_name = data.get("game")
                year = data.get("year")
                film_list = [f"{game_name} ({year})" if year else game_name]

            # Process categories (film-specific character groups)
            # Some files use "categories", others use "film_data"
            categories = data.get("categories", {})
            if not isinstance(categories, dict):
                categories = {}  # Sometimes categories is a list of role names, not character data

            # Check for film_data as alternative to categories
            film_data = data.get("film_data", {})
            if isinstance(film_data, dict) and film_data:
                categories = {**categories, **film_data}

            # Also check for direct characters array (KH style)
            # Some files have characters as a list, others as a dict of groups
            raw_characters = data.get("characters", [])
            direct_characters = []
            if isinstance(raw_characters, list):
                direct_characters = raw_characters
            elif isinstance(raw_characters, dict):
                # Blue Sky style: characters is a dict with group names as keys
                # e.g., {"main_trio": [...], "recurring_main": [...]}
                for group_name, group_chars in raw_characters.items():
                    if isinstance(group_chars, list):
                        direct_characters.extend(group_chars)

            # Map category keys to film info
            category_film_map = {}  # category_key -> (film_id, year)

            for cat_key, cat_data in categories.items():
                if not isinstance(cat_data, dict):
                    continue
                cat_chars = cat_data.get("characters", [])
                if not cat_chars:
                    continue

                # Try to determine the film for this category
                year = cat_data.get("year")
                film_title = None
                film_id = None

                # Match category to film from films list
                for film_str in film_list:
                    title, f_year = parse_film_title_year(film_str)
                    if year and f_year == year:
                        film_title = title
                        break
                    # Try matching by category key
                    if slugify(title) in cat_key or cat_key in slugify(title):
                        film_title = title
                        year = f_year
                        break

                if film_title and year:
                    film_id = slugify(f"{film_title}-{year}")
                elif film_title:
                    film_id = slugify(film_title)
                elif year:
                    film_id = slugify(f"{franchise_name}-{year}")

                if film_id:
                    category_film_map[cat_key] = (film_id, film_title or franchise_name, year)

                    # Create film record
                    if film_id not in films:
                        films[film_id] = {
                            "id": film_id,
                            "title": film_title or franchise_name,
                            "year": year or 0,
                            "franchise_id": franchise_id,
                            "studio_id": studio_id,
                            "character_count": 0,
                        }
                        if film_id not in franchises[franchise_id]["film_ids"]:
                            franchises[franchise_id]["film_ids"].append(film_id)
                        studios[studio_id]["film_count"] += 1

            # Also create films from film_list that weren't matched
            for film_str in film_list:
                title, year = parse_film_title_year(film_str)
                if year:
                    film_id = slugify(f"{title}-{year}")
                else:
                    film_id = slugify(title)

                if film_id not in films:
                    films[film_id] = {
                        "id": film_id,
                        "title": title,
                        "year": year or 0,
                        "franchise_id": franchise_id,
                        "studio_id": studio_id,
                        "character_count": 0,
                    }
                    if film_id not in franchises[franchise_id]["film_ids"]:
                        franchises[franchise_id]["film_ids"].append(film_id)
                    studios[studio_id]["film_count"] += 1

            # Process characters from categories
            for cat_key, cat_data in categories.items():
                if not isinstance(cat_data, dict):
                    continue
                cat_chars = cat_data.get("characters", [])

                film_info = category_film_map.get(cat_key)
                film_id = film_info[0] if film_info else None

                for char in cat_chars:
                    if not isinstance(char, dict):
                        continue
                    char_record = create_character_record(
                        char, char_id_counter, franchise_id, studio_id, film_id
                    )
                    characters.append(char_record)
                    char_id_counter += 1
                    studios[studio_id]["character_count"] += 1
                    if film_id and film_id in films:
                        films[film_id]["character_count"] += 1

            # Process direct characters (no category/film association)
            for char in direct_characters:
                if not isinstance(char, dict):
                    continue
                # For direct characters, try to use the first film
                film_id = None
                if film_list:
                    title, year = parse_film_title_year(film_list[0])
                    film_id = slugify(f"{title}-{year}") if year else slugify(title)

                char_record = create_character_record(
                    char, char_id_counter, franchise_id, studio_id, film_id
                )
                characters.append(char_record)
                char_id_counter += 1
                studios[studio_id]["character_count"] += 1
                if film_id and film_id in films:
                    films[film_id]["character_count"] += 1

    return studios, franchises, films, characters


def create_character_record(
    char: dict, char_id: int, franchise_id: str, studio_id: str, film_id: str | None
) -> dict:
    """Create a normalized character record."""
    name = char.get("name", "Unknown")
    return {
        "id": f"{slugify(name)}-{char_id}",
        "name": name,
        "role": char.get("role", "unknown"),
        "voice_actor": char.get("voice_actor"),
        "species": char.get("species", "unknown"),
        "gender": char.get("gender", "unknown"),
        "notes": char.get("notes"),
        "film_ids": json.dumps([film_id] if film_id else []),
        "franchise_id": franchise_id,
        "studio_id": studio_id,
    }


def compute_aggregations(characters: list, films: list) -> dict:
    """Compute aggregation tables for analytics."""
    # Gender by year
    gender_by_year = defaultdict(lambda: {"male": 0, "female": 0, "other": 0, "total": 0})
    for char in characters:
        film_ids = json.loads(char["film_ids"])
        for film_id in film_ids:
            film = next((f for f in films if f["id"] == film_id), None)
            if film and film["year"]:
                year = film["year"]
                gender = char["gender"]
                if gender == "male":
                    gender_by_year[year]["male"] += 1
                elif gender == "female":
                    gender_by_year[year]["female"] += 1
                else:
                    gender_by_year[year]["other"] += 1
                gender_by_year[year]["total"] += 1

    gender_by_year_list = [
        {"year": year, **counts}
        for year, counts in sorted(gender_by_year.items())
        if year > 0
    ]

    # Gender by role
    gender_by_role = defaultdict(lambda: {"male": 0, "female": 0, "other": 0})
    for char in characters:
        role = char["role"]
        gender = char["gender"]
        if gender == "male":
            gender_by_role[role]["male"] += 1
        elif gender == "female":
            gender_by_role[role]["female"] += 1
        else:
            gender_by_role[role]["other"] += 1

    gender_by_role_list = [
        {"role": role, **counts}
        for role, counts in sorted(gender_by_role.items())
    ]

    # Top talents
    talent_stats = defaultdict(lambda: {"film_count": 0, "character_count": 0, "studios": set()})
    for char in characters:
        actor = char["voice_actor"]
        if actor and actor.lower() not in ["null", "n/a", "none", "tba", "unknown"]:
            talent_stats[actor]["character_count"] += 1
            talent_stats[actor]["studios"].add(char["studio_id"])
            film_ids = json.loads(char["film_ids"])
            talent_stats[actor]["film_count"] += len(film_ids)

    top_talents = sorted(
        [
            {
                "name": name,
                "film_count": stats["film_count"],
                "character_count": stats["character_count"],
                "studios": json.dumps(list(stats["studios"])),
                "estimated_earnings": stats["character_count"] * 100000000,  # Placeholder
            }
            for name, stats in talent_stats.items()
        ],
        key=lambda x: x["character_count"],
        reverse=True,
    )[:50]

    return {
        "gender_by_year": gender_by_year_list,
        "gender_by_role": gender_by_role_list,
        "top_talents": top_talents,
    }


def write_parquet(records: list, output_path: Path):
    """Write records to a Parquet file."""
    if not records:
        print(f"Skipping {output_path.name}: no records")
        return

    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_path, compression='snappy')
    size_kb = output_path.stat().st_size / 1024
    print(f"Written: {output_path.name} ({len(records)} records, {size_kb:.1f} KB)")


def main():
    print("Processing source JSON files from data/...")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process all data
    studios, franchises, films, characters = process_all_data()

    # Convert to lists
    studios_list = list(studios.values())
    franchises_list = list(franchises.values())
    films_list = list(films.values())

    # Convert array fields to JSON strings for Parquet
    for studio in studios_list:
        studio["franchise_ids"] = json.dumps(studio["franchise_ids"])
    for franchise in franchises_list:
        franchise["film_ids"] = json.dumps(franchise["film_ids"])

    # Compute aggregations
    aggregations = compute_aggregations(characters, films_list)

    # Write Parquet files
    print("\nWriting Parquet files...")
    write_parquet(studios_list, OUTPUT_DIR / "studios.parquet")
    write_parquet(franchises_list, OUTPUT_DIR / "franchises.parquet")
    write_parquet(films_list, OUTPUT_DIR / "films.parquet")
    write_parquet(characters, OUTPUT_DIR / "characters.parquet")
    write_parquet(aggregations["gender_by_year"], OUTPUT_DIR / "gender_by_year.parquet")
    write_parquet(aggregations["gender_by_role"], OUTPUT_DIR / "gender_by_role.parquet")
    write_parquet(aggregations["top_talents"], OUTPUT_DIR / "top_talents.parquet")

    # Summary
    print(f"\nSummary:")
    print(f"  Studios: {len(studios_list)}")
    print(f"  Franchises: {len(franchises_list)}")
    print(f"  Films: {len(films_list)}")
    print(f"  Characters: {len(characters)}")
    print(f"  Characters with film associations: {sum(1 for c in characters if json.loads(c['film_ids']))}")

    print(f"\nGenerated files in {OUTPUT_DIR}:")
    total_size = 0
    for f in sorted(OUTPUT_DIR.glob("*.parquet")):
        size = f.stat().st_size
        total_size += size
        print(f"  {f.name}: {size / 1024:.1f} KB")
    print(f"  Total: {total_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
