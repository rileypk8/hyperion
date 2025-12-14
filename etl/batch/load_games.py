#!/usr/bin/env python3
"""Direct batch loader for games - extracts from JSON and loads to PostgreSQL."""

import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GamesLoader:
    """
    Extracts game data from JSON files and loads directly to PostgreSQL.

    Handles:
    - franchises table
    - media table (with media_type='game')
    - games table (developer, publisher)
    - platforms table
    - game_platforms junction table
    """

    GAME_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")
    GAME_DIRS = ["disney_interactive_characters", "kingdom_hearts_characters"]

    PLATFORM_MAP = {
        "playstation 2": "PlayStation 2", "ps2": "PlayStation 2",
        "playstation 3": "PlayStation 3", "ps3": "PlayStation 3",
        "playstation 4": "PlayStation 4", "ps4": "PlayStation 4",
        "playstation 5": "PlayStation 5", "ps5": "PlayStation 5",
        "playstation portable": "PlayStation Portable", "psp": "PlayStation Portable",
        "xbox": "Xbox", "xbox 360": "Xbox 360", "xbox one": "Xbox One",
        "xbox series x/s": "Xbox Series X/S",
        "nintendo ds": "Nintendo DS", "ds": "Nintendo DS",
        "nintendo 3ds": "Nintendo 3DS", "3ds": "Nintendo 3DS",
        "nintendo switch": "Nintendo Switch", "switch": "Nintendo Switch",
        "game boy advance": "Game Boy Advance", "gba": "Game Boy Advance",
        "wii": "Nintendo Wii", "wii u": "Nintendo Wii U",
        "pc": "PC", "ios": "iOS", "android": "Android", "mobile": "iOS",
    }

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.seen_games = set()
        # Caches
        self.franchise_cache = {}
        self.media_cache = {}
        self.platform_cache = {}

    def parse_game_string(self, game_str: str) -> tuple[str, int] | None:
        match = self.GAME_PATTERN.match(game_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def normalize_platforms(self, platform_str: str) -> list[str]:
        if not platform_str:
            return []
        parts = re.split(r"[/,]", platform_str)
        platforms = []
        for part in parts:
            part = part.strip().lower()
            if part in self.PLATFORM_MAP:
                platforms.append(self.PLATFORM_MAP[part])
            elif part:
                platforms.append(part.title())
        return list(set(platforms))

    def extract_games(self):
        """Extract game records from JSON files."""
        for game_dir in self.GAME_DIRS:
            dir_path = self.data_dir / game_dir
            if not dir_path.exists():
                continue

            for json_file in dir_path.glob("*.json"):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except json.JSONDecodeError:
                    continue

                if not isinstance(data, dict):
                    continue

                franchise = data.get("franchise", "Unknown")
                studio = data.get("studio", "Disney Interactive Studios")
                publisher = studio.split("/")[0].strip() if "/" in studio else studio
                default_developer = studio.split("/")[-1].strip() if "/" in studio else None

                games_list = data.get("games", [])
                categories = data.get("categories", {})

                for game_str in games_list:
                    parsed = self.parse_game_string(game_str)
                    if not parsed:
                        continue

                    title, year = parsed
                    game_key = (title.lower(), year)

                    if game_key in self.seen_games:
                        continue
                    self.seen_games.add(game_key)

                    developer = default_developer
                    platforms = []

                    for cat_name, cat_data in categories.items():
                        if isinstance(cat_data, dict) and cat_data.get("year") == year:
                            developer = cat_data.get("developer", developer)
                            platforms = self.normalize_platforms(cat_data.get("platform", ""))
                            break

                    yield {
                        "title": title,
                        "year": year,
                        "franchise": franchise,
                        "developer": developer,
                        "publisher": publisher,
                        "platforms": platforms,
                    }

    def ensure_franchise(self, conn, franchise_name: str) -> int:
        if franchise_name in self.franchise_cache:
            return self.franchise_cache[franchise_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM franchises WHERE name = %s", (franchise_name,))
            result = cursor.fetchone()
            if result:
                self.franchise_cache[franchise_name] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO franchises (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (franchise_name,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.franchise_cache[franchise_name] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM franchises WHERE name = %s", (franchise_name,))
            result = cursor.fetchone()
            self.franchise_cache[franchise_name] = result[0]
            return result[0]

    def ensure_media(self, conn, title: str, year: int, franchise_id: int) -> int:
        cache_key = (title.lower(), year, "game")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'game'",
                (title, year)
            )
            result = cursor.fetchone()
            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO media (title, year, franchise_id, media_type)
                VALUES (%s, %s, %s, 'game')
                ON CONFLICT (title, year, media_type) DO NOTHING
                RETURNING id
                """,
                (title, year, franchise_id)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.media_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'game'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def get_platform_id(self, conn, platform_name: str) -> int | None:
        if not platform_name:
            return None
        if platform_name in self.platform_cache:
            return self.platform_cache[platform_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM platforms WHERE name = %s", (platform_name,))
            result = cursor.fetchone()
            if result:
                self.platform_cache[platform_name] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO platforms (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (platform_name,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.platform_cache[platform_name] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM platforms WHERE name = %s", (platform_name,))
            result = cursor.fetchone()
            if result:
                self.platform_cache[platform_name] = result[0]
                return result[0]
            return None

    def load(self, dry_run: bool = False) -> int:
        """Load all games to PostgreSQL."""
        games = list(self.extract_games())
        logger.info(f"Found {len(games)} games to load")

        if dry_run:
            for game in games:
                platforms = ", ".join(game["platforms"]) if game["platforms"] else "Unknown"
                print(f"{game['title']} ({game['year']}) - {game['developer']} - [{platforms}]")
            return len(games)

        with get_db_connection() as conn:
            count = 0
            for game in games:
                try:
                    franchise_id = self.ensure_franchise(conn, game["franchise"])
                    media_id = self.ensure_media(conn, game["title"], game["year"], franchise_id)

                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO games (media_id, developer, publisher)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO UPDATE SET
                                developer = COALESCE(EXCLUDED.developer, games.developer),
                                publisher = COALESCE(EXCLUDED.publisher, games.publisher)
                            RETURNING id
                            """,
                            (media_id, game.get("developer"), game.get("publisher"))
                        )
                        result = cursor.fetchone()
                        game_id = result[0] if result else None
                        conn.commit()

                    if game_id and game.get("platforms"):
                        for platform_name in game["platforms"]:
                            platform_id = self.get_platform_id(conn, platform_name)
                            if platform_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO game_platforms (game_id, platform_id)
                                        VALUES (%s, %s)
                                        ON CONFLICT (game_id, platform_id) DO NOTHING
                                        """,
                                        (game_id, platform_id)
                                    )
                                    conn.commit()

                    count += 1
                    if count % 20 == 0:
                        logger.info(f"Loaded {count} games...")

                except Exception as e:
                    logger.error(f"Error loading game {game['title']}: {e}")
                    conn.rollback()

            logger.info(f"Finished loading {count} games")
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load games from JSON to PostgreSQL")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print games without loading")
    args = parser.parse_args()

    loader = GamesLoader(data_dir=args.data_dir)
    loader.load(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
