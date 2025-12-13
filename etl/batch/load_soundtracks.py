#!/usr/bin/env python3
"""Direct batch loader for soundtracks - extracts from JSON and loads to PostgreSQL."""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SoundtracksLoader:
    """
    Extracts soundtrack and song data from wdas_soundtracks_complete.json
    and loads directly to PostgreSQL.

    Handles:
    - media table (parent records with media_type='soundtrack')
    - soundtracks table (child records with label, source_media_id)
    - songs table (tracks on soundtracks)
    - song_credits table (M:N junction for songs <-> talent <-> credit_type)
    - talent table (composers, performers)
    """

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.soundtrack_file = self.data_dir / "wdas_soundtracks_complete.json"
        # Caches
        self.franchise_cache = {}
        self.media_cache = {}
        self.talent_cache = {}
        self.credit_type_cache = {}

    def load_data(self) -> list[dict]:
        if not self.soundtrack_file.exists():
            logger.warning(f"Soundtracks file not found: {self.soundtrack_file}")
            return []
        try:
            with open(self.soundtrack_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse soundtracks file: {e}")
            return []

    def extract_soundtracks(self):
        """Extract soundtrack records with songs and credits."""
        data = self.load_data()

        for film_entry in data:
            film_title = film_entry.get("film")
            film_year = film_entry.get("film_year")
            if not film_title or not film_year:
                continue

            for release in film_entry.get("releases", []):
                soundtrack_title = release.get("title")
                if not soundtrack_title:
                    continue

                composers = release.get("composers", [])
                conductor = release.get("conductor")
                label = release.get("label")

                songs = []
                for idx, track in enumerate(release.get("tracks", []), start=1):
                    song_title = track.get("title")
                    if not song_title:
                        continue

                    performers = []
                    for perf in track.get("performers", []):
                        performers.append({
                            "name": perf.get("name"),
                            "type": perf.get("type", "performer"),
                            "character": perf.get("character"),
                        })

                    songs.append({
                        "title": song_title,
                        "track_number": idx,
                        "type": track.get("type", "song"),
                        "composer": track.get("composer"),
                        "performers": performers,
                    })

                credits = []
                for composer in composers:
                    if composer and composer != "Various Classical":
                        credits.append({"name": composer, "type": "composer"})
                if conductor:
                    credits.append({"name": conductor, "type": "conductor"})

                yield {
                    "title": soundtrack_title,
                    "film_title": film_title,
                    "film_year": film_year,
                    "label": label,
                    "songs": songs,
                    "credits": credits,
                }

    def get_franchise_id(self, conn, franchise_name: str) -> int:
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

    def get_source_media_id(self, conn, film_title: str, film_year: int) -> int | None:
        cache_key = (film_title.lower(), film_year, "film")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'film'",
                (film_title, film_year)
            )
            result = cursor.fetchone()
            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]
            return None

    def ensure_soundtrack_media(self, conn, title: str, year: int, franchise_id: int) -> int:
        cache_key = (title.lower(), year, "soundtrack")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'soundtrack'",
                (title, year)
            )
            result = cursor.fetchone()
            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO media (title, year, franchise_id, media_type)
                VALUES (%s, %s, %s, 'soundtrack')
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
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'soundtrack'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def get_or_create_talent_id(self, conn, talent_name: str) -> int | None:
        if not talent_name:
            return None
        talent_name = talent_name.strip()
        if talent_name in self.talent_cache:
            return self.talent_cache[talent_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM talent WHERE name = %s", (talent_name,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[talent_name] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO talent (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (talent_name,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.talent_cache[talent_name] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM talent WHERE name = %s", (talent_name,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[talent_name] = result[0]
                return result[0]
            return None

    def get_credit_type_id(self, conn, credit_type: str) -> int | None:
        if not credit_type:
            return None
        credit_type = credit_type.lower()

        type_map = {"character_voice": "performer", "voice": "performer", "singer": "performer"}
        credit_type = type_map.get(credit_type, credit_type)

        if credit_type in self.credit_type_cache:
            return self.credit_type_cache[credit_type]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM credit_types WHERE name = %s", (credit_type,))
            result = cursor.fetchone()
            if result:
                self.credit_type_cache[credit_type] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO credit_types (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (credit_type,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.credit_type_cache[credit_type] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM credit_types WHERE name = %s", (credit_type,))
            result = cursor.fetchone()
            if result:
                self.credit_type_cache[credit_type] = result[0]
                return result[0]
            return None

    def load(self, dry_run: bool = False) -> int:
        """Load all soundtracks to PostgreSQL."""
        soundtracks = list(self.extract_soundtracks())
        song_count = sum(len(s.get("songs", [])) for s in soundtracks)
        logger.info(f"Found {len(soundtracks)} soundtracks with {song_count} songs to load")

        if dry_run:
            for st in soundtracks[:20]:
                print(f"{st['title']}")
                print(f"  Film: {st['film_title']} ({st['film_year']})")
                print(f"  Songs: {len(st['songs'])}, Credits: {len(st['credits'])}")
            if len(soundtracks) > 20:
                print(f"... and {len(soundtracks) - 20} more")
            return len(soundtracks)

        with get_db_connection() as conn:
            count = 0
            for st in soundtracks:
                try:
                    franchise_id = self.get_franchise_id(conn, st["film_title"])
                    source_media_id = self.get_source_media_id(conn, st["film_title"], st["film_year"])
                    media_id = self.ensure_soundtrack_media(conn, st["title"], st["film_year"], franchise_id)

                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO soundtracks (media_id, source_media_id, label)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO UPDATE SET
                                source_media_id = COALESCE(EXCLUDED.source_media_id, soundtracks.source_media_id),
                                label = COALESCE(EXCLUDED.label, soundtracks.label)
                            RETURNING id
                            """,
                            (media_id, source_media_id, st.get("label"))
                        )
                        result = cursor.fetchone()
                        soundtrack_id = result[0] if result else None
                        conn.commit()

                    if not soundtrack_id:
                        continue

                    for song in st.get("songs", []):
                        with get_cursor(conn) as cursor:
                            cursor.execute(
                                """
                                INSERT INTO songs (soundtrack_id, title, track_number)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (soundtrack_id, title) DO NOTHING
                                RETURNING id
                                """,
                                (soundtrack_id, song["title"], song.get("track_number"))
                            )
                            result = cursor.fetchone()
                            song_id = result[0] if result else None
                            conn.commit()

                        if not song_id:
                            with get_cursor(conn) as cursor:
                                cursor.execute(
                                    "SELECT id FROM songs WHERE soundtrack_id = %s AND title = %s",
                                    (soundtrack_id, song["title"])
                                )
                                result = cursor.fetchone()
                                song_id = result[0] if result else None

                        if not song_id:
                            continue

                        for performer in song.get("performers", []):
                            talent_id = self.get_or_create_talent_id(conn, performer.get("name"))
                            credit_type_id = self.get_credit_type_id(conn, performer.get("type", "performer"))
                            if talent_id and credit_type_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO song_credits (song_id, talent_id, credit_type_id)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (song_id, talent_id, credit_type_id) DO NOTHING
                                        """,
                                        (song_id, talent_id, credit_type_id)
                                    )
                                    conn.commit()

                        if song.get("composer"):
                            talent_id = self.get_or_create_talent_id(conn, song["composer"])
                            credit_type_id = self.get_credit_type_id(conn, "composer")
                            if talent_id and credit_type_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO song_credits (song_id, talent_id, credit_type_id)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (song_id, talent_id, credit_type_id) DO NOTHING
                                        """,
                                        (song_id, talent_id, credit_type_id)
                                    )
                                    conn.commit()

                    count += 1
                    if count % 20 == 0:
                        logger.info(f"Loaded {count} soundtracks...")

                except Exception as e:
                    logger.error(f"Error loading soundtrack {st.get('title')}: {e}")
                    conn.rollback()

            logger.info(f"Finished loading {count} soundtracks")
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load soundtracks from JSON to PostgreSQL")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print soundtracks without loading")
    args = parser.parse_args()

    loader = SoundtracksLoader(data_dir=args.data_dir)
    loader.load(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
