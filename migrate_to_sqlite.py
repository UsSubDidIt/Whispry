import json
import sqlite3
import logging

# --- Configuration ---
JSON_DATA_FILE = "whispry_data.json"  # Your old JSON data file
DB_FILE = "whispry_data.db"  # The SQLite database file (should be the same as in whispry_bot.py)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def migrate_data():
    """Migrates data from JSON to SQLite."""

    try:
        with open(JSON_DATA_FILE, 'r') as f:
            json_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error reading JSON data: {e}")
        return

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            # Migrate bot data
            bots_data = json_data.get("bots", {})
            for owner_id_str, bot_data in bots_data.items():
                for token, details in bot_data.items():
                    start_message = details.get("start_message", "")
                    first_reply = details.get("first_reply", "")
                    cursor.execute("""
                        INSERT INTO bots (owner_id, token, start_message, first_reply)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(owner_id, token) DO UPDATE
                        SET start_message = excluded.start_message,
                            first_reply = excluded.first_reply
                    """, (owner_id_str, token, start_message, first_reply))

            # Migrate message mappings
            message_mappings = json_data.get("message_mappings", {})
            for owner_id_str, mappings in message_mappings.items():
                for forwarded_message_id_str, user_id in mappings.items():
                    cursor.execute("""
                        INSERT INTO message_mappings (owner_id, forwarded_message_id, user_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT(owner_id, forwarded_message_id) DO NOTHING
                    """, (owner_id_str, forwarded_message_id_str, int(user_id))) # Convert user_id to int


            # Migrate message counts (This part is crucial)
            for owner_id_str, bot_data in bots_data.items():
                for token, _ in bot_data.items():
                    #  We need to *estimate* the message count. The JSON data
                    #  doesn't store the count directly.  We'll make a reasonable
                    #  guess based on the message mappings.  A more accurate
                    #  count would require analyzing the Telegram chat history,
                    #  which isn't possible through the Bot API.
                    message_count = 0
                    if owner_id_str in message_mappings:
                        message_count = len(message_mappings[owner_id_str]) # Estimate: number of mappings

                    cursor.execute("""
                        INSERT INTO message_counts (token, owner_id, message_count)
                        VALUES (?, ?, ?)
                        ON CONFLICT(token) DO UPDATE SET message_count = excluded.message_count
                    """, (token, int(owner_id_str), message_count))

            conn.commit()
            logger.info("Data migration completed successfully.")

    except sqlite3.Error as e:
        logger.exception(f"SQLite error during migration: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred during migration: {e}")


if __name__ == "__main__":
    migrate_data()