import json
import telebot
from telebot import types
import threading
import time
import logging
import re
import sqlite3
import dotenv
import os

dotenv.load_dotenv()

# --- Configuration ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
# DATA_FILE = "whispry_data.json"  # No longer used
DB_FILE = "whispry_data.db"  # SQLite database file
DELETE_WEBHOOKS_ON_STARTUP = True
DELETE_WEBHOOKS_ON_NEW_BOT = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_first_key_with_prefix(data_dict, prefix):
    """Finds the first key in a dictionary that starts with a specific prefix."""
    for key in data_dict:
        if key.startswith(prefix):
            return key
    return None

def init_db():
    """Initializes the SQLite database."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS message_counts (
                token TEXT PRIMARY KEY,
                owner_id INTEGER,
                message_count INTEGER DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS message_mappings (
                owner_id TEXT,
                forwarded_message_id TEXT,
                user_id INTEGER,
                PRIMARY KEY (owner_id, forwarded_message_id)
            )
        """)
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS bots (
               owner_id TEXT,
               token TEXT,
               start_message TEXT,
               first_reply TEXT,
               PRIMARY KEY (owner_id, token)
           )
        """)

        conn.commit()


# --- Data Structures ---
class WhispryBot:
    def __init__(self, token, owner_id, main_bot, start_message="", first_reply=""):
        self.token = token
        self.owner_id = owner_id
        self.bot = telebot.TeleBot(token, parse_mode="HTML")
        self.main_bot = main_bot
        self.start_message = start_message
        self.first_reply = first_reply
        self.message_counter = self.get_initial_message_count()  # Load from DB
        self.setup_handlers()
        self.running = True
        self.thread = threading.Thread(target=self.run_polling)
        self.thread.start()


    def get_initial_message_count(self):
        """Retrieves the initial message count from the database."""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT message_count FROM message_counts WHERE token = ?", (self.token,))
            result = cursor.fetchone()
            return result[0] if result else 0

    def increment_message_count(self):
        """Increments the message count in memory and the database."""
        self.message_counter += 1
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO message_counts (token, owner_id, message_count)
                VALUES (?, ?, ?)
                ON CONFLICT(token) DO UPDATE SET message_count = message_count + 1
            """, (self.token, self.owner_id, self.message_counter))  # Use parameterized query
            conn.commit()

    def setup_handlers(self):
        # General command handler (for /about and /help)
        @self.bot.message_handler(commands=['about', 'help'])
        def handle_general_commands(message):
            if message.chat.type == 'private':
                if message.text == '/about':
                    about_text = "This bot is made with @WhispryBot, an ads-free feedback bot."
                    self.bot.send_message(message.chat.id, about_text)
                elif message.text == '/help':
                    self.bot.send_message(message.chat.id, "This is a feedback bot. Send messages to contact the owner.")

        @self.bot.message_handler(commands=['start'])
        def handle_start(message):
            if message.chat.type == 'private':
                start_text = self.start_message or "Welcome!"
                start_text += "\n\nPowered by @WhispryBot"
                self.bot.send_message(message.chat.id, start_text)

        @self.bot.message_handler(func=lambda message: True, content_types=['text', 'photo', 'video', 'document', 'audio', 'voice', 'sticker'])
        def handle_all_messages(message):
            if message.chat.type == 'private':
                try:
                    if message.text and message.text.startswith('/'):
                        return

                    if message.reply_to_message is None:
                        if self.first_reply and self.message_counter == 0:
                            forwarded_msg = self.bot.forward_message(self.owner_id, message.chat.id, message.message_id)
                            self.bot.send_message(self.owner_id, self.first_reply, reply_to_message_id=forwarded_msg.message_id)
                        else:
                            forwarded_msg = self.bot.forward_message(self.owner_id, message.chat.id, message.message_id)
                        main_whispry.store_message_mapping(self.owner_id, forwarded_msg.message_id, message.chat.id)
                        self.increment_message_count()  # Increment and save
                    else:
                        user_id = main_whispry.get_user_id_from_message_id(self.owner_id, message.reply_to_message.message_id)
                        if user_id:
                            if message.text:
                                self.bot.send_message(user_id, message.text)
                            elif message.photo:
                                self.bot.send_photo(user_id, message.photo[-1].file_id, caption=message.caption)
                            elif message.video:
                                self.bot.send_video(user_id, message.video.file_id, caption=message.caption)
                            elif message.document:
                                self.bot.send_document(user_id, message.document.file_id, caption=message.caption)
                            elif message.audio:
                                self.bot.send_audio(user_id, message.audio.file_id, caption=message.caption)
                            elif message.voice:
                                self.bot.send_voice(user_id, message.voice.file_id)
                            elif message.sticker:
                                self.bot.send_sticker(user_id, message.sticker.file_id)
                            self.increment_message_count()  # Increment and save
                        else:
                            logger.error("Original user not found in mapping.")
                except telebot.apihelper.ApiTelegramException as e:
                    logger.exception(f"Telegram API Error: {e}")
                    if "bot was blocked by the user" in str(e).lower():
                        self.main_bot.send_message(self.owner_id, "The user has blocked your bot.")
                    elif "forbidden: bot can't initiate conversation with a user" in str(e).lower():
                        self.main_bot.send_message(self.owner_id, "The user has not started a conversation with your bot.")
                    else:
                        self.main_bot.send_message(self.owner_id, "An error occurred while sending a message.")

    def run_polling(self):
        logger.info(f"Starting polling for bot {self.bot.get_me().username} (owner: {self.owner_id})")
        while self.running:
            try:
                self.bot.polling(none_stop=True, interval=1, timeout=30)
            except Exception as e:
                logger.exception(f"Polling error: {e}")
                time.sleep(15)
        logger.info("Stopped polling")

    def stop_polling(self):
        self.running = False
        self.bot.stop_polling()
        self.thread.join()

    def get_stats(self):
        return self.message_counter


class Whispry:
    def __init__(self, bot_token):
        self.bot = telebot.TeleBot(bot_token)
        init_db() # Initialize the database
        self.bots = {}
        self.message_mappings = {}  # Still in memory, but persisted to DB
        self.load_bots()
        self.load_message_mappings() # Load mappings from DB
        self.setup_handlers()
        self.total_bots_count = 0
        self.total_messages_count = 0
        self.update_stats()

        if DELETE_WEBHOOKS_ON_STARTUP:
            self.delete_all_webhooks()


    def load_bots(self):
        """Loads bot data from the database."""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT owner_id, token, start_message, first_reply FROM bots")
            for row in cursor.fetchall():
                owner_id, token, start_message, first_reply = row
                owner_id = int(owner_id)  # Ensure owner_id is an integer
                if token not in self.bots:
                    try:
                        whispry_instance = WhispryBot(token, owner_id, self.bot, start_message, first_reply)
                        self.bots[token] = whispry_instance
                        logger.info(f"Loaded bot {token[-6:]} for owner {owner_id}")
                    except Exception as e:
                        logger.exception(f"Failed to load bot {token[-6:]}: {e}")

    def load_message_mappings(self):
        """Loads message mappings from the database."""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT owner_id, forwarded_message_id, user_id FROM message_mappings")
            for row in cursor.fetchall():
                owner_id, forwarded_message_id, user_id = row
                # Ensure keys are strings, values are integers (as appropriate)
                if owner_id not in self.message_mappings:
                    self.message_mappings[owner_id] = {}
                self.message_mappings[owner_id][forwarded_message_id] = int(user_id)

    def setup_handlers(self):
        @self.bot.message_handler(commands=['newbot'])
        def handle_newbot(message):
            user_id_str = str(message.from_user.id)
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM bots WHERE owner_id = ?", (user_id_str,))
                bot_count = cursor.fetchone()[0]

            if bot_count >= 50:
                self.bot.reply_to(message, "You have reached the maximum number of bots (50).")
                return
            self.bot.reply_to(message, "Please send me the bot token.")
            self.bot.register_next_step_handler(message, self.process_token)

        @self.bot.message_handler(commands=['start'])
        def handle_start(message):
            self.update_stats()
            start_message = (
                "Hi, I'm Whispry.\n"
                "A privacy-first feedback bot.\n"
                f"Managing {self.total_bots_count} bots and {self.total_messages_count} messages.\n"
                "Send /help for commands."
            )
            self.bot.reply_to(message, start_message)

        @self.bot.message_handler(commands=['help'])
        def handle_help(message):
            help_text = (
                "Commands:\n"
                "/newbot - Create a new bot.\n"
                "/mybots - Manage your bots.\n"
                "/start - Display welcome message.\n"
                "/about - Information about Whispry.\n"
                "/help - Show this help."
            )
            self.bot.reply_to(message, help_text)

        @self.bot.message_handler(commands=['about'])
        def handle_about(message):
            about_text = (
                "Hi, I'm Whispry.\n"
                "An ads-free feedback bot.\n\n"
                "Contact us:\n"
                "Channel | https://t.me/IsWhispry\n"
                "Group | https://t.me/WhispryComm"
            )
            self.bot.reply_to(message, about_text)

        @self.bot.message_handler(commands=['mybots'])
        def handle_mybots(message):
            user_id_str = str(message.from_user.id)
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT token, start_message, first_reply FROM bots WHERE owner_id = ?", (user_id_str,))
                bot_data = cursor.fetchall()

            if bot_data:
                bots = {row[0]: {"start_message": row[1], "first_reply": row[2]} for row in bot_data}
                self.send_bot_list(message.chat.id, user_id_str, bots, 1)
            else:
                self.bot.reply_to(message, "You don't have any bots yet. Use /newbot.")

            @self.bot.callback_query_handler(func=lambda call: True)
            def callback_query(call):
                data = call.data.split(":")
                if data[0] == "manage":
                    user_id_str, token = data[1], data[2]
                    self.manage_bot(call.message.chat.id, user_id_str, token)
                elif data[0] == "page":
                    user_id_str, page = data[1], int(data[2])
                    with sqlite3.connect(DB_FILE) as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT token, start_message, first_reply FROM bots WHERE owner_id = ?", (user_id_str,))
                        bot_data = cursor.fetchall()
                        bots = {row[0]: {"start_message": row[1], "first_reply": row[2]} for row in bot_data}
                    self.send_bot_list(call.message.chat.id, user_id_str, bots, page)

                elif data[0] == "delete":
                    _, user_id_str, token = data
                    self.delete_bot(call, user_id_str, token)
                elif data[0] == "set_start":
                    _, user_id_str, token = data
                    self.bot.send_message(call.message.chat.id, "Enter the new /start message:")
                    self.bot.register_next_step_handler(call.message, self.process_start_message, token)
                elif data[0] == "set_first_reply":
                    _, user_id_str, token = data
                    self.bot.send_message(call.message.chat.id, "Enter the auto-reply for the first message:")
                    self.bot.register_next_step_handler(call.message, self.process_first_reply_message, token)


    def send_bot_list(self, chat_id, user_id_str, bots, page, per_page=5):
        bot_names = list(bots.keys())
        total_bots = len(bot_names)
        total_pages = (total_bots + per_page - 1) // per_page
        start = (page - 1) * per_page
        end = min(start + per_page, total_bots)
        current_page_bots = bot_names[start:end]

        keyboard = types.InlineKeyboardMarkup(row_width=2)
        buttons = []
        for token in current_page_bots:
            bot_username = self.bots[token].bot.get_me().username
            callback_data = f"manage:{user_id_str}:{token}"
            button = types.InlineKeyboardButton(text=bot_username, callback_data=callback_data)
            buttons.append(button)
        keyboard.add(*buttons)

        nav_buttons = []
        if page > 1:
            nav_buttons.append(types.InlineKeyboardButton(text="⬅️ Previous", callback_data=f"page:{user_id_str}:{page - 1}"))
        if page < total_pages:
            nav_buttons.append(types.InlineKeyboardButton(text="➡️ Next", callback_data=f"page:{user_id_str}:{page + 1}"))
        if nav_buttons:
            keyboard.row(*nav_buttons)

        if page == 1:
            self.bot.send_message(chat_id, "Select a bot to manage:", reply_markup=keyboard)
        else:
            #  Since we are loading bots on demand, we don't need to store the last_list_message_id
            self.bot.send_message(chat_id, "Select a bot to manage:", reply_markup=keyboard)


    def manage_bot(self, chat_id, user_id_str, token):
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        delete_button = types.InlineKeyboardButton(text="Delete Bot", callback_data=f"delete:{user_id_str}:{token}")
        start_button = types.InlineKeyboardButton(text="Set /start Message", callback_data=f"set_start:{user_id_str}:{token}")
        first_reply_button = types.InlineKeyboardButton(text="Set First Reply", callback_data=f"set_first_reply:{user_id_str}:{token}")
        keyboard.add(delete_button, start_button, first_reply_button)
        self.bot.send_message(chat_id, "Choose an action:", reply_markup=keyboard)

    def process_start_message(self, message, token):
        start_message = message.text
        user_id_str = str(message.from_user.id)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE bots SET start_message = ? WHERE owner_id = ? AND token = ?",
                           (start_message, user_id_str, token))
            conn.commit()
        # Update the bot instance in memory
        if token in self.bots:
            self.bots[token].start_message = start_message
        self.bot.reply_to(message, "Start message set.")


    def process_first_reply_message(self, message, token):
        first_reply = message.text
        user_id_str = str(message.from_user.id)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE bots SET first_reply = ? WHERE owner_id = ? AND token = ?",
                           (first_reply, user_id_str, token))
            conn.commit()

        if token in self.bots:
             self.bots[token].first_reply = first_reply
        self.bot.reply_to(message, "First reply message set.")

    def delete_bot(self, call, user_id_str, token):
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                # Delete from bots table
                cursor.execute("DELETE FROM bots WHERE owner_id = ? AND token = ?", (user_id_str, token))

                # Delete from message_counts (using the token, which is the primary key)
                cursor.execute("DELETE FROM message_counts WHERE token = ?", (token,))
                conn.commit()

            if token in self.bots:
                self.bots[token].stop_polling()
                del self.bots[token]

            self.bot.edit_message_text("Bot deleted.", call.message.chat.id, call.message.message_id)
            self.update_stats()

        except Exception as e:
            logger.exception(f"Error deleting bot: {e}")
            self.bot.edit_message_text(f"Error: {e}", call.message.chat.id, call.message.message_id)

    def process_token(self, message):
        token = message.text.strip()
        user_id = message.from_user.id
        user_id_str = str(user_id)

        try:
            if not re.match(r"^[0-9]+:[a-zA-Z0-9_-]+$", token):
                self.bot.reply_to(message, "Invalid token format.")
                return

            temp_bot = telebot.TeleBot(token, parse_mode=None)
            if DELETE_WEBHOOKS_ON_NEW_BOT:
                temp_bot.delete_webhook()
            temp_bot.get_me()

            if token in self.bots:
                self.bot.reply_to(message, "This bot is already managed.")
                return

            whispry_instance = WhispryBot(token, user_id, self.bot)
            self.bots[token] = whispry_instance

            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bots (owner_id, token, start_message, first_reply)
                    VALUES (?, ?, ?, ?)
                """, (user_id_str, token, "", ""))  # Insert into bots table
                conn.commit()

            self.update_stats()
            bot_username = whispry_instance.bot.get_me().username
            self.bot.reply_to(message, f"Bot @{bot_username} added!")

        except telebot.apihelper.ApiTelegramException as e:
            logger.exception(f"Telegram API Error: {e}")
            self.bot.reply_to(message, f"Invalid token or API error: {e}.")
        except Exception as e:
            logger.exception(f"Error: {e}")
            self.bot.reply_to(message, "An unexpected error occurred.")

    def store_message_mapping(self, owner_id, forwarded_message_id, user_id):
        owner_id_str = str(owner_id)
        forwarded_message_id_str = str(forwarded_message_id)
        user_id_int = int(user_id)

        if owner_id_str not in self.message_mappings:
            self.message_mappings[owner_id_str] = {}
        self.message_mappings[owner_id_str][forwarded_message_id_str] = user_id_int

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO message_mappings (owner_id, forwarded_message_id, user_id)
                VALUES (?, ?, ?)
                ON CONFLICT(owner_id, forwarded_message_id) DO NOTHING  -- Prevent duplicates
            """, (owner_id_str, forwarded_message_id_str, user_id_int))
            conn.commit()


    def get_user_id_from_message_id(self, owner_id, forwarded_message_id):
        owner_id_str = str(owner_id)
        forwarded_message_id_str = str(forwarded_message_id)

        # First try to get it from the in-memory cache
        if owner_id_str in self.message_mappings and forwarded_message_id_str in self.message_mappings[owner_id_str]:
            return self.message_mappings[owner_id_str][forwarded_message_id_str]

        # If not in memory, try to get it from the database
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM message_mappings WHERE owner_id = ? AND forwarded_message_id = ?",
                           (owner_id_str, forwarded_message_id_str))
            result = cursor.fetchone()
            if result:
                user_id = int(result[0])  # Convert to integer
                # Update the in-memory cache
                if owner_id_str not in self.message_mappings:
                    self.message_mappings[owner_id_str] = {}
                self.message_mappings[owner_id_str][forwarded_message_id_str] = user_id
                return user_id
            else:
                return None


    def update_stats(self):
        """Updates the total bot and message counts."""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT token) FROM bots")
            self.total_bots_count = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(message_count) FROM message_counts")
            total_messages = cursor.fetchone()[0]
            self.total_messages_count = total_messages if total_messages is not None else 0

    def run(self):
        logger.info("Starting Whispry main bot...")
        self.bot.infinity_polling()

    def delete_all_webhooks(self):
        """Deletes webhooks for all known bots."""
        logger.info("Deleting webhooks for all bots...")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT token FROM bots")
            for row in cursor.fetchall():
                token = row[0]
                try:
                    temp_bot = telebot.TeleBot(token, parse_mode=None)
                    temp_bot.delete_webhook()
                    logger.info(f"Webhook deleted for bot {token[-6:]}")
                except Exception as e:
                    logger.exception(f"Failed to delete webhook for bot {token[-6:]}: {e}")
        logger.info("Finished deleting webhooks.")

if __name__ == "__main__":
    main_whispry = Whispry(BOT_TOKEN)
    main_whispry.run()