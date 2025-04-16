import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
)
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerChannel, InputPeerUser
import asyncio
import re
import sqlite3
from threading import Thread
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Состояния для conversation handler
(
    PHONE_NUMBER,
    API_DATA,
    CODE,
    PASSWORD,
    GROUP_SETTINGS,
    MESSAGE_SETTINGS,
    ADD_GROUP,
    TOGGLE_MESSAGING,
    MESSAGE_TEXT,
) = range(9)

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    
    # Таблица пользователей
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Таблица аккаунтов Telegram
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS telegram_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        phone TEXT,
        api_id TEXT,
        api_hash TEXT,
        session_string TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')
    
    # Таблица групп для рассылки
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS target_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        group_id TEXT,
        group_title TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')
    
    # Таблица сообщений
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        message_text TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')
    
    # Таблица логов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        details TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')
    
    conn.commit()
    conn.close()

init_db()

class DatabaseManager:
    @staticmethod
    def log_action(user_id, action, details=""):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO logs (user_id, action, details) VALUES (?, ?, ?)",
            (user_id, action, details)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def get_user_accounts(user_id):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM telegram_accounts WHERE user_id = ? AND is_active = 1",
            (user_id,)
        )
        accounts = cursor.fetchall()
        conn.close()
        return accounts

    @staticmethod
    def add_account(user_id, phone, api_id=None, api_hash=None, session_string=None):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO telegram_accounts (user_id, phone, api_id, api_hash, session_string) VALUES (?, ?, ?, ?, ?)",
            (user_id, phone, api_id, api_hash, session_string)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def add_group(user_id, group_id, group_title=""):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO target_groups (user_id, group_id, group_title) VALUES (?, ?, ?)",
            (user_id, group_id, group_title)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def get_user_groups(user_id):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM target_groups WHERE user_id = ? AND is_active = 1",
            (user_id,)
        )
        groups = cursor.fetchall()
        conn.close()
        return groups

    @staticmethod
    def save_message(user_id, message_text):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (user_id, message_text) VALUES (?, ?)",
            (user_id, message_text)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def get_last_message(user_id):
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT message_text FROM messages WHERE user_id = ? AND is_active = 1 ORDER BY id DESC LIMIT 1",
            (user_id,)
        )
        message = cursor.fetchone()
        conn.close()
        return message[0] if message else None

class TelegramAccountManager:
    def __init__(self):
        self.active_clients = {}
        self.verification_codes = {}

    async def connect_account(self, api_id, api_hash, phone, session_string=None):
        client = TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash
        )
        
        try:
            if not await client.is_user_authorized():
                await client.start(phone)
                session_string = client.session.save()
                
            self.active_clients[phone] = client
            return client, session_string
        except Exception as e:
            logger.error(f"Ошибка подключения аккаунта {phone}: {e}")
            raise

    async def send_message_to_group(self, client, group_id, message):
        try:
            entity = await client.get_entity(group_id)
            await client.send_message(entity, message)
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения в группу {group_id}: {e}")
            return False

account_manager = TelegramAccountManager()

def start(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    DatabaseManager.log_action(user_id, "start_command")
    
    # Регистрируем пользователя если его нет
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()
    
    keyboard = [
        [InlineKeyboardButton("Подключить аккаунт", callback_data='connect_account')],
        [InlineKeyboardButton("Настроить БОТа", callback_data='configure_bot')],
        [InlineKeyboardButton("Описание", callback_data='description')],
        [InlineKeyboardButton("Поддержка", callback_data='support')],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = """
# Posting to Chats  
  
    """
    
    update.message.reply_text(text, reply_markup=reply_markup)

def connect_account_menu(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "connect_account_menu")
    
    accounts = DatabaseManager.get_user_accounts(user_id)
    buttons = []
    
    if len(accounts) < 10:
        buttons.append([InlineKeyboardButton("+ Добавить аккаунт", callback_data='add_account')])
    
    buttons.append([InlineKeyboardButton("Назад", callback_data='back')])
    
    reply_markup = InlineKeyboardMarkup(buttons)
    
    accounts_text = "\n".join([f"✅ {acc[2]}" for acc in accounts]) if accounts else "Нет подключенных аккаунтов"
    
    query.edit_message_text(
        text=f"Меню подключения аккаунтов:\n\nПодключенные аккаунты:\n{accounts_text}",
        reply_markup=reply_markup
    )

def add_account_menu(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "add_account_menu")
    
    keyboard = [
        [InlineKeyboardButton("Подключение по номеру телефона", callback_data='connect_phone')],
        [InlineKeyboardButton("Подключение по API", callback_data='connect_api')],
        [InlineKeyboardButton("Назад", callback_data='back')],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        text="Выберите способ подключения аккаунта:",
        reply_markup=reply_markup
    )

def request_phone_number(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "request_phone_number")
    
    query.edit_message_text(
        text="Введите номер телефона в международном формате (например, +79123456789):",
    )
    return PHONE_NUMBER

def handle_phone_number(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    phone = update.message.text
    
    if not re.match(r'^\+[0-9]{11,15}$', phone):
        update.message.reply_text("Неверный формат номера. Пожалуйста, введите номер в международном формате (например, +79123456789):")
        return PHONE_NUMBER
    
    context.user_data['phone'] = phone
    DatabaseManager.log_action(user_id, "phone_number_entered", phone)
    
    # Здесь должна быть логика отправки кода подтверждения
    # В демо-версии просто запрашиваем код
    update.message.reply_text("Код подтверждения отправлен на ваш номер. Введите код:")
    
    return CODE

def handle_code(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    code = update.message.text
    
    if not code.isdigit() or len(code) != 5:
        update.message.reply_text("Код должен состоять из 5 цифр. Пожалуйста, введите код снова:")
        return CODE
    
    DatabaseManager.log_action(user_id, "code_entered", "code_received")
    
    # В реальном боте здесь проверка кода
    # Для демо просто сохраняем аккаунт
    phone = context.user_data['phone']
    DatabaseManager.add_account(user_id, phone)
    
    update.message.reply_text("Аккаунт успешно подключен!")
    start(update, context)
    
    return ConversationHandler.END

def request_api_data(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "request_api_data")
    
    api_instructions = """
Для подключения аккаунта вам понадобятся:
1. API ID и API HASH (получить на my.telegram.org)
2. Номер телефона аккаунта

Введите данные в формате:
api_id:api_hash:phone_number

Пример:
123456:abcdef123456abcdef123456abcdef12:+79123456789
    """
    
    query.edit_message_text(api_instructions)
    return API_DATA

def handle_api_data(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    text = update.message.text
    
    try:
        api_id, api_hash, phone = text.split(':')
        
        if not all([api_id.isdigit(), len(api_hash) == 32, re.match(r'^\+[0-9]{11,15}$', phone)]):
            raise ValueError("Неверный формат данных")
            
        DatabaseManager.log_action(user_id, "api_data_entered", f"api_id: {api_id}")
        
        # В реальном боте здесь подключение аккаунта
        # Для демо просто сохраняем
        DatabaseManager.add_account(user_id, phone, api_id, api_hash)
        
        update.message.reply_text("Данные API приняты. Попытка подключения аккаунта...")
        
        # Запускаем подключение в фоновом режиме
        Thread(target=connect_account_background, args=(user_id, api_id, api_hash, phone)).start()
        
    except Exception as e:
        logger.error(f"Ошибка обработки API данных: {e}")
        update.message.reply_text("Неверный формат данных. Пожалуйста, введите данные в формате: api_id:api_hash:phone_number")
        return API_DATA
    
    start(update, context)
    return ConversationHandler.END

def connect_account_background(user_id, api_id, api_hash, phone):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        client = TelegramClient(StringSession(), api_id, api_hash)
        client.start(phone)
        
        session_string = client.session.save()
        
        # Обновляем запись в базе данных
        conn = sqlite3.connect('bot_data.db')
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE telegram_accounts SET session_string = ? WHERE user_id = ? AND phone = ?",
            (session_string, user_id, phone)
        )
        conn.commit()
        conn.close()
        
        logger.info(f"Аккаунт {phone} успешно подключен")
    except Exception as e:
        logger.error(f"Ошибка подключения аккаунта {phone}: {e}")

def configure_bot_menu(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "configure_bot_menu")
    
    keyboard = [
        [InlineKeyboardButton("Настроить рассылку в группах", callback_data='group_messaging')],
        [InlineKeyboardButton("Настроить сообщение", callback_data='set_message')],
        [InlineKeyboardButton("Запустить рассылку", callback_data='start_mailing')],
        [InlineKeyboardButton("Назад", callback_data='back')],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        text="Меню настройки бота:",
        reply_markup=reply_markup
    )

def group_messaging_menu(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "group_messaging_menu")
    
    groups = DatabaseManager.get_user_groups(user_id)
    groups_list = "\n".join([f"• {group[2] or group[1]}" for group in groups]) if groups else "Нет добавленных групп"
    
    keyboard = [
        [InlineKeyboardButton("Добавить группу", callback_data='add_group')],
        [InlineKeyboardButton("Удалить группу", callback_data='remove_group')],
        [InlineKeyboardButton("Назад", callback_data='back')],
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query.edit_message_text(
        text=f"Настройка рассылки в группах:\n\nДобавленные группы:\n{groups_list}",
        reply_markup=reply_markup
    )

def request_group_info(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "request_group_info")
    
    query.edit_message_text(
        text="Введите username группы (например, @groupname) или ID группы (начинается с -100):",
    )
    return ADD_GROUP

def handle_group_info(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    group_id = update.message.text
    DatabaseManager.log_action(user_id, "group_info_entered", group_id)
    
    DatabaseManager.add_group(user_id, group_id)
    
    update.message.reply_text(f"Группа {group_id} добавлена для рассылки!")
    start(update, context)
    
    return ConversationHandler.END

def request_message_text(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "request_message_text")
    
    last_message = DatabaseManager.get_last_message(user_id)
    hint = f"\n\nТекущее сообщение:\n{last_message}" if last_message else ""
    
    query.edit_message_text(
        text=f"Введите текст сообщения для рассылки:{hint}",
    )
    return MESSAGE_TEXT

def handle_message_text(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    message_text = update.message.text
    DatabaseManager.log_action(user_id, "message_text_entered", f"length: {len(message_text)}")
    
    DatabaseManager.save_message(user_id, message_text)
    
    update.message.reply_text("Текст сообщения сохранен!")
    start(update, context)
    
    return ConversationHandler.END

def start_mailing(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    DatabaseManager.log_action(user_id, "start_mailing_attempt")
    
    # Проверяем наличие необходимых данных
    accounts = DatabaseManager.get_user_accounts(user_id)
    groups = DatabaseManager.get_user_groups(user_id)
    message = DatabaseManager.get_last_message(user_id)
    
    if not accounts:
        query.edit_message_text("Нет подключенных аккаунтов!")
        return
    if not groups:
        query.edit_message_text("Нет добавленных групп!")
        return
    if not message:
        query.edit_message_text("Не задано сообщение для рассылки!")
        return
    
    query.edit_message_text("Начинаю рассылку...")
    
    # Запускаем рассылку в фоновом режиме
    Thread(target=run_mailing_background, args=(user_id,)).start()

def run_mailing_background(user_id):
    try:
        accounts = DatabaseManager.get_user_accounts(user_id)
        groups = DatabaseManager.get_user_groups(user_id)
        message = DatabaseManager.get_last_message(user_id)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        for account in accounts:
            if not account[4]:  # Проверяем наличие session_string
                continue
                
            try:
                client = TelegramClient(
                    StringSession(account[4]),
                    account[3],  # api_id
                    account[2]   # api_hash
                )
                
                with client:
                    for group in groups:
                        try:
                            client.loop.run_until_complete(
                                client.send_message(group[1], message)
                            )
                            logger.info(f"Сообщение отправлено в группу {group[1]} с аккаунта {account[1]}")
                            DatabaseManager.log_action(user_id, "message_sent", f"account: {account[1]}, group: {group[1]}")
                        except Exception as e:
                            logger.error(f"Ошибка отправки в группу {group[1]}: {e}")
                            DatabaseManager.log_action(user_id, "send_error", f"group: {group[1]}, error: {str(e)}")
                            
            except Exception as e:
                logger.error(f"Ошибка работы с аккаунтом {account[1]}: {e}")
                DatabaseManager.log_action(user_id, "account_error", f"account: {account[1]}, error: {str(e)}")
                
    except Exception as e:
        logger.error(f"Ошибка в процессе рассылки: {e}")
        DatabaseManager.log_action(user_id, "mailing_error", str(e))

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    
    if update and update.effective_user:
        DatabaseManager.log_action(
            update.effective_user.id,
            "error",
            str(context.error)
        )
def show_stats(update: Update, context: CallbackContext) -> None:
    """Показывает статистику бота"""
    user_id = update.effective_user.id
    DatabaseManager.log_action(user_id, "view_stats")
    
    conn = sqlite3.connect('bot_data.db')
    cursor = conn.cursor()
    
    # Получаем общее количество пользователей
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    # Получаем количество активных рассылок
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM messages WHERE is_active = 1")
    active_mailings = cursor.fetchone()[0]
    
    # Получаем общее количество подключенных аккаунтов
    cursor.execute("SELECT COUNT(*) FROM telegram_accounts WHERE is_active = 1")
    total_accounts = cursor.fetchone()[0]
    
    # Получаем количество целевых групп
    cursor.execute("SELECT COUNT(*) FROM target_groups WHERE is_active = 1")
    total_groups = cursor.fetchone()[0]
    
    conn.close()
    
    stats_text = f"""
📊 Статистика бота:

👥 Всего пользователей: {total_users}
📨 Активных рассылок: {active_mailings}
📱 Подключенных аккаунтов: {total_accounts}
🗂 Целевых групп: {total_groups}
    """
    
    update.message.reply_text(stats_text)
    
def main() -> None:
    # Проверяем наличие обязательных переменных
    if not os.getenv('BOT_TOKEN'):
        logger.error("Не задан BOT_TOKEN в переменных окружения!")
        return
    
    if not os.getenv('API_ID') or not os.getenv('API_HASH'):
        logger.warning("API_ID и/или API_HASH не заданы. Некоторые функции могут не работать.")
    
    updater = Updater(os.getenv('BOT_TOKEN'))
    dispatcher = updater.dispatcher
    
    # Обработчики команд
    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('stats', show_stats))
    
    # Обработчики кнопок
    dispatcher.add_handler(CallbackQueryHandler(connect_account_menu, pattern='^connect_account$'))
    dispatcher.add_handler(CallbackQueryHandler(add_account_menu, pattern='^add_account$'))
    dispatcher.add_handler(CallbackQueryHandler(configure_bot_menu, pattern='^configure_bot$'))
    dispatcher.add_handler(CallbackQueryHandler(group_messaging_menu, pattern='^group_messaging$'))
    dispatcher.add_handler(CallbackQueryHandler(start_mailing, pattern='^start_mailing$'))
    dispatcher.add_handler(CallbackQueryHandler(start, pattern='^back$'))
    
    # Conversation handlers
    conv_handler_phone = ConversationHandler(
        entry_points=[CallbackQueryHandler(request_phone_number, pattern='^connect_phone$')],
        states={
            PHONE_NUMBER: [MessageHandler(Filters.text & ~Filters.command, handle_phone_number)],
            CODE: [MessageHandler(Filters.text & ~Filters.command, handle_code)],
        },
        fallbacks=[],
    )
    
    conv_handler_api = ConversationHandler(
        entry_points=[CallbackQueryHandler(request_api_data, pattern='^connect_api$')],
        states={
            API_DATA: [MessageHandler(Filters.text & ~Filters.command, handle_api_data)],
        },
        fallbacks=[],
    )
    
    conv_handler_group = ConversationHandler(
        entry_points=[CallbackQueryHandler(request_group_info, pattern='^add_group$')],
        states={
            ADD_GROUP: [MessageHandler(Filters.text & ~Filters.command, handle_group_info)],
        },
        fallbacks=[],
    )
    
    conv_handler_message = ConversationHandler(
        entry_points=[CallbackQueryHandler(request_message_text, pattern='^set_message$')],
        states={
            MESSAGE_TEXT: [MessageHandler(Filters.text & ~Filters.command, handle_message_text)],
        },
        fallbacks=[],
    )
    
    dispatcher.add_handler(conv_handler_phone)
    dispatcher.add_handler(conv_handler_api)
    dispatcher.add_handler(conv_handler_group)
    dispatcher.add_handler(conv_handler_message)
    
    # Обработчик ошибок
    dispatcher.add_error_handler(error_handler)
    
    # Запуск бота
    updater.start_polling()
    logger.info("Бот запущен и готов к работе")
    updater.idle()

if __name__ == '__main__':
    main()