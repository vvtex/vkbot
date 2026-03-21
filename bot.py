#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import sqlite3
import logging
import threading
import time
import traceback
from datetime import datetime, timedelta

try:
    import vk_api
    from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
    from vk_api.keyboard import VkKeyboard, VkKeyboardColor
    from vk_api.utils import get_random_id
    from vk_api import VkUpload
    from vk_api.exceptions import ApiError
except ImportError:
    print("Установите vk-api: pip install vk-api")
    sys.exit(1)

# Импортируем классы ботов
from bots import (
    SurveyBot,
    HairdresserBot,
    SepticBot,
    RoofBot,
    BankruptcyBot,
    ValuationBot,
)

# ================== НАСТРОЙКА ЛОГИРОВАНИЯ ==================================
DATA_DIR = os.getenv('DATA_DIR', '.')
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

log_file = os.path.join(DATA_DIR, 'bot.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('VK_bot')

# ================== КОНФИГУРАЦИЯ ===========================================
GROUP_ID_STR = os.getenv('GROUP_ID')
API_TOKEN = os.getenv('API_TOKEN')
admin_ids_str = os.getenv('ADMIN_IDS')

if not GROUP_ID_STR or not API_TOKEN or not admin_ids_str:
    logger.error("Не заданы переменные окружения: GROUP_ID, API_TOKEN, ADMIN_IDS")
    sys.exit(1)

def parse_group_id(group_id_str):
    if group_id_str.startswith('club'):
        return int(group_id_str[4:])
    return int(group_id_str)

GROUP_ID = parse_group_id(GROUP_ID_STR)

ADMIN_IDS = []
for part in admin_ids_str.split(','):
    part = part.strip()
    if part.isdigit():
        ADMIN_IDS.append(int(part))
if not ADMIN_IDS:
    logger.error("Список ADMIN_IDS пуст")
    sys.exit(1)

DB_FILE = os.getenv('DB_FILE', os.path.join(DATA_DIR, 'bot_database.db'))
BOT_ENABLED = True
BOT_DISABLED_UNTIL = None
TIMEOUT_MINUTES = 10

# ================== РАБОТА С БАЗОЙ ДАННЫХ ==================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vk_id INTEGER UNIQUE NOT NULL,
            first_name TEXT,
            last_name TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_interaction DATETIME DEFAULT CURRENT_TIMESTAMP,
            subscribed BOOLEAN DEFAULT 0,
            current_state TEXT,
            is_blocked BOOLEAN DEFAULT 0,
            bot_name TEXT
        )
    ''')
    try:
        cur.execute('ALTER TABLE users ADD COLUMN is_blocked BOOLEAN DEFAULT 0')
    except:
        pass
    try:
        cur.execute('ALTER TABLE users ADD COLUMN bot_name TEXT')
    except:
        pass

    cur.execute('''
        CREATE TABLE IF NOT EXISTS survey_answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            answered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            request_text TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'new',
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_vk_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            details TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_or_create_user(vk_id, first_name='', last_name=''):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE vk_id = ?', (vk_id,))
    user = cur.fetchone()
    if user:
        cur.execute('UPDATE users SET last_interaction = CURRENT_TIMESTAMP WHERE vk_id = ?', (vk_id,))
        conn.commit()
        conn.close()
        return dict(user)
    else:
        cur.execute('''
            INSERT INTO users (vk_id, first_name, last_name, first_seen, last_interaction, is_blocked)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
        ''', (vk_id, first_name, last_name))
        conn.commit()
        user_id = cur.lastrowid
        conn.close()
        return {
            'id': user_id,
            'vk_id': vk_id,
            'first_name': first_name,
            'last_name': last_name,
            'subscribed': 0,
            'current_state': None,
            'is_blocked': 0,
            'bot_name': None
        }

def update_user_state(vk_id, state, bot_name=None):
    conn = get_db_connection()
    cur = conn.cursor()
    if bot_name is not None:
        cur.execute('UPDATE users SET current_state = ?, bot_name = ?, last_interaction = CURRENT_TIMESTAMP WHERE vk_id = ?',
                    (state, bot_name, vk_id))
    else:
        cur.execute('UPDATE users SET current_state = ?, last_interaction = CURRENT_TIMESTAMP WHERE vk_id = ?',
                    (state, vk_id))
    conn.commit()
    conn.close()

def clear_user_state(vk_id):
    update_user_state(vk_id, None, None)

def save_answer(vk_id, question, answer):
    user = get_or_create_user(vk_id)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO survey_answers (user_id, question, answer) VALUES (?, ?, ?)',
                (user['id'], question, answer))
    conn.commit()
    conn.close()

def save_request(vk_id, request_text):
    user = get_or_create_user(vk_id)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO requests (user_id, request_text) VALUES (?, ?)',
                (user['id'], request_text))
    conn.commit()
    conn.close()
    return user

def is_admin(vk_id):
    return vk_id in ADMIN_IDS

def log_admin_action(admin_vk_id, action, details=''):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO admin_log (admin_vk_id, action, details) VALUES (?, ?, ?)',
                (admin_vk_id, action, details))
    conn.commit()
    conn.close()

def is_user_blocked(vk_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT is_blocked FROM users WHERE vk_id = ?', (vk_id,))
    row = cur.fetchone()
    conn.close()
    return row and row['is_blocked'] == 1

def set_user_block(vk_id, block=True):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET is_blocked = ? WHERE vk_id = ?', (1 if block else 0, vk_id))
    if block:
        cur.execute('UPDATE users SET current_state = NULL WHERE vk_id = ?', (vk_id,))
    conn.commit()
    conn.close()

# ================== КЛАВИАТУРЫ =============================================
def get_main_menu_keyboard():
    kb = VkKeyboard(one_time=False)
    kb.add_button('📢 Акции', color=VkKeyboardColor.PRIMARY)
    kb.add_button('📝 Отправить заявку', color=VkKeyboardColor.PRIMARY)
    kb.add_button('🤖 Боты', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_bots_menu_keyboard():
    kb = VkKeyboard(one_time=False, inline=True)
    bots = [
        ("📋 Опрос по продвижению", "bot_survey"),
        ("💇 Запись в парикмахерскую", "bot_hairdresser"),
        ("🚰 Септик и откачка", "bot_septic"),
        ("🏠 Замер кровли и стройматериалы", "bot_roof"),
        ("⚖️ Банкротство", "bot_bankruptcy"),
        ("📊 Независимая оценка", "bot_valuation"),
    ]
    for name, payload in bots:
        kb.add_callback_button(name, color=VkKeyboardColor.PRIMARY, payload={"type": "select_bot", "bot": payload})
        kb.add_line()
    kb.add_callback_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"type": "main_menu"})
    return kb.get_keyboard()

def get_yes_no_keyboard():
    kb = VkKeyboard(inline=True)
    kb.add_callback_button("Да", color=VkKeyboardColor.POSITIVE, payload={"type": "consent", "answer": "yes"})
    kb.add_callback_button("Нет", color=VkKeyboardColor.NEGATIVE, payload={"type": "consent", "answer": "no"})
    return kb.get_keyboard()

def get_empty_keyboard():
    return VkKeyboard.get_empty_keyboard()

# ================== ОСНОВНОЙ КЛАСС БОТА ====================================
class VKBot:
    def __init__(self, group_id, token):
        self.group_id = group_id
        self.token = token
        self.vk_session = vk_api.VkApi(token=token)
        try:
            self.longpoll = VkBotLongPoll(self.vk_session, group_id)
        except Exception as e:
            logger.error(f"Ошибка при инициализации LongPoll: {e}")
            sys.exit(1)

        self.vk = self.vk_session.get_api()
        self.upload = VkUpload(self.vk_session)
        self.enabled = BOT_ENABLED
        self.disabled_until = BOT_DISABLED_UNTIL

        # Словарь для временных данных (например, для выбора бота)
        self.user_temp_data = {}

        # Регистрируем доступные боты
        self.bots = {
            "bot_survey": SurveyBot,
            "bot_hairdresser": HairdresserBot,
            "bot_septic": SepticBot,
            "bot_roof": RoofBot,
            "bot_bankruptcy": BankruptcyBot,
            "bot_valuation": ValuationBot,
        }

    def send_message(self, user_id, message, keyboard=None, attachment=None):
        try:
            self.vk.messages.send(
                user_id=user_id,
                random_id=get_random_id(),
                message=message,
                keyboard=keyboard,
                attachment=attachment
            )
        except ApiError as e:
            if e.code in [901, 404406378]:
                logger.warning(f"Не удалось отправить сообщение {user_id}: {e}")
            else:
                logger.error(f"Ошибка отправки {user_id}: {e}")
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")

    def edit_message(self, user_id, message_id, message_text, keyboard=None):
        try:
            self.vk.messages.edit(
                peer_id=user_id,
                message_id=message_id,
                message=message_text,
                keyboard=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка редактирования: {e}")

    def answer_callback(self, event):
        try:
            self.vk.messages.sendMessageEventAnswer(
                event_id=event.object['event_id'],
                user_id=event.object['user_id'],
                peer_id=event.object['peer_id']
            )
        except Exception as e:
            logger.error(f"Ошибка ответа callback: {e}")

    def notify_admins(self, message, attachment=None):
        for admin_id in ADMIN_IDS:
            self.send_message(admin_id, message, attachment=attachment)

    def is_bot_disabled(self):
        if not self.enabled:
            return True
        if self.disabled_until and datetime.now() < self.disabled_until:
            return True
        return False

    def handle_event(self, event):
        try:
            if self.is_bot_disabled():
                if event.from_user:
                    user_id = event.message['from_id']
                    if is_admin(user_id) and event.message['text'].startswith('/enable'):
                        self.process_admin_command(event)
                return

            if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
                self.handle_message(event)
            elif event.type == VkBotEventType.MESSAGE_EVENT:
                self.answer_callback(event)
                self.handle_callback(event)
        except Exception as e:
            logger.error(f"Необработанное исключение: {e}")
            logger.error(traceback.format_exc())

    def handle_message(self, event):
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        first_name = event.message.get('first_name', '')
        last_name = event.message.get('last_name', '')
        user = get_or_create_user(user_id, first_name, last_name)

        if is_user_blocked(user_id):
            logger.info(f"Заблокированный пользователь {user_id} игнорируется")
            return

        # Проверка таймаута
        if user['current_state']:
            last_interaction = datetime.strptime(user['last_interaction'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - last_interaction > timedelta(minutes=TIMEOUT_MINUTES):
                logger.info(f"Таймаут для {user_id}, сброс")
                if user_id in self.user_temp_data:
                    del self.user_temp_data[user_id]
                clear_user_state(user_id)
                self.send_message(user_id, "⏳ Время ожидания истекло. Возвращаю в главное меню.")
                self.send_main_menu(user_id)
                return

        # Уведомление админов о сообщении
        if not is_admin(user_id) and not text.startswith('/'):
            self.notify_admins(
                f"📩 **Новое сообщение**\nОт: {first_name} {last_name} (id{user_id})\nТекст: {text}"
            )

        # Обработка команд
        if text.lower() == 'меню':
            self.reset_user(user_id)
            self.send_main_menu(user_id)
            return

        # Если пользователь находится в диалоге с каким-то ботом
        if user['current_state'] and user['bot_name']:
            bot_name = user['bot_name']
            if bot_name in self.bots:
                bot_instance = self.bots[bot_name](self, user_id)
                bot_instance.handle_message(text, user['current_state'])
            else:
                logger.warning(f"Неизвестный бот {bot_name}, сброс")
                self.reset_user(user_id)
                self.send_main_menu(user_id)
            return

        # Обработка кнопок главного меню
        if text == '📢 Акции':
            self.show_promotions(user_id)
        elif text == '📝 Отправить заявку':
            self.start_request(user_id)
        elif text == '🤖 Боты':
            self.show_bots_menu(user_id)
        else:
            self.send_main_menu(user_id)

    def handle_callback(self, event):
        user_id = event.object['user_id']
        payload = event.object['payload']
        user = get_or_create_user(user_id)

        if is_user_blocked(user_id):
            return

        # Обработка выбора бота
        if payload.get('type') == 'select_bot':
            bot_key = payload.get('bot')
            if bot_key in self.bots:
                # Сохраняем выбранного бота во временные данные и запрашиваем согласие
                self.user_temp_data[user_id] = {'selected_bot': bot_key}
                self.send_message(user_id,
                    f"Вы выбрали бота **{self.get_bot_name(bot_key)}**. Вы согласны на сбор и обработку ваших данных?",
                    keyboard=get_yes_no_keyboard())
            else:
                self.send_message(user_id, "Неизвестный бот.")
            return

        # Обработка согласия/отказа
        elif payload.get('type') == 'consent':
            answer = payload.get('answer')
            selected_bot = self.user_temp_data.get(user_id, {}).get('selected_bot')
            if not selected_bot:
                self.send_message(user_id, "Что-то пошло не так. Попробуйте выбрать бота заново.")
                self.show_bots_menu(user_id)
                return

            if answer == 'yes':
                # Запускаем бота
                bot_instance = self.bots[selected_bot](self, user_id)
                bot_instance.start()
                # Удаляем временные данные
                if user_id in self.user_temp_data:
                    del self.user_temp_data[user_id]
            else:
                self.send_message(user_id, "Хорошо, возвращаемся в меню ботов.")
                self.show_bots_menu(user_id)
                if user_id in self.user_temp_data:
                    del self.user_temp_data[user_id]
            return

        # Обработка возврата в главное меню
        elif payload.get('type') == 'main_menu':
            self.send_main_menu(user_id)
            return

    def get_bot_name(self, bot_key):
        names = {
            "bot_survey": "Опрос по продвижению",
            "bot_hairdresser": "Запись в парикмахерскую",
            "bot_septic": "Септик и откачка",
            "bot_roof": "Замер кровли и стройматериалы",
            "bot_bankruptcy": "Банкротство",
            "bot_valuation": "Независимая оценка",
        }
        return names.get(bot_key, "Неизвестный бот")

    def reset_user(self, user_id):
        if user_id in self.user_temp_data:
            del self.user_temp_data[user_id]
        clear_user_state(user_id)

    def send_main_menu(self, user_id):
        msg = (
            "🌟 **Добро пожаловать!** Я — многофункциональный бот.\n"
            "Выберите действие:\n"
            "─────────────────────\n"
            "📢 **Акции** – посмотреть текущие предложения\n"
            "📝 **Отправить заявку** – оставить контакт для связи\n"
            "🤖 **Боты** – запустить одного из специализированных ботов"
        )
        kb = get_main_menu_keyboard()
        self.send_message(user_id, msg, kb)

    def show_bots_menu(self, user_id):
        msg = "Выберите бота, которого хотите запустить:"
        kb = get_bots_menu_keyboard()
        self.send_message(user_id, msg, keyboard=kb)

    def show_promotions(self, user_id):
        # заглушка – можно загрузить из БД
        self.send_message(user_id, "📢 Акции:\nСкоро появятся...")
        self.send_main_menu(user_id)

    def start_request(self, user_id):
        # стандартный сбор контактов (имя, телефон, email)
        self.user_temp_data[user_id] = {'request': {}}
        update_user_state(user_id, 'request_name', 'request')
        self.send_message(user_id, "📝 Введите ваше имя:", get_empty_keyboard())

    def handle_request_response(self, event, user):
        # этот метод используется только если мы в состоянии request_name и т.п.
        # но мы не вызываем его в handle_message, потому что если пользователь в состоянии с bot_name='request', то он попадёт в ветку для ботов,
        # а у нас нет бота с именем 'request'. Поэтому обработаем здесь же.
        # Можно добавить отдельный обработчик для состояния request.
        pass

    # ============= АДМИНИСТРИРОВАНИЕ (сокращённо) ==========================
    def process_admin_command(self, event):
        # Здесь можно добавить команды админа
        pass

# ================== ЗАПУСК БОТА ===========================================
def main():
    init_db()
    bot = VKBot(GROUP_ID, API_TOKEN)
    logger.info("Бот запущен")
    while True:
        try:
            for event in bot.longpoll.listen():
                bot.handle_event(event)
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()