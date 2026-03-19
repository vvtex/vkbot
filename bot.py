#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ВКонтакте бот для сбора потребностей в услугах по продвижению сайтов.
Версия 3.2 (добавлены уведомления админам о сообщениях и опросах)
"""

import os
import sys
import sqlite3
import logging
import threading
import time
import csv
import io
from datetime import datetime, timedelta

try:
    import vk_api
    from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
    from vk_api.keyboard import VkKeyboard, VkKeyboardColor
    from vk_api.utils import get_random_id
    from vk_api import VkUpload
except ImportError:
    print("="*60)
    print("ОШИБКА: не установлена библиотека vk_api.")
    print("Установите её командой: pip install vk-api")
    print("="*60)
    sys.exit(1)

# ================== НАСТРОЙКА ЛОГИРОВАНИЯ ==================================
DATA_DIR = os.getenv('DATA_DIR', '.').rstrip('/')
if not os.path.exists(DATA_DIR):
    try:
        os.makedirs(DATA_DIR)
    except Exception as e:
        print(f"Не удалось создать директорию {DATA_DIR}: {e}")
        sys.exit(1)

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
# ===========================================================================

# ================== КОНФИГУРАЦИЯ ===========================================
GROUP_ID_STR = os.getenv('GROUP_ID')
API_TOKEN = os.getenv('API_TOKEN')
admin_ids_str = os.getenv('ADMIN_IDS')

if not GROUP_ID_STR:
    logger.error("Переменная окружения GROUP_ID не задана.")
    sys.exit(1)
if not API_TOKEN:
    logger.error("Переменная окружения API_TOKEN не задана.")
    sys.exit(1)
if not admin_ids_str:
    logger.error("Переменная окружения ADMIN_IDS не задана.")
    sys.exit(1)

def parse_group_id(group_id_str):
    if group_id_str.startswith('club'):
        return int(group_id_str[4:])
    else:
        return int(group_id_str)

try:
    GROUP_ID = parse_group_id(GROUP_ID_STR)
except ValueError:
    logger.error("GROUP_ID должен быть числом или строкой вида club<число>.")
    sys.exit(1)

ADMIN_IDS = []
for part in admin_ids_str.split(','):
    part = part.strip()
    if part.isdigit():
        ADMIN_IDS.append(int(part))
    else:
        logger.warning(f"Некорректный ID администратора пропущен: {part}")

if not ADMIN_IDS:
    logger.error("Список ADMIN_IDS пуст или не содержит корректных ID.")
    sys.exit(1)

DB_FILE = os.getenv('DB_FILE', os.path.join(DATA_DIR, 'bot_database.db'))
DEFAULT_MAILING_TIME = os.getenv('DEFAULT_MAILING_TIME', '10:00')
MAILING_CHECK_INTERVAL = int(os.getenv('MAILING_CHECK_INTERVAL', '60'))

BOT_ENABLED = True
BOT_DISABLED_UNTIL = None
# ===========================================================================

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
            current_state TEXT
        )
    ''')
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
        CREATE TABLE IF NOT EXISTS promotions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            periodicity TEXT CHECK(periodicity IN ('daily', 'weekly')) DEFAULT 'daily',
            last_sent DATETIME
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            promotion_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (promotion_id) REFERENCES promotions (id) ON DELETE CASCADE,
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
    logger.info("База данных инициализирована. Файл: %s", DB_FILE)

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
            INSERT INTO users (vk_id, first_name, last_name, first_seen, last_interaction)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
            'current_state': None
        }

def update_user_state(vk_id, state):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET current_state = ?, last_interaction = CURRENT_TIMESTAMP WHERE vk_id = ?', (state, vk_id))
    conn.commit()
    conn.close()

def clear_user_state(vk_id):
    update_user_state(vk_id, None)

def save_answer(vk_id, question, answer):
    user = get_or_create_user(vk_id)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO survey_answers (user_id, question, answer) VALUES (?, ?, ?)',
                (user['id'], question, answer))
    conn.commit()
    conn.close()

def set_subscription(vk_id, subscribed):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET subscribed = ? WHERE vk_id = ?', (1 if subscribed else 0, vk_id))
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

def add_promotion(title, text, start_date, end_date, periodicity='daily'):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO promotions (title, text, start_date, end_date, periodicity, is_active)
        VALUES (?, ?, ?, ?, ?, 1)
    ''', (title, text, start_date, end_date, periodicity))
    conn.commit()
    promo_id = cur.lastrowid
    conn.close()
    return promo_id

def get_active_promotions():
    today = datetime.now().date().isoformat()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT * FROM promotions
        WHERE is_active = 1 AND start_date <= ? AND end_date >= ?
        ORDER BY created_at DESC
    ''', (today, today))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_all_promotions():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM promotions ORDER BY created_at DESC')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_promotion(promo_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM promotions WHERE id = ?', (promo_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def update_promotion(promo_id, **kwargs):
    allowed = {'title', 'text', 'start_date', 'end_date', 'is_active', 'periodicity', 'last_sent'}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    set_clause = ', '.join([f"{k} = ?" for k in updates.keys()])
    values = list(updates.values()) + [promo_id]
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'UPDATE promotions SET {set_clause} WHERE id = ?', values)
    conn.commit()
    conn.close()

def delete_promotion(promo_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM promotions WHERE id = ?', (promo_id,))
    conn.commit()
    conn.close()

def get_subscribers():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT id, vk_id FROM users WHERE subscribed = 1')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def log_sent(promo_id, user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO sent_log (promotion_id, user_id) VALUES (?, ?)', (promo_id, user_id))
    conn.commit()
    conn.close()

def was_sent_today(promo_id, user_id):
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT 1 FROM sent_log
        WHERE promotion_id = ? AND user_id = ? AND sent_at >= ?
    ''', (promo_id, user_id, today_start))
    result = cur.fetchone() is not None
    conn.close()
    return result

def was_sent_this_week(promo_id, user_id):
    now = datetime.now()
    week_start = now - timedelta(days=now.weekday())
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT 1 FROM sent_log
        WHERE promotion_id = ? AND user_id = ? AND sent_at >= ?
    ''', (promo_id, user_id, week_start))
    result = cur.fetchone() is not None
    conn.close()
    return result

# ================== КЛАВИАТУРЫ =============================================
def get_main_menu_keyboard():
    kb = VkKeyboard(one_time=False)
    kb.add_button('📢 Акции', color=VkKeyboardColor.PRIMARY)
    kb.add_button('📝 Отправить заявку', color=VkKeyboardColor.PRIMARY)
    kb.add_button('❓ Пройти опрос', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_admin_keyboard():
    kb = VkKeyboard(one_time=False, inline=False)
    kb.add_button('📊 Статистика', color=VkKeyboardColor.PRIMARY)
    kb.add_button('📋 Ответы', color=VkKeyboardColor.PRIMARY)
    kb.add_button('📤 Экспорт данных', color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button('➕ Добавить акцию', color=VkKeyboardColor.POSITIVE)
    kb.add_button('📋 Список акций', color=VkKeyboardColor.PRIMARY)
    kb.add_button('✏️ Редактировать акцию', color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button('❌ Удалить акцию', color=VkKeyboardColor.NEGATIVE)
    kb.add_button('📨 Разослать акцию', color=VkKeyboardColor.POSITIVE)
    kb.add_button('⏰ Установить время', color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button('💬 Ответить пользователю', color=VkKeyboardColor.PRIMARY)
    kb.add_button('🚫 Отключить бота', color=VkKeyboardColor.NEGATIVE)
    kb.add_button('✅ Включить бота', color=VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button('🔙 Главное меню', color=VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def get_empty_keyboard():
    return VkKeyboard.get_empty_keyboard()

def get_keyboard_yes_no():
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()

def get_keyboard_audit():
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button('Уже делали', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_keyboard_content():
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button('Частично', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_keyboard_advertising():
    kb = VkKeyboard(one_time=True)
    kb.add_button('Реклама', color=VkKeyboardColor.PRIMARY)
    kb.add_button('Аудит', color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button('Оба варианта', color=VkKeyboardColor.POSITIVE)
    return kb.get_keyboard()

def get_keyboard_subscribe():
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()

# ================== ОСНОВНОЙ КЛАСС БОТА ====================================
class VKBot:
    def __init__(self, group_id, token):
        self.group_id = group_id
        self.token = token
        self.vk_session = vk_api.VkApi(token=token)
        try:
            self.longpoll = VkBotLongPoll(self.vk_session, group_id)
        except vk_api.exceptions.ApiError as e:
            if "longpoll for this group is not enabled" in str(e):
                logger.error("Ошибка: Long Poll API не включён для этого сообщества.")
            else:
                logger.error(f"Ошибка при инициализации LongPoll: {e}")
            sys.exit(1)

        self.vk = self.vk_session.get_api()
        self.upload = VkUpload(self.vk_session)
        self.enabled = BOT_ENABLED
        self.disabled_until = BOT_DISABLED_UNTIL

        self.admin_states = {}       # состояния для админов
        self.user_temp_data = {}     # временные данные для заявок (имя, телефон, email)

    def send_message(self, user_id, message, keyboard=None, attachment=None):
        try:
            self.vk.messages.send(
                user_id=user_id,
                random_id=get_random_id(),
                message=message,
                keyboard=keyboard,
                attachment=attachment
            )
        except vk_api.exceptions.ApiError as e:
            logger.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}")

    def notify_admins(self, message, attachment=None):
        """Отправить уведомление всем администраторам."""
        for admin_id in ADMIN_IDS:
            self.send_message(admin_id, message, attachment=attachment)

    def is_bot_disabled(self):
        if not self.enabled:
            return True
        if self.disabled_until and datetime.now() < self.disabled_until:
            return True
        return False

    def handle_event(self, event):
        if self.is_bot_disabled():
            if event.from_user:
                user_id = event.message['from_id']
                if is_admin(user_id) and event.message['text'].startswith('/enable'):
                    self.process_admin_command(event)
            return

        if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
            self.handle_message(event)

    def handle_message(self, event):
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        first_name = event.message.get('first_name', '')
        last_name = event.message.get('last_name', '')
        user = get_or_create_user(user_id, first_name, last_name)

        # Уведомление админов о новом сообщении от пользователя (кроме команд)
        if not is_admin(user_id) and not text.startswith('/'):
            self.notify_admins(
                f"📩 **Новое сообщение**\n"
                f"От: {first_name} {last_name} (id{user_id})\n"
                f"Текст: {text}"
            )

        # Команда "Меню" сбрасывает всё и показывает главное меню
        if text.lower() == 'меню':
            if user_id in self.user_temp_data:
                del self.user_temp_data[user_id]
            clear_user_state(user_id)
            self.send_main_menu(user_id)
            return

        # Состояния администратора
        if user_id in self.admin_states:
            self.handle_admin_state_input(event)
            return

        # Команды администратора
        if text.startswith('/') and is_admin(user_id):
            self.process_admin_command(event)
            return

        # Обработка состояний пользователя (опрос, заявка)
        if user['current_state']:
            self.handle_stateful_response(event, user)
            return

        # Кнопки главного меню
        if text == '📢 Акции':
            self.show_promotions(user_id)
        elif text == '📝 Отправить заявку':
            self.start_request(user_id)
        elif text == '❓ Пройти опрос':
            self.start_survey(user_id)
        else:
            self.send_main_menu(user_id)

    # ============= МЕТОДЫ ДЛЯ ОБЫЧНЫХ ПОЛЬЗОВАТЕЛЕЙ =========================
    def send_main_menu(self, user_id):
        msg = (
            "🌟 **Добро пожаловать!** Я помогу вам подобрать услуги для продвижения вашего сайта.\n\n"
            "Выберите действие:\n"
            "─────────────────────\n"
            "📢 **Акции** – посмотреть текущие предложения\n"
            "📝 **Отправить заявку** – оставить контакт для связи\n"
            "❓ **Пройти опрос** – ответить на несколько вопросов, чтобы мы лучше поняли ваши потребности"
        )
        kb = get_main_menu_keyboard()
        self.send_message(user_id, msg, kb)

    def show_promotions(self, user_id):
        promos = get_active_promotions()
        if not promos:
            msg = "📢 На данный момент нет активных акций."
            self.send_message(user_id, msg)
        else:
            lines = ["🎁 **Актуальные акции:**\n"]
            for p in promos:
                lines.append(
                    f"**{p['title']}**\n"
                    f"{p['text']}\n"
                    f"📅 Срок: {p['start_date']} — {p['end_date']}\n"
                    f"{'─'*30}"
                )
            full_msg = '\n'.join(lines)
            self.send_message(user_id, full_msg)
        self.send_main_menu(user_id)

    def start_request(self, user_id):
        """Начинает поэтапный сбор имени, телефона и email."""
        self.user_temp_data[user_id] = {}
        update_user_state(user_id, 'request_name')
        msg = "📝 Введите ваше имя:"
        self.send_message(user_id, msg, get_empty_keyboard())

    def handle_request_response(self, event, user):
        user_id = user['vk_id']
        state = user['current_state']
        text = event.message['text'].strip()

        if state == 'request_name':
            self.user_temp_data[user_id]['name'] = text
            update_user_state(user_id, 'request_phone')
            self.send_message(user_id, "📞 Введите ваш контактный телефон:", get_empty_keyboard())

        elif state == 'request_phone':
            self.user_temp_data[user_id]['phone'] = text
            update_user_state(user_id, 'request_email')
            self.send_message(user_id, "📧 Введите ваш email:", get_empty_keyboard())

        elif state == 'request_email':
            self.user_temp_data[user_id]['email'] = text
            data = self.user_temp_data.pop(user_id, {})
            name = data.get('name', '')
            phone = data.get('phone', '')
            email = data.get('email', '')
            full_request = f"Имя: {name}\nТелефон: {phone}\nEmail: {email}"
            user_info = save_request(user_id, full_request)

            # Уведомление администраторам
            notification = (
                f"📬 **Новая заявка** от пользователя\n"
                f"ID: {user_id}\n"
                f"Имя: {user_info['first_name']} {user_info['last_name']}\n"
                f"{full_request}"
            )
            self.notify_admins(notification)

            # Подтверждение пользователю
            self.send_message(user_id, "✅ Спасибо! Мы свяжемся с вами в ближайшее время.", get_empty_keyboard())
            clear_user_state(user_id)
            self.send_main_menu(user_id)

    def start_survey(self, user_id):
        update_user_state(user_id, 'q1')
        msg = "🔹 **Вопрос 1 из 4**\nЕсть ли у вас сайт?"
        kb = get_keyboard_yes_no()
        self.send_message(user_id, msg, kb)

    def handle_survey_response(self, event, user):
        user_id = user['vk_id']
        state = user['current_state']
        text = event.message['text'].strip()

        if state == 'q1':
            if text not in ['Да', 'Нет']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Есть ли у вас сайт?', text)
            update_user_state(user_id, 'q2')
            msg = "🔹 **Вопрос 2 из 4**\nНужно ли провести аудит сайта?"
            kb = get_keyboard_audit()
            self.send_message(user_id, msg, kb)

        elif state == 'q2':
            if text not in ['Да', 'Нет', 'Уже делали']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Нужно ли провести аудит сайта?', text)
            update_user_state(user_id, 'q3')
            msg = "🔹 **Вопрос 3 из 4**\nТребуется ли переделать контент?"
            kb = get_keyboard_content()
            self.send_message(user_id, msg, kb)

        elif state == 'q3':
            if text not in ['Да', 'Нет', 'Частично']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Требуется ли переделать контент?', text)
            update_user_state(user_id, 'q4')
            msg = "🔹 **Вопрос 4 из 4**\nВам нужна реклама или только аудит?"
            kb = get_keyboard_advertising()
            self.send_message(user_id, msg, kb)

        elif state == 'q4':
            if text not in ['Реклама', 'Аудит', 'Оба варианта']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Вам нужна реклама или только аудит?', text)
            update_user_state(user_id, 'subscribe')
            msg = ("✅ Спасибо за ответы! Мы свяжемся с вами в ближайшее время.\n\n"
                   "Хотите получать информацию о наших акциях?")
            kb = get_keyboard_subscribe()
            self.send_message(user_id, msg, kb)

        elif state == 'subscribe':
            if text not in ['Да', 'Нет']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            subscribed = (text == 'Да')
            set_subscription(user_id, subscribed)

            # Отправляем администраторам результаты опроса
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('''
                SELECT question, answer, answered_at FROM survey_answers
                WHERE user_id = ? ORDER BY answered_at
            ''', (user['id'],))
            answers_rows = cur.fetchall()
            conn.close()
            if answers_rows:
                answers_text = '\n'.join([f"{row['question']}: {row['answer']}" for row in answers_rows])
                admin_msg = (f"📝 **Новый опрос пройден**\n"
                             f"Пользователь: {user['first_name']} {user['last_name']} (id{user_id})\n"
                             f"Ответы:\n{answers_text}")
                self.notify_admins(admin_msg)

            clear_user_state(user_id)
            if subscribed:
                msg = "🔔 Отлично! Вы подписались на акции. Будем присылать самое интересное."
            else:
                msg = "⏸️ Хорошо, если передумаете - всегда можете написать мне."
            self.send_message(user_id, msg, get_empty_keyboard())
            self.send_main_menu(user_id)

    def handle_stateful_response(self, event, user):
        state = user['current_state']
        if state.startswith('q') or state == 'subscribe':
            self.handle_survey_response(event, user)
        elif state in ('request_name', 'request_phone', 'request_email'):
            self.handle_request_response(event, user)
        else:
            logger.warning(f"Неизвестное состояние {state}, сбрасываем")
            clear_user_state(user['vk_id'])
            self.send_main_menu(user['vk_id'])

    # ============= АДМИНИСТРИРОВАНИЕ ========================================
    def process_admin_command(self, event):
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        parts = text.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if user_id in self.admin_states:
            self.handle_admin_state_input(event)
            return

        # Команды для входа в админ-панель (добавлена /adminka)
        if cmd in ('/admin', '/adminka', '/start', '/help'):
            self.show_admin_panel(user_id)
        elif cmd == '/stats':
            self.admin_stats(user_id)
        elif cmd == '/answers':
            self.admin_answers(user_id, args)
        elif cmd == '/export':
            self.admin_export(user_id)
        elif cmd == '/add_promo':
            self.start_add_promo(user_id)
        elif cmd == '/list_promo':
            self.admin_list_promos(user_id)
        elif cmd == '/edit_promo':
            if len(args) < 1:
                self.send_message(user_id, "Использование: /edit_promo <id>")
                return
            try:
                promo_id = int(args[0])
                self.start_edit_promo(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID акции должен быть числом.")
        elif cmd == '/delete_promo':
            if len(args) < 1:
                self.send_message(user_id, "Использование: /delete_promo <id>")
                return
            try:
                promo_id = int(args[0])
                self.start_delete_promo(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID акции должен быть числом.")
        elif cmd == '/send_promo':
            if len(args) < 1:
                self.send_message(user_id, "Использование: /send_promo <id>")
                return
            try:
                promo_id = int(args[0])
                self.start_manual_send(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID акции должен быть числом.")
        elif cmd == '/set_mailing_time':
            if len(args) < 1:
                self.send_message(user_id, "Использование: /set_mailing_time <HH:MM>")
                return
            self.admin_set_mailing_time(user_id, args[0])
        elif cmd == '/reply':
            if len(args) < 2:
                self.send_message(user_id, "Использование: /reply <vk_id> <текст>")
                return
            target_vk_id = args[0]
            reply_text = ' '.join(args[1:])
            self.admin_reply(user_id, target_vk_id, reply_text)
        elif cmd == '/disable':
            self.admin_disable(user_id)
        elif cmd == '/enable':
            self.admin_enable(user_id)
        else:
            self.handle_admin_button(user_id, text)

    def handle_admin_button(self, user_id, button_text):
        if button_text == '📊 Статистика':
            self.admin_stats(user_id)
        elif button_text == '📋 Ответы':
            self.admin_answers(user_id, [])
        elif button_text == '📤 Экспорт данных':
            self.admin_export(user_id)
        elif button_text == '➕ Добавить акцию':
            self.start_add_promo(user_id)
        elif button_text == '📋 Список акций':
            self.admin_list_promos(user_id)
        elif button_text == '✏️ Редактировать акцию':
            self.send_message(user_id, "Введите ID акции для редактирования:")
            self.admin_states[user_id] = {'action': 'edit_promo_input_id', 'step': 0}
        elif button_text == '❌ Удалить акцию':
            self.send_message(user_id, "Введите ID акции для удаления:")
            self.admin_states[user_id] = {'action': 'delete_promo_input_id', 'step': 0}
        elif button_text == '📨 Разослать акцию':
            self.send_message(user_id, "Введите ID акции для ручной рассылки:")
            self.admin_states[user_id] = {'action': 'manual_send_input_id', 'step': 0}
        elif button_text == '⏰ Установить время':
            self.send_message(user_id, "Введите новое время рассылки в формате HH:MM (например, 10:00):")
            self.admin_states[user_id] = {'action': 'set_mailing_time_input', 'step': 0}
        elif button_text == '💬 Ответить пользователю':
            self.send_message(user_id, "Введите ID пользователя VK и текст сообщения через пробел (например, 123456789 Привет!):")
            self.admin_states[user_id] = {'action': 'reply_input', 'step': 0}
        elif button_text == '🚫 Отключить бота':
            self.admin_disable(user_id)
        elif button_text == '✅ Включить бота':
            self.admin_enable(user_id)
        elif button_text == '🔙 Главное меню':
            self.send_main_menu(user_id)
        else:
            self.show_admin_panel(user_id)

    def handle_admin_state_input(self, event):
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        state = self.admin_states.get(user_id)
        if not state:
            return

        action = state['action']

        if action == 'add_promo':
            self.handle_add_promo_input(user_id, text, state)
        elif action == 'edit_promo':
            self.handle_edit_promo_input(user_id, text, state)
        elif action == 'edit_promo_input_id':
            try:
                promo_id = int(text)
                self.start_edit_promo(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID должен быть числом. Попробуйте снова.")
                del self.admin_states[user_id]
                self.show_admin_panel(user_id)
        elif action == 'delete_promo_input_id':
            try:
                promo_id = int(text)
                self.start_delete_promo(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID должен быть числом. Попробуйте снова.")
                del self.admin_states[user_id]
                self.show_admin_panel(user_id)
        elif action == 'manual_send_input_id':
            try:
                promo_id = int(text)
                self.start_manual_send(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID должен быть числом. Попробуйте снова.")
                del self.admin_states[user_id]
                self.show_admin_panel(user_id)
        elif action == 'set_mailing_time_input':
            self.admin_set_mailing_time(user_id, text)
            del self.admin_states[user_id]
            self.show_admin_panel(user_id)
        elif action == 'reply_input':
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                self.send_message(user_id, "Нужно указать ID и текст. Пример: 123456789 Привет!")
                return
            target_id, reply_text = parts[0], parts[1]
            self.admin_reply(user_id, target_id, reply_text)
            del self.admin_states[user_id]
            self.show_admin_panel(user_id)
        else:
            logger.warning(f"Неизвестное действие админа {action}")
            del self.admin_states[user_id]
            self.show_admin_panel(user_id)

    def show_admin_panel(self, user_id):
        msg = (
            "🔧 **АДМИНИСТРИРОВАНИЕ**\n"
            "══════════════════════════════\n"
            "👤 **Пользовательские функции**\n"
            "(доступны также обычным пользователям)\n"
            "────────────────────────\n"
            "📢 Акции\n"
            "📝 Отправить заявку\n"
            "❓ Пройти опрос\n\n"
            "⚙️ **Управление ботом**\n"
            "────────────────────────\n"
            "📊 Статистика\n"
            "📋 Ответы\n"
            "📤 Экспорт данных\n"
            "➕ Добавить акцию\n"
            "📋 Список акций\n"
            "✏️ Редактировать акцию\n"
            "❌ Удалить акцию\n"
            "📨 Разослать акцию вручную\n"
            "⏰ Установить время рассылки\n"
            "💬 Ответить пользователю\n"
            "🚫 Отключить бота\n"
            "✅ Включить бота\n\n"
            "Используйте кнопки ниже для выбора действия."
        )
        kb = get_admin_keyboard()
        self.send_message(user_id, msg, kb)

    def admin_stats(self, user_id):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) as total FROM users')
        total = cur.fetchone()['total']
        cur.execute('SELECT COUNT(*) as total FROM users WHERE subscribed = 1')
        subscribed = cur.fetchone()['total']
        cur.execute('SELECT COUNT(DISTINCT user_id) as total FROM survey_answers')
        surveyed = cur.fetchone()['total']
        conn.close()
        msg = (f"📊 **Статистика**\n"
               f"Всего пользователей: {total}\n"
               f"Прошли опрос: {surveyed}\n"
               f"Подписаны на акции: {subscribed}")
        self.send_message(user_id, msg)
        log_admin_action(user_id, 'stats', msg)
        self.show_admin_panel(user_id)

    def admin_answers(self, user_id, args):
        date_filter = None
        if args and args[0]:
            date_filter = args[0]
        conn = get_db_connection()
        cur = conn.cursor()
        if date_filter:
            cur.execute('''
                SELECT u.vk_id, u.first_name, u.last_name, sa.question, sa.answer, sa.answered_at
                FROM survey_answers sa
                JOIN users u ON sa.user_id = u.id
                WHERE DATE(sa.answered_at) = ?
                ORDER BY sa.answered_at DESC
            ''', (date_filter,))
        else:
            cur.execute('''
                SELECT u.vk_id, u.first_name, u.last_name, sa.question, sa.answer, sa.answered_at
                FROM survey_answers sa
                JOIN users u ON sa.user_id = u.id
                ORDER BY sa.answered_at DESC
                LIMIT 50
            ''')
        rows = cur.fetchall()
        conn.close()
        if not rows:
            self.send_message(user_id, "Нет ответов за указанный период.")
        else:
            msg_lines = ["📝 **Ответы пользователей**:"]
            for row in rows:
                line = (f"[{row['answered_at']}] {row['first_name']} {row['last_name']} (id{row['vk_id']}):\n"
                        f"  {row['question']} → {row['answer']}")
                msg_lines.append(line)
            chunk_size = 5
            for i in range(0, len(msg_lines), chunk_size):
                chunk = '\n\n'.join(msg_lines[i:i+chunk_size])
                self.send_message(user_id, chunk)
        log_admin_action(user_id, 'answers', f"date={date_filter if date_filter else 'all'}")
        self.show_admin_panel(user_id)

    def admin_export(self, user_id):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            SELECT u.vk_id, u.first_name, u.last_name, u.first_seen, u.last_interaction, u.subscribed,
                   sa.question, sa.answer, sa.answered_at
            FROM users u
            LEFT JOIN survey_answers sa ON u.id = sa.user_id
            ORDER BY u.vk_id, sa.answered_at
        ''')
        rows = cur.fetchall()
        conn.close()
        if not rows:
            self.send_message(user_id, "Нет данных для экспорта.")
            self.show_admin_panel(user_id)
            return

        output = io.StringIO()
        writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['vk_id', 'first_name', 'last_name', 'first_seen', 'last_interaction', 'subscribed',
                         'question', 'answer', 'answered_at'])
        for row in rows:
            writer.writerow([row['vk_id'], row['first_name'], row['last_name'], row['first_seen'],
                             row['last_interaction'], row['subscribed'], row['question'], row['answer'],
                             row['answered_at']])
        csv_data = output.getvalue().encode('utf-8-sig')
        output.close()

        try:
            doc = self.upload.document_message(
                peer_id=user_id,
                file=io.BytesIO(csv_data),
                title='survey_export.csv'
            )
            attachment = f"doc{doc['doc']['owner_id']}_{doc['doc']['id']}"
            self.send_message(user_id, "Файл с данными:", attachment=attachment)
            log_admin_action(user_id, 'export', 'CSV exported')
        except Exception as e:
            logger.error(f"Ошибка загрузки документа: {e}")
            self.send_message(user_id, f"Не удалось загрузить файл. Ошибка: {e}")
        self.show_admin_panel(user_id)

    # ---------- Управление акциями (админ) ----------
    def start_add_promo(self, user_id):
        self.admin_states[user_id] = {'action': 'add_promo', 'step': 1, 'data': {}}
        self.send_message(user_id, "Введите **название** акции:")

    def handle_add_promo_input(self, user_id, text, state):
        step = state['step']
        data = state['data']

        if step == 1:
            data['title'] = text
            state['step'] = 2
            self.send_message(user_id, "Введите **текст** акции (описание, условия):")
        elif step == 2:
            data['text'] = text
            state['step'] = 3
            self.send_message(user_id, "Введите **дату начала** в формате ГГГГ-ММ-ДД (например, 2025-04-01):")
        elif step == 3:
            try:
                datetime.strptime(text, '%Y-%m-%d')
                data['start_date'] = text
                state['step'] = 4
                self.send_message(user_id, "Введите **дату окончания** в формате ГГГГ-ММ-ДД:")
            except ValueError:
                self.send_message(user_id, "Неверный формат даты. Повторите ввод (ГГГГ-ММ-ДД):")
                return
        elif step == 4:
            try:
                datetime.strptime(text, '%Y-%m-%d')
                data['end_date'] = text
                state['step'] = 5
                self.send_message(user_id, "Введите **периодичность** (daily или weekly):")
            except ValueError:
                self.send_message(user_id, "Неверный формат даты. Повторите ввод (ГГГГ-ММ-ДД):")
                return
        elif step == 5:
            if text.lower() not in ['daily', 'weekly']:
                self.send_message(user_id, "Периодичность должна быть 'daily' или 'weekly'. Повторите:")
                return
            data['periodicity'] = text.lower()
            promo_id = add_promotion(data['title'], data['text'], data['start_date'], data['end_date'], data['periodicity'])
            self.send_message(user_id, f"✅ Акция успешно добавлена! ID: {promo_id}")
            log_admin_action(user_id, 'add_promo', f"promo_id={promo_id}, title={data['title']}")
            del self.admin_states[user_id]
            self.show_admin_panel(user_id)

    def start_edit_promo(self, user_id, promo_id):
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            self.show_admin_panel(user_id)
            return
        self.admin_states[user_id] = {'action': 'edit_promo', 'step': 1, 'data': promo, 'promo_id': promo_id}
        self.send_message(user_id, f"Редактирование акции ID {promo_id}\n"
                                   f"Текущее название: {promo['title']}\n"
                                   "Введите новое название (или отправьте 'пропустить'):")

    def handle_edit_promo_input(self, user_id, text, state):
        step = state['step']
        promo_id = state['promo_id']
        current = state['data']

        if step == 1:
            if text.lower() != 'пропустить':
                update_promotion(promo_id, title=text)
                current['title'] = text
            state['step'] = 2
            self.send_message(user_id, f"Текущий текст: {current['text']}\nВведите новый текст акции (или 'пропустить'):")
        elif step == 2:
            if text.lower() != 'пропустить':
                update_promotion(promo_id, text=text)
                current['text'] = text
            state['step'] = 3
            self.send_message(user_id, f"Текущая дата начала: {current['start_date']}\nВведите новую дату начала (ГГГГ-ММ-ДД) или 'пропустить':")
        elif step == 3:
            if text.lower() != 'пропустить':
                try:
                    datetime.strptime(text, '%Y-%m-%d')
                    update_promotion(promo_id, start_date=text)
                    current['start_date'] = text
                except ValueError:
                    self.send_message(user_id, "Неверный формат даты. Поле не обновлено.")
            state['step'] = 4
            self.send_message(user_id, f"Текущая дата окончания: {current['end_date']}\nВведите новую дату окончания (ГГГГ-ММ-ДД) или 'пропустить':")
        elif step == 4:
            if text.lower() != 'пропустить':
                try:
                    datetime.strptime(text, '%Y-%m-%d')
                    update_promotion(promo_id, end_date=text)
                    current['end_date'] = text
                except ValueError:
                    self.send_message(user_id, "Неверный формат даты. Поле не обновлено.")
            state['step'] = 5
            self.send_message(user_id, f"Текущая периодичность: {current['periodicity']}\nВведите новую периодичность (daily/weekly) или 'пропустить':")
        elif step == 5:
            if text.lower() != 'пропустить' and text.lower() in ['daily', 'weekly']:
                update_promotion(promo_id, periodicity=text.lower())
                current['periodicity'] = text.lower()
            elif text.lower() != 'пропустить':
                self.send_message(user_id, "Неверное значение. Поле не обновлено.")
            self.send_message(user_id, f"✅ Акция ID {promo_id} успешно отредактирована.")
            log_admin_action(user_id, 'edit_promo', f"promo_id={promo_id}")
            del self.admin_states[user_id]
            self.show_admin_panel(user_id)

    def start_delete_promo(self, user_id, promo_id):
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            self.show_admin_panel(user_id)
            return
        delete_promotion(promo_id)
        self.send_message(user_id, f"✅ Акция ID {promo_id} удалена.")
        log_admin_action(user_id, 'delete_promo', f"promo_id={promo_id}")
        self.show_admin_panel(user_id)

    def start_manual_send(self, user_id, promo_id):
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            self.show_admin_panel(user_id)
            return
        subscribers = get_subscribers()
        if not subscribers:
            self.send_message(user_id, "Нет подписчиков для рассылки.")
            self.show_admin_panel(user_id)
            return
        sent_count = 0
        for sub in subscribers:
            if promo['periodicity'] == 'daily' and was_sent_today(promo_id, sub['id']):
                continue
            if promo['periodicity'] == 'weekly' and was_sent_this_week(promo_id, sub['id']):
                continue
            msg = f"🎉 **Акция: {promo['title']}**\n\n{promo['text']}"
            self.send_message(sub['vk_id'], msg)
            log_sent(promo_id, sub['id'])
            sent_count += 1
            time.sleep(0.3)
        update_promotion(promo_id, last_sent=datetime.now().isoformat())
        self.send_message(user_id, f"✅ Рассылка завершена. Отправлено {sent_count} подписчикам.")
        log_admin_action(user_id, 'manual_send', f"promo_id={promo_id}, sent={sent_count}")
        self.show_admin_panel(user_id)

    def admin_list_promos(self, user_id):
        promos = get_all_promotions()
        if not promos:
            self.send_message(user_id, "Нет акций.")
        else:
            msg_lines = ["📋 **Список акций**:"]
            for p in promos:
                status = "🟢 активна" if p['is_active'] else "🔴 неактивна"
                msg_lines.append(f"ID {p['id']}: {p['title']} ({p['start_date']} - {p['end_date']}) - {status}, период: {p['periodicity']}")
            self.send_message(user_id, '\n'.join(msg_lines))
        log_admin_action(user_id, 'list_promo')
        self.show_admin_panel(user_id)

    def admin_set_mailing_time(self, user_id, time_str):
        try:
            datetime.strptime(time_str, '%H:%M')
            global DEFAULT_MAILING_TIME
            DEFAULT_MAILING_TIME = time_str
            self.send_message(user_id, f"⏰ Время автоматической рассылки установлено на {time_str}")
            log_admin_action(user_id, 'set_mailing_time', time_str)
        except ValueError:
            self.send_message(user_id, "Неверный формат времени. Используйте HH:MM (например, 10:00)")
        self.show_admin_panel(user_id)

    def admin_reply(self, admin_id, target_vk_id, text):
        try:
            target_vk_id = int(target_vk_id)
            self.send_message(target_vk_id, f"📨 **Сообщение от администратора:**\n{text}")
            self.send_message(admin_id, f"✅ Сообщение отправлено пользователю {target_vk_id}")
            log_admin_action(admin_id, 'reply', f"to={target_vk_id}, text={text[:50]}...")
        except ValueError:
            self.send_message(admin_id, "Неверный ID пользователя. Должен быть числом.")
        except Exception as e:
            self.send_message(admin_id, f"Ошибка при отправке: {e}")
        self.show_admin_panel(admin_id)

    def admin_disable(self, user_id):
        self.enabled = False
        self.disabled_until = None
        self.send_message(user_id, "🚫 Бот отключен. Для включения используйте /enable или кнопку '✅ Включить бота'.")
        log_admin_action(user_id, 'disable')
        self.show_admin_panel(user_id)

    def admin_enable(self, user_id):
        self.enabled = True
        self.disabled_until = None
        self.send_message(user_id, "✅ Бот включен.")
        log_admin_action(user_id, 'enable')
        self.show_admin_panel(user_id)

    # ============= АВТОМАТИЧЕСКАЯ РАССЫЛКА ===================================
    def mailing_worker(self):
        logger.info("Поток рассылки запущен.")
        while True:
            try:
                now = datetime.now()
                target_time = datetime.strptime(DEFAULT_MAILING_TIME, '%H:%M').time()
                if now.hour == target_time.hour and now.minute == target_time.minute:
                    self.perform_mailing()
                    time.sleep(61)
                else:
                    time.sleep(MAILING_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Ошибка в потоке рассылки: {e}")
                time.sleep(60)

    def perform_mailing(self):
        logger.info("Запуск автоматической рассылки.")
        if self.is_bot_disabled():
            logger.info("Бот отключен, рассылка не производится.")
            return

        active_promos = get_active_promotions()
        if not active_promos:
            logger.info("Нет активных акций для рассылки.")
            return

        subscribers = get_subscribers()
        if not subscribers:
            logger.info("Нет подписчиков.")
            return

        for sub in subscribers:
            sent = False
            for promo in active_promos:
                promo_id = promo['id']
                if promo['periodicity'] == 'daily' and was_sent_today(promo_id, sub['id']):
                    continue
                if promo['periodicity'] == 'weekly' and was_sent_this_week(promo_id, sub['id']):
                    continue
                msg = f"🎉 **Акция: {promo['title']}**\n\n{promo['text']}"
                self.send_message(sub['vk_id'], msg)
                log_sent(promo_id, sub['id'])
                sent = True
                update_promotion(promo_id, last_sent=datetime.now().isoformat())
                time.sleep(0.3)
                break
            if not sent:
                logger.debug(f"Пользователь {sub['vk_id']} уже получил все акции в этом периоде.")

        logger.info("Автоматическая рассылка завершена.")

# ================== ЗАПУСК БОТА ===========================================
def main():
    init_db()
    bot = VKBot(GROUP_ID, API_TOKEN)

    mailing_thread = threading.Thread(target=bot.mailing_worker, daemon=True)
    mailing_thread.start()

    logger.info("Бот запущен и ожидает сообщения...")
    while True:
        try:
            for event in bot.longpoll.listen():
                bot.handle_event(event)
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
