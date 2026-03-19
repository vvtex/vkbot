#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ВКонтакте бот для сбора потребностей в услугах по продвижению сайтов.
Версия 1.0
Разработан в соответствии с ТЗ.
"""

import sqlite3
import logging
import threading
import time
import csv
import io
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.utils import get_random_id

# ================== КОНФИГУРАЦИЯ (замените на свои данные) ==================
VK_GROUP_ID = '123456789'                # ID группы ВК
VK_API_TOKEN = 'vk1.a.xxxxxxxxxxxx'      # Токен сообщества (ключ доступа)
ADMIN_IDS = [123456, 789012]              # ID администраторов ВК (можно добавить позже через БД)

# Настройки базы данных
DB_FILE = 'bot_database.db'

# Настройки рассылки (по умолчанию)
DEFAULT_MAILING_TIME = "10:00"            # Время ежедневной рассылки
MAILING_CHECK_INTERVAL = 60               # Интервал проверки расписания (сек)

# Флаг для отключения бота (команда /disable)
BOT_ENABLED = True
BOT_DISABLED_UNTIL = None                 # Если не None, время, до которого бот отключен

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('VK_bot')
# ===========================================================================

# ================== РАБОТА С БАЗОЙ ДАННЫХ ==================================
def init_db():
    """Создание таблиц, если их нет."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Таблица пользователей
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

    # Таблица ответов на опрос
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

    # Таблица администраторов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vk_id INTEGER UNIQUE NOT NULL
        )
    ''')

    # Таблица акций
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

    # Таблица лога отправок
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

    # Таблица лога действий администраторов
    cur.execute('''
        CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_vk_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            details TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Добавляем администраторов из списка ADMIN_IDS, если их нет
    for admin_id in ADMIN_IDS:
        cur.execute('INSERT OR IGNORE INTO admins (vk_id) VALUES (?)', (admin_id,))

    conn.commit()
    conn.close()
    logger.info("База данных инициализирована.")

def get_db_connection():
    """Возвращает соединение с БД с настроенным row_factory."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# Функции для работы с пользователями
def get_or_create_user(vk_id, first_name='', last_name=''):
    """Получить пользователя по vk_id, если нет — создать."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE vk_id = ?', (vk_id,))
    user = cur.fetchone()
    if user:
        # Обновляем время последнего взаимодействия
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
            'subscribed': False,
            'current_state': None
        }

def update_user_state(vk_id, state):
    """Обновить состояние пользователя."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET current_state = ?, last_interaction = CURRENT_TIMESTAMP WHERE vk_id = ?', (state, vk_id))
    conn.commit()
    conn.close()

def save_answer(vk_id, question, answer):
    """Сохранить ответ пользователя на вопрос."""
    user = get_or_create_user(vk_id)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO survey_answers (user_id, question, answer) VALUES (?, ?, ?)',
                (user['id'], question, answer))
    conn.commit()
    conn.close()

def set_subscription(vk_id, subscribed):
    """Установить подписку на рассылку."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET subscribed = ? WHERE vk_id = ?', (1 if subscribed else 0, vk_id))
    conn.commit()
    conn.close()

def is_admin(vk_id):
    """Проверяет, является ли пользователь администратором."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM admins WHERE vk_id = ?', (vk_id,))
    result = cur.fetchone() is not None
    conn.close()
    return result

def log_admin_action(admin_vk_id, action, details=''):
    """Записать действие администратора в лог."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO admin_log (admin_vk_id, action, details) VALUES (?, ?, ?)',
                (admin_vk_id, action, details))
    conn.commit()
    conn.close()

# Функции для работы с акциями
def add_promotion(title, text, start_date, end_date, periodicity='daily'):
    """Добавить новую акцию."""
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
    """Вернуть список активных акций на текущий момент."""
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

def get_promotion(promo_id):
    """Получить акцию по ID."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM promotions WHERE id = ?', (promo_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def update_promotion(promo_id, **kwargs):
    """Обновить поля акции."""
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
    """Удалить акцию."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM promotions WHERE id = ?', (promo_id,))
    conn.commit()
    conn.close()

def get_subscribers():
    """Получить список подписанных пользователей (id в БД и vk_id)."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT id, vk_id FROM users WHERE subscribed = 1')
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def log_sent(promo_id, user_id):
    """Записать факт отправки акции пользователю."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO sent_log (promotion_id, user_id) VALUES (?, ?)', (promo_id, user_id))
    conn.commit()
    conn.close()

def was_sent_today(promo_id, user_id):
    """Проверяет, отправлялась ли данная акция пользователю сегодня."""
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
    """Проверяет, отправлялась ли акция на этой неделе (для weekly)."""
    now = datetime.now()
    week_start = now - timedelta(days=now.weekday())  # понедельник
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
def get_keyboard_start():
    """Клавиатура для первого сообщения."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Начать', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Не сейчас', color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()

def get_keyboard_yes_no():
    """Клавиатура Да/Нет."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()

def get_keyboard_audit():
    """Клавиатура для вопроса про аудит (Да/Нет/Уже делали)."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button('Уже делали', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_keyboard_content():
    """Клавиатура для вопроса про контент (Да/Нет/Частично)."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button('Частично', color=VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def get_keyboard_advertising():
    """Клавиатура для вопроса про рекламу/аудит."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Реклама', color=VkKeyboardColor.PRIMARY)
    kb.add_button('Аудит', color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button('Оба варианта', color=VkKeyboardColor.POSITIVE)
    return kb.get_keyboard()

def get_keyboard_subscribe():
    """Клавиатура для вопроса о подписке."""
    kb = VkKeyboard(one_time=True)
    kb.add_button('Да', color=VkKeyboardColor.POSITIVE)
    kb.add_button('Нет', color=VkKeyboardColor.NEGATIVE)
    return kb.get_keyboard()

def get_empty_keyboard():
    """Пустая клавиатура (убрать кнопки)."""
    return VkKeyboard.get_empty_keyboard()

# ================== ОСНОВНОЙ КЛАСС БОТА ====================================
class VKBot:
    def __init__(self, group_id, token):
        self.group_id = group_id
        self.token = token
        self.vk_session = vk_api.VkApi(token=token)
        self.longpoll = VkBotLongPoll(self.vk_session, group_id)
        self.vk = self.vk_session.get_api()
        self.enabled = BOT_ENABLED
        self.disabled_until = BOT_DISABLED_UNTIL

        # Состояния админов для создания/редактирования акций
        self.admin_states = {}  # vk_id -> {'action': 'add_promo', 'step': 0, 'data': {}}

    def send_message(self, user_id, message, keyboard=None, attachment=None):
        """Универсальный метод отправки сообщения с обработкой ошибок."""
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

    def is_bot_disabled(self):
        """Проверяет, отключен ли бот."""
        if not self.enabled:
            return True
        if self.disabled_until and datetime.now() < self.disabled_until:
            return True
        return False

    def handle_event(self, event):
        """Обрабатывает входящее событие (сообщение)."""
        if self.is_bot_disabled():
            # Если бот отключен, игнорируем все, кроме команд админа на включение
            if event.from_user:
                user_id = event.message['from_id']
                if is_admin(user_id) and event.message['text'].startswith('/enable'):
                    self.process_admin_command(event)
            return

        if event.type == VkBotEventType.MESSAGE_NEW and event.from_user:
            self.handle_message(event)

    def handle_message(self, event):
        """Обрабатывает текстовое сообщение от пользователя."""
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        # Получаем информацию о пользователе (имя, фамилию) из события, если есть
        # В VkBotEventType.MESSAGE_NEW может быть информация о пользователе
        # Для простоты можем запросить отдельно, но пока используем то, что есть
        first_name = event.message.get('first_name', '')
        last_name = event.message.get('last_name', '')
        user = get_or_create_user(user_id, first_name, last_name)

        # Проверяем, является ли пользователь администратором и начинается ли сообщение с '/'
        if text.startswith('/') and is_admin(user_id):
            self.process_admin_command(event)
            return

        # Если пользователь в процессе опроса (состояние не None)
        if user['current_state']:
            self.handle_survey_response(event, user)
            return

        # Обычное сообщение (не команда и не в опросе)
        self.send_welcome(user_id)

    def send_welcome(self, user_id):
        """Отправляет приветственное сообщение с предложением опроса."""
        msg = ("Здравствуйте! Я помогу подобрать услуги для продвижения вашего сайта.\n"
               "Пройдём небольшой опрос?")
        kb = get_keyboard_start()
        self.send_message(user_id, msg, kb)

    def start_survey(self, user_id):
        """Начинает опрос: устанавливает состояние q1 и задаёт первый вопрос."""
        update_user_state(user_id, 'q1')
        msg = "Вопрос 1: Есть ли у вас сайт?"
        kb = get_keyboard_yes_no()
        self.send_message(user_id, msg, kb)

    def handle_survey_response(self, event, user):
        """Обрабатывает ответ на текущий вопрос опроса."""
        user_id = user['vk_id']
        state = user['current_state']
        text = event.message['text'].strip()

        # Сохраняем ответ в зависимости от состояния
        if state == 'q1':
            # Вопрос 1: Есть ли сайт? Да/Нет
            if text not in ['Да', 'Нет']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Есть ли у вас сайт?', text)
            # Переход к следующему вопросу
            update_user_state(user_id, 'q2')
            msg = "Вопрос 2: Нужно ли провести аудит сайта?"
            kb = get_keyboard_audit()
            self.send_message(user_id, msg, kb)

        elif state == 'q2':
            if text not in ['Да', 'Нет', 'Уже делали']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Нужно ли провести аудит сайта?', text)
            update_user_state(user_id, 'q3')
            msg = "Вопрос 3: Требуется ли переделать контент?"
            kb = get_keyboard_content()
            self.send_message(user_id, msg, kb)

        elif state == 'q3':
            if text not in ['Да', 'Нет', 'Частично']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Требуется ли переделать контент?', text)
            update_user_state(user_id, 'q4')
            msg = "Вопрос 4: Вам нужна реклама или только аудит?"
            kb = get_keyboard_advertising()
            self.send_message(user_id, msg, kb)

        elif state == 'q4':
            if text not in ['Реклама', 'Аудит', 'Оба варианта']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            save_answer(user_id, 'Вам нужна реклама или только аудит?', text)
            # Опрос завершён, спрашиваем про подписку
            update_user_state(user_id, 'subscribe')
            msg = ("Спасибо за ответы! Мы свяжемся с вами в ближайшее время.\n"
                   "Хотите получать информацию о наших акциях?")
            kb = get_keyboard_subscribe()
            self.send_message(user_id, msg, kb)

        elif state == 'subscribe':
            if text not in ['Да', 'Нет']:
                self.send_message(user_id, "Пожалуйста, выберите один из вариантов на кнопках.")
                return
            subscribed = (text == 'Да')
            set_subscription(user_id, subscribed)
            update_user_state(user_id, None)  # завершаем опрос
            if subscribed:
                msg = "Отлично! Вы подписались на акции. Будем присылать самое интересное."
            else:
                msg = "Хорошо, если передумаете - всегда можете написать мне."
            self.send_message(user_id, msg, get_empty_keyboard())
            # Дополнительно можно записать в лог админа, но это не требуется

        else:
            # Неизвестное состояние - сброс
            logger.warning(f"Неизвестное состояние {state} для пользователя {user_id}, сбрасываем.")
            update_user_state(user_id, None)
            self.send_welcome(user_id)

    # ============= АДМИНИСТРИРОВАНИЕ ========================================
    def process_admin_command(self, event):
        """Обрабатывает команды администратора."""
        user_id = event.message['from_id']
        text = event.message['text'].strip()

        # Разбиваем команду и аргументы
        parts = text.split()
        cmd = parts[0].lower()
        args = parts[1:]

        # Проверяем, не находится ли админ в процессе ввода данных (добавление акции и т.п.)
        if user_id in self.admin_states:
            self.handle_admin_state_input(event)
            return

        # Обработка команд
        if cmd == '/start' or cmd == '/help':
            self.send_admin_help(user_id)

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
                self.admin_delete_promo(user_id, promo_id)
            except ValueError:
                self.send_message(user_id, "ID акции должен быть числом.")

        elif cmd == '/send_promo':
            if len(args) < 1:
                self.send_message(user_id, "Использование: /send_promo <id>")
                return
            try:
                promo_id = int(args[0])
                self.admin_send_promo_manual(user_id, promo_id)
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
            self.send_message(user_id, "Неизвестная команда. Введите /help для списка команд.")

    def send_admin_help(self, user_id):
        """Отправляет справку по командам администратора."""
        help_text = (
            "🔧 **Команды администратора**\n"
            "/stats - статистика пользователей\n"
            "/answers [дата] - ответы пользователей (можно указать дату ГГГГ-ММ-ДД)\n"
            "/export - экспорт данных в CSV\n"
            "/add_promo - добавить акцию\n"
            "/list_promo - список акций\n"
            "/edit_promo <id> - редактировать акцию\n"
            "/delete_promo <id> - удалить акцию\n"
            "/send_promo <id> - ручная рассылка акции\n"
            "/set_mailing_time <HH:MM> - установить время рассылки\n"
            "/reply <vk_id> <текст> - ответить пользователю\n"
            "/disable - временно отключить бота\n"
            "/enable - включить бота"
        )
        self.send_message(user_id, help_text)

    def admin_stats(self, user_id):
        """Выводит статистику."""
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

    def admin_answers(self, user_id, args):
        """Показывает ответы пользователей (с возможностью фильтра по дате)."""
        date_filter = None
        if args:
            date_filter = args[0]  # ожидаем YYYY-MM-DD
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
            return
        # Формируем сообщение частями, т.к. может быть много данных
        msg_lines = ["📝 **Ответы пользователей**:"]
        for row in rows:
            line = (f"[{row['answered_at']}] {row['first_name']} {row['last_name']} (id{row['vk_id']}):\n"
                    f"  {row['question']} → {row['answer']}")
            msg_lines.append(line)
        # Отправляем по частям, не более 4-5 ответов за раз из-за лимита длины
        chunk_size = 5
        for i in range(0, len(msg_lines), chunk_size):
            chunk = '\n\n'.join(msg_lines[i:i+chunk_size])
            self.send_message(user_id, chunk)
        log_admin_action(user_id, 'answers', f"date={date_filter if date_filter else 'all'}")

    def admin_export(self, user_id):
        """Экспорт данных в CSV и отправка файлом."""
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
            return
        # Создаем CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['vk_id', 'first_name', 'last_name', 'first_seen', 'last_interaction', 'subscribed',
                         'question', 'answer', 'answered_at'])
        for row in rows:
            writer.writerow([row['vk_id'], row['first_name'], row['last_name'], row['first_seen'],
                             row['last_interaction'], row['subscribed'], row['question'], row['answer'],
                             row['answered_at']])
        csv_data = output.getvalue().encode('utf-8-sig')
        output.close()
        # Отправляем файл
        self.upload_and_send_document(user_id, csv_data, 'survey_export.csv')
        log_admin_action(user_id, 'export', 'CSV exported')

    def upload_and_send_document(self, user_id, file_data, filename):
        """Загружает документ на сервер ВК и отправляет пользователю."""
        try:
            # Получаем адрес сервера для загрузки
            upload_server = self.vk.docs.getMessagesUploadServer(type='doc', peer_id=user_id)['upload_url']
            # Загружаем файл
            with io.BytesIO(file_data) as f:
                response = vk_api.uploader.upload(upload_server, {'file': (filename, f)})
            # Сохраняем документ
            doc = self.vk.docs.save(file=response['file'], title=filename)['doc']
            attachment = f"doc{doc['owner_id']}_{doc['id']}"
            self.send_message(user_id, "Файл с данными:", attachment=attachment)
        except Exception as e:
            logger.error(f"Ошибка загрузки документа: {e}")
            self.send_message(user_id, "Не удалось загрузить файл. Ошибка: " + str(e))

    # ---------- Управление акциями (админ) ----------
    def start_add_promo(self, user_id):
        """Начинает процесс добавления акции: запрашивает название."""
        self.admin_states[user_id] = {'action': 'add_promo', 'step': 1, 'data': {}}
        self.send_message(user_id, "Введите **название** акции:")

    def start_edit_promo(self, user_id, promo_id):
        """Начинает процесс редактирования акции."""
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            return
        self.admin_states[user_id] = {'action': 'edit_promo', 'step': 1, 'data': promo, 'promo_id': promo_id}
        # Показываем текущие данные и предлагаем ввести новое название (или оставить)
        self.send_message(user_id, f"Редактирование акции ID {promo_id}\n"
                                   f"Текущее название: {promo['title']}\n"
                                   "Введите новое название (или отправьте 'пропустить'):")

    def handle_admin_state_input(self, event):
        """Обрабатывает ввод данных от администратора в процессе создания/редактирования."""
        user_id = event.message['from_id']
        text = event.message['text'].strip()
        state = self.admin_states.get(user_id)
        if not state:
            return

        if state['action'] == 'add_promo':
            self.handle_add_promo_input(user_id, text, state)
        elif state['action'] == 'edit_promo':
            self.handle_edit_promo_input(user_id, text, state)

    def handle_add_promo_input(self, user_id, text, state):
        """Пошаговый сбор данных для новой акции."""
        step = state['step']
        data = state['data']

        if step == 1:
            # Получили название
            data['title'] = text
            state['step'] = 2
            self.send_message(user_id, "Введите **текст** акции (описание, условия):")

        elif step == 2:
            data['text'] = text
            state['step'] = 3
            self.send_message(user_id, "Введите **дату начала** в формате ГГГГ-ММ-ДД (например, 2025-04-01):")

        elif step == 3:
            # Проверка формата даты
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
            # Сохраняем акцию
            promo_id = add_promotion(data['title'], data['text'], data['start_date'], data['end_date'], data['periodicity'])
            self.send_message(user_id, f"✅ Акция успешно добавлена! ID: {promo_id}")
            log_admin_action(user_id, 'add_promo', f"promo_id={promo_id}, title={data['title']}")
            # Удаляем состояние
            del self.admin_states[user_id]

    def handle_edit_promo_input(self, user_id, text, state):
        """Редактирование полей акции с возможностью пропустить."""
        step = state['step']
        promo_id = state['promo_id']
        current = state['data']

        if step == 1:  # название
            if text.lower() != 'пропустить':
                update_promotion(promo_id, title=text)
                current['title'] = text
            state['step'] = 2
            self.send_message(user_id, f"Текущий текст: {current['text']}\nВведите новый текст акции (или 'пропустить'):")

        elif step == 2:  # текст
            if text.lower() != 'пропустить':
                update_promotion(promo_id, text=text)
                current['text'] = text
            state['step'] = 3
            self.send_message(user_id, f"Текущая дата начала: {current['start_date']}\nВведите новую дату начала (ГГГГ-ММ-ДД) или 'пропустить':")

        elif step == 3:  # дата начала
            if text.lower() != 'пропустить':
                try:
                    datetime.strptime(text, '%Y-%m-%d')
                    update_promotion(promo_id, start_date=text)
                    current['start_date'] = text
                except ValueError:
                    self.send_message(user_id, "Неверный формат даты. Поле не обновлено.")
            state['step'] = 4
            self.send_message(user_id, f"Текущая дата окончания: {current['end_date']}\nВведите новую дату окончания (ГГГГ-ММ-ДД) или 'пропустить':")

        elif step == 4:  # дата окончания
            if text.lower() != 'пропустить':
                try:
                    datetime.strptime(text, '%Y-%m-%d')
                    update_promotion(promo_id, end_date=text)
                    current['end_date'] = text
                except ValueError:
                    self.send_message(user_id, "Неверный формат даты. Поле не обновлено.")
            state['step'] = 5
            self.send_message(user_id, f"Текущая периодичность: {current['periodicity']}\nВведите новую периодичность (daily/weekly) или 'пропустить':")

        elif step == 5:  # периодичность
            if text.lower() != 'пропустить' and text.lower() in ['daily', 'weekly']:
                update_promotion(promo_id, periodicity=text.lower())
                current['periodicity'] = text.lower()
            elif text.lower() != 'пропустить':
                self.send_message(user_id, "Неверное значение. Поле не обновлено.")
            self.send_message(user_id, f"✅ Акция ID {promo_id} успешно отредактирована.")
            log_admin_action(user_id, 'edit_promo', f"promo_id={promo_id}")
            del self.admin_states[user_id]

    def admin_list_promos(self, user_id):
        """Выводит список всех акций."""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT id, title, start_date, end_date, is_active, periodicity FROM promotions ORDER BY created_at DESC')
        rows = cur.fetchall()
        conn.close()
        if not rows:
            self.send_message(user_id, "Нет акций.")
            return
        msg_lines = ["📋 **Список акций**:"]
        for row in rows:
            status = "🟢 активна" if row['is_active'] else "🔴 неактивна"
            msg_lines.append(f"ID {row['id']}: {row['title']} ({row['start_date']} - {row['end_date']}) - {status}, период: {row['periodicity']}")
        self.send_message(user_id, '\n'.join(msg_lines))

    def admin_delete_promo(self, user_id, promo_id):
        """Удаляет акцию."""
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            return
        delete_promotion(promo_id)
        self.send_message(user_id, f"✅ Акция ID {promo_id} удалена.")
        log_admin_action(user_id, 'delete_promo', f"promo_id={promo_id}")

    def admin_send_promo_manual(self, user_id, promo_id):
        """Ручная рассылка акции всем подписчикам."""
        promo = get_promotion(promo_id)
        if not promo:
            self.send_message(user_id, f"Акция с ID {promo_id} не найдена.")
            return
        subscribers = get_subscribers()
        if not subscribers:
            self.send_message(user_id, "Нет подписчиков для рассылки.")
            return
        sent_count = 0
        for sub in subscribers:
            # Проверяем, отправлялась ли уже сегодня (если daily) или на неделе (weekly)
            if promo['periodicity'] == 'daily' and was_sent_today(promo_id, sub['id']):
                continue
            if promo['periodicity'] == 'weekly' and was_sent_this_week(promo_id, sub['id']):
                continue
            # Отправляем
            msg = f"🎉 **Акция: {promo['title']}**\n\n{promo['text']}"
            self.send_message(sub['vk_id'], msg)
            log_sent(promo_id, sub['id'])
            sent_count += 1
            # Небольшая задержка, чтобы не превысить лимиты
            time.sleep(0.3)
        # Обновляем last_sent
        update_promotion(promo_id, last_sent=datetime.now().isoformat())
        self.send_message(user_id, f"✅ Рассылка завершена. Отправлено {sent_count} подписчикам.")
        log_admin_action(user_id, 'manual_send', f"promo_id={promo_id}, sent={sent_count}")

    def admin_set_mailing_time(self, user_id, time_str):
        """Устанавливает время автоматической рассылки."""
        try:
            datetime.strptime(time_str, '%H:%M')
            global DEFAULT_MAILING_TIME
            DEFAULT_MAILING_TIME = time_str
            self.send_message(user_id, f"⏰ Время автоматической рассылки установлено на {time_str}")
            log_admin_action(user_id, 'set_mailing_time', time_str)
        except ValueError:
            self.send_message(user_id, "Неверный формат времени. Используйте HH:MM (например, 10:00)")

    def admin_reply(self, admin_id, target_vk_id, text):
        """Отправляет сообщение указанному пользователю от имени бота."""
        try:
            target_vk_id = int(target_vk_id)
            self.send_message(target_vk_id, f"📨 Сообщение от администратора:\n{text}")
            self.send_message(admin_id, f"✅ Сообщение отправлено пользователю {target_vk_id}")
            log_admin_action(admin_id, 'reply', f"to={target_vk_id}, text={text[:50]}...")
        except ValueError:
            self.send_message(admin_id, "Неверный ID пользователя. Должен быть числом.")
        except Exception as e:
            self.send_message(admin_id, f"Ошибка при отправке: {e}")

    def admin_disable(self, user_id):
        """Отключает бота (глобально)."""
        self.enabled = False
        self.disabled_until = None
        self.send_message(user_id, "🚫 Бот отключен. Для включения используйте /enable")
        log_admin_action(user_id, 'disable')

    def admin_enable(self, user_id):
        """Включает бота."""
        self.enabled = True
        self.disabled_until = None
        self.send_message(user_id, "✅ Бот включен.")
        log_admin_action(user_id, 'enable')

    # ============= АВТОМАТИЧЕСКАЯ РАССЫЛКА ===================================
    def mailing_worker(self):
        """Фоновый поток, проверяющий расписание и запускающий рассылку."""
        logger.info("Поток рассылки запущен.")
        while True:
            try:
                now = datetime.now()
                # Проверяем, совпадает ли текущее время с настроенным временем рассылки
                target_time = datetime.strptime(DEFAULT_MAILING_TIME, '%H:%M').time()
                if now.hour == target_time.hour and now.minute == target_time.minute:
                    self.perform_mailing()
                    # Ждем минуту, чтобы не запустить повторно в ту же минуту
                    time.sleep(61)
                else:
                    time.sleep(MAILING_CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Ошибка в потоке рассылки: {e}")
                time.sleep(60)

    def perform_mailing(self):
        """Выполняет рассылку активных акций подписчикам."""
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

        # Для каждого подписчика выбираем акцию, которую ещё не отправляли в текущем периоде
        # (если несколько акций - отправляем одну, можно ротировать по дате последней отправки)
        # Простейший вариант: отправляем первую подходящую акцию.
        for sub in subscribers:
            sent = False
            for promo in active_promos:
                promo_id = promo['id']
                if promo['periodicity'] == 'daily' and was_sent_today(promo_id, sub['id']):
                    continue
                if promo['periodicity'] == 'weekly' and was_sent_this_week(promo_id, sub['id']):
                    continue
                # Отправляем
                msg = f"🎉 **Акция: {promo['title']}**\n\n{promo['text']}"
                self.send_message(sub['vk_id'], msg)
                log_sent(promo_id, sub['id'])
                sent = True
                # Обновляем last_sent у акции
                update_promotion(promo_id, last_sent=datetime.now().isoformat())
                # Небольшая задержка
                time.sleep(0.3)
                break  # Отправили одну акцию пользователю
            if not sent:
                logger.debug(f"Пользователь {sub['vk_id']} уже получил все акции в этом периоде.")

        logger.info("Автоматическая рассылка завершена.")

# ================== ЗАПУСК БОТА ===========================================
def main():
    init_db()
    bot = VKBot(VK_GROUP_ID, VK_API_TOKEN)

    # Запускаем поток рассылки
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
