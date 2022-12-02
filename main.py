"""
Основной файл, который исполняет Yandex Cloud Function при ображении пользователя в бота
Для обработки сообщений используется библиотека aiogram
В качестве БД используется Yandex DataBase. Методы для работы с базой находятся в файле db.py

Получение сообщение от телеграма происходит через webhook.
Это означает, что когда пользователь пишет боту, телеграм шлет сообшение в яндекс функцию.
сообщения могут быть двух видов:
    -сообщение с текстом, который написал пользователь(message)
    -сообщение с информацией о кнопке, которую нажал пользователь(callback_query)
Сообщение преобразовывается и подается на вход библиотеке aiogram.
В скрипте прописаны хендлеры(обработчики) на все сообщения, которые бот должен обработать.
Каждое сообщение проходит по списку хендлеров сверху вниз, пока не найдет свой обработчик.

Переменные окружения:
-TELEGRAM_KEY(токен телеграм бота)
-YDB_ENDPOING(адрес сервера базы данных)
-YDB_DATABASE(имя базы данных)
-ADMIN_TGID(ID админа бота в телеграме)
-TRIGERS(названия тригеров через зяпятую)

тригеры - специальные сообщения, которые посылает яндекс клауд по расписанию
под эти сообщения в скрипте есть специальные обработчики

В боте есть многоуровненые сценарии. Чтобы понимать где находится пользователь, 
там где нужно сохраняем состояние пользователя в колонку state таблицы users
"""

from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import Text

import os
import random
import logging
import uuid
import json
import datetime
import db
import traceback

# настройка aiogram
bot = Bot(os.environ.get('TELEGRAM_KEY'))
dp = Dispatcher(bot)

# Logger initialization and logging level setting
log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO').upper())

# через какое время и о чем будем уведомлять пользователя:
# пользователь начал пользоваться ботом, но еще не зарегистрировался на встречу
notification_order0 = datetime.timedelta(days=7)
# пользователь зарегистрировался на встречу, но партнер не находится. Первое
notification_order1 = datetime.timedelta(days=1)
# пользователь зарегистрировался на встречу, но партнер не находится. Второе
notification_order2 = datetime.timedelta(days=5)
# скоро дедлайн по встрече
notification_deadline = datetime.timedelta(days=7)
# дедлайн по встрече наступил
meeting_deadline = datetime.timedelta(days=10)

# список чаптеров трайба
chapters = ['Пользовательские интерфейсы', 'SberCare', 'Безбумажный мир', 'Обращения', 'Боты', 'Сопровождение', 'Платформенные сервисы', 'ЕСЗК', 'Аналитика', 'Штаб']

# инфа по кнопкам в экране редактирования профиля
profile_btns = {
    "Фамилия и Имя 📝": {'reply': "Введи фамилию и имя через пробел", 'state': 'set_fi', 'col': 'fi'},
    "Чаптер 🏦": {'reply': "Выбери свой чаптер", 'state': 'set_chapter', 'col': 'chapter'},
    "Команда, роль, задачи 💼": {'reply': "Введи свою команду, роль и задачи", 
                                 'state': 'set_role_and_tasks', 'col': 'role_and_tasks'},
    "Хобби, увлечения 🥁": {'reply': "Введи свои хобби и увлечения", 'state': 'set_info', 
                                         'col': 'info'},
    "Фотография 📷": {'reply': "Загрузи свою фотографию", 'state': 'set_photo', 
                                   'col': 'photo'},
    "instagram 🔗": {'reply': "Введи свой профиль в instagram", 'state': 'set_social_profile', 
                                  'col': 'social_profile'}
}

# тексты, которые посылаем пользователям. Тут все тексты, где не требуется вставлять какую-то инфу из переменных
texts = {
    "welcome_msg": """
Приветствуем тебя! 👋
С помощью этого бота ты сможешь принять участие в <b>рандомных</b> встречах, чтобы познакомиться с коллегами, с которыми работаешь, найти товарищей по интересам.

⚠️ Бот создан для коллег из трайба <b>Забота о клиентах</b>.

Чтобы начать, заполни свой профиль 📋
""",
    "feedback_question": "Что бы вы хотели сказать разработчику бота random coffee?",
    "empty_profile": "Пока что твой профиль пустой. Заполни его",
    "inactive_status": "Сейчас ты неактивен \nДля поиска коллеги нажми <b>Зарегистрироваться на встречу</b> ",
    "order_check" : '''Погоди..
Для регистрации на встречу нужно заполнить как минимум фамилию и имя, чаптер, а также команду и твою роль 👇''',
    "cancel_meeting_check": "Отменяем встречу?\nТвой коллега будет грустить 😥",
    "meeting_canceled_user": "Встреча отменена 👌\n\n",
    "meeting_canceled_partner": '''Упс..\nТвой коллега не сможет провести встречу 😭''',
    "chapter_change": "Для того, чтобы изменить чаптер, нужно завершить текущую встречу",
    "meeting_finished_order": "Мы закинули тебя в очередь на поиск нового коллеги, совсем скоро оповестим тебя 😉",
    "meeting_finished_w_review": "Спасибо за отзыв о встрече",
    "hello": "Привет! 👋",
    "meeting_finished_partner": "Твой партнер оповестил бота о завершении встречи",
    "meeting_finished_user": "Спасибо за оповещение о встрече",
    "deadline": '''Привет! 👋
Дедлайн по встрече наступил ⏰

Мы сделали тебя <b>неактивным</b>.
Для участия во встречах нажми <b>Зарегистрироваться на встречу</b> ''',
    "deadline_notification": '''Привет! 👋
Прошла неделя, а ты еще не провел встречу?
Есть варианты:
<b>-встреча прошла</b>, но забыл проинформировать нас - сделай это сейчас по кнопке снизу
<b>-встречу не проводили</b> - спишись с коллегой, у тебя есть еще 3 дня!
<b>-Не сможешь провести встречу</b> - отмени встречу по кнопке снизу''',
    "order_notification2": """Привет! 👋
Мы про тебя не забыли!
На данный момент <b>нет новых</b> коллег для встречи 😭
Придется еще подождать..

А пока отправь своим коллегам ссылку-приглашение на этого бота📢
https://t.me/sbercare_bots_random_coffee_bot""",
    "order_notification1": """Привет! 👋
Свободных коллег для встречи нет, <b>ожидай</b> приглашения

А пока отправь своим коллегам ссылку-приглашение на этого бота📢
https://t.me/sbercare_bots_random_coffee_bot""",
    "order_notification1_empty_data": """\n\nТакже мы обратили внимание, что большая часть твоей анкеты не заполнена...
Твоему коллеге будет проще провести с тобой встречу, если ты укажешь по себе информацию 📝

Можешь заполнить по кнопке <b>профиль</b> 👇""",
    "newbie_notification": """Привет! 👋
Твои коллеги ждут тебя..

Прошла неделя, а ты так и не зарегистрировался на встречу 😢

Сделай это прямо сейчас по кнопке снизу👇"""
}

def btn(text, callback_data=None):
    """
    функция, которая настраивает кнопку, которую покажем пользователю
    в чем настройка: опционально можно сделать изменить текст сообщения, которое кнопка пошлет в бота при нажатии
    """
    if callback_data == None:
        callback_data = text
    return types.InlineKeyboardButton(text, callback_data=callback_data)

async def send_msg(recipient, msg, reply_markup=None):
    """
    функция отправляет сообщение и логирует его в таблицу messagesfrombot
    """
    try:
        await bot.send_message(recipient, msg, reply_markup=reply_markup, parse_mode='html')
        db.messagesfrombot_add(recipient, msg)
    except Exception:
        print('инфа от бота: 3')
        print('инфа от бота: ' + str(traceback.format_exc()))
        print('инфа от бота: 4')

@dp.message_handler(commands='start')
async def cmd_start(message: types.Message): 
    """
    ответ бота на команду start
    """
    markup = types.InlineKeyboardMarkup()
    markup.add(btn('Заполнить профиль 📝', callback_data='/profile'))
    await send_msg(message.from_user.id, texts['welcome_msg'], reply_markup=markup)

@dp.callback_query_handler(text='/profile')
async def profile_query(query: types.CallbackQuery):
    """
    ответ бота на нажатие кнопки профиль
    """
    # вызываем профиль и кнопки
    await profile_united(query.from_user.id, msg="Твой профиль: \n", state='profile')

@dp.message_handler(commands='profile')
async def profile(message: types.Message):
    """
    ответ бота на команду profile
    """
    await profile_united(message.from_user.id, msg="Твой профиль: \n", state='profile')

@dp.message_handler(commands='change_status')
async def change_status(message: types.Message):
    """
    ответ бота на команду change_status
    """
    await default_united(message.from_user.id)

@dp.message_handler(commands='feedback')
async def feedback_command(message: types.Message):
    """
    ответ бота на команду feedback
    """
    markup = feedback_markup()
    await send_msg(message.from_user.id, texts['feedback_question'], reply_markup=markup)
    # сохраняем состояние
    db.Users().update(message.from_user.id, state='set_feedback')

def profile_func_markup():
    """
    функция показывает кнопки на экране редактирования профиля
    """
    markup = types.InlineKeyboardMarkup()
    for profile_btn in profile_btns.keys():
        markup.add(btn(profile_btn))
    markup.add(btn('Завершить редактирование профиля ✅', callback_data='/default'))
    return markup

def profile_func(user_profile, msg):
    """
    функция показывает профиль пользователя
    """
    msg_in = msg
    profile_dict = {v['col']: k for k,v in profile_btns.items()}
    if user_profile['fi'] != None:
        msg += f"{profile_dict['fi']}: {user_profile['fi']}{' @' + user_profile['username'] if user_profile['username'] != '' else ''}\n"
    if user_profile['chapter'] != None:
        msg += f"{profile_dict['chapter']}: {user_profile['chapter']}\n"
    if user_profile['role_and_tasks'] != None:
        msg += f"{profile_dict['role_and_tasks']}: {user_profile['role_and_tasks']}\n"
    if user_profile['info'] != None:
        msg += f"{profile_dict['info'].split('(')[0]}: {user_profile['info']}\n"
    if user_profile['social_profile'] != None:
        msg += f"{profile_dict['social_profile'].split('(')[0]}: {user_profile['social_profile']}\n"
    if msg == msg_in:
        msg = texts['empty_profile']
    return msg, user_profile

@dp.callback_query_handler(text='/default')
async def default(query: types.CallbackQuery):
    """
    ответ бота на нажатие кнопки перехода в экран статуса
    """
    await default_united(query.from_user.id)

def default_func(tgid, msg=None):
    """
    функция формирует текст для экрана статуса
    """
    tgid_user = db.Users().get_user(tgid)
    if tgid_user['status'] == 'active':
        msg += f"Ты зарегистрирован на встречу в чаптере <b>{tgid_user['chapter']}</b>. Ожидай приглашения"
        
    elif tgid_user['status'] == 'inactive':
        msg += texts['inactive_status']

    elif tgid_user['status'] == 'in_process':
        # получаем инфу по партнеру
        partner = db.Meetings_user().get_user_partner(tgid)
        partner = db.Users().get_user(partner)
        deadline = db.Meetings().current_user_meeting(tgid)[0]['end_dt']
        deadline = datetime.datetime.fromtimestamp(deadline).strftime("%Y-%m-%d")
        msg += f"У тебя назначена встреча\nТвой партнер: {partner['fi']}{' @' + partner['username'] if partner['username'] != '' else ''}.\nДедлайн: {deadline}"
    return msg

def default_func_markup(tgid):
    """
    функция формирует кнопки для экрана статуса
    """
    tgid_user = db.Users().get_user(tgid)
    markup = types.InlineKeyboardMarkup()
    if tgid_user['status'] == 'active':
        markup.add(btn('Отменить регистрацию на встречу ❌', callback_data='/cancel_order'))
        
    elif tgid_user['status'] == 'inactive':
        markup.add(btn('Зарегистрироваться на встречу 🔍', callback_data='/order'))

    elif tgid_user['status'] == 'in_process':
        markup.add(btn('Посмотреть профиль партнера 📰', callback_data='/partner_profile'))
        markup.add(btn('Проинформировать о прошедшей встрече ✅', callback_data='/meeting_inform'))
        markup.add(btn('Отменить встречу ❌', callback_data='/cancel_meeting_request'))

    markup.add(btn('Профиль 📋', callback_data='/profile'))
    markup.add(btn('Дать фидбек по работе бота 🎤', callback_data='/feedback'))
    return markup

async def default_united(tgid, msg=None, text=True, msg_first=None):
    """
    функция показывающая пользователю экран статуса
    """
    if msg == None:
        msg = ""
    
    markup = default_func_markup(tgid)
    if text:
        msg = default_func(tgid, msg=msg)
    
    if msg_first:
        await send_msg(tgid, msg_first)
    await send_msg(tgid, msg, reply_markup=markup)
    # сохраняем состояние
    db.Users().update(tgid, state='default')

@dp.callback_query_handler(text='/order')
async def order(query: types.CallbackQuery):
    """
    функция обрабатывающая кнопку Зарегистрироваться на встречу
    """
    global user
    # проверяем что у пользователя статус inactive и что у него уже сейчас нет активного ордера
    if (db.Orders().current_order(query.from_user.id) == []) and (user['status'] == 'inactive'): 
        # проверяем, что заполнены поля фи, чаптер, роль
        if user['fi'] != None and user['chapter'] != None and user['role_and_tasks'] != None:
            # обновляем статус
            db.Users().update(user['tgid'], status='active')
            user = db.Users().get_user(query.from_user.id)

            # логируем заявку на встречу
            db.Orders().new(query.from_user.id)

            # ищем партнера
            active_users = db.Users().get_for_meeting(user['chapter'])
            active_users, done, current_user = await search_partner(active_users, tgid=query.from_user.id)
            # если партнер не найден, то выводим дефолтный экран
            if done == False:
                await default_united(query.from_user.id)
        else:
            msg = texts['order_check']
            markup = types.InlineKeyboardMarkup()
            markup.add(btn('Заполнить профиль 📝', callback_data='/profile'))
            await send_msg(query.from_user.id, msg, reply_markup=markup)

async def cancel_order_user(user_tgid):
    global user
    user = db.Users().get_user(user_tgid)
    # отменяем заявку на встречу, если она есть
    if (db.Orders().current_order(user_tgid) != []) and (user['status'] == 'active'):
        db.Users().update(user_tgid, status='inactive')
        user = db.Users().get_user(user_tgid)
        db.Orders().delete_order(user_tgid)
        await default_united(user_tgid)

@dp.callback_query_handler(text='/cancel_order')
async def cancel_order(query: types.CallbackQuery):
    """
    функция обрабатывает нажатие на кнопку Отменить регистрацию на встречу
    """
    await cancel_order_user(query.from_user.id)
    

@dp.callback_query_handler(text='/cancel_meeting_request')
async def cancel_meeting_request(query: types.CallbackQuery):
    """
    функция обрабатывает нажатие кнопки Отменить встречу
    """
    # спрашиваем про отмену встречи, если она есть
    if db.Meetings().current_user_meeting(query.from_user.id) != [] and (user['status'] == 'in_process'):
        msg = texts['cancel_meeting_check']
        markup = types.InlineKeyboardMarkup()
        markup.add(btn('Отменяем ❌', callback_data='/cancel_meeting'))
        markup.add(btn('Назад 🔙', callback_data='/default'))
        await send_msg(query.from_user.id, msg, reply_markup=markup)

@dp.callback_query_handler(text='/cancel_meeting')
async def cancel_meeting(query: types.CallbackQuery):
    """
    функция обрабатывает нажатие на кнопку Отменяем ❌ (подтверждение отмены встречи)
    """
    await finish_meeting_user(query.from_user.id, 'rejection')

@dp.callback_query_handler(lambda q: q.data in profile_btns.keys())
async def profile_btn(query: types.CallbackQuery):
    """
    Функция обрабатывает нажатие кнопок на экране редактирования профиля
    """
    text = query.data
    if text in profile_btns.keys():
        msg = profile_btns[text]['reply']
        markup = types.InlineKeyboardMarkup()
        chapter_text = list({k: v for k, v in profile_btns.items() if v['col'] == 'chapter'}.keys())[0]
        if text == chapter_text:
            if (db.Orders().current_order(query.from_user.id) == []) and (user['status'] == 'inactive'): 
                for chapter in chapters:
                    markup.add(btn(f'{chapter}'))
            else:
                msg = texts['chapter_change']
                markup.add(btn('Отменить регистрацию на встречу ❌', callback_data='/cancel_order'))

        markup.add(btn("Назад 🔙", callback_data="/profile"))
        await send_msg(query.from_user.id, msg, reply_markup=markup)
        if msg == profile_btns[text]['reply']:
            db.Users().update(query.from_user.id, state=profile_btns[text]['state'])

async def profile_united(tgid, msg=None, to_user=None, state=None, markup=True):
    """
    функция формирует экран просмотра профиля
    """
    user_profile = db.Users().get_user(tgid)
    # markup - либо True, либо свои кнопки
    if to_user == None:
        to_user = tgid
    if msg == None:
        msg = ""
    if markup==True:
        markup = profile_func_markup()
    msg, user_profile = profile_func(user_profile, msg)
    if user_profile['photo'] != None:
        await bot.send_photo(to_user, user_profile['photo'], caption=msg, reply_markup=markup)
    else:
        await bot.send_message(to_user, msg, reply_markup=markup)
    db.messagesfrombot_add(to_user, msg)
    if state:
        db.Users().update(user_profile['tgid'], state=state)

async def finish_meeting_user(tgid, status, review=None):
    """
    функция завершает встречу по одной из причин: done/deadline/rejection
    """
    global user
    user = db.Users().get_user(tgid)
    if db.Meetings().current_user_meeting(tgid) != [] and (user['status'] == 'in_process'):
        partner, meeting_id = finish_meeting(tgid, status)
        if status == 'done':
            # логируем отзыв о встрече, если он есть
            if review:
                db.meeting_reviews_add(tgid, meeting_id, review)
                msg_user = texts['meeting_finished_w_review']
            else:
                msg_user = texts['meeting_finished_user']
            msg_user = msg_user + '\n\n' + texts['meeting_finished_order']
            msg_partner = texts['hello'] + '\n' + texts['meeting_finished_partner'] + '\n\n' + texts['meeting_finished_order']
        elif status == 'deadline':
            msg_user = texts['deadline']
            msg_partner = texts['deadline']
        elif status == 'rejection':
            # заливаем строку в таблицу rejections
            db.rejection_add(tgid, meeting_id)
            msg_user = texts['meeting_canceled_user']
            msg_partner = texts['meeting_canceled_partner'] + '\n\n' + texts["meeting_finished_order"]
        # нотификации пользователям
        # пишем юзеру
        await default_united(tgid, msg=msg_user, text=False)
        # пишем партнеру
        await default_united(partner, msg=msg_partner, text=False) 


def feedback_markup():
    """
    ф-я формирует кнопки для экрана ввода фидбека боту
    """
    markup = types.InlineKeyboardMarkup()
    markup.add(btn('Ничего не хочу сказать', callback_data='/default'))
    return markup

@dp.callback_query_handler(text='/feedback')
async def feedback(query: types.CallbackQuery):
    """
    функция формирует экран ввода фидбека боту
    """
    markup = feedback_markup()
    msg = 'Что бы вы хотели сказать разработчику бота random coffee?'
    await send_msg(query.from_user.id, msg, reply_markup=markup)
    db.Users().update(query.from_user.id, state='set_feedback')

@dp.callback_query_handler(text='/meeting_inform')
async def meeting_inform_query(query: types.CallbackQuery):
    """
    ф-я обрабатывает нажатие на кнопку Проинформировать о прошедшей встрече
    """
    if db.Meetings().current_user_meeting(query.from_user.id) != [] and (user['status'] == 'in_process'):
        msg = 'Как прошла встреча? Напиши отзыв'
        markup = types.InlineKeyboardMarkup()
        markup.add(btn('Без отзыва', callback_data='/set_meeting_inform'))
        markup.add(btn('Назад 🔙', callback_data='/default'))
        await send_msg(query.from_user.id, msg, reply_markup=markup)
        db.Users().update(query.from_user.id, state='set_meeting_review')

def finish_meeting(tgid, status):
    """
    ф-я логирует завершение встречи в базе данных
    """
    # находим партнера
    partner = db.Meetings_user().get_user_partner(tgid)
    # ставим законченной встрече result = status
    meeting_id = db.Meetings().current_user_meeting(tgid)[0]['meeting_id']
    db.Meetings().update(meeting_id, result=status, end_dt=db.now())
    # меняем статусы у пользователей и ставим "в очередь"(если нужно)
    for user_tgid in [tgid, partner]:
        if (status == 'done') or (status=='rejection' and user_tgid==partner):
            user_status = 'active'
            db.Orders().new(user_tgid)
        else:
            user_status = 'inactive'
        db.Users().update(user_tgid, status=user_status)
    return partner, meeting_id

@dp.callback_query_handler(text='/set_meeting_inform')
async def set_meeting_inform(query: types.CallbackQuery):
    """
    ф-я обрабатывает нажатие на кнопку завершения встречи без отзыва
    """
    await finish_meeting_user(query.from_user.id, 'done')

async def do_start_meeting_user(tgid):
    """
    ф-я уведомляет пользователя о найденном партнере
    """
    # показываем дефолтный экран юзеру
    # получаем инфу по партнеру
    partner = db.Users().get_user(db.Meetings_user().get_user_partner(tgid))

    msg = f'''Мы нашли тебе коллегу для встречи - {partner.fi}{' @' + partner.username if partner.username != '' else ''} 🥳!
Постарайся провести встречу в течение <b>одной недели</b>.\n
<b>Провели встречу</b> - проинформируй об успешности встречи по кнопке снизу
<b>Не сможешь провести встречу</b> - отмени ее также по кнопке снизу '''
    await default_united(tgid, msg=msg, text=False)

@dp.callback_query_handler(text='/partner_profile')
async def partner_profile(query: types.CallbackQuery):
    """
    ф-я обрабатывает нажатие на кнопку Профиль партнера
    """
    # получаем инфу по партнеру
    if db.Meetings().current_user_meeting(query.from_user.id) != [] and (user['status'] == 'in_process'):
        partner = db.Meetings_user().get_user_partner(query.from_user.id)
        markup = types.InlineKeyboardMarkup()
        markup.add(btn('Назад 🔙', callback_data='/default'))
        await profile_united(partner, msg="Профиль твоего партнера:\n", state='default', to_user=query.from_user.id, markup=markup)

def do_start_meeting_db(tgid, partner):
    """
    ф-я логирует новую встречу в базе данных
    """
    # получаем meeting_id
    meeting_id = str(uuid.uuid4())

    # логируем встречу в таблицу meetings
    db.Meetings().add(meeting_id, db.Users().get_user(tgid)['chapter'], meeting_deadline)

    for client in [tgid, partner]:
        # меняем статус
        db.Users().update(client, status='in_process')
        #апдейтим meeting_id в orders
        db.Orders().update_meeting(client, meeting_id)

    # логируем встречу в таблицу meetings_user
    db.Meetings_user().add(meeting_id, tgid, partner)
    db.Meetings_user().add(meeting_id, partner, tgid)

async def start_meeting(tgid, partner):
    """
    ф-я создает новую встречу
    """
    logging.info(f"инфа от бота: Starting meeting for user {tgid}")
    print(f"инфа от бота: Starting meeting for user {tgid}")

    # логируем встречу в базу данных
    do_start_meeting_db(tgid, partner)

    # отправляем нотификации
    await do_start_meeting_user(tgid)
    await do_start_meeting_user(partner)

async def search_partner(active_users, tgid=None):
    """
    ф-я ищет партнеров для встречи
    """

    done = False
    if tgid==None:
        tgid = active_users[0]
    partners = active_users.copy()
    partners.remove(tgid)
    if partners!=[]:
        # получаем неподходящих партнеров(те, которые уже встречались с юзером, или у которых неудачная встреча была последней)
        stop_partners = db.Meetings_user().check_partners(tgid)
        # отсекаем стоплист
        partners = [i for i in partners if i not in stop_partners]
        # если список партнеров не пуст, то берем первого
        if type(partners) == list and partners != []:
            partner = partners[0]
            await start_meeting(tgid, partner)
            active_users.remove(tgid)
            active_users.remove(partner)
            done = True
        if tgid in active_users:
            active_users.remove(tgid)
    else:
        active_users.remove(tgid)
    return active_users, done, tgid

async def do_check_deadlines():
    """
    ф-я проверяет проверяет дедлайны встреч
    """
    # получаем встречи, у которых дедлайн прошел
    meetings = db.Meetings().deadline_meetings()
    for meeting in meetings:
        # получаем участников
        partners = db.Meetings_user().get_partners(meeting)
        await finish_meeting_user(partners[0], 'deadline')

    # получаем встречи, у которых дедлайн скоро будет
    meetings = db.Meetings().deadline_meetings(deadline = notification_deadline)
    for meeting in meetings:
        # получаем участников
        partners = db.Meetings_user().get_partners(meeting)
        for partner in partners:
            msg = texts['deadline_notification']
            await default_united(partner, msg=msg, text=False)
        db.Meetings().update(meeting, notification_id=1)

async def do_notification_order(status, current_user):
    """
    бот уведомляет пользователей, которые зарегистрированы на встречу, но партнер пока не найден
    уведомляет, что партнер ищется. Пользователь не брошен. Есть два уведомления, в разное время
    Время задается в начале скрипта
    """
    if status == False:
        order = db.Orders().current_order(current_user)[0]
        if db.now() > datetime.datetime.fromtimestamp(order['order_dt']) + notification_order2 and order['notification_id'] == 1:
            msg = texts['order_notification2']
            await default_united(current_user, msg=msg, text=False)
            # апдейтим notification_id
            db.Orders().update(current_user, notification_id=2)

        elif db.now() > datetime.datetime.fromtimestamp(order['order_dt']) + notification_order1 and order['notification_id'] == None:
            msg = texts["order_notification1"]

            tgid = db.Users().get_user(current_user)
            if tgid['info'] == None and tgid['photo'] == None and tgid['social_profile'] == None:
                msg += texts["order_notification1_empty_data"]
            await default_united(current_user, msg=msg, text=False)
            # апдейтим notification_id
            db.Orders().update(current_user, notification_id=1)

@dp.message_handler(commands='statistics')
async def cmd_statistics(message: types.Message): 
    """
    ф-я показывает статистику по запросу
    """
    users_cnt = db.Statistics().users_cnt()
    users_per_chapter = db.Statistics().users_per_chapter()
    meetings_per_week = db.Statistics().meetings_per_week()
    meetings_per_chapter = db.Statistics().meetings_per_chapter()
    users_meetings = db.Statistics().users_meetings()
    msg = f'''
<b>{users_cnt} - людей зарегистрировано</b>

<b>Пользователей по чаптерам:</b>
{users_per_chapter}

<b>Кол-во проведенных встреч по неделям за последние 90 дней(номера недель с начала года):</b>
{meetings_per_week}

<b>Кол-во проведенных встреч по чаптерам:</b>
{meetings_per_chapter}

<b>кол-во людей, которые провели N встреч:</b>
{users_meetings}

    '''
    await send_msg(message.from_user.id, msg)

@dp.message_handler()
@dp.message_handler(content_types=['photo'])
async def other_messages(message: types.Message):
    """
    функция для всех сообщений(не кнопки), где нужно учитывать состояния
    последовательно ищем фразы из конкретных состояний и обрабатываем
    """

    #проверяем на заполнения данных профиля
    profile_state_col = {v['state']: v['col'] for k, v in profile_btns.items()}
    if user['state'] in profile_state_col.keys():
        msg = None
        state_photo = {v: k for k,v in profile_state_col.items()}['photo']
        if ('photo' in dict(message).keys()) and (user['state'] == state_photo):
            msg = message.photo[0].file_id
        elif ('text' in dict(message).keys()) and (user['state'] != state_photo):
            msg = message.text
        if msg:
            db.Users().update(message.from_user.id, cols={profile_state_col[user['state']]: msg})
            msg = 'Данные внесены'
            await send_msg(message.from_user.id, msg)
            await profile_united(message.from_user.id, msg="Твой профиль: \n", state='profile')
    
    # закрываем встречу после получения отзыва о встрече
    elif user['state'] == 'set_meeting_review':
        await finish_meeting_user(message.from_user.id, 'done', review=message.text)
    # пишем фидбек о боте в БД
    elif user['state'] == 'set_feedback':
        db.feedback_add(message.from_user.id, message.text)
        await default_united(message.from_user.id, msg_first='Спасибо за обратную связь')

@dp.callback_query_handler()
async def other_queries(query: types.CallbackQuery):
    """
    хендлер для кнопок, где нужно учитывать состояния
    """
    # проверяем на выбор чаптера
    profile_state_col = {v['state']: v['col'] for k, v in profile_btns.items()}
    state_chapter = {v: k for k,v in profile_state_col.items()}['chapter']
    if user['state'] == state_chapter:
        if (db.Orders().current_order(query.from_user.id) == []) and (user['status'] == 'inactive'): 
            # заполняем колонку chapter в таблице users
            db.Users().update(query.from_user.id, cols={profile_state_col[user['state']]: query.data})
            msg = 'Данные внесены'
            await send_msg(query.from_user.id, msg)
            await profile_united(query.from_user.id, msg="Твой профиль: \n", state='profile')
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(btn("Назад 🔙", callback_data="/profile"))
            msg = 'Для того, чтобы изменить чаптер, нужно завершить текущую встречу'
            await send_msg(query.from_user.id, msg, reply_markup=markup)

async def do_check_newbies():
    """
    получаем список юзеров, которых нужно оповестить, что они зарегались в боте, но не зарегались на встречу
    """
    newbies = db.Users().get_newbies(notification_order0)
    for newbie in newbies:
        msg = texts['newbie_notification']
        await default_united(newbie, msg=msg, text=False)
        # апдейтим notification_id
        db.Users().update(newbie, notification_id=1)

async def check_meetings():
    """
    Назначаем встречи активным юзерам
    """
    user_chapters = db.db_operation(f"select distinct chapter from {db.tables['users']} ")
    user_chapters = [i['chapter'] for i in user_chapters]
    for chapter in user_chapters:
        active_users = db.Users().get_for_meeting(chapter)
        while active_users != []:
            active_users, status, current_user = await search_partner(active_users)
            # нотификации по заявкам на встречу, у кого не найден партнер
            await do_notification_order(status, current_user)

    # закрываем встречи по их дедлайнам
    await do_check_deadlines()

    # проверка новичков, которые не провели ни одной встречи
    await do_check_newbies()

async def process_event(event, dp: Dispatcher):
    """
    Конвертируем сообщение от телеграма в нашу функцию в сообщение на вход aiogram
    """

    update = json.loads(event['body'])
    log.debug('Update: ' + str(update))

    # парсим сообщение
    msg = db.message_parse(update)
    
    # пишем сообщение в базу
    db.message_log(msg)
    # получаем информацию по человеку из таблицы users
    global user
    user = db.Users().check_new_user(msg)
    
    Bot.set_current(dp.bot)
    update = types.Update.to_object(update)
    # обрабатываем сообщение
    await dp.process_update(update)


async def handler(event, context):
    """
    хендлер яндекс функции. Именно эту функцию скрипта запускает яндекс функция для обработки сообщения
    """
    print(event)
    try:
        if 'httpMethod' in event.keys():
            if event['httpMethod'] == 'POST':
                # print(json.loads(event['body']))
                if 'callback_query' in json.loads(event['body']) or \
                    'message' in json.loads(event['body']):
                    # если сообщение не старше пол минуты, то обрабатываем.
                    if datetime.datetime.fromtimestamp(event['requestContext']['requestTimeEpoch']) >\
                    (datetime.datetime.now() - datetime.timedelta(seconds=30)):
                        await process_event(event, dp)
                    return {'statusCode': 200, 'body': 'ok'}
                # если получили сообщение от телеграма, что пользователь заблокировал бота, 
                # то завершаем регистрацию на встречу, если у этого пользователя она есть
                # то завершаем встречу, если у этого пользователя она есть
                elif 'my_chat_member' in json.loads(event['body']) and \
                    json.loads(event['body'])['my_chat_member']['new_chat_member']['status'] == 'kicked':
                    rej_user = json.loads(event['body'])['my_chat_member']['from']['id']
                    await cancel_order_user(rej_user)
                    await finish_meeting_user(rej_user, 'rejection')
                    return {'statusCode': 200, 'body': 'ok'}
            else:
                print('инфа от бота: левое сообщение')
                # отправляем сообщение админу бота
                await send_msg(int(os.environ.get('ADMIN_TGID')), 'левое соообщение: \n' + str(event))
                return {'statusCode': 200, 'body': 'ok'}
        # если в бота пришел наш тригер, то запускаем поиск партнерой, проверку дедлайнов, уведомления
        elif any(x in str(event) for x in os.environ.get('TRIGERS').split(',')):
            await check_meetings()
            return {'statusCode': 200, 'body': 'ok'}
    except Exception:
        print(f'инфа от бота: 1')
        print(str(traceback.format_exc()))
        # отправляем ошибку админу бота
        await send_msg(int(os.environ.get('ADMIN_TGID')), str(traceback.format_exc()))
        print(f'инфа от бота: 2')
    return {'statusCode': 405}