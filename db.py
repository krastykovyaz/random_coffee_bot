"""
скрипт с методами для работы с yandex database
Взаимодействие с БД происходит через собственный коннектор - библиотеку ydb
"""

import contextlib
import datetime
import time
import ydb
import os
import uuid
import logging

# подключаемя к базе данных
# create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

def execute_query(session):
    # create the transaction and execute query.
    # print(str(query))
    return session.transaction().execute(
        query,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    )

# все таблицы для работы бота
tables = {
'users': 'users',
'orders': 'orders',
'meetings_user': 'meetings_user',
'meetings': 'meetings',
'messages': 'messages',
'messagesfrombot': 'messagesfrombot',
'feedback': 'feedback',
'meeting_reviews': 'meeting_reviews',
'rejections': 'rejections'
}

def quotes(s):
    """
    ф-я для очистки кавычек, которые не принимает yandex database
    """
    for i in ['\'', '\"']:
        s = s.replace(i, '')
    return s

def line_dict(d):
    """
    ф-я парсит словарь аргументов и раскладывает корректно переменные
    """
    line_d = {}
    for k, v in d.items():
        if type(v) == dict:
            for kk, vv in v.items():
                line_d[kk] = vv
        else:
            line_d[k] = v
    return line_d

def data_type(v):
    """
    ф-я преобразовывает дату в формат БД яндекса
    """
    if type(v) == datetime.datetime:
        v = f"datetime('{v.strftime('%Y-%m-%dT%H:%M:%SZ')}')"
    elif type(v) == str:
        v = f"'{v}'"
    else:
        v = str(v)
    return v

def db_select(table, *where_custom, **where):
    """
    ф-я для селектов к БД

    в **where подаем условия на колонки
    (только для условий равенства, которые должны быть соединены оператором and),
    Можно вставить словарем. Это пережует функция line_dict
    """
    where = line_dict(where)
    q = f"""
    select * from {table}
    {' where ' if where_custom or where else ''} 
    {' and '.join([f"{k} = {data_type(v)}" for k,v in where.items()])} 
    """
    if where_custom:
        if where:
            q += ' and '
        q += ' and '.join(where_custom)
    q = q.replace('= None', 'is null')
    
    global query
    query = q
    rows = pool.retry_operation_sync(execute_query)
    return rows[0].rows

def db_update(table, cols, *where_custom, **where):
    """
    ф-я для апдейтов в БД
    """
    where = line_dict(where)
    q = f"""
    update {table}
    set {' , '.join([f"{k} = {data_type(v)}".replace('= None', '= null') for k,v in cols.items()])}
    {' where ' if where_custom or where else ''} 
    {' and '.join([f"{k} = {data_type(v)}".replace('= None', 'is null') for k,v in where.items()])} 
    """
    if where_custom:
        if where:
            q += ' and '
        q += ' and '.join(where_custom)
    
    global query
    query = q
    pool.retry_operation_sync(execute_query)

def db_insert(table, **kwargs):
    """
    ф-я для инсерта в БД

    в **kwargs подаем колонки строки, которые вставляем
    Можно вставить словарем. Это пережует функция line_dict
    """
    
    kwargs = line_dict(kwargs)
    kwargs['row_id'] = str(uuid.uuid4())
    q = f"""
    insert into {table} ({','.join(kwargs.keys())})
    values ({','.join([data_type(i) for i in kwargs.values()])})
    """
    global query
    query = q
    pool.retry_operation_sync(execute_query)

def db_operation(q):
    """
    кастомное обращение к БД, когда вышеописанные методы не подходят
    """
    global query
    query = q
    rows = pool.retry_operation_sync(execute_query)
    if 'select' in q:
        return rows[0].rows

class Users():
    """
    Класс для работы с таблицей users(информация по пользователям)
    """
    table = tables['users']

    def check_new_user(self, msg):
        # получаем инфо по юзеру
        rows = db_select(self.table, tgid=msg['tgid'])
        # если его нет в базе, то регистрируем
        if rows == []:
            # создаем нового юзера
            cols = {}
            for col in ['tgid', 'username', 'first_name', 'last_name']:
                cols[col] = msg[col]
            cols['reg_dt'] = now()
            cols['status'] = 'inactive'
            db_insert(self.table, cols=cols)
            logging.info(f"инфа от бота: creating new user {cols['tgid']}")
            print(f"инфа от бота: creating new user {cols['tgid']}")
            # получаем инфо по юзеру
            rows = db_select(self.table, tgid=msg['tgid'])
        return rows[0]

    def get_user(self, tgid):
        # получаем инфо по юзеру
        rows = db_select(self.table, tgid=tgid)
        return rows[0]

    def update(self, tgid, **kwargs):
        kwargs = line_dict(kwargs)
        db_update(self.table, kwargs, where={'tgid':tgid})

    def get_for_meeting(self, chapter):
        """
        ф-я получает список людей из чаптера, которые зарегистрированы на встречу
        """
        rows = db_select(self.table, chapter=chapter, status='active')
        rows = [i['tgid'] for i in rows]
        return rows
    
    def get_newbies(self, notification_order0):
        """
        ф-я получает список людей, которые начали пользоваться ботом, 
        но в течении notification_order0 не зарегистрировались на встречу
        """
        q = f'''
            select t1.tgid as tgid from {tables['users']} as t1 
            left join {tables['meetings_user']} as t2 on t1.tgid = t2.tgid
            where t2.tgid is null and (reg_dt + DateTime::IntervalFromSeconds({int(notification_order0.total_seconds())}) < {data_type(now())})
            and t1.notification_id is null
            '''
        rows = db_operation(q)
        rows = [i['tgid'] for i in rows]
        return rows

class Meetings_user():
    """
    класс для работы с таблицей meeting_user(связка встречи, пользователя и партнера)
    """
    table = tables['meetings_user']

    def get_user_partner(self, tgid):
        """
        ф-я получает партнера пользователя в его текущей встрече
        """
        q = f'''select tgid_partner from (select meeting_id, tgid_partner from {self.table} 
            where tgid = {tgid}) as t1 
            join (select meeting_id from {tables['meetings']} 
            where result='in_process') as t2 on t1.meeting_id = t2.meeting_id
            '''
        rows = db_operation(q)
        return rows[0]['tgid_partner']

    def add(self, meeting_id, tgid, partner):
        """
        ф-я логирует партнера пользователя
        """
        db_insert(self.table, meeting_id=meeting_id, tgid=tgid, tgid_partner=partner)
    
    def check_partners(self, user_tgid):
        """
        ф-я проверяет партнера, что пользователь с ним еще не провидил успешную встречу,
        а также проверяет, чтобы у обоих последняя встреча не являлась неуспешной встречей 
        с этим партнером
        """
        q = f'''
            $s = (select max(row_id) from meetings_user where tgid = {user_tgid});
            select distinct t1.tgid as tgid from (
            select tgid from users where tgid <> {user_tgid} and status='active'
            ) as t1
            join (
            select row_id, tgid, meeting_id from meetings_user where tgid_partner = {user_tgid}
            ) as t2 on t1.tgid = t2.tgid 
            join (
            select meeting_id, result from meetings
            ) as t3 on t2.meeting_id = t3.meeting_id
            join (
            select tgid, max(row_id) as max_row_id from meetings_user group by tgid
            ) as t4 on t2.tgid = t4.tgid
            where result ='done' or t2.row_id = t4.max_row_id
            or t2.row_id = $s;
            '''
        rows = db_operation(q)
        rows = [i['tgid'] for i in rows]
        return rows
    
    def get_partners(self, meeting_id):
        rows = db_select(self.table, meeting_id=meeting_id)
        rows = [i['tgid'] for i in rows]
        return rows
            

class Meetings():
    """
    класс для работы с таблицей meetings(встречи)
    """
    table = tables['meetings']

    def current_user_meeting(self, tgid):
        """
        ф-я находит айди текущей встречи пользователя
        """
        q = f'''
            select t2.* from 
            (select meeting_id, tgid_partner from {tables['meetings_user']} where tgid = {tgid}) as t1 
            join (select * from {self.table} where result='in_process') as t2 
            on t1.meeting_id = t2.meeting_id
            '''
        rows = db_operation(q)
        return rows

    def update(self, meeting_id, **kwargs):
        """
        ф-я апдейтит строку с переданным meeting_id
        """
        db_update(self.table, kwargs, where={'meeting_id':meeting_id})
    
    def add(self, meeting_id, chapter, meeting_deadline):
        """
        ф-я добавляет новую встречу
        """
        db_insert(self.table, meeting_id=meeting_id, created_dt=now(), end_dt=now() + meeting_deadline,
            result='in_process', chapter=chapter)

    def deadline_meetings(self, deadline=None):
        """
        ф-я находит айдишники встреч, у которых прошел дедлайн
        используется как для уведомления о скором дедлайне, 
        так и для закрытия встреч, у которых дедайн уже наступил
        """
        if deadline == None:
            deadline = now()
            rows = db_select(self.table, f"end_dt < {data_type(deadline)}", result='in_process')
        else:
            rows = db_select(self.table, f"created_dt + DateTime::IntervalFromSeconds({int(deadline.total_seconds())}) < {data_type(now())}", result='in_process', notification_id=None)
        rows = [i['meeting_id'] for i in rows]
        return rows
        
class Orders():
    """
    класс для работы с таблицей orders(регистрации на встречу)
    """
    table = tables['orders']

    def current_order(self, tgid):
        """
        ф-я находит текущую регистрацию пользователя на встречу
        """
        rows = db_select(self.table, tgid=tgid, meeting_id=None)
        return rows
    
    def new(self, tgid):
        """
        ф-я логирует новую регистрацию
        """
        db_insert(self.table, tgid=tgid, order_dt=now())

    def delete_order(self, tgid):
        """
        ф-я удаляет регистрацию на встречу, если пользователь отменяет её
        """
        q = f''' delete from {self.table} where tgid = {tgid} and meeting_id is null '''
        db_operation(q)
    
    def update_meeting(self, tgid, meeting_id):
        """
        ф-я заполняет айди встречи у регистрации пользователя
        """
        db_update(self.table, {'meeting_id':meeting_id}, where={'tgid':tgid, 'meeting_id':None})

    def update(self, tgid, **kwargs):
        """
        ф-я для апдейта колонки в таблице
        """
        db_update(self.table, kwargs, where={'tgid':tgid})

class Statistics():
    """
    Класс с функциями для показа статистики
    """

    def users_cnt(self):
        """
        ф-я показывает кол-во зарегистрированных людей
        """
        q = f'''
            select count(*) cnt from {tables['users']}
            '''
        rows = db_operation(q)
        return rows[0]['cnt']
    
    def users_per_chapter(self):
        """

        """
        q = f'''
            select chapter, count(*) as cnt from {tables['users']}
            group by chapter
            order by cnt desc
            '''
        rows = db_operation(q)
        if rows != []:
            rows = [f"{row['cnt']} - {row['chapter']}" for row in rows]
            rows = '\n'.join(rows)
        else:
            rows = 'нет данных'
        return rows
    
    def meetings_per_week(self):
        """

        """
        q = f'''
            select end_week, count(*) as cnt 
            from {tables['meetings']}
            where result = 'done' and 
            CurrentUtcDatetime() - DateTime::IntervalFromDays(90) < end_dt
            group by DateTime::GetWeekOfYear(end_dt) as end_week
            order by end_week desc
            '''
        rows = db_operation(q)
        if rows != []:
            rows = [f"{row['cnt']} - кол-во встреч на неделе {row['end_week']}" for row in rows]
            rows = '\n'.join(rows)
        else:
            rows = 'нет данных'
        return rows
    
    def meetings_per_chapter(self):
        """

        """
        q = f'''
            select chapter, count(*) as cnt 
            from {tables['meetings']}
            where result = 'done' 
            group by chapter
            order by cnt desc
            '''
        rows = db_operation(q)
        if rows != []:
            rows = [f"{row['cnt']} - {row['chapter']}" for row in rows]
            rows = '\n'.join(rows)
        else:
            rows = 'нет данных'
        return rows

    def users_meetings(self):
        """

        """
        q = f'''
            $m = (select t1.tgid as tgid, count(*) cnt from {tables['meetings_user']} t1 
            join {tables['meetings']} t2 on t1.meeting_id = t2.meeting_id 
            where 
            result = 'done'
            group by t1.tgid);
            select cnt as meetings_cnt, count(*) as cnt_users  from $m as m
            group by cnt
            order by meetings_cnt desc
            '''
        rows = db_operation(q)
        if rows != []:
            rows = [f"{row['cnt_users']} - людей провели {row['meetings_cnt']} встреч" for row in rows]
            rows = '\n'.join(rows)
        else:
            rows = 'нет данных'
        return rows


def feedback_add(tgid, feedback):
    """
    ф-я логирует новый фидбек
    """
    db_insert(tables['feedback'], tgid=tgid, feedback=feedback, message_dt=now())

def rejection_add(tgid, meeting_id):
    """
    ф-я логирует отказ пользователя от встречи с партнером
    """
    db_insert(tables['rejections'], tgid=tgid, meeting_id=meeting_id, reject_dt=now())

def meeting_reviews_add(tgid, meeting_id, review):
    """
    ф-я логирует отзыв о встрече
    """
    db_insert(tables['meeting_reviews'], tgid=tgid, meeting_id=meeting_id, message_dt=now(), review=review)

def messagesfrombot_add(tgid, message):
    """
    ф-я логирует сообщение бота пользователю в таблицу messagesfrombot
    """
    db_insert(tables['messagesfrombot'], tgid=tgid, message=quotes(message), message_dt=now())

def message_log(msg):
    """
    ф-я логирует сообщение пользователя в таблицу messages
    """
    db_insert(tables['messages'], tgid=msg['tgid'], message=quotes(msg['text']), message_dt=now())

def message_parse(update):
    """
    ф-я парсит сырое сообщение телеграма, берет то, что нужно и раскладывает в удобный формат
    сообщения могут быть двух видов:
    -сообщение с текстом, который написал пользователь(message)
    -сообщение с информацией из кнопки, которую нажал пользователь(callback_query)
    """
    msg = {}
    if 'callback_query' in update.keys():
        msg['tgid'] = update['callback_query']['from']['id']
        for col in ['username', 'first_name', 'last_name']:
            if col in update['callback_query']['from'].keys():
                msg[col] = update['callback_query']['from'][col]
            else:
                msg[col] = ''
        msg['text'] = update['callback_query']['data']
        msg['timestamp'] = update['callback_query']['message']['date']
        return msg
    elif 'message' in update.keys():
        msg['tgid'] = update['message']['from']['id']
        for col in ['username', 'first_name', 'last_name']:
            if col in update['message']['from'].keys():
                msg[col] = update['message']['from'][col]
            else:
                msg[col] = ''
        if 'text' in update['message'].keys():
            msg['text'] = update['message']['text']
        elif 'photo' in update['message'].keys():
            msg['text'] = '_photo_'
        else:
            msg['text'] = 'unknown_message'
        msg['timestamp'] = update['message']['date']
        return msg

def now():
    """
    ф-я переводит время сервера(гринвич) в московское время
    """
    return datetime.datetime.utcnow() + datetime.timedelta(hours=3)
