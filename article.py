import vk_api
from vk_api import VkUpload
from vk_api.utils import get_random_id
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from datetime import datetime
import pymysql.cursors
import time
import requests
import random
import os
import json
import urllib.request
import re
import threading

def get_connection():
    connection = pymysql.connect(host='localhost',
                                 user='%',
                                 password='%',
                                 db='%',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def add_to_database(functionMode, x):
    #Создаем новую сессию
    connection = get_connection()
    #Наш запрос
    cursor = connection.cursor()
    sql = "INSERT INTO mode (Id_User, Mode) VALUES (%s, %s) ON DUPLICATE KEY UPDATE Mode = %s"
    #Выполняем наш запрос и вставляем свои значения
    cursor.execute(sql, (x, functionMode, functionMode))
    connection.commit()
    return functionMode

def number_is_num(num):
    if num.isdigit() == True:
        if int(num) >= 0 and int(num) <= 100:
            return True
    return False
    
def add_url_photo(id, url):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET photo = %s WHERE iduser = %s"
            cursor.execute(sql, (url, id))
            connection.commit()
    finally:
        connection.close()
    return

def update_ready(id, ready):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET ready = %s WHERE iduser = %s"
            cursor.execute(sql, (ready, id))
            connection.commit()
    finally:
        connection.close()
    return
    
def take_ready(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT ready FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            ready = cursor.fetchone()
            if ready == None:
                finaltext = "У вас пока нет анкеты, чтобы настраивать этот параметр"
            elif str(ready["ready"]) == '0':
                finaltext = "В данный момент у тебя стоит запрет на поиск"
            elif str(ready["ready"]) == '1':
                finaltext = "В данный момент у тебя разрешен поиск"
            else:
                finaltext = "Произошла какая-то ошибка, обратись к админу"
    finally:   
        connection.close()
    return finaltext

def check_alarm(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT message_about FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            ready = cursor.fetchone()
            if ready == None:
                finaltext = "Произошла какая-то ошибка, обратись к админу"
            elif str(ready["message_about"]) == '0':
                finaltext = "В данный момент у тебя разрешены уведомления"
            elif str(ready["message_about"]) == '1':
                finaltext = "В данный момент у тебя запрещены уведомления"
            else:
                finaltext = "Произошла какая-то ошибка, обратись к админу"
    finally:  
        connection.close()
    return finaltext

def update_alarm(id, alarm):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE user SET message_about = %s WHERE iduser = %s"
            cursor.execute(sql, (alarm, id))
            connection.commit()
    finally:
        connection.close()
    return

def pol_is_pol(pol):
    if pol == 'Ж' or pol == 'М':
        return True
    return False

def pol_is_poisk(pol):
    if pol == 'Ищу Ж' or pol == 'Ищу М' or pol == 'Без разницы':
        return True
    return False

def msg_is_yes_no(textmsg):
    if textmsg == 'Да' or textmsg == 'Нет':
        return True
    return False
    
def take_photo_url(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT photo FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            photo = cursor.fetchone()
            photo_take = str(photo["photo"])
            if photo == ():
                finalphoto = "0"
            elif photo_take == "0":
                finalphoto = "0"
            else:
                finalphoto = photo_take
    finally:
        connection.close()
    return finalphoto

def check_m_w(id, textmsg):
    if str(textmsg) == 'М':
        textmsg_bool = '0'
    if str(textmsg) == 'Ж':
        textmsg_bool = '1'
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            gender = cursor.fetchone()
            if gender["last_read"] == None:
                finaltext = True
            elif str(gender["gender"]) == textmsg_bool:
                finaltext = True
            elif str(gender["gender"]) != textmsg_bool:
                finaltext = False
            else:
                finaltext = True
    finally:   
        connection.close()
    return finaltext
    
def check_reaction(id, id_pont):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM relations WHERE idone = %s AND idtwo = %s"
            cursor.execute(sql, (id_pont, id))
            row = cursor.fetchone()
            if row == None:
                finaltext = "0"
            elif str(row["reaction"]) == "1":
                finaltext = "1"
            else:
                finaltext = "0"
        connection.commit()
    finally:   
        connection.close()
    return finaltext

def add_new_temp_line(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user_temp WHERE iduser_temp = %s"
            cursor.execute(sql, (id))
            row = cursor.fetchone()
            if row == None:
                sql = "INSERT INTO user_temp (iduser_temp) VALUES (%s)"
                cursor.execute(sql, (id))
        connection.commit()
    finally:   
        connection.close()
    return

def check_settings(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            finalt = cursor.fetchone()
            if finalt == None:
                finaltext = "0"
            elif str(finalt["blacklist"]) == '1':
                finaltext = "К сожалению, вы заблокированы в системе и не можете пользоваться ботом"
            else:
                finaltext = "0"
    finally:   
        connection.close()
    return finaltext

def check_before(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            finalt = cursor.fetchone()
            if finalt == None:
                finaltext = "У тебя нет фото и анкеты\n\nИзменить фото и анкету ты можешь в Настройках"
            elif str(finalt["blacklist"]) == '1':
                finaltext = "К сожалению, вы заблокированы в системе и не можете пользоваться ботом"
            elif finalt["name"] == None and str(finalt["photo"]) != '0':
                finaltext = "У тебя есть фото, но нет анкеты\n\nЗаполнить анкету ты можешь в Настройках"
            elif finalt["name"] == None and str(finalt["photo"]) == '0':
                finaltext = "У тебя нет фото и анкеты\n\nИзменить фото и анкету ты можешь в Настройках"
            elif finalt["name"] != None and str(finalt["photo"]) == '0':
                finaltext = "У тебя есть анкета, но нет фото\n\nЗагрузить фото ты можешь в Настройках"
            elif finalt["name"] != None and str(finalt["photo"]) != '0':
                finaltext = "0"
            if finalt != None and str(finalt["blacklist"]) == '0':
                if str(finalt["ready"]) == '0' and finaltext == '0':
                    finaltext = "Ты не разрешил поиск в Настройках\n\nДля этого зайди в Настройки и выбери пункт «Включить/выключить поиск»"
    finally:   
        connection.close()
    return finaltext

def take_data(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            name = cursor.fetchone()
            if name["name"] == None:
                finaltext = None
            else:
                sql = "SELECT gender FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                gender = cursor.fetchone()
                sql = "SELECT age FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                age = cursor.fetchone()
                if str(gender["gender"]) == '0':
                    pol = 'М'
                else:
                    pol = 'Ж'
                sql = "SELECT profile FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                profile = cursor.fetchone()
                if str(age["age"]) == '0' and str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol
                elif str(age["age"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + "\n\n" + str(profile["profile"])
                elif str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"])
                else:
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"]) + "\n\n" + str(profile["profile"])
    finally:
        connection.close()
    return finaltext

def take_temp_data(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name FROM user_temp WHERE iduser_temp = %s"
            cursor.execute(sql, (id))
            name = cursor.fetchone()
            if name["name"] == None:
                finaltext = "Что-то пошло не так с данными, обратись к админу"
            else:
                sql = "SELECT age FROM user_temp WHERE iduser_temp = %s"
                cursor.execute(sql, (id))
                age = cursor.fetchone()
                sql = "SELECT gender FROM user_temp WHERE iduser_temp = %s"
                cursor.execute(sql, (id))
                gender = cursor.fetchone()
                if str(gender["gender"]) == '0':
                    pol = 'М'
                else:
                    pol = 'Ж'
                sql = "SELECT profile FROM user_temp WHERE iduser_temp = %s"
                cursor.execute(sql, (id))
                profile = cursor.fetchone()
                sql = "SELECT name FROM user_temp WHERE iduser_temp = %s"
                cursor.execute(sql, (id))
                name = cursor.fetchone()
                if str(age["age"]) == '0' and str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol
                elif str(age["age"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + "\n\n" + str(profile["profile"])
                elif str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"])
                else:
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"]) + "\n\n" + str(profile["profile"])
    finally:
        connection.close()
    return finaltext
    
def take_data(id):
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT name FROM user WHERE iduser = %s"
            cursor.execute(sql, (id))
            name = cursor.fetchone()
            if name["name"] == None:
                finaltext = None
            else:
                sql = "SELECT gender FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                gender = cursor.fetchone()
                sql = "SELECT age FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                age = cursor.fetchone()
                if str(gender["gender"]) == '0':
                    pol = 'М'
                else:
                    pol = 'Ж'
                sql = "SELECT profile FROM user WHERE iduser = %s"
                cursor.execute(sql, (id))
                profile = cursor.fetchone()
                if str(age["age"]) == '0' and str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol
                elif str(age["age"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + "\n\n" + str(profile["profile"])
                elif str(profile["profile"]) == '0':
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"])
                else:
                    finaltext = str(name["name"]) + ", " + pol + ", " + str(age["age"]) + "\n\n" + str(profile["profile"])
    finally:
        connection.close()
    return finaltext

def main():
    threading.Thread(target=main_main).start()
    
def main_main():
    while True:
        threading.current_thread().name = "main_main"
        session = requests.Session()
        vk_session = vk_api.VkApi(token="%")
        vk = vk_session.get_api()
        upload = VkUpload(vk_session)
        longpoll = VkBotLongPoll(vk_session, "%")
        lock = threading.Lock()
        try:
            for event in longpoll.listen():
                if event.type == VkBotEventType.MESSAGE_NEW:
                    if str(threading.current_thread().name) != str(event.obj.from_id):
                        lock.acquire()
                        try:
                            flag = 0
                            for thread in threading.enumerate():
                                if str(thread.name) == str(event.obj.from_id):
                                    flag = 1
                                    break
                            if flag == 0:
                                user_get = vk.users.get(user_ids = (event.obj.from_id))
                                first_name = user_get[0]["first_name"]
                                for i in range(0,5):
                                        try:
                                            vk.messages.send(
                                                user_id=event.obj.from_id,
                                                random_id=get_random_id(),
                                                keyboard=open('keyboard.json', 'r', encoding='UTF-8').read(),
                                                message="Бот готов к работе c тобой, " + str(first_name))
                                        except Exception as err:
                                            print(err)
                                            print("Ошибка отправки сообщения у id" + str(event.obj.from_id))
                                            time.sleep(0.5)
                                            continue
                                        break
                                id = event.obj.from_id
                                date_last = datetime.now()
                                threading.Thread(target=main_start, args=(id,)).start()
                        finally:
                            lock.release()
                    else:
                        print("zashli")
                        lock5 = threading.Lock()
                        lock5.acquire()
                        try:
                            print("zashli2")
                            listthread = []
                            for thread in threading.enumerate():
                                    listthread.append(str(thread.name))
                            print("eto listthread")
                            print(listthread)
                            connection = get_connection()
                            try:
                                with connection.cursor() as cursor:
                                    listsql = []
                                    sql = "SELECT idthread FROM thread_list"
                                    cursor.execute(sql, (id))
                                    listsql = [idthread['idthread'] for idthread in cursor.fetchall()]
                                    result = list(set(listsql) - set(listthread))
                                    print("eto result")
                                    print(result)
                                    for name_thread in result:
                                        sql = "DELETE FROM thread_list WHERE idthread = %s"
                                        cursor.execute(sql, (name_thread))
                                    result = list(set(listthread) - set(listsql))
                                    print("eto result 2")
                                    print(result)
                                    for name_thread in result:
                                        sql = "INSERT INTO thread_list (idthread) VALUES (%s)"
                                        cursor.execute(sql, (name_thread))
                                    connection.commit()
                            finally:
                                connection.close()
                        finally:
                            lock5.release()
        except Exception:
            pass
