import os
from datetime import datetime

from DB.db_tables import create_commands, delete_commands
import psycopg2
from configparser import ConfigParser
import sqlalchemy
from VK.Search_data import status_dict, sex_dict
from Cache import cache
import sys


class DB():

    def __init__(self, dir_path='\\DB', DB_ERROR=False):
        self.dir_path = dir_path
        db_settings = self.__get_db_settings__()
        self.connection = None
        self.sa_connection = None
        self.db_name = db_settings.get('db_name')
        self.db_user = db_settings.get('user')
        self.db_password = db_settings.get('password')
        self.db_port = db_settings.get('port')
        if DB_ERROR:
            self.connection = None
            self.sa_connection = None
        else:
            self._get_db_connection(db_settings)
            self._get_sa_connection()



    def _get_sa_connection(self):
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{self.db_user}:{self.db_password}@localhost:{self.db_port}/{self.db_name}')
        try:
            self.sa_connection = engine.connect()
        except  sqlalchemy.exc.OperationalError as er:
            #inf = sys.exc_info()
            print(f"Ошибка подключения '{er}'")
            self.sa_connection = False


    def __get_db_settings__(self):
        config = ConfigParser()
        config.read(os.getcwd() + self.dir_path + '\\db_settings.ini')
        return {
        'db_name' : config.get('DB', 'db_name'),
        'user' : config.get('DB', 'user'),
        'password' : config.get('DB', 'password'),
        'port' : config.getint('DB', 'port')
        }

    def _get_db_connection(self, db_settings):
        try:
            self.connection = psycopg2.connect(
            database=db_settings['db_name'],
            user=db_settings['user'],
            password=db_settings['password'],
            port=db_settings['port']
            )
        except psycopg2.OperationalError as e:
            #inf = sys.exc_info()
            print(f"Ошибка подключения '{e}'")
            self.connection = False
            return self.connection
        return self.connection

    def execute_query(self, query):
        self.connection.autocommit = True
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
        except psycopg2.OperationalError as e:
            print(e)


    def create_tables(self):
        for query in create_commands:
            self.execute_query(query=query)

    def user_exist(self, user_id, table_name):
        try:
            return self.sa_connection.execute(f'''SELECT id FROM {table_name} WHERE id = {user_id}''').fetchone()
        except sqlalchemy.exc.OperationalError as er:
            return ('DB error', )
        except AttributeError as at_er:
            return ('DB error', )

    def _delete_tables(self):
        for query in delete_commands:
            self.execute_query(query=query)

    def add_client(self, vk_id, c_name, c_surname, c_sex, to_cache_only=False):

        if to_cache_only:
            cache.Clients[vk_id] = {'c_name':c_name, 'c_surname':c_surname, 'c_sex':c_sex, 'c_upd': f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}'}
        else:
            self.sa_connection.execute(f'''INSERT INTO Clients(id, c_name, c_surname, c_sex, c_upd) 
                                        VALUES('{vk_id}', '{c_name}','{c_surname}', {c_sex},'{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}')
                                        ON CONFLICT (id) DO UPDATE SET
                                        c_name = '{c_name}', 
                                        c_surname = '{c_surname}', 
                                        c_sex = {c_sex}, 
                                        c_upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' ''')

    def save_search(self, user_id, column_name, column_value, DB_ERROR=False):
        # cl_id = self.client_id_by_vk_id(user_id)
        if not DB_ERROR:
            founded_line = self.sa_connection.execute(f'''SELECT * FROM searches WHERE client_id = {user_id}''').fetchone()
        else:
            founded_line = cache.Searches.get(user_id)

        if DB_ERROR:
            u_search = cache.Searches.get(user_id)
            if u_search == None:
                cache.Searches[user_id] = dict()
            cache.Searches[user_id][column_name] = column_value
            cache.Searches[user_id]['upd'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        else:
            if founded_line:
                if type(column_value) == str:
                    column_value = f"'{column_value}'"
                self.sa_connection.execute(f'''UPDATE searches SET {column_name} = {column_value}, upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' WHERE client_id = {user_id}''')
            else:
                self.sa_connection.execute(f'''INSERT INTO Searches(client_id, {column_name}, upd)
                                            VALUES({user_id}, {column_value}, '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}')''')

    def delete_search_params(self, user_id):
        self.sa_connection.execute(f'''DELETE FROM Searches WHERE client_id = {user_id}''')

    def clear_table_clients_users(self, cl_id, DB_ERROR):

        if not DB_ERROR:
            self.sa_connection.execute(f'''DELETE FROM Search_Users WHERE search_id IN (SELECT id FROM Searches WHERE client_id = {cl_id} LIMIT 1)''')
        else:
            cache.clear_table_Search_users(cl_id)


    def get_current_search_params(self, user_id, DB_ERROR=False):

        if not DB_ERROR:
            search_params = self.sa_connection.execute(f'''SELECT city, status_id, sex, min_age, max_age FROM searches WHERE client_id = {user_id}''').fetchone()
        else:
            cache_search_params = cache.get_dict_of_search_params(user_id)
            if cache_search_params == None:
                search_params = None
            else:
                search_params = (cache_search_params['city'],
                             cache_search_params['status_id'],
                             cache_search_params['sex'],
                             cache_search_params['min_age'],
                             cache_search_params['max_age'])

        if search_params:
            if search_params != None:
                current_search_status = status_dict.get(str(search_params[1]))
                current_search_sex = sex_dict.get(str(search_params[2]))
            else:
                current_search_status = ''
                current_search_sex = ''


            search_str = f'''       Город: {search_params[0]}\n         Статус: {current_search_status}\n        Пол: {current_search_sex}\n        от {search_params[3]}\n         до {search_params[4]}'''
        else:
            search_str = f'''       Город: \n       Статус: \n      Пол: \n         от: \n         до: '''
        return f'   Параметры поиска:\n     {search_str.replace("None", "")}'

    def get_dict_of_search_params(self, user_id, DB_ERROR=False):

        if DB_ERROR:
            return cache.get_dict_of_search_params(user_id)
        else:
            search_params = self.sa_connection.execute(
                f'''SELECT city, city_id, status_id, sex, min_age, max_age, upd, id FROM searches WHERE client_id = {user_id}''').fetchone()
            if search_params == None:
                return None
            else:

                return {'city':search_params[0], 'city_id':search_params[1],
                    'status_id':search_params[2], 'sex':search_params[3],
                    'min_age':search_params[4], 'max_age':search_params[5], 'upd': search_params[6], 'id': search_params[7],
                    'upd_dt': search_params[6]}

    def find_users(self, user_id):
        '''Поиск в БД'''

        search_res = self.sa_connection.execute(f'''SELECT u_id, u_name, u_surname, u_age FROM Search_users
                                                    JOIN Users ON Users.id = Search_users.u_id 
                                                    WHERE Search_users.u_id != {user_id}
                                                    AND Search_users.u_id NOT IN (
                                SELECT user_id FROM Clients_Users
                                WHERE Clients_Users.client_id = {user_id}) LIMIT 1''').fetchone()
        return search_res


    def add_users_into_Users(self, user_id, search_list, DB_ERROR=False):

        if not DB_ERROR:
            record_list = [ (i["vk_id"], i["u_name"], i["u_surname"], i["u_status"], i["u_age"], i["u_sex"]) for i in search_list]
            cursor = self.connection.cursor()
            insert_query = f'''INSERT INTO USERS(id, u_name, u_surname, u_status, u_age, u_sex, u_upd) VALUES (%s,%s,%s,%s,%s,%s,'{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}') 
                                ON CONFLICT(id) DO NOTHING'''
            result = cursor.executemany(insert_query, record_list)
            self.connection.commit()
            print('test')
        else:
            for i in search_list:
                cache.Users[i['vk_id']] = {'u_name':i['u_name'], 'u_surname':i['u_surname'], 'u_status':i['u_status'],
                                           'u_age':i['u_age'], 'u_sex':i['u_sex']}

    def add_into_Search_Users(self, user_id, search_list, DB_ERROR=False):

        if not DB_ERROR:
            search_id = self.sa_connection.execute(f'''SELECT id FROM Searches WHERE client_id = {user_id} LIMIT 1''').fetchone()[0]

            record_list = [(i["vk_id"], search_id) for i in search_list]

            self.clear_table_search_users(search_id)
            cursor = self.connection.cursor()
            insert_query = f'''INSERT INTO Search_Users(u_id, search_id) VALUES (%s,%s)'''
            result = cursor.executemany(insert_query, record_list)
            self.connection.commit()
        else:
            client_users_list = cache.Search_users.get(user_id, [])

            for i in search_list:
                client_users_list.append({'user_id': i['vk_id']})

            cache.Search_users[user_id] = client_users_list

    def delete_photos_from_bd(self, user_id, DB_ERROR=False):
        if not DB_ERROR:
            self.sa_connection.execute(f'''DELETE FROM User_Photos WHERE user_id = {user_id}''')
        else:
            cache.User_photos.pop(user_id, None)

    def add_photos_to_bd(self, user_id, user_photos, DB_ERROR=False):

        if not DB_ERROR:
            record_list = [(user_id, i) for i in user_photos]

            cursor = self.connection.cursor()
            insert_query = f'''INSERT INTO User_Photos(user_id, photo_id) VALUES (%s,%s)'''
            result = cursor.executemany(insert_query, record_list)
            self.connection.commit()
        else:
            photos_list = cache.User_photos.get(user_id, [])

            for i in user_photos:
                photos_list.append(i)

            cache.User_photos[user_id] = photos_list

    def clear_table_search_users(self, search_id):

        self.sa_connection.execute(f'''DELETE FROM Search_Users WHERE search_id = {search_id}''')


    def add_user_to_base(self, user_data):

        user_exists = self.user_exist(user_data['vk_id'], 'Users')

        if user_exists == None:
            self.sa_connection.execute(f'''INSERT INTO Users(vk_id, u_name, u_surname, u_age, u_birthyear, u_sex, u_upd)
                        VALUES({user_data['vk_id']}, '{user_data['u_name']}', '{user_data['u_surname']}',
                        {user_data['u_age']}, {user_data['u_birthyear']}, {user_data['u_sex']}, 
                        '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}')''')
        else:
            self.sa_connection.execute(f'''UPDATE Users SET vk_id = {user_data['vk_id']}, 
                                        u_name = {user_data['u_name']},
                                        u_surname = {user_data['u_surname']},
                                        u_age = {user_data['u_age']},
                                        u_birthyear = {user_data['u_birthyear']},
                                        u_sex = {user_data['u_sex']},
                                        upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}')''')

    def add_to_photos(self, user_id, photo_list):
        for ph_id in photo_list:
            photo_exist = self.sa_connection.execute(f'''SELECT id FROM Photos WHERE photo_id = '{ph_id}' ''').fetchone()
            if not photo_exist:
                user_db_id = self.sa_connection.execute(f'''SELECT id FROM Users WHERE vk_id = {user_id}''').fetchone()[0]
                self.sa_connection.execute(f'''INSERT INTO Photos(likes_count, photo_id, user_id, upd) 
                        VALUES(0, '{ph_id}', {user_db_id}, '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}') ''')

    def get_client_db_id(self, client_id):
        res = self.sa_connection.execute(f'''SELECT id FROM Clients WHERE vk_id = {client_id}''').fetchone()
        if res != None:
            return res[0]
        else:
            return None

    def get_user_db_id(self, user_id):
        res = self.sa_connection.execute(f'''SELECT id FROM Users WHERE vk_id = {user_id}''').fetchone()
        if res != None:
            return res[0]
        else:
            return None

    def add_update_to_clients_users(self, client_id, user_id, u_liked=False, u_banned=False, DB_ERROR=False):
        if not DB_ERROR:
            str_exist = self.sa_connection.execute(f'''SELECT Clients_Users.id FROM Clients_Users 
                                                    JOIN Clients ON Clients.id = Clients_Users.client_id 
                                                    WHERE client_id = {client_id} AND
                                                    user_id = {user_id}''').fetchone()
        else:
            cl_exist = cache.Clients_Users.get(client_id)
            if cl_exist:
                str_exist = user_id in cl_exist
            else:
                str_exist = None

        if not DB_ERROR:
            if str_exist == None:

                self.sa_connection.execute(f'''INSERT INTO Clients_Users(client_id, user_id, liked, banned, upd)
                    VALUES({client_id}, {user_id}, '{u_liked}', '{u_banned}', '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' )''')

            else:
                self.sa_connection.execute(f'''UPDATE Clients_Users SET client_id = {client_id},
                    user_id = {user_id}, liked = '{u_liked}', banned = '{u_banned}',
                    upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' WHERE client_id = {client_id}
                    AND user_id = {user_id}''')
        else:
            # users_of_clients = cache.Clients_Users.get(client_id,[])
            # cache.Clients_Users[client_id] = {'user_id': user_id, 'liked': u_liked, 'banned': u_banned, 'upd': f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}'}
            uc_by_client = cache.Clients_Users.setdefault(client_id,[])
            uc_by_client.append({'user_id': user_id, 'liked': u_liked, 'banned': u_banned, 'upd': f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}'})

    def get_last_showed_user(self, client_id, DB_ERROR=False):

        if not DB_ERROR:
            res = self.sa_connection.execute(f'''SELECT user_id FROM Clients_Users WHERE client_id = 2061397
                                        ORDER BY upd DESC 
                                        LIMIT 1''').fetchone()
        else:
            res = cache.get_last_showed_user(client_id)

        return res

    def add_to_ignore_in_db(self, client_id, last_showed_user, DB_ERROR=False):

        if not DB_ERROR:
            self.sa_connection.execute(f'''UPDATE Clients_Users SET client_id = {client_id},
                        user_id = {last_showed_user}, banned = '{True}',
                        upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' WHERE client_id = {client_id}
                        AND user_id = {last_showed_user}''')
        else:
            cache.add_to_ignore(client_id, last_showed_user)

    def add_to_favourites_in_db(self, client_id, last_showed_user, DB_ERROR=False):
        if not DB_ERROR:
            self.sa_connection.execute(f'''UPDATE Clients_Users SET client_id = {client_id},
                        user_id = {last_showed_user}, liked = '{True}',
                        upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' WHERE client_id = {client_id}
                        AND user_id = {last_showed_user}''')
        else:
            cache.add_to_favourites(client_id, last_showed_user)

    def get_banned_users(self, client_id, DB_ERROR=False):

        if not DB_ERROR:
            res = self.sa_connection.execute(f'''SELECT cu.user_id, us.u_name, us.u_surname, us.u_age FROM Clients_users cu
                                                LEFT JOIN Users us ON us.id = cu.user_id
                                                WHERE cu.client_id = {client_id} AND cu.banned = {True}''').fetchall()
        else:
            res = cache.get_list_of_banned(client_id)

        return res

    def get_favourite_users(self, client_id, DB_ERROR=False):

        if not DB_ERROR:
            res = self.sa_connection.execute(f'''SELECT cu.user_id, us.u_name, us.u_surname, us.u_age FROM Clients_users cu
                                                LEFT JOIN Users us ON us.id = cu.user_id
                                                WHERE cu.client_id = {client_id} AND cu.liked = {True}''').fetchall()
        else:
            res = cache.get_list_of_liked(client_id)

        return res

    def user_have_search_params(self, user_id):
        search_params = self.sa_connection.execute(f'''SELECT id, city_id, status_id, sex, min_age, max_age, upd FROM Searches s 
                                        LEFT JOIN Clients on s.id = Clients.id
                                        WHERE Clients.id = {user_id}''').fetch_one()
        if search_params == None:
            return False, None
        else:
            return True, search_params


    def _from_cache_to_Clients(self):


        for key, value in cache.Clients.items():
            self.sa_connection.execute(f'''INSERT INTO Clients(id, c_name, c_surname, c_sex, c_upd)
                                VALUES({key}, '{value['c_name']}', '{value['c_surname']}', {value['c_sex']}, 
                                '{value['c_upd']}' )
                                ON CONFLICT (id) DO UPDATE SET
                                c_name = '{value['c_name']}', 
                                c_surname = '{value['c_surname']}', 
                                c_sex = {value['c_sex']}, 
                                c_upd = '{value['c_upd']}' ''')

    def _from_cache_to_Users(self):
        for key, value in cache.Users.items():
            self.sa_connection.execute(f'''INSERT INTO USERS(id, u_name, u_surname, u_status, u_age, u_sex, u_upd) 
                                VALUES({key}, '{value['u_name']}', '{value['u_surname']}', 
                                {value['u_status'] if value['u_status'] != None else 'Null'}, 
                                {value['u_age']}, {value['u_sex']},'{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' )
                                ON CONFLICT (id) DO UPDATE SET
                                u_name = '{value['u_name']}', 
                                u_surname = '{value['u_surname']}', 
                                u_sex = {value['u_sex']}, 
                                u_age = {value['u_age']},
                                u_status = {value['u_status'] if value['u_status'] != None else 'Null'},
                                u_upd = '{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' ''')

    def _from_cache_to_Clients_Users(self):

        for key, value in cache.Clients_Users.items():
            for cl_couple in value:
                self.sa_connection.execute(f'''INSERT INTO Clients_Users(client_id, user_id, liked, banned,  upd)
                                VALUES({key}, {cl_couple['user_id']}, {cl_couple['liked']}, {cl_couple['banned']}, 
                                '{cl_couple['upd']}')
                                ON CONFLICT (client_id, user_id) DO UPDATE SET
                                client_id = {key},
                                user_id = {cl_couple['user_id']},
                                liked = {cl_couple['liked']},
                                banned = {cl_couple['banned']},
                                upd = '{cl_couple['upd']}'
                                ''')

    def _from_cache_to_Searches(self):


        for cl_id in cache.Searches.keys():
            self.delete_search_params(cl_id)
            res = self.sa_connection.execute(f'''INSERT INTO Searches(client_id, city, city_id, status_id, sex, min_age, max_age, upd)
                                VALUES({cl_id}, '{cache.Searches[cl_id]['city']}', 
                                {cache.Searches[cl_id]['city_id'] if cache.Searches[cl_id]['city_id'] != None else 'Null'}, 
                                {cache.Searches[cl_id]['status_id'] if cache.Searches[cl_id]['status_id'] != None else 'Null'},
                                {cache.Searches[cl_id]['sex'] if cache.Searches[cl_id]['sex'] != None else 'Null'},
                                {cache.Searches[cl_id]['min_age'] if cache.Searches[cl_id]['min_age'] != None else 'Null'},
                                {cache.Searches[cl_id]['max_age'] if cache.Searches[cl_id]['max_age'] != None else 'Null'}, 
                                '{cache.Searches[cl_id]['upd']}')''')


    def _from_cache_to_Search_Users(self):

        for cl_id in cache.Search_users.keys():
            search_id = self.sa_connection.execute(f'''SELECT id FROM Searches WHERE client_id = {cl_id}''').fetchone()
            self.clear_table_search_users(search_id[0])

            record_list = [(f_user['user_id'], search_id[0]) for f_user in cache.Search_users[cl_id]]
            cursor = self.connection.cursor()
            insert_query = f'''INSERT INTO Search_Users(u_id, search_id) VALUES (%s,%s) ON CONFLICT (search_id, u_id) DO NOTHING'''
            # self.sa_connection.execute(f'''INSERT INTO Search_Users(u_id, search_id) VALUES ({s_us}, {search_id})
            #                             ON CONFLICT (u_id, search_id) DO NOTHING''')

            result = cursor.executemany(insert_query, record_list)
            self.connection.commit()


    def write_from_cache_to_DB(self):
        self._from_cache_to_Clients()
        self._from_cache_to_Users()
        self._from_cache_to_Clients_Users()
        self._from_cache_to_Searches()
        self._from_cache_to_Search_Users()


if __name__=='__main__':
    print('test')
