import datetime
import operator
from collections import OrderedDict
import pandas as pd


'''CLIENTS
    id integer primary key,
    c_name character varying(100) not null,
    c_surname character varying(100) not null,
    c_sex integer not null'''
Clients = dict()

'''USERS
    id integer primary key,
    u_name character varying(100) not null,
    u_surname character varying(100) not null,
    u_status integer,
    u_age integer,
    u_sex integer'''
Users = dict()

'''CLIENTS_USERS
    client_id integer references Clients(id) not null,
    user_id integer references Users(id) not null,
    liked boolean,
    banned boolean,
    upd timestamptz'''
Clients_Users = dict()

'''SEARCH_USERS
    key: *cl_id integer references Users(id) not null,
    Value: //[dict()]
    search_id integer references Searches(id) not null
    user_id'''
Search_users = dict()

'''SEARCHES
    client_id integer references Clients(id) not null unique,
    city character varying(100),
    city_id integer,
    status_id integer,
    sex integer,
    min_age integer,
    max_age integer,'''
Searches = dict()

'''User_photos(
            photo_id character varying(100) primary key,
            user_id integer references Users(id) not null'''
User_photos = dict()


def get_last_showed_user(client_id):
    res = Clients_Users.get(client_id)
    if res == None:
        return None
    else:
        f_data = sorted(res, key=lambda k: k['upd'], reverse=True)
        if len(f_data) == 0:
            return None
        else:
            return (f_data[0]['user_id'],)

def add_to_ignore(client_id, last_showed_user):
    f_res = Clients_Users.get(client_id)
    f_dict = list(filter(lambda item: item['user_id'] == last_showed_user, f_res))
    if len(f_dict) > 0:
        f_dict[0]['banned'] = True

def add_to_favourites(client_id, last_showed_user):
    f_res = Clients_Users.get(client_id)
    f_dict = list(filter(lambda item: item['user_id'] == last_showed_user, f_res))
    if len(f_dict) > 0:
        f_dict[0]['liked'] = True

def get_list_of_banned(client_id):

    res = Clients_Users.get(client_id)

    if len(res) > 0:
        banned_users = []
        for user in res:
            if user['banned']:
                banned_users.append((user['user_id'], Users.get(user['user_id'])['u_name'],
                                 Users.get(user['user_id'])['u_surname'], Users.get(user['user_id'])['u_age']))
        return banned_users
    else:
        res_with_info = []


    return res_with_info


def get_list_of_liked(client_id):

    res = Clients_Users.get(client_id)

    if len(res) > 0:
        liked_users = []
        for user in res:
            if user['liked']:
                liked_users.append((user['user_id'], Users.get(user['user_id'])['u_name'],
                                     Users.get(user['user_id'])['u_surname'], Users.get(user['user_id'])['u_age']))
        return liked_users
    else:
        return []


def get_dict_of_search_params(user_id):

    s_data = Searches.get(user_id)
    if s_data == None:
        return None
    else:
        s_data.setdefault('city', None)
        s_data.setdefault('city_id', None)
        s_data.setdefault('status_id', None)
        s_data.setdefault('sex', None)
        s_data.setdefault('min_age', None)
        s_data.setdefault('max_age', None)
        s_data.setdefault('upd', '')
        if s_data['upd'] == '':
            s_data.setdefault('upd_dt', None)
        else:
            s_data.setdefault('upd_dt', datetime.datetime.strptime(s_data['upd'], '%Y-%m-%d %H:%M:%S'))
        return s_data

def clear_table_Search_users(cl_id):
    c_res = Search_users.get(cl_id)
    if c_res != None:
        c_res = []

def find_users(user_id):
    list_users = []
    res = Search_users.get(user_id)
    if res != None:
        res_cu = Clients_Users.get(user_id)
        viewed_users = [] if res_cu == None else [i_user['user_id'] for i_user in res_cu]
        # list_users = [(i_res,) for i_res in res if i_res not in viewed_users]
        list_users = [(i_res['user_id'], Users[i_res['user_id']]['u_name'],
                       Users[i_res['user_id']]['u_surname'], Users[i_res['user_id']]['u_age'])
                      for i_res in res if i_res['user_id'] not in viewed_users]
        return list_users[0]

    else:
        return None

def fill_t_for_test():
    Clients_Users['2'] = [{'user_id': 200011, 'liked': False, 'banned':False, 'upd': ''},
                          {'user_id': 200022, 'liked': False, 'banned':False, 'upd': ''},
                          {'user_id': 200033, 'liked': False, 'banned':False, 'upd': ''},
                          {'user_id': 200044, 'liked': False, 'banned':False, 'upd': ''},
                          {'user_id': 200055, 'liked': False, 'banned':False, 'upd': ''}]
    Clients_Users['3'] = [{'user_id': 300011, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 300022, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 300033, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 300044, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 300055, 'liked': False, 'banned': False, 'upd': ''}]
    Clients_Users['4'] = [{'user_id': 400011, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 400022, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 400033, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 400044, 'liked': False, 'banned': False, 'upd': ''},
                          {'user_id': 400055, 'liked': False, 'banned': False, 'upd': ''}]

    Search_users['2'] = [200033, 200034, 200035, 200036, 2000373, 200037]

if __name__ == '__main__':
    fill_t_for_test()
    #add_to_ignore('3', 300044)
    find_users('2')
