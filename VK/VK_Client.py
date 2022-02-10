import vk_api
import vk_api.exceptions as v_ex
from DB.db_app import DB
from Common.Common_functions import date_by_parts, age_by_bdate
from Cache import cache


class VK_Client():

    def __init__(self, u_token, user_id, DB_ERROR=False):
        self.u_token = u_token
        self.vk = vk_api.VkApi(token=u_token)
        self.user_id = user_id
        self.DB = DB(DB_ERROR=DB_ERROR)
        if not DB_ERROR:
            self.search_params = self.DB.get_dict_of_search_params(user_id, self.DB.connection == False)
        else:
            self.search_params = cache.get_dict_of_search_params(user_id)

    def find_city(self, city_str):
        try:
            finded_city = self.vk.method('database.getCities', {'country_id': 1, 'q': city_str, 'need_all': 0, 'count':1})
            if finded_city['count'] > 0:
                return True, finded_city['items'][0]['id'], finded_city['items'][0]['title']
            else:
                return False, 'город на найден', ''
        except v_ex.ApiError as er:
            return False, er.error['error_msg'], ''

    def find_next(self, DB_ERROR=False):

        if DB_ERROR == True:
            res = cache.find_users(self.user_id)
        else:
            res = self.DB.find_users(self.user_id)


        if res != None:
            f_user = {'user_link': f'http://vk.com/id{res[0]}', 'user_id': res[0], 'attachments': []}
            user_photos = self.get_user_popular_photos(user_id=res[0])
            f_user['attachments'] = user_photos
            f_user['name'] = res[1]
            f_user['age'] = res[3]
            self.DB.delete_photos_from_bd(res[0], DB_ERROR)
            self.DB.add_photos_to_bd(res[0], user_photos, DB_ERROR)
            self.DB.add_update_to_clients_users(self.user_id, res[0], DB_ERROR=DB_ERROR)
            return True, f_user

        else:
            offset = 0
            self.get_users_from_vk(self.search_params)
            res = self.DB.find_users(self.user_id)
            if res == None:
                return False, 'Пользователи не найдены'
            else:
                f_user = {'user_link': f'http://vk.com/id{res[0]}', 'user_id': res[0], 'attachments': []}
                user_photos = self.get_user_popular_photos(user_id=res[0])
                f_user['attachments'] = user_photos
                f_user['name'] = res[1]
                f_user['age'] = res[3]
                self.DB.delete_photos_from_bd(res[0])
                self.DB.add_photos_to_bd(res[0], user_photos, DB_ERROR)
                self.DB.add_update_to_clients_users(self.user_id, res[0], DB_ERROR=DB_ERROR)
                return True, f_user


    def get_user_popular_photos(self, user_id, count=3):
        params = {'owner_id': user_id, 'extended': 1, 'album_id': 'profile'}
        try:
            res = self.vk.method('photos.get', params)
            photos_stat = {}

            for photo in res['items']:
                photos_stat[photo['id']] = photo['likes']['count'] + photo['comments']['count']

            sorted_dict = {k: photos_stat[k] for k in sorted(photos_stat, key=photos_stat.get, reverse=True)}
            print('test')
            popular_photos = []

            for p in sorted_dict:
                if len(popular_photos) == count:
                    break
                popular_photos.append(str(p))

        except v_ex.ApiError as er:
            popular_photos = self.get_profile_photo_of_closed_user(user_id)

        return popular_photos


    def get_profile_photo_of_closed_user(self, user_id):
        res = self.vk.method('users.get', {'user_ids': user_id, 'fields': 'crop_photo'})

        if res[0].get('crop_photo') != None:
            return [f'{res[0]["crop_photo"]["photo"]["id"]}']
        else:
            return []

    def get_users_from_vk(self, search_params, offset=0, search_count=7000, DB_ERROR=False):

        s_params = {'offset': offset, 'sort': 0, 'count': 900, 'fields': 'sex, bdate, photo_id, relation'}
        if search_params['city_id'] != None:
            s_params['city'] = search_params['city_id']
        s_params['country'] = 1
        if search_params['status_id'] != None:
            s_params['status'] = search_params['status_id']
        if search_params['sex'] != None:
            s_params['sex'] = search_params['sex']
        if search_params['min_age'] != None:
            s_params['age_from'] = search_params['min_age']
        if search_params['max_age'] != None:
            s_params['age_to'] = search_params['max_age']


        search_list = []
        find_else = True
        while find_else:
            res = self.find_people_in_vk(s_params)
            for vk_user in res:
                user_data = {'vk_id': vk_user['id'], 'u_name': vk_user['first_name'], 'u_surname': vk_user['last_name'],
                         'u_age': age_by_bdate(vk_user.get('bdate')),
                         'u_birthyear': date_by_parts(vk_user.get('bdate'))['year'],
                         'u_status': vk_user.get('relation'),
                         'u_sex': vk_user['sex']}
                search_list.append(user_data)
            if len(search_list) >= search_count:
                find_else = False
            if len(res) == 0:
                find_else = False
            s_params['offset'] += len(res)

        self.DB.add_users_into_Users(self.user_id, search_list, DB_ERROR)
        self.DB.add_into_Search_Users(self.user_id, search_list, DB_ERROR)

        return len(search_list)

    def find_people_in_vk(self, search_params):
        response = self.vk.method('users.search', search_params)
        vk_users = response['items']

        return vk_users

    def download_users_from_vk(self, search_params, offset):

        s_params = {'offset': 0, 'sort': 0, 'count': 100, 'fields': 'sex, bdate, photo_id'}
        if search_params['city_id'] != None:
            s_params['city'] = search_params['city_id']
        s_params['country'] = 1
        if search_params['status_id'] != None:
            s_params['status'] = search_params['status_id']
        if search_params['sex'] != None:
            s_params['sex'] = search_params['sex']
        if search_params['min_age'] != None:
            s_params['age_from'] = search_params['min_age']
        if search_params['max_age'] != None:
            s_params['age_to'] = search_params['max_age']

        search_list = []
        try:
            response = self.vk.method('users.search', s_params)
            vk_users = response['items']
            last_user = len(vk_users)
            for vk_user in vk_users:


                user_data = {'vk_id': vk_user['id'], 'u_name': vk_user['first_name'], 'u_surname':vk_user['last_name'],
                             'u_age': age_by_bdate(vk_user['bdate']), 'u_birthyear': date_by_parts(vk_user['bdate'])['year'],
                             'u_sex':vk_user['sex']} #, 'u_photo_id': vk_user['photo_id'] if 'photo_id' in vk_user.keys() else []

                self.DB.add_user_to_base(user_data)

            return True, '', ''

        except v_ex.ApiError as er:
            return False, er.error['error_msg'], ''

    def add_to_black_list(self, last_showed_user):
        try:
            response = self.vk.method('account.ban', {'owner_id': f'{last_showed_user}'})
        except vk_api.exceptions.ApiError as er:
            return

    def add_to_favourites(self, last_showed_user):
        try:
            response = self.vk.method('fave.addPage', {'user_id': f'{last_showed_user}'})
        except vk_api.exceptions.ApiError as er:
            return

if __name__ == '__main__':
    print('d')