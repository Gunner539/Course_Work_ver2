from datetime import datetime


def date_by_parts(v_date):
    '''для обработки дат в формате "dd.mm.yy"'''
    if v_date == None:
        return {'day': 0,
                    'month': 0,
                    'year': 0}
    try:
        if v_date == '':
            return {'day': 0,
                    'month': 0,
                    'year': 0}
        else:
            parts = v_date.split('.')
            return {'day': int(parts[0]),
                    'month': int(parts[1]),
                    'year': int(parts[2])}
    except IndexError as ie:
        return {'day': 0,
                'month': 0,
                'year': 0}

def age_by_bdate(bdate):
    '''ожидается дата в формате "dd.mm.yy"'''
    date_parts = date_by_parts(bdate)
    if date_parts == None:
        return 0

    if date_parts['year'] != 0:
       year_now = datetime.now()
       age = int(year_now.year) - date_parts['year']
       return age
    else:
        return 0

def prepare_photos_list_for_sending(user_id, photos_list):

    if user_id == 0 or user_id == None:
        return []
    if type(photos_list) != list:
        return []
    if len(photos_list) == 0:
        return []

    photo_links = []

    for photo_id in photos_list:
        photo_links.append(f'photo{user_id}_{photo_id}')

    return photo_links

def check_days_difference(date1, date2=datetime.utcnow()):
    dt1 = datetime.fromisoformat(date1.strftime('%Y-%m-%d %H:%M:%S'))
    dt2 = date2
    difference = (dt2 - dt1).seconds
    return difference

if __name__ == '__main__':

    print('test')


