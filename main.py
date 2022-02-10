from configparser import ConfigParser
from DB import db_app, db_tables
from VK import Bot


def start_bot():

    config = ConfigParser()
    config.read('config.ini')
    gr_token = config.get('VK', 'gr_token')
    user_token = config.get('VK', 'pers_token')
    VK_api = Bot.VK_bot(gr_token=gr_token, u_token=user_token)
    VK_api.start_bot()

if __name__ == '__main__':

    dbase = db_app.DB()

    config = ConfigParser()
    config.read('config.ini')
    first_start = config.getboolean('settings', 'first_start')
    if first_start:
        dbase.create_tables()
        config.set('settings', 'first_start', 'False')
        with open('config.ini', 'w') as configfile:
            config.write(configfile)

    # dbase = db_app.DB()
    # dbase._delete_tables()
    # dbase.create_tables()
    # print('tables created')
    start_bot()