import os
import json


def delete_file(file_path):
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
    except Exception as error_del:
        print('error in deleting file: ' + str(error_del))


def load_json(file_path):
    dict_json = dict()
    try:
        with open(file_path, 'r') as json_file:
            dict_json = json.load(json_file)
    except Exception as error_read:
        print('error in reading json: '+str(error_read))
    finally:
        return dict_json


def json_dump(file_path, dict_json):
    try:
        with open(file_path, 'w') as json_file:
            json.dump(dict_json, json_file)
    except Exception as error_write:
        print('error in writing json: '+str(error_write))


def zero_null(num_str_bool):
    if num_str_bool == 'number':
        return 0
    elif num_str_bool == 'string':
        return ''
    else:
        return False


def zero_None(data_dict, key_, key_type='number'):
    if key_ not in data_dict.keys():
        return zero_null(key_type)
    else:
        if data_dict[key_] is None:
            return zero_null(key_type)
        else:
            return data_dict[key_]
