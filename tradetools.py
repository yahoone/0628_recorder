
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
