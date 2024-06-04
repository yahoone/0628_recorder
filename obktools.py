
def zero_null(is_str):
    if is_str:
        return ''
    else:
        return 0


def zero_None(data_dict, key_, key_str=False):
    if key_ not in data_dict.keys():
        return zero_null(key_str)
    else:
        if data_dict[key_] is None:
            return zero_null(key_str)
        else:
            return data_dict[key_]


def obk_differ(obk1, obk2):
    if len(obk1) != len(obk2):
        return True
    else:
        for j in range(len(obk1)):
            if len(obk1[j]) != len(obk2[j]):
                return True
            else:
                for k in range(len(obk1[j])):
                    if obk1[j][k] != obk2[j][k]:
                        return True
    return False


def obk_split(obk_, depth_, ps_):
    if depth_ > len(obk_) or depth_ < 1:
        return 0.0
    else:
        if len(obk_[depth_ - 1]) < 2:
            return 0.0
        else:
            if ps_ == 'price':
                return obk_[depth_-1][0]
            elif ps_ == 'size':
                return obk_[depth_-1][1]
            else:
                return 0.0
