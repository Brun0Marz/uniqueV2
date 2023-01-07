def str2bool(v):
    if v in ['True','T','t','true']:
        return True
    elif v in ['False','false','F','f']:
        return False
    else:
        return None