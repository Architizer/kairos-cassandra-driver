

def calculate_irtime(config, timestamp):
    '''Calculates i_time & r_time based on timeseries config'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
        r_time = config['r_calc'].to_bucket(timestamp)
    else:
        r_time = -1
    return i_time, r_time
