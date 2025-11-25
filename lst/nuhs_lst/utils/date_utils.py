from datetime import datetime, date, timedelta

DT_FORMAT = "%Y-%m-%d"
DTIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DELTA_FORMAT = "%d %H:%M:%S"

HL7_DTIME_FORMAT = "%Y%m%d%H%M%S"


def from_hldtime_to_dtime(hdtime):
    dtime = datetime.strptime(hdtime, HL7_DTIME_FORMAT)
    return to_dtime_str(dtime)


def to_dt(dtStr):
    # datetime.strptime("2015-02-24T13:00:00-08:00", "%Y-%B-%dT%H:%M:%S-%H:%M").date()
    dtStr = dtStr.split(" ")[0].strip()
    dt = datetime.strptime(dtStr, DT_FORMAT).date()
    # print(str(dt))
    return dt


def to_dtime(dtimeStr):
    # print('dtimeStr: '+str(dtimeStr))
    # print('DTIME_FORMAT: '+str(DTIME_FORMAT))
    dtime = datetime.strptime(dtimeStr, DTIME_FORMAT)
    return dtime


def to_dtime_str(dtime):
    return dtime.strftime(DTIME_FORMAT)


def to_dt_str(dt):
    return dt.strftime(DT_FORMAT)


def get_prev_dt_str(dtStr):
    dt = to_dt(dtStr) - timedelta(days=1)
    return to_dt_str(dt)


def get_next_dt_str(dtStr):
    dt = to_dt(dtStr) + timedelta(days=1)
    return to_dt_str(dt)


def get_today_dt_str():
    today = datetime.today()
    return today.strftime("%Y-%m-%d")


def diff_str(d1, d2):
    return diff(to_dt(d1), to_dt(d2))


def diff(d1, d2):
    """
    Assume d2 > d1
    """
    return (d2 - d1).days


def diff_year_str(d1, d2):
    return diff_year(to_dt(d1), to_dt(d2))


def diff_year(d1, d2):
    return int((d2 - d1).days / 365.25)


def inc_delta_dt_str(dtStr, deltaStr):
    dt = to_dt(dtStr)
    t = datetime.strptime(deltaStr, DELTA_FORMAT)
    # ...and use datetime's hour, min and sec properties to build a timedelta
    delta = timedelta(days=t.day, hours=t.hour, minutes=t.minute, seconds=t.second)
    return to_dt_str(dt + delta)


def dec_delta_dt_str(dtStr, deltaStr):
    dt = to_dt(dtStr)
    t = datetime.strptime(deltaStr, DELTA_FORMAT)
    # ...and use datetime's hour, min and sec properties to build a timedelta
    delta = timedelta(days=t.day, hours=t.hour, minutes=t.minute, seconds=t.second)
    return to_dt_str(dt - delta)


def inc_delta_dtime_str(dtStr, deltaStr):
    dt = to_dtime(dtStr)
    t = datetime.strptime(deltaStr, DELTA_FORMAT)
    # ...and use datetime's hour, min and sec properties to build a timedelta
    delta = timedelta(days=t.day, hours=t.hour, minutes=t.minute, seconds=t.second)
    return to_dtime_str(dt + delta)


def dec_delta_dtime_str(dtStr, deltaStr):
    dt = to_dtime(dtStr)
    t = datetime.strptime(deltaStr, DELTA_FORMAT)
    # ...and use datetime's hour, min and sec properties to build a timedelta
    delta = timedelta(days=t.day, hours=t.hour, minutes=t.minute, seconds=t.second)
    return to_dtime_str(dt - delta)
