from datetime import datetime, timedelta
from collections import namedtuple


def get_frontend_vals():
    '''Returns a named tuple of dynamic date ranges that match Redash's front-end
    '''

    ranges = calculate_ranges()
    singles = calculate_singletons()

    valkeys = [k for k in ranges.keys()] + [k for k in singles.keys()]

    Values = namedtuple('Values', ' '.join(valkeys))

    return Values(**ranges, **singles)


def calculate_ranges():

    # SuperToday is more specific than datetime.datetime.today
    SuperToday = namedtuple('SuperToday', 'month day year weeknum weekday')
    DateRange = namedtuple('DateRange', 'start end')

    today = datetime.today()
    _ymd = (today.month, today.day)

    t = SuperToday(*_ymd, *today.isocalendar())

    ranges = {}

    # THIS WEEK

    start = datetime.strptime(f"{t.year}-{t.weeknum}-1", '%G-%V-%u')
    end = start + timedelta(days=6)

    ranges['d_this_week'] = DateRange(start, end)

    #  THIS MONTH

    start = datetime.strptime(f"{t.year}-{t.month}-1", '%Y-%m-%d')
    e_year, e_month = (t.year, t.month+1) if t.month < 12 else (t.year+1, 1)
    end = datetime.strptime(
        f"{e_year}-{e_month}-1", '%Y-%m-%d') \
        - timedelta(days=1)

    ranges['d_this_month'] = DateRange(start, end)

    # THIS YEAR

    start = datetime.strptime(f"{t.year}-1-1", '%Y-%m-%d')
    end = datetime.strptime(f"{t.year}-12-31", '%Y-%m-%d')

    ranges['d_this_year'] = DateRange(start, end)

    # LAST WEEK

    start = datetime.strptime(f"{t.year}-{t.weeknum-1}-1", '%G-%V-%u')
    end = start + timedelta(days=6)

    ranges['d_last_week'] = DateRange(start, end)

    # LAST MONTH

    s_year, s_month = (t.year-1, 12) if t.month == 1 else (t.year, t.month-1)

    start = datetime.strptime(f"{s_year}-{s_month}-1", '%Y-%m-%d')
    end = datetime.strptime(f"{t.year}-{t.month}-1", '%Y-%m-%d') \
        - timedelta(days=1)

    ranges['d_last_month'] = DateRange(start, end)

    # LAST YEAR

    start = datetime.strptime(f"{t.year-1}-1-1", '%Y-%m-%d')
    end = datetime.strptime(f"{t.year-1}-12-31", '%Y-%m-%d')

    ranges['d_last_year'] = DateRange(start, end)

    # LAST X DAYS

    def make_x_days_date_range(today, days):
        start = today - timedelta(days=days)
        end = today

        return DateRange(start, end)

    for x in [7, 14, 30, 60, 90]:
        ranges[f"d_last_{x}_days"] = make_x_days_date_range(today, x)

    return ranges


def calculate_singletons():

    today = datetime.today()
    d_now = datetime.strptime(
        f"{today.year}-{today.month}-{today.day}", '%Y-%m-%d')
    d_yesterday = d_now - timedelta(days=1)

    return dict(d_now=d_now, d_yesterday=d_yesterday)
