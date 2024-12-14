from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta


tz: ZoneInfo = ZoneInfo(
    os.getenv('AIRFLOW__CORE_DEFAULT_TIMEZONE', 'Asia/Bangkok')
)


def get_dt_now(only_date: bool = True) -> datetime:
    dt: datetime = datetime.now(tz=tz)
    return (
        dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if only_date else dt
    )


def first(iterable, default=None, condition=lambda x: True):
    """
    Returns the first item in the `iterable` that
    satisfies the `condition`.

    If the condition is not given, returns the first item of
    the iterable.

    If the `default` argument is given and the iterable is empty,
    or if it has no items matching the condition, the `default` argument
    is returned if it matches the condition.

    The `default` argument being None is the same as it not being given.

    Raises `StopIteration` if no item satisfying the condition is found
    and default is not given or doesn't satisfy the condition.

    Examples:
        >>> first( (1,2,3), condition=lambda _: _ % 2 == 0)
        2
        >>> first(range(3, 100))
        3
        >>> first( () )
        Traceback (most recent call last):
        ...
        StopIteration
        >>> first([], default=1)
        1
        >>> first([], default=1, condition=lambda _: _ % 2 == 0)
        Traceback (most recent call last):
        ...
        StopIteration
        >>> first([1,3,5], default=1, condition=lambda _: _ % 2 == 0)
        Traceback (most recent call last):
        ...
        StopIteration

    Ref: https://stackoverflow.com/questions/2361426/ -
            get-the-first-item-from-an-iterable-that-matches-a-condition
    """
    try:
        return next(x for x in iterable if condition(x))
    except StopIteration:
        if default is not None and condition(default):
            return default
        else:
            raise


def last_day_of_month(dt: datetime) -> datetime:
    """
    :param dt:
    :rtype: datetime

    Examples:
        >>> last_day_of_month(datetime(2024, 2, 29))
        datetime.datetime(2024, 2, 29, 0, 0)
        >>> last_day_of_month(datetime(2024, 1, 31) + relativedelta(months=1))
        datetime.datetime(2024, 2, 29, 0, 0)
        >>> last_day_of_month(datetime(2024, 2, 29) + relativedelta(months=1))
        datetime.datetime(2024, 3, 31, 0, 0)

        >>> datetime(2024, 1, 31) + relativedelta(months=1)
        datetime.datetime(2024, 2, 29, 0, 0)
        >>> datetime(2024, 2, 29) + relativedelta(months=1)
        datetime.datetime(2024, 3, 29, 0, 0)
    """
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = dt.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)


def closest_quarter(dt: datetime) -> datetime:
    """
    :param dt:
    :rtype: datetime

    Examples:
        >>> closest_quarter(datetime(2024, 9, 25))
        datetime.datetime(2024, 9, 30, 0, 0)
        >>> closest_quarter(datetime(2024, 2, 13))
        datetime.datetime(2023, 12, 31, 0, 0)
    """
    # candidate list, nicely enough none of these
    # are in February, so the month lengths are fixed
    candidates: list[datetime] = [
        datetime(dt.year - 1, 12, 31, 0),
        datetime(dt.year, 3, 31, 0),
        datetime(dt.year, 6, 30, 0),
        datetime(dt.year, 9, 30, 0),
        datetime(dt.year, 12, 31, 0),
    ]
    # take the minimum according to the absolute distance to
    # the target date.
    return min(candidates, key=lambda d: abs(dt - d))


def last_day_of_quarter(dt: datetime) -> datetime:
    """
    :param dt:
    :return:

    Examples:
        >>> last_day_of_quarter(datetime(2024, 2, 16))
        datetime.datetime(2024, 3, 31, 0, 0)
        >>> last_day_of_quarter(datetime(2024, 12, 31))
        datetime.datetime(2024, 12, 31, 0, 0)
        >>> last_day_of_quarter(datetime(2024, 8, 1))
        datetime.datetime(2024, 9, 30, 0, 0)
        >>> last_day_of_quarter(datetime(2024, 9, 30))
        datetime.datetime(2024, 9, 30, 0, 0)
    """
    candidates: list[datetime] = [
        datetime(dt.year - 1, 12, 31, 0),
        datetime(dt.year, 3, 31, 0),
        datetime(dt.year, 6, 30, 0),
        datetime(dt.year, 9, 30, 0),
        datetime(dt.year, 12, 31, 0),
    ]
    return first(candidates, condition=lambda x: x >= dt)


def add_date_with_freq(dt: datetime, freq: str, prev: bool = False) -> datetime:
    """
    :param dt:
    :param freq:
    :param prev:
    :rtype: datetime

    Examples:
        >>> add_date_with_freq(datetime(2024, 1, 3), freq='D')
        datetime.datetime(2024, 1, 4, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 3), freq='D', prev=True)
        datetime.datetime(2024, 1, 2, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 3), freq='W')
        datetime.datetime(2024, 1, 10, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 3), freq='W', prev=True)
        datetime.datetime(2023, 12, 27, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 3), freq='M')
        datetime.datetime(2024, 2, 3, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 31), freq='M')
        datetime.datetime(2024, 2, 29, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 17), freq='Q')
        datetime.datetime(2024, 4, 17, 0, 0)
        >>> add_date_with_freq(datetime(2024, 1, 31), freq='Q')
        datetime.datetime(2024, 4, 30, 0, 0)
        >>> add_date_with_freq(datetime(2025, 12, 31), freq='Q')
        datetime.datetime(2026, 3, 31, 0, 0)
        >>> add_date_with_freq(datetime(2024, 5, 21), freq='Y')
        datetime.datetime(2025, 5, 21, 0, 0)
        >>> add_date_with_freq(datetime(2024, 5, 31), freq='Y')
        datetime.datetime(2025, 5, 31, 0, 0)
        >>> add_date_with_freq(datetime(2024, 5, 31), freq='Y', prev=True)
        datetime.datetime(2023, 5, 31, 0, 0)
    """
    operator: int = -1 if prev else 1
    if freq == 'D':
        return dt + timedelta(days=1 * operator)
    elif freq == 'W':
        return dt + timedelta(days=7 * operator)
    elif freq == 'M':
        if dt == last_day_of_month(dt):
            return last_day_of_month(dt + relativedelta(months=1 * operator))
        return dt + relativedelta(months=1 * operator)
    elif freq == 'Q':
        if dt == last_day_of_month(dt):
            return last_day_of_month(dt + relativedelta(months=3 * operator))
        return dt + relativedelta(months=3 * operator)
    elif freq == 'Y':
        if dt == last_day_of_month(dt):
            return last_day_of_month(
                dt + relativedelta(years=1 * operator)
            )
        return dt + relativedelta(years=1 * operator)
    raise ValueError(
        f"The asat date logic does not support for frequency: {freq}"
    )


def calc_data_date_with_freq(dt: datetime, freq: str) -> datetime:
    """
    :param dt:
    :param freq:
    :rtype: datetime

        Examples:
            >>> calc_data_date_with_freq(datetime(2024, 1, 13), freq='D')
            datetime.datetime(2024, 1, 13, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 1, 3), freq='W')
            datetime.datetime(2024, 1, 3, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 1, 3), freq='M')
            datetime.datetime(2023, 12, 31, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 1, 31), freq='M')
            datetime.datetime(2024, 1, 31, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 1, 31), freq='Q')
            datetime.datetime(2023, 12, 31, 0, 0)
            >>> calc_data_date_with_freq(datetime(2025, 12, 31), freq='Q')
            datetime.datetime(2025, 12, 31, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 12, 31), freq='Y')
            datetime.datetime(2024, 12, 31, 0, 0)
            >>> calc_data_date_with_freq(datetime(2024, 5, 31), freq='Y')
            datetime.datetime(2023, 12, 31, 0, 0)
        """
    if freq == 'D' or freq == 'W':
        return dt
    elif freq == 'M':
        if dt != last_day_of_month(dt):
            return last_day_of_month(dt) - relativedelta(months=1)
        return dt
    elif freq == 'Q':
        if dt != last_day_of_quarter(dt):
            return last_day_of_month(
                last_day_of_quarter(dt) - relativedelta(months=3)
            )
        return dt
    elif freq == 'Y':
        if dt != dt.replace(month=12, day=31):
            return dt.replace(month=12, day=31) - relativedelta(years=1)
        return dt
    raise ValueError(
        f"The calculate asat date logic does not support for frequency: "
        f"{freq}"
    )
