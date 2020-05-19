#!/usr/bin/env python

import sys
import datetime


MONTH_INDEX = 0
DAY_INDEX = 1
YEAR_INDEX = 2


TODAY = datetime.date.today()

def calc_longevity(start_date, end_date):

    if end_date < TODAY:
        return (end_date - start_date).days
    return (TODAY - start_date).days


class TestDatasource(object):
    def __init__(self, service_object_registry):
        self.services = service_object_registry


    def lookup_longevity(self, target_field_name, source_record, field_value_map):
        # TODO: should probably screen inputs for end dates which precede start dates

        start_date_arr = field_value_map.get_value('start_date', source_record)
        end_date_arr = field_value_map.get_value('end_date', source_record)

        start_date = datetime.date(start_date_arr[YEAR_INDEX],
                                   start_date_arr[MONTH_INDEX],
                                   start_date_arr[DAY_INDEX])

        end_date = datetime.date(end_date_arr[YEAR_INDEX],
                                 end_date_arr[MONTH_INDEX],
                                 end_date_arr[DAY_INDEX])

        return calc_longevity(start_date, end_date)

