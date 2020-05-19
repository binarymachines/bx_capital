#!/usr/bin/env python

from constants import START_YEAR, YEAR_DIM_CARDINALITY


def line_array_generator(**kwargs):
    
    id = 1
    for i in range(YEAR_DIM_CARDINALITY):
        yield [id, START_YEAR + i]
        id += 1 