#!/usr/bin/env python

from constants import DAY_DIM_CARDINALITY


def line_array_generator(**kwargs):
    
    id = 1
    for i in range(DAY_DIM_CARDINALITY):
        yield [id, 1 + i]
        id += 1 