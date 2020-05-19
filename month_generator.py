#!/bin/bash


MONTHS = [
    '"January"',
    '"February"',
    '"March"',
    '"April"',
    '"May"',
    '"June"',
    '"July"',
    '"August"',
    '"September"',
    '"October"',
    '"November"',
    '"December"'
]


def line_array_generator(**kwargs):
    id = 1
    for month in MONTHS:
        yield [id, month]
        id += 1
