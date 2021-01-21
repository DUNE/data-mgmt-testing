#!/usr/bin/env python
import warnings


#Filters out any dictionaries that don't have one of a given set of keys
def require_fields(fields, dicts):
    rejects = []
    accepts = []
    if type(dicts) == type({}):
        accepts = [dicts]
    else:
        accepts = dicts

    for dict in accepts:
        keys = dict.keys()
        for f in fields:
            if not f in keys:
                rejects.append(dict)
                accepts.remove(dict)
                break

    return accepts, rejects

#Rejects dictionaries if they have a key from a given list of keys
def exclude_fields(fields, dicts):
    rejects = []
    accepts = []
    if type(dicts) == type({}):
        accepts = [dicts]
    else:
        accepts = dicts

    for dict in accepts:
        keys = dict.keys()
        for f in fields:
            if f in keys:
                rejects.append(dict)
                accepts.remove(dict)
                break

    return accepts, rejects

#Checks if a dictionary's field has any of a given set of values and returns
def field_has_val(field, values, dict):
    vals = []
    if type(values) != type(vals):
        vals = [values]
    else:
        vals = values
    try:
        if dict[field] in vals:
            return True
        return False
    except:
        print("Error: Could not determine if field " + str(field) + " had value in " + str(vals))
        return False

#Converts to a standard time format
def time_standards(time_string, h_offset=0):
    parts = None
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    try:
        parts = time_string.split('\W')
    except:
        print("Could not split string " + str(time_string))
        return None

    #Gets the date of the event
    d = int(parts[1])
    m = months.index(parts[0]) + 1
    y = int(parts[2])

    #Gets the time of the event
    h = int(parts[3])
    m = int(parts[4])
    s = int(parts[5])
    ms = int(parts[6])
