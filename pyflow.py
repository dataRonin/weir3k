#!/usr/bin env python
# -*- coding: utf-8 -*-

from scipy.interpolate import interp1d
import numpy as np
import datetime
import csv
import pymssql
import sys
import os
import math


# import itertools if it's the old python
if sys.version_info >= (3,0):
    import itertools
else:
    pass

"""
pyFLOW.py is a single file version of all the other flow calculators
The inputs to pyFLOW.py are sitecode, wateryear, "csv"
"""

def fc():
    """ Connection to SQL server """

    # Connect to MSSQL Server
    conn = pymssql.connect(server="stewartia.forestry.oregonstate.edu:1433",
                           user="ltermeta",
                           password="$CFdb4LterWeb!",
                           )

    cur = conn.cursor()

    return conn, cur

def get_equation_sets(cur, sitecode, wateryear):
    """
    Get the equation sets by ids to associate with the notch on and notch off, and to create a look up table for the adjustment.

    EXAMPLE:
    eqns = get_equation_sets(cur, 'GSWSMA', 2015)

    RETURNS:
    {'A3': {'tuple_date': [(datetime.datetime(1979, 10, 1, 0, 1), datetime.datetime(1995, 10, 1, 0, 0)), (datetime.datetime(1995, 10, 1, 0, 1), datetime.datetime(2051, 1, 1, 0, 0))], 'eqn_set': ['32', '35']}}

    """

    od = {}

    # start the collection on january 1 of the prior water year, to make sure we get enough equations
    start_test_DT = datetime.datetime(int(wateryear)-1, 1, 1, 0, 0)
    start_test = datetime.datetime.strftime(start_test_DT, '%Y-%m-%d %H:%M:%S')

    # where there are many years / methods
    if sitecode not in ['GSWSMA', 'GSWSMF', 'GSCC01', 'GSCC02','GSCC03', 'GSCC04']:

        sql = "SELECT eq_set, eq_ver, eqn_set_code, bgn_date_time, end_date_time FROM fsdbdata.dbo.HF00204 WHERE sitecode like \'" + sitecode +"\' and bgn_date_time > \'" + start_test + "\'"

    elif sitecode in ['GSWSMA', 'GSWSMF', 'GSCC01', 'GSCC02','GSCC03', 'GSCC04']:

        # these sites have a starting date for their current equation that is far older than last year or the year before.
        sql = "SELECT eq_set, eq_ver, eqn_set_code, bgn_date_time, end_date_time FROM fsdbdata.dbo.HF00204 WHERE sitecode like \'" + sitecode +"\'"

    cur.execute(sql)

    for row in cur:

        # eqn set + eqn ver
        cat_name = str(row[0]) + str(row[1])

        # add to the output
        if cat_name not in od:
            od[cat_name] = {'tuple_date': [(datetime.datetime.strptime(str(row[3]), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S'))], 'eqn_set': [str(row[2]).lstrip(' ')]}

        elif cat_name in od:
            od[cat_name]['tuple_date'].append((datetime.datetime.strptime(str(row[3]), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S')))

            od[cat_name]['eqn_set'].append(str(row[2]).lstrip(' '))

    return od


def get_equations_by_value(cur, sitecode, o):
    """
    Using the limited to one site code dictionary created by get_equation_set, get the parameters of the specific equations from HF00203. YOUR OUTPUT VARIABLE MUST MATCH YOUR THIRD INPUT ARGUMENT!

    This will modify the structure of "o" -- or whatever you call the equation dictionary-- i.e. this process is not reversable in pyflow.

    EXAMPLE:

    o = get_equations_by_value(cur, 'GSWSMA', o)

    RETURNS:

    o = {'A3': {'eqns': {0.509: [3.568, 1.741562], 2.54: [3.856196, 2.168731]}, 'acres': '1436.0', 'tuple_date': [(datetime.datetime(1979, 10, 1, 0, 1), datetime.datetime(1995, 10, 1, 0, 0)), (datetime.datetime(1995, 10, 1, 0, 1), datetime.datetime(2051, 1, 1, 0, 0))], 'eqn_set': ['32', '35']}}

    WHICH DIFFERS FROM WHAT 'o' WAS BEFORE:

    o = {'A3': {'tuple_date': [(datetime.datetime(1979, 10, 1, 0, 1), datetime.datetime(1995, 10, 1, 0, 0)), (datetime.datetime(1995, 10, 1, 0, 1), datetime.datetime(2051, 1, 1, 0, 0))], 'eqn_set': ['32', '35']}}
    """

    sql = "SELECT eq_set, eq_ver, eq_num, ws_acres, max_ht, ln_a, b from fsdbdata.dbo.HF00203 where sitecode like \'" + sitecode + "\' order by max_ht asc"

    cur.execute(sql)

    for row in cur:

        # this is the combination of set and version, like A3 or B1, etc.
        cat_name = str(row[0]) + str(row[1])

        # skip sets and versions not in the output already selected for this time range
        if cat_name not in o:
            continue

        elif cat_name in o:

            # if acres aren't listed, update with acres
            if 'acres' not in o:
                o[cat_name].update({'acres': str(row[3])})
            else:
                pass

            # compute the reference lookup
            # the "key" is the max height, parameter "ln_a" is the first item in the list and parameter "b" is the second item in the list.
            reference = {round(float(str(row[4])),3) : [round(float(str(row[5])),7), round(float(str(row[6])),7)]}

            # if no equations found yet, add this new equation
            if 'eqns' not in o[cat_name]:
                o[cat_name].update({'eqns': reference})

            # otherwise if an equation has been found, check that the max height has not already been added and if that's the case, add the new max height, ln_a, and b parameters
            elif 'eqns' in o[cat_name]:
                if round(float(str(row[4])),3) not in o[cat_name]['eqns']:
                    o[cat_name]['eqns'][round(float(str(row[4])),3)] = [round(float(str(row[5])),7), round(float(str(row[6])),7)]

                elif round(float(str(row[4])),3) in o[cat_name]['eqns']:
                    print(" Error : Same Max Height already listed for " + sitecode + ". You may have already tried to process this equation set. ")

    return o

def get_data_from_sql(cur, sitecode, wateryear):
    """ get data from sql server - used for checking values"""

    print("This SQL function has never been used or tested. Please proceed with caution.")
    # create first and final days for generating the SQL query
    first_day_DT = datetime.datetime(int(wateryear)-1, 9, 30, 23, 55)
    first_day = datetime.datetime.strftime(first_day_DT, '%Y-%m-%d %H:%M:%S')

    last_day_DT = datetime.datetime(int(wateryear), 10, 1, 0, 5)
    last_day = datetime.datetime.strftime(last_day_DT, '%Y-%m-%d %H:%M:%S')

    query = "select DATE_TIME, STAGE, INST_Q, TOTAL_Q_INT, MEAN_Q, MEAN_Q_AREA, INST_Q_AREA from fsdbdata.dbo.HF00401 where SITECODE like \'" + sitecode  + "\' and DATE_TIME >= \'" + first_day + "\' and DATE_TIME <= \'" + last_day + "\' order by DATE_TIME asc"
    cur.execute(query)

    # gather this dictionary containing the raw values from the database
    od = {}

    for row in cur:

        dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

        # on the stage change, subtract one second
        if dt.second == 1:
            dt -= datetime.timedelta(seconds=1)

        if dt not in od:
            od[dt] = {'stage': str(row[1]), 'inst_q': str(row[2]), 'total_q_int': str(row[3]), 'mean_q': str(row[4]), 'mean_q_area': str(row[5]), 'inst_q_area' : str(row[6])}

        elif dt in od:
            print("date = > %s is already in the db-- if on a notch event, disregard warning" %(str(row[0])))

    return od

def get_data_from_csv(csvfilename):
    """
    Gets the data from a csv-file. By default based on the main loop, it will look in your /working/ directory for a file which contains '_re'.

    Outputs a look-up dictionary : {datetime : 'val': 0.2, 'fval' : a, 'event' : na}
    """

    # if an input value is 'nan' then make it 'None' as a string
    naner = lambda x: 'None' if x == 'nan' else x

    od = {}

    # if data could not be found, append to this dictionary
    bad_flags_and_values = {}

    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'

    with open(csvfilename, mode) as readfile:
        reader = csv.reader(readfile)

        for row in reader:

            # import the date from column 1 - will always be in column 1.
            dt = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')

            # get the correct value from column 4 -- this is the column which contains the adjusted data!
            try:
                val = naner(str(row[4]))

            except Exception:
                val = naner(str(row[3]))

                # send to the output list
                if dt not in bad_flags_and_values:
                    bad_flags_and_values[dt] = {'val': val}
                elif dt in bad_flags_and_values:
                    if 'val' not in bad_flags_and_values[dt]:
                        bad_flags_and_values[dt].update({'val': val})
                    elif 'val' in bad_flags_and_values[dt]:
                        bad_flags_and_values[dt].update({'duplication': True})


            # get the flag from column 5
            try:
                flag = str(row[5])
            except Exception:
                flag = str(row[3])
                if dt not in bad_flags_and_values:
                    bad_flags_and_values[dt] = {'flag': flag}
                elif dt in bad_flags_and_values:
                    if 'flag' not in bad_flags_and_values[dt]:
                        bad_flags_and_values[dt].update({'flag': flag})
                    elif 'flag' in bad_flags_and_values[dt]:
                        bad_flags_and_values[dt].update({'duplication_flag': True})

            # get the event from column 6
            try:
                event = str(row[6])
            except Exception:
                event = str(row[4])

            # before the maintenance event (notch), by one reading, also give a flag "MAINTV"
            if event == "MAINTE":
                prior_date = max(od.keys())
                od[prior_date]['event']="MAINTV"
            else:
                pass

            if dt not in od:
                od[dt] = {'val': val, 'fval': flag, 'event' : event}

            elif dt in od:
                pass

    return od, bad_flags_and_values

def set_up_iterators(o2, o1, wateryear):
    """ Bin the incoming data into the appropriate equation sets
    and create some iterators

    od = {'b1' : 'flags' : <iterator>, 'vals' :<iterator> }
    I am confident that this section is working
    """
    od = {}

    # "high-resolution dictionary"
    hr_d = sorted(list(o2.keys()))

    # ex. GSWSMA, 2015 : datetime.datetime(2014, 10, 1, 0, 0)
    first_date = hr_d[0]
    # ex. GSWSMA, 2015 : datetime.datetime(2014, 10, 1, 0, 5) (one past end)
    last_date = hr_d[-1]


    for each_set in sorted(list(o1.keys())):
        list_of_tuples_sorted = sorted(list(o1[each_set]['tuple_date']))

        for each_tuple in list_of_tuples_sorted:

            # if the last date of the tuple comes before the data starts, pass it
            if each_tuple[1] <= first_date:
                #print("I found a tuple with " + datetime.datetime.strftime(each_tuple[1], '%Y-%m-%d %H:%M:%S') + " as its end date, and this is larger than the first data observation at " + datetime.datetime.strftime(first_date, '%Y-%m-%d %H:%M:%S' ) + ", so I am not using this equation")
                continue

            # if the first date of the tuple occurs after the final date in the data, continue
            if each_tuple[0] > last_date:
                #print("I found a tuple that begins on " + datetime.datetime.strftime(each_tuple[0], '%Y-%m-%d %H:%M:%S') + " and this is larger than the final observation in the data on " + datetime.datetime.strftime(last_date, '%Y-%m-%d %H:%M:%S'))
                continue

            # if the first date of the tuple is less than the first date of data, begin the use of that equation set with the first date of data;
            # otherwise, if its not but it's also not after the last date, begin with the tuples date
            if each_tuple[0] <= first_date:

                begin_on = first_date
            else:
                begin_on = each_tuple[0]

            if each_tuple[1].year > 2049:
                end_on = last_date+datetime.timedelta(minutes=5)
            else:
                end_on = each_tuple[1]+datetime.timedelta(minutes=5)

            # should not fail even if the "end on" is beyond its range because it is still less than this
            dts = [x for x in hr_d if x >= begin_on and x <= end_on]


            print("Data Found ! Under the group of eqn_set and eqn_number \'" + each_set + "\', which starts on " + datetime.datetime.strftime(begin_on, '%Y-%m-%d %H:%M:%S') + " and ends on " + datetime.datetime.strftime(end_on, '%Y-%m-%d %H:%M:%S'))


            final_val = o2[max(o2.keys())]['val']
            all_hts = [o2[x]['val'] for x in dts]

            if end_on == datetime.datetime(int(wateryear), 10,1,0,5):
                dts.append(end_on)
                all_hts.append(final_val)

            raw_dts = iter(dts)
            raw_hts = iter(all_hts)

            if each_set not in od:
                od[each_set] = {'raw_dts': [raw_dts], 'raw_hts':[raw_hts]}

            elif each_set in od:
                od[each_set]['raw_dts'].append(raw_dts)
                od[each_set]['raw_hts'].append(raw_hts)
    return od

def get_samples_dates(cur, sitecode, wateryear):
    """ Creates a list of tuple date ranges between the starting date and the ending date - base on the begining date, anything afterward doesn't get to count
    """

    startdate = datetime.datetime.strftime(datetime.datetime(int(wateryear)-1,10,1,0,0), '%Y-%m-%d %H:%M:%S')
    enddate = datetime.datetime.strftime(datetime.datetime(int(wateryear),10,1,0,5), '%Y-%m-%d %H:%M:%S' )

    if sitecode not in ["GSWSMA", "GSWSMF"]:

        query = "select date_time from fsdbdata.dbo.cf00206 where sitecode like \'" + sitecode + "\' and date_time >= \'" + startdate + "\' and date_time < \'" + enddate + "\' order by date_time asc"
    else:

        query = "select date_time from fsdbdata.dbo.cf00206 where sitecode like \'GSMACK\' and date_time >= \'" + startdate + "\' and date_time < \'" + enddate + "\' order by date_time asc"

    cur.execute(query)

    # list of tuples containing start and end dates
    Sdate_list = []

    for row in cur:

        dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

        if dt.minute%5 != 0:
            print(str(dt.minute) + " -- original number of minutes in date")
            five_minus = 5 - dt.minute%5
            new_dt = dt + datetime.timedelta(minutes=five_minus)

            dt = new_dt
            print(str(dt.minute) + "-- number of minutes in new date")

        Sdate_list.append(dt)

    # get the first and last dates from the list and then bound them with that water year; put the beginning of wy in position 0 with the start of the list with it; pop the last date and replace with the last date bounded by the water year.
    try:
        first_date = Sdate_list[0]
        last_date = Sdate_list[-1]

    except Exception:
        print("no S-dates are within the wateryear-based date range specified")
        return None

    # to the front of the list, add on the starting date
    Sdate_list.insert(0,datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S'))
    # to the end of the list add on the ending date
    Sdate_list.append(datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')-datetime.timedelta(minutes=5))

    return Sdate_list


def loop_over_data(o3, o1):
    """
    This is a function wrapper for the data iterators, it identifies the iterators in each key, identifies the set of rating equations associated with that key, and runs the `flow` on that data, returning the results.
    """

    # final output dictionary
    od_1 = {}

    # each of the tuples, i.e. 'C1', 'B1'
    for each_key in sorted(list(o3.keys())):

        # if we have 1 time of C1 and 2 times of B1, we should hit the B1 2 x I think...
        raw_dts_1 = o3[each_key]['raw_dts']
        raw_hts_1 = o3[each_key]['raw_hts']

        # rating calib is the possible calibrations: ex. {0.509: [3.568, 1.741562], 2.54: [3.856196, 2.168731]} which is {max height: [ln_a, b]}
        rating_calib = o1[each_key]['eqns']

        # the numerical name of the eqn set
        eq_sets = o1[each_key]['eqn_set']

        # for each iterator in the tuple set, i.e. B1 or C1, may only have 1 or 2 index
        for index, value in enumerate(raw_dts_1):
            print("the key processed is " + each_key + " and the index is " + str(index))

            # the equation set name; i.e. "3" or "4" or "2"
            computed_eq_set = eq_sets[index]

            # create an output structure called od_2
            od_2 = flow_the_data(raw_dts_1[index], raw_hts_1[index], rating_calib, desired=300)

            # this is the list of dates, stages, instq, total_q, and mean_q, in order, by dates
            computed_dates = sorted(list(od_2.keys()))

            #print("the number of values in this date structure were " + str(len(od_2)))

            computed_stages = [od_2[x]['stage'] for x in computed_dates]
            computed_inst_q = [od_2[x]['inst_q'] for x in computed_dates]
            computed_total_q = [od_2[x]['total_q'] for x in computed_dates]
            computed_mean_q = [od_2[x]['mean_q'] for x in computed_dates]

            for index, each_date in enumerate(computed_dates):

                if each_date not in od_1:
                    od_1[each_date] = {'stage': computed_stages[index], 'inst_q': computed_inst_q[index], 'total_q' : computed_total_q[index], 'mean_q': computed_mean_q[index], 'eqn_set' : computed_eq_set}

                elif each_date in od_1:
                    print("this date has already been included in the lookup")

            #print("....Processed all found data for eqn_set + eqn_num \'" + each_key + "\' over " + str(index) + " values ")
        #print(".....Finished processing data for eqn_set + eqn_num :" + each_key)

    return od_1


def check_value_versus_keys(rating_calib, value):
    """
    Recursively scans the parameters for the log-function to find those that apply, returning a minimum threshold (maximum height of previous value) and the value (max height) that is the upper threshold
    """

    for index, each_value in enumerate(sorted(rating_calib.keys())):

        if value <= each_value:
            try:
                lower_value = each_value[index-1]
            except Exception:
                lower_value = 0
            return lower_value, each_value
        else:
            pass

def interpolate_raw(first_value, second_value, interval_length):
    """ Returns appropriate linear interpolation,
    usually is 5 minutes.
    Sadly you'd be numpy-free without this.
    """

    fxn_interp = interp1d([0,interval_length],[first_value, second_value])

    # a np_array containing the interpolated values
    try:
        return fxn_interp(xrange(0, interval_length))
    except Exception:
        try:
            return fxn_interp(range(0, interval_length))
        except Exception:
            return fxn_interp(range(0, int(interval_length)))


def check_interval_length(first_date, second_date, desired=300):
    """
    Check to be sure the interval is the correct length
    the default correct length is 5 minutes
    """
    if type(first_date) == str:
        dt1 = datetime.datetime.strptime(first_date,'%Y-%m-%d %H:%M:%S')
    else:
        pass

    if type(second_date) == str:
        dt2 = datetime.datetime.strptime(second_date,'%Y-%m-%d %H:%M:%S')
    else:
        pass

    try:
        dt_diff = second_date - first_date
    except Exception:
        dt_diff = dt2-dt1

    if dt_diff.seconds == 300 and dt_diff.days == 0:
        return 5
    else:
        # make sure to include the whole days too
        #print("interval is not the right length!")
        #print("using " + str(int(dt_diff.seconds)/60))
        return int(dt_diff.days)*1440 + int(dt_diff.seconds)/60

def logfunc(a,b,x):
    """ the transform we need to solve for winters """
    try:
        return math.exp(a + b*math.log(x))
    except Exception:
        return None

def flag_daily_streams(output_5, output_daily):
    """ Assign daily flags based on quality of data """

    od = {}

    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'

    # open the five minute file and use it to make the daily file.
    with open(output_5,mode) as readfile:
        reader = csv.reader(readfile)

        # update to python 3 -- check the file reader.
        if sys.version_info >=(3,0):
            next(reader)
        else:
            reader.next()

        for row in reader:
            # get date-time, flag, etc.
            dt = datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S')
            flag = str(row[13])
            # set new date-time to be based on year, month, day
            new_dt = datetime.datetime(dt.year, dt.month, dt.day)

            # now append all flags to a list like 2015-05-15 : ['A', 'A', 'A']
            if new_dt not in od:
                od[new_dt] = [flag]
            elif new_dt in od:
                od[new_dt].append(flag)
    od_1 = {}

    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'

    with open(output_daily, mode) as readfile:
        reader = csv.reader(readfile)
        if sys.version_info >= (3,0):
            next(reader)
        else:
            reader.next()

        for row in reader:
            dt = datetime.datetime.strptime(str(row[4]), '%Y-%m-%d')
            other_stuff = [str(x) for x in row]

            od_1[dt] = other_stuff

    for each_key in od.keys():

        percent_m = len([x for x in od[each_key] if x == "M"])/len(od[each_key])
        percent_e = len([x for x in od[each_key] if x == "E"])/len(od[each_key])
        percent_q = len([x for x in od[each_key] if x == "Q"])/len(od[each_key])

        if percent_m > 0.2:
            daily_flag = "M"
            od_1[each_key].append(daily_flag)
        elif percent_e > 0.05:
            daily_flag = "E"
        elif percent_q > 0.05:
            daily_flag = "Q"
        elif percent_m + percent_e + percent_q > 0.05:
            daily_flag = "Q"
        else:
            daily_flag = "A"

        #dt = datetime.datetime.strptime(each_key, '%Y-%m-%d')
        od_1[each_key].append(daily_flag)


    if sys.version_info >= (3,0):
        mode = 'w'
    else:
        mode = 'wb'


    daily_file_name = "flagged_" + output_daily
    with open(daily_file_name, mode) as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

        writer.writerow(["STCODE","FORMAT","SITECODE","WATERYEAR","DATE","MEAN_Q","MAX_Q","MIN_Q","MEAN_Q_AREA","TOTAL_Q_AREA", "DAILY_FLAG"])
        for each_new_key in sorted(od_1.keys()):

            writer.writerow(od_1[each_new_key])

def flow_the_data(raw_dts, raw_hts, rating_calib, desired=300):
    """
    the actual computation occurs here
    the desired interval is 300 seconds, or "5 minutes"
    """

    od = {}

    # initial values - database precision is 6. Since we need to round it out, go to 7.
    # You might have to play with this if the numbers are off a little bit.
    # Not going out enough or too much will throw you into the wrong equation set.

    if sys.version_info >= (3,0):
        try:
            this_stage = round(float(next(raw_hts)),7)
        except Exception:
            import pdb; pdb.set_trace()
    else:
        this_stage = round(float(raw_hts.next()),7)

    print("the first stage is " + str(this_stage))

    if sys.version_info >= (3,0):
        this_date = next(raw_dts)
    else:
        this_date = raw_dts.next()
    print("the first date is " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S'))

    # Iteration will continue until we run out of values... exception is thrown and results returned
    while True:

        try:
            try:
                low_cutoff, this_max = check_value_versus_keys(rating_calib, this_stage)

            except TypeError:
                import pdb; pdb.set_trace()
                try:
                    low_cutoff, this_max = None, None
                    print("A type error occurred, check if there is a None in the data on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S'))
                except Exception:
                    print("the exception was thrown here and od was returned")
                    import pdb; pdb.set_trace()
                    return od

            try:
                if sys.version_info >= (3,0):
                    next_stage = round(float(next(raw_hts)),7)
                else:
                    next_stage = round(float(raw_hts.next()),7)

                if sys.version_info >= (3,0):
                    next_date = next(raw_dts)
                else:
                    next_date = raw_dts.next()

            except Exception:
                # iterate over the "next" values that don't work - i.e. you want to pass over the value that doesn't work because it is a None or NaN ...
                #import pdb; pdb.set_trace()
                if sys.version_info >= (3,0):
                    discard_output1 = next(raw_hts)
                else:
                    discard_output1 = raw_hts.next()

                if sys.version_info >= (3,0):
                    discard_output2 = next(raw_dts)
                else:
                    discard_output2 = raw_dts.next()

                print("unexpected next value of " + str(discard_output1) + " for " + datetime.datetime.strftime(discard_output2, '%Y-%m-%d %H:%M:%S'))
                print("skipping this value and continuing")

                if str(discard_output1) == "nan":
                    discard_output1 = "None"

                # dealing with when you have an interval that comes off a missing value - if there isn't a stage, all outputs is none -- if there is a stage, give the stage
                if str(discard_output1)=="None":
                    od[discard_output2] = {'stage': None, 'inst_q': None, 'total_q': None, 'mean_q':None}
                else:
                    od[discard_output2] ={'stage': discard_output1, 'inst_q': None, 'total_q': None, 'mean_q': None}

                #import pdb; pdb.set_trace()
                continue

            #if this_date == datetime.datetime(2014,10,2,0,0):
            #    import pdb; pdb.set_trace()
            # makes sure that the interval is the correct length (300 seconds == 5 minutes). if it is not, returns the appropriate length. If you want not five minutes add in a third arguement for a different stamp like: interval_length = check_interval_length(this_date, next_date, desired = 100) or whatever you want
            interval_length = check_interval_length(this_date, next_date)

            # HAPPIEST CASE: if the next stage is the same height as this stage height and they are 5 minutes apart then we can take the calculated value for this height and integrate it over 300 seconds (5 minutes)
            if next_stage == this_stage and interval_length == desired/60:

                #print "desired interval length is " + str(desired/60)

                #print "next stage is this stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S')

                inst_q = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], this_stage)

                # record the values
                if this_date not in od:
                    try:
                        od[this_date] ={'stage': round(this_stage,3), 'inst_q': inst_q, 'total_q': desired*inst_q, 'mean_q': inst_q}

                    except Exception:
                        # this might happen if there is no flow or "negative flow"
                        od[this_date] ={'stage': round(this_stage,3), 'inst_q': None, 'total_q': None, 'mean_q': None}
                else:
                    print("this error should never occur - " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " has already been processed for this site and value!!")

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date
                continue

            # NEXT HAPPIEST CASE: if the next stage height is in the same "bracket" as this stage height and they are five minutes apart then we can do the trapezoid method
            elif next_stage <= this_max and next_stage > low_cutoff and interval_length==desired/60:

                #print "next stage is LIKE stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')

                # current q, q in 5 minutes
                instq_now  = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], this_stage)
                instq_next = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], next_stage)

                try:
                    # 1/2 * 300 seconds interval  * (base 1 + base 2)
                    traps = 0.5*desired*(instq_now + instq_next)

                    # record the values
                    if this_date not in od:
                        od[this_date] = {'stage': this_stage, 'inst_q': instq_now, 'total_q': traps, 'mean_q': traps/desired}
                    else:
                        print("this error should never occur - " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " has already been processed for this site and value!!")

                except Exception:

                    # record the values - in this case there is some problem with the processing of now or later, don't know which
                    if this_date not in od:
                        od[this_date] = {'stage': this_stage, 'inst_q': None, 'total_q': None, 'mean_q': None}
                    else:
                        print("this error should never occur - " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " has already been processed for this site and value!!")

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date

                continue


            # if the next stage > this_max or the next_stage <= the low cutoff or the interval length is not 5
            else:
                try:

                    # if its not in that same range we get a new calibration
                    low_cutoff, this_max = check_value_versus_keys(rating_calib, next_stage)

                except Exception:
                    # will be called  if the value is none/nan/unexpected
                    print("Adam has assigned an unexceptable missing code or the data value is way out of the allowed range of the max height, please check on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + ", converting to Pythonic None")

                    if str(next_stage) == "nan":
                        next_stage = None
                    elif next_stage > this_max:
                        next_stage = None

                    #else:
                    #    next_stage = "None"

                    return od

                #print "next stage is UNLIKE stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S')

                # interpolate for one minute for each value
                one_minute_heights = interpolate_raw(this_stage, next_stage, interval_length)

                # blank for wrong length interval - ex. when sparse
                pseudo_dates = []

                # if the interval is the wrong length, create fake date stamps where the minutes are 5
                if interval_length != 5:
                    pseudo_dates = drange(this_date, next_date, datetime.timedelta(minutes=5))

                # append the one minute values to here
                local_sum = []

                # number of seconds in the total interval
                interval_length_seconds = interval_length*60

                # for each one minute height, compute the correct q from the rating equations
                for each_height in one_minute_heights:

                    _, local_max = check_value_versus_keys(rating_calib, each_height)

                    try:
                        # this is essentially cf/minute
                        instq = 60*logfunc(rating_calib[local_max][0], rating_calib[local_max][1], each_height)

                    except Exception:
                        instq = None

                    # this will give back a number of instq values
                    local_sum.append(instq)

                # if the pseudo dates exist because the interval is the wrong length
                if this_date not in od and pseudo_dates != []:

                    # iterate over the local sum and create 5 minute values
                    for index, each_instq in enumerate(local_sum):

                        # if the index is a multiple of 5, a new list is started
                        if index%5 == 0:
                            mini_sum = []
                            mini_sum.append(each_instq)
                            # assign height to first value
                            my_height = one_minute_heights[index]

                        # append to that list each incoming cfm
                        elif index%5 <4:
                            mini_sum.append(each_instq)

                        # and then if you have 5 values in it, stick that with a 5 minute date thing and add to the reference
                        elif index%5 == 4:

                            this_total = sum([x for x in mini_sum if str(x) != 'None'])
                            this_inst = mini_sum[0]/60
                            # mean is in cfs
                            this_mean = sum([x for x in mini_sum if str(x) != 'None'])/300

                            if sys.version_info >=(3,0):
                                my_date = next(pseudo_dates)
                            else:
                                my_date = pseudo_dates.next()

                            try:
                                od[my_date] ={'stage': round(my_height,3), 'inst_q': this_inst, 'total_q': this_total, 'mean_q': this_mean}

                            except Exception:
                                od[my_date]= {'stage': round(my_height,3), 'inst_q': None, 'total_q' : None, 'mean_q': None}

                elif this_date not in od and pseudo_dates == []:
                    # as long as there actually is some data
                    if len([x for x in local_sum if str(x) != 'None']) != 0:

                        try:
                            od[this_date] = {'stage': round(this_stage,3), 'inst_q': local_sum[0]/60, 'total_q' : sum([x for x in local_sum if str(x) != 'None']), 'mean_q': sum([x for x in local_sum if str(x) != 'None'])/interval_length_seconds}

                        except Exception:
                            # strage cases where there is a "one second value" when the code switches
                            test_date = datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')[-1]

                            if test_date != 0:
                                print("this is a one second measurement on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " which will go into the next value")
                                pass
                            else:
                                import pdb; pdb.set_trace

                    # if for some reason there isn't some data
                    elif len([x for x in local_sum if str(x) != 'None']) == 0:
                        od[this_date]= {'stage': round(this_stage,3), 'inst_q': None, 'total_q' : None, 'mean_q': None}

                else:
                    print("this is an error")

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date

                #print("date is" + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S'))

        except StopIteration:
            break

    return od

def drange(start, stop, step):
    """ fraction/date range generator """
    r = start
    while r < stop:
        yield r
        r += step

def quickly_recheck_data(data_in_csv):
    """
    Checks the input data for values that are un-expected; i.e. cannot be turned to float numericals, etc.
    """

    # initiate with a value of none
    bad = []
    for each_day in sorted(list(data_in_csv.keys())):
        try:
            valid = float(data_in_csv[each_day])
        except Exception:
            bad.append((each_day, valid))

    if bad != []:
        print("there are bad values on : -->")
        for each_tuple in bad:
            try:
                print(datetime.datetime.strftime(each_tuple[0],'%Y-%m-%d %H:%M:%s') + " : " + str(each_tuple[1]))
            except Exception:
                print(datetime.datetime.strftime(each_tuple[0],'%Y-%m-%d %H:%M:%s') + " : " + 'Output forced to \'None\'')
    else:
        pass

def to_area(sitecode, instq, totalq, meanq):
    """ converts the values to the area"""

    areas = {'GSWS01': 237., 'GSWS02': 149., 'GSWS03': 250., 'GSWS06':32, 'GSWS07':38., 'GSWS08':53., 'GSWS09':21., 'GSWS10':25.3, 'GSWSMA':1436., 'GSWSMF':1436., 'GSCC01':171., 'GSCC02': 169., 'GSCC03': 123., 'GSCC04':120.}

    acres_to_cfs = areas[sitecode]*43560.
    acres_to_sqmiles = areas[sitecode]*0.0015625

    try:
        # total q in inches per acre
        total_q_area_ft = (totalq/acres_to_cfs)
        # fixed -- total q area incheas should be feet * 12 not divided by 12
        total_q_area_inches = total_q_area_ft*12.

    except Exception:
        total_q_area_ft = None
        total_q_area_inches = None

    try:
        # cfs per square mile
        inst_q_area = (instq/acres_to_sqmiles)
    except Exception:
        inst_q_area = None

    try:
        # mean cfs per square mile
        mean_q_area = (meanq/acres_to_sqmiles)
    except Exception:
        mean_q_area = None

    return inst_q_area, total_q_area_inches, mean_q_area

def name_my_csv(sitecode, wateryear, type_of_data):
    """
    Name CSV's from the main loop based on a simple criterion
    """

    if type_of_data not in ["d","s","m"]:
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_high.csv"
    elif type_of_data == "d":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_daily.csv"
    elif type_of_data == "s":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_spoints.csv"
    elif type_of_data == "m":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_monthly.csv"
    else:
        csvfilename = "TEMP_CSV.csv"
        print("TEMP_CSV.csv used for output! WARNING!")

    return csvfilename

def print_five_minute_file(final_dictionary, sitecode, wateryear, interval_length, original_data, sample_dates):
    """ Creates the five minute values -- now including sample dates!"""

    if sample_dates != None:
        # go from 1 to end of sample dates because we added in the first day to do the first "calculation"
        ordered_samples = iter(sample_dates[1:])
        # test each sample in order rather than all
        if sys.version_info >= (3,0):
            given_sample = next(ordered_samples)
            mode = 'w'
        else:
            given_sample = ordered_samples.next()
            mode = 'wb'
    else:
        # give some ridiculous value for given sample so that it will never test "S"
        given_sample = datetime.datetime(1,1,1,0,0)

    csvfilename = name_my_csv(sitecode, wateryear, interval_length)

    if sys.version_info >=(3,0):
        mode = 'w'
    else:
        mode = 'wb'

    with open(csvfilename, mode) as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

        writer.writerow(['STCODE', 'FORMAT', 'SITECODE', 'WATERYEAR', 'DATE_TIME', 'EQN_SET_CODE', 'STAGE', 'INST_Q', 'INST_Q_AREA', 'INTERVAL', 'MEAN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_INT', 'EST_CODE', 'EVENT_CODE'])

        sorted_dates = sorted(list(final_dictionary.keys()))


        #import pdb; pdb.set_trace()

        for index, each_date in enumerate(sorted_dates):
            stage = final_dictionary[each_date]['stage']
            instq = final_dictionary[each_date]['inst_q']
            totalq = final_dictionary[each_date]['total_q']
            eqn_set = final_dictionary[each_date]['eqn_set']
            try:
                flag  = original_data[each_date]['fval']
            except KeyError:
                flag  = 'E'

            try:
                event = original_data[each_date]['event']
            except KeyError:
                event = 'NA'

            try:
                # test that a date is not a sample date
                if each_date == given_sample:
                    flag = 'S'
                    if sys.version_info >= (3,0):
                        given_sample = next(ordered_samples)
                    else:
                        given_sample = ordered_samples.next()
                else:
                    pass
            except Exception:
                pass

            # if its not the first value - the mean value computed to the "end" of the interval should be reflected in the previous entry; the total also
            if index != 0:
                meanq = final_dictionary[sorted_dates[index-1]]['mean_q']
                totalq = final_dictionary[sorted_dates[index-1]]['total_q']
            else:
                meanq = final_dictionary[each_date]['mean_q']
                totalq = final_dictionary[each_date]['total_q']

            iqa, tqa, mqa =  to_area(sitecode, instq, totalq, meanq)

            dt = datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S')
            study_code = "HF004"
            entity = 1

            interval = interval_length

            # sometimes Adam's flag has extra quotes in it. Sometimes it doesn't.
            if "\"M\"" in flag:
                flag = "M"
            else:
                pass

            # if the data is None because of some failure to estimate the height we need to mark it as missing.
            if str(stage) == "None" or str(instq) == "None" or str(totalq) == "None":
                flag = "M"

            try:
                new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, round(float(stage),3), round(float(instq),3), round(float(iqa),3), str(interval), round(float(meanq),3), round(float(mqa),3), round(float(tqa),7), flag, event]

            except Exception:
                # testing if the str(stage) == "None" or not is better than testing for None and "None"
                if str(stage) == "None":
                    new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, 'None', 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]

                elif str(stage) != "None":
                    try:
                        new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, round(float(stage),3), 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]
                    except Exception:
                        new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, 'None', 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]

            writer.writerow(new_row)


    #print("Finished processing the five minute data, output location : " + csvfilename)

def create_monthly_files(sitecode, wateryear, daily_dictionary):
    """
    Creates the monthly files for your site and wateryear based on a daily reference table you created in the main loop.
    """
    md = {}

    # name of monthly csv file
    csvfilename_m = name_my_csv(sitecode, wateryear, "m")
    print(csvfilename_m)

    stcode = 'HF004'
    format = '3'
    sorted_dates = sorted(list(daily_dictionary.keys()))

    if sys.version_info >= (3,0):
        mode = 'w'
    else:
        mode = 'wb'

    # we already have a way to write the daily csv, so this is just for monthly and annual
    with open(csvfilename_m, mode) as writefile_m:
        writer_m = csv.writer(writefile_m, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers_m = ['STCODE', 'FORMAT', 'SITECODE', 'ANNUAL_YEAR', 'WATERYEAR', 'MONTH', 'MEAN_Q', 'MAX_Q', 'MIN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_AREA', 'ESTCODE','ESTDAYS', 'TOTAL_DAYS']

        writer_m.writerow(headers_m)

        for each_day in sorted_dates:

            month_found = each_day.month
            year_found = each_day.year

            if each_day >= datetime.datetime(int(wateryear), 10, 1, 0, 0):
                continue
            else:
                pass

            if month_found not in md:
                md[month_found]={'mean':[], 'max':[], 'min': [], 'mqa': [], 'tqa':[], 'flag':[]}
                md[month_found]['mean'].append(daily_dictionary[each_day]['mean'])
                md[month_found]['max'].append(daily_dictionary[each_day]['max'])
                md[month_found]['min'].append(daily_dictionary[each_day]['min'])
                md[month_found]['mqa'].append(daily_dictionary[each_day]['mqa'])
                md[month_found]['tqa'].append(daily_dictionary[each_day]['tqa'])
                md[month_found]['flag'].append(daily_dictionary[each_day]['flag'])

            elif month_found in md:
                md[month_found]['mean'].append(daily_dictionary[each_day]['mean'])
                md[month_found]['max'].append(daily_dictionary[each_day]['max'])
                md[month_found]['min'].append(daily_dictionary[each_day]['min'])
                md[month_found]['mqa'].append(daily_dictionary[each_day]['mqa'])
                md[month_found]['tqa'].append(daily_dictionary[each_day]['tqa'])
                md[month_found]['flag'].append(daily_dictionary[each_day]['flag'])


        # reorganize the months to reflect the water year, and if months are missing, then do not try to find them
        month_keys = [10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        real_keys = list(sorted(md.keys()))

        shared_keys = [x for x in month_keys if x in real_keys]

        for each_month in shared_keys:
            num_est = len([x for x in md[each_month]['flag'] if x == 'E'])
            num_question = len([x for x in md[each_month]['flag'] if x == 'Q'])
            num_missing = len([x for x in md[each_month]['flag'] if x == 'M'])
            num_tot = len([x for x in md[each_month]['flag']])


            if num_est/num_tot >= 0.05:
                monthly_flag = "E"
            elif num_question/num_tot > 0.05:
                monthly_flag = "Q"
            elif num_missing/num_tot >= 0.2:
                monthly_flag = "M"
            elif num_missing + num_est + num_question >= 0.05:
                monthly_flag = "Q"
            else:
                monthly_flag = "A"


            if str(each_month) == "10" or str(each_month) == "11" or str(each_month) == "12":
                this_year = str(int(wateryear) -1)
            else:
                this_year = str(wateryear)

            month_mean = str(round(sum([float(x) for x in md[each_month]['mean'] if str(x) != "None"])/len([float(x) for x in md[each_month]['mean'] if str(x) != "None"]),4))
            month_max = str(round(max([float(x) for x in md[each_month]['max'] if str(x) != "None"]),4))
            month_min = str(round(min([float(x) for x in md[each_month]['min'] if str(x) != "None"]),4))
            month_mqa = str(round(sum([float(x) for x in md[each_month]['mqa'] if str(x) != "None"])/len([float(x) for x in md[each_month]['mean'] if str(x) != "None"]),4))
            month_tqa = str(round(sum([float(x) for x in md[each_month]['tqa'] if str(x) != "None"]),4))

            writer_m.writerow([stcode, format, sitecode, str(this_year), wateryear, str(each_month), month_mean, month_max, month_min, month_mqa, month_tqa, monthly_flag, str(num_est), str(num_tot)])


def compute_daily_dictionary(sitecode, wateryear, final_dictionary, original_dictionary):
    """
    Computes daily values as a dictionary of monthly/ annual values
    """
    daily_d = {}
    output_d = {}

    for each_date in sorted(list(final_dictionary.keys())):

        alt_date = datetime.datetime(each_date.year, each_date.month, each_date.day)

        if alt_date not in daily_d:

            daily_d[alt_date] = {'means':[final_dictionary[each_date]['mean_q']], 'insts':[final_dictionary[each_date]['inst_q']], 'tots':[final_dictionary[each_date]['total_q']], 'flags':[original_dictionary[each_date]['fval']]}

        elif alt_date in daily_d:

            daily_d[alt_date]['means'].append(final_dictionary[each_date]['mean_q'])
            daily_d[alt_date]['insts'].append(final_dictionary[each_date]['inst_q'])
            daily_d[alt_date]['tots'].append(final_dictionary[each_date]['total_q'])

            try:
                daily_d[alt_date]['flags'].append(original_dictionary[each_date]['fval'])
            except KeyError:
                daily_d[alt_date]['flags'].append('E')


    for each_alternate_date in sorted(list(daily_d.keys())):

        percent_m = len([x for x in daily_d[each_alternate_date]['flags'] if x == "M"])/len(daily_d[each_alternate_date])
        percent_e = len([x for x in daily_d[each_alternate_date]['flags']  if x == "E"])/len(daily_d[each_alternate_date])
        percent_q = len([x for x in daily_d[each_alternate_date]['flags']  if x == "Q"])/len(daily_d[each_alternate_date])

        if percent_m > 0.2:
            daily_flag = "M"
            #daily_d[each_alternate_date].append(daily_flag)
        elif percent_e > 0.05:
            daily_flag = "E"
        elif percent_q > 0.05:
            daily_flag = "Q"
        elif percent_m + percent_e + percent_q > 0.05:
            daily_flag = "Q"
        else:
            daily_flag = "A"

        try:
            _, tqa, mqa = to_area(sitecode, None, sum(daily_d[each_alternate_date]['tots']), sum(daily_d[each_alternate_date]['means'])/len(daily_d[each_alternate_date]['means']))

        except Exception:
            # find the total number of values which are not Null
            mean_from_not_none_tot = sum([float(x) for x in daily_d[each_alternate_date]['means'] if str(x) !="None"])/len([float(x) for x in daily_d[each_alternate_date]['means'] if str(x) !="None"])

            not_none_tot = sum([float(x) for x in daily_d[each_alternate_date]['tots'] if str(x) != 'None'])
            _, tqa, mqa = to_area(sitecode, None, not_none_tot, mean_from_not_none_tot)

        try:
            # same format as the csv for daily but to a dictionary
            if each_alternate_date not in output_d:
                output_d[each_alternate_date] = {'mean': str(round(sum([float(x) for x in daily_d[each_alternate_date]['means'] if str(x) != "None"])/len([float(x) for x in daily_d[each_alternate_date]['means'] if str(x) != "None"]),4)), 'max': str(round(max([float(x) for x in daily_d[each_alternate_date]['insts'] if str(x) != "None"]),4)), 'min': str(round(min([float(x) for x in daily_d[each_alternate_date]['insts'] if str(x) != "None"]),4)), 'mqa': str(round(mqa),4), 'tqa': str(round(tqa),4), 'flag': daily_flag }

            elif each_alternate_date in output_d:
                print("the alternate date is already listed?")

        except Exception:

            not_none_mean_day = [x for x in daily_d[each_alternate_date]['means'] if str(x) != 'None']
            not_none_inst_day = [x for x in daily_d[each_alternate_date]['insts'] if str(x) != 'None']

            try:

                if each_alternate_date not in output_d:
                    output_d[each_alternate_date] = {'mean': str(round(sum(not_none_mean_day)/len(not_none_mean_day),4)), 'max': str(round(max(not_none_inst_day),4)), 'min': str(round(min(not_none_inst_day),4)), 'mqa': str(round(mqa,4)), 'tqa': str(round(tqa,4)), 'flag': daily_flag}
                elif each_alternate_date in output_d:
                    print("the alternate date is already listed- part 2 error with Nones")

            except Exception:
                if each_alternate_date not in output_d:
                    output_d[each_alternate_date] = {'mean': str(round(sum(not_none_mean_day)/len(not_none_mean_day),4)), 'max': str(round(max(not_none_inst_day),4)), 'min': str(round(min(not_none_inst_day),4)), 'mqa': "None", 'tqa': "None", 'flag': daily_flag}
                elif each_alternate_date in output_d:
                    print("the alternate date is already listed- part 2 error with Nones")


    return output_d

def print_daily_values(sitecode, wateryear, final_dictionary, original_dictionary):
    """
    creates a daily output csv
    """

    naner = lambda x: 'None' if x == 'nan' else x

    csvfilename = name_my_csv(sitecode, wateryear, "d")

    daily_d = {}

    stcode = 'HF004'
    format = '2'
    sorted_dates = sorted(list(final_dictionary.keys()))


    if sys.version_info >= (3,0):
        mode = 'w'
    else:
        mode = 'wb'

    with open(csvfilename, mode) as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers = ['STCODE', 'FORMAT', 'SITECODE', 'WATERYEAR', 'DATE', 'MEAN_Q', 'MAX_Q', 'MIN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_AREA', 'ESTCODE']

        writer.writerow(headers)

        for each_date in sorted(list(final_dictionary.keys())):

            alt_date = datetime.datetime(each_date.year, each_date.month, each_date.day)

            if alt_date not in daily_d:

                # at least one date must be present and we prefer midnight
                daily_d[alt_date] = {'means': naner([final_dictionary[each_date]['mean_q']]), 'insts': naner([final_dictionary[each_date]['inst_q']]), 'tots': naner([final_dictionary[each_date]['total_q']]), 'flags':[original_dictionary[each_date]['fval']]}

            elif alt_date in daily_d:

                daily_d[alt_date]['means'].append(naner(final_dictionary[each_date]['mean_q']))
                daily_d[alt_date]['insts'].append(naner(final_dictionary[each_date]['inst_q']))
                daily_d[alt_date]['tots'].append(naner(final_dictionary[each_date]['total_q']))

                try:
                    daily_d[alt_date]['flags'].append(original_dictionary[each_date]['fval'])
                except KeyError:
                    daily_d[alt_date]['flags'].append('E')


        for each_alternate_date in sorted(daily_d.keys()):

            percent_m = len([x for x in daily_d[each_alternate_date]['flags'] if x == "M"])/len(daily_d[each_alternate_date])
            percent_e = len([x for x in daily_d[each_alternate_date]['flags']  if x == "E"])/len(daily_d[each_alternate_date])
            percent_q = len([x for x in daily_d[each_alternate_date]['flags']  if x == "Q"])/len(daily_d[each_alternate_date])


            # flags re-ordered to fit our daily method
            if percent_m > 0.2:
                daily_flag = "M"

            elif percent_e > 0.05:
                daily_flag = "E"

            elif percent_q > 0.05:
                daily_flag = "Q"

            elif percent_m + percent_e + percent_q > 0.05:
                daily_flag = "Q"

            else:
                daily_flag = "A"


            try:
                _, tqa, mqa = to_area(sitecode, None, sum(daily_d[each_alternate_date]['tots']), np.mean(daily_d[each_alternate_date]['means']))

            except Exception:
                not_none_tot = sum([x for x in daily_d[each_alternate_date]['tots'] if str(x) != 'None'])
                mean_from_not_none_tot = np.mean([x for x in daily_d[each_alternate_date]['means'] if str(x) !='None'])
                _, tqa, mqa = to_area(sitecode, None, not_none_tot, mean_from_not_none_tot)

            try:
                new_row = [stcode, format, sitecode, wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(sum(daily_d[each_alternate_date]['means'])/len(daily_d[each_alternate_date]['means']),4)), str(round(max(daily_d[each_alternate_date]['insts']),4)), str(round(min(daily_d[each_alternate_date]['insts']),4)), str(round(mqa,4)), str(round(tqa,4)), daily_flag]

            except Exception:

                not_none_mean_day = [float(x) for x in daily_d[each_alternate_date]['means'] if str(x) != 'None']
                not_none_inst_day = [float(x) for x in daily_d[each_alternate_date]['insts'] if str(x) != 'None']

                try:
                    new_row = [stcode, format, sitecode , wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(sum(not_none_mean_day)/len(not_none_mean_day),4)), str(round(max(not_none_inst_day),4)), str(round(min(not_none_inst_day),4)), str(round(mqa,4)), str(round(tqa,4)), daily_flag]

                except Exception:
                    import pdb; pdb.set_trace()
                    new_row = [stcode, format, sitecode , wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(sum(not_none_mean_day)/len(not_none_inst_day),4)), str(round(max(not_none_inst_day),4)), str(round(min(not_none_inst_day),4)), "None", "None", daily_flag]

            writer.writerow(new_row)

def print_sdate_values(wateryear, final_dictionary, sitecode_in, sDate_list):
    """ prints the sdates and total q area between them if if it possible"""

    sDate_d = {}
    areas = {'GSWS01': 237., 'GSWS02': 149., 'GSWS03': 250., 'GSWS06':32, 'GSWS07':38., 'GSWS08':53., 'GSWS09':21., 'GSWS10':25.3, 'GSWSMA':1436., 'GSWSMF':1436., 'GSCC01':171., 'GSCC02': 169., 'GSCC03': 123., 'GSCC04':120.}

    stcode = 'HF004'
    format = '6'
    sitecode = sitecode_in
    sorted_dates = sorted(list(final_dictionary.keys()))

    csvfilename = name_my_csv(sitecode_in, wateryear, 's')

    if sys.version_info >= (3,0):
        mode = 'w'
    else:
        mode = 'wb'

    with open(csvfilename, mode) as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers = ['STCODE', 'FORMAT' ,'SITECODE', 'WATERYEAR', 'BEGIN_DATETIME', 'END_DATETIME', 'TOTAL_Q_SMPL', 'ESTCODE']

        writer.writerow(headers)

        # add an extra copy of the final day to act as a buffer for the second index
        sDate_list.append(datetime.datetime(int(wateryear),10,1,0,0))

        starting = iter(sDate_list)

        if sys.version_info >= (3,0):
            this_date = next(starting)
            subsequent = next(starting)
        else:
            this_date = starting.next()
            subsequent = starting.next()

        if this_date.minute%5 != 0:
            add_to_date = datetime.timedelta(minutes = (5-this_date.minute%5))
            this_date = this_date + add_to_date
            print("added minutes to date")

        if subsequent.minute%5 != 0:
            add_to_date = datetime.timedelta(minutes = (5-subsequent.minutes%5))
            subsequent = subsequent + add_to_date
            print("added minutes to date")


        # these are the final outputs from the data
        sorted_dates = sorted(list(final_dictionary.keys()))

        for each_date in sorted_dates:

            try:

                if type(this_date) != datetime.datetime:
                    this_date = datetime.datetime.strptime(this_date, '%Y-%m-%d %H:%M:%S')
                    #print("converted this date to correct format")

                if type(each_date) != datetime.datetime:
                    each_date = datetime.datetime.strptime(each_date, '%Y-%m-%d %H:%M:%S')
                    #print("converted each date to correct format")

                if each_date>= this_date and each_date<subsequent:

                    if this_date not in sDate_d:
                        sDate_d[this_date] = {'total_q':[final_dictionary[each_date]['total_q']] }
                    elif this_date in sDate_d:
                        if final_dictionary[each_date]['total_q'] != None:
                            sDate_d[this_date]['total_q'].append(final_dictionary[each_date]['total_q'])
                        else:
                            pass

                elif each_date == subsequent:

                    this_date = subsequent

                    if sys.version_info >= (3,0):
                        subsequent = next(starting)
                    else:
                        subsequent = starting.next()

                    if this_date not in sDate_d:
                        sDate_d[this_date] = {'total_q':[final_dictionary[each_date]['total_q']] }
                    elif this_date in sDate_d:
                        if final_dictionary[each_date]['total_q'] !=None:
                            sDate_d[this_date]['total_q'].append(final_dictionary[each_date]['total_q'])
                        else:
                            pass

                elif each_date > subsequent and subsequent.minutes%5 != 0:
                    remainder = subsequent.minutes%5
                    five_minus = 5-remainder
                    subsequent += datetime.timedelta(minutes = five_minus)
                    print(subsequent)

                    if each_date == subsequent:

                        this_date = subsequent

                        if sys.version_info >= (3,0):
                            subsequent = next(starting)
                        else:
                            subsequent = starting.next()

                        if this_date not in sDate_d:
                            sDate_d[this_date] = {'total_q':[final_dictionary[each_date]['total_q']] }
                        elif this_date in sDate_d:
                            if final_dictionary[each_date]['total_q'] !=None:
                                sDate_d[this_date]['total_q'].append(final_dictionary[each_date]['total_q'])
                            else:
                                pass
                elif each_date > subsequent and subsequent.minutes%5 == 0:
                    import pdb; pdb.set_trace()
                    # if you are at the end of the data you can comment this in to see what dates still exist
                    # print "found: " + datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S') + " which is bigger than the last day"

            except Exception:
                pass


        for index,each_date in enumerate(sorted(list(sDate_d.keys()))):

            # create a date that you can print based on the first date
            print_date = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')

            try:
                print_date_2 = datetime.datetime.strftime(sorted(sDate_d.keys())[index + 1], '%Y-%m-%d %H:%M:%S')
            except Exception:
                print("S-points have been output to the final available date.")
                return True

            sDate_d[each_date].update({'sample_total': sum(sDate_d[each_date]['total_q'])*12/(43560*areas[sitecode])})

            new_row = [stcode, format, sitecode, wateryear, print_date, print_date_2, round(sDate_d[each_date]['sample_total'],3)]

            writer.writerow(new_row)

if __name__ == "__main__":

    sitecode = sys.argv[1]
    wateryear = sys.argv[2]
    filetype = sys.argv[3]

    print("Now processing \'pyflow\' for sitecode \'" + str(sitecode) + "\' and wateryear \'" + str(wateryear) + "\', with a source of was \'" + str(filetype) + "\'")

    if filetype.lower() == "csv":

        csvfilename = os.path.join(sitecode.upper() + "_" + str(wateryear) + "_working", sitecode.upper() + "_" + str(wateryear) + "_re.csv")
        print("......Getting data from csv file :\'" + csvfilename + "\', which is located in your \'working\' directory. I always get files ending in \'_re\'")

        # new: bfav is bad flags and values, which may indicate some problems in the data
        o2, bfav = get_data_from_csv(csvfilename)

        if bfav != {}:
            quickly_recheck_data(o2)

            if sys.version_info >= (3,0):
                value = input("It appears your data may not be complete. Press 'y' to continue or enter to quit")
            else:
                value = raw_input("It appears your data may not be complete. Press 'y' to continue or enter to quit")

            if value != 'y':
                sys.exit("Exiting. Please check the adjusted data in " + csvfilename)

    elif filetype.lower() == "sql":
        conn, cur = fc()
        print(".....Getting data from SQL Server... warning, this function has NEVER been used before. ")
        first_day = datetime.datetime(int(wateryear)-1, 10, 1, 0, 0)
        last_day = datetime.datetime(wateryear, 10, 1, 0, 0)
        o2 = get_data_from_sql(cur, sitecode, first_day, last_day)

    else:
        print(" I have no idea where you want to get the data from, try \'csv\' or \'sql\' ")

    # connect to server to get the data
    conn, cur = fc()

    # get the equation sets you need to run this analysis
    o = get_equation_sets(cur, sitecode, wateryear)

    # modify that dictionary to have the maxheight mapped to ln_a and b
    o1 = get_equations_by_value(cur, sitecode, o)

    # get the sample dates.
    sd = get_samples_dates(cur, sitecode, wateryear)

    # create iterators for the pyflow
    o3 = set_up_iterators(o2, o1, wateryear)



    # go through the data
    o4 = loop_over_data(o3, o1)

    print("... now printing the five minute file to csv ... ")
    print_five_minute_file(o4, sitecode, wateryear, 5, o2, sd)

    print("... now printing the daily file to csv ...")
    print_daily_values(sitecode, wateryear, o4, o2)

    if sd != None:
        print("... now printing the S codes to csv ... ")
        print_sdate_values(wateryear, o4, sitecode, sd)
    else:
       pass

    print("... now printing the monthly file to csv ...")
    o_daily = compute_daily_dictionary(sitecode, wateryear, o4, o2)
    create_monthly_files(sitecode, wateryear, o_daily)


    print("Finished creating your pyflow. see the root of your directory for the files :)")