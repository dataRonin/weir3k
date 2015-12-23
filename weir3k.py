import csv
import datetime
import os
import fnmatch
import sys
import shutil
import pymssql
import os.path
from itertools import islice
import math
import numpy as np
import matplotlib.pyplot as plt
#import mpld3
import matplotlib.dates as mdates
import matplotlib
import errno
from scipy.interpolate import interp1d


"""
A script for correction of streamflow.
Version 2.0.1
Fox Sparky Peterson
Creative Commons ShareAlike 3.0 License
You are free to share, copy, transmit, and adapt this work, but you must provide attribution to Fox Peterson and ShareAlike in kind.
"""

def make_sure_path_exists(path):
    """ A cross platform solution for making a path correctly.

    :os.umask(0000): sets the permissions on the directory to all openness, so that you can write the new images within the files you create
    If that seems to not work, you will want to change the name of the directory each run so you can fix this; I'm not sure how your computer will respond.
    """
    try:
        os.umask(0000)
        os.mkdir(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def string_correct(sitecode_raw, wateryear_raw):
    """ Makes sure the cases are correct - uppercase sitecode, integer wateryear

    :sitecode_raw: is the sitecode (i.e. GSWS01)
    :wateryear: integer year
    """
    sitecode = sitecode_raw.upper()
    wateryear = int(wateryear_raw)

    return sitecode, wateryear

def find_files(sitecode, wateryear, subfolder):
    """ Finds the raw data files by searching 'raw_data' directory as a subfolder

    :sitecode: ex. GSWS01
    :wateryear: ex. 2010
    :subfolder: ex. '~/myData/working/'
    """

    raw_data_file = []
    for root, dir, names in os.walk(subfolder):
        for x in names:

            # exclude filenames with bak or BAK
            if 'bak' in x or 'BAK' in x:
                continue

            # append possible files to the list
            if sitecode in x and str(wateryear) in x:
                raw_data_file.append(os.path.join(subfolder,x))

            elif sitecode.lower() in x and str(wateryear) in x:
                raw_data_file.append(os.path.join(subfolder,x))

            else:
                pass


    return raw_data_file

def find_root_files(sitecode, wateryear):
    """ Finds the files containing 'first' that are not in 'raw_data' directory

    """
    scary_file = []
    for root, dir, files in os.walk('.', topdown="False"):
        if 'raw_data' not in root and 'backups' not in root and 'working' not in root:
            for name in files:

                # if the file name contains first and it's not in raw_data
                if 'first' in name:
                    # and if it also has the sitecode in its name
                    if sitecode.upper() in name and str(wateryear) in name:
                        scary_file.append(name)
                    elif sitecode.lower() in name and str(wateryear) in name:
                        scary_file.append(name)
                    else:
                        pass

    return scary_file

def create_subfolders(sitecode, wateryear):
    """ Creates directories for images and for working data

    **Inputs**
    :sitecode: ex. GSWS01
    :wateryear: ex. 2010

    **Returns**
    :dir_images: directory containing the images, such as /GSWS01_2010_images/
    :dir_working: directory containing the working data, such as /GSWS01_2010_working/
    :dir_backup: directory containing the backup data, to keep track of it
    """

    # directory of images; path to images with a slash in case
    dir_images = str(sitecode) + "_" + str(wateryear) + "_" + "images"

    # directory of working data; where the 7 column data lives
    dir_working = str(sitecode) + "_" + str(wateryear) + "_" + "working"

    # directory for backups -- so you can take a step back if you mess up.
    dir_backup = str(sitecode) + "_" + str(wateryear) + "_" + "backups"

    # create a directory for images if it does not already exist in your folder
    try:
        make_sure_path_exists(dir_images)
    except Exception:
        pass

    # create a directory for working files if it's not in your folder
    try:
        make_sure_path_exists(dir_working)
    except Exception:
        pass

    # create a directory for backup files if it's not in your folder
    try:
        make_sure_path_exists(dir_backup)
    except Exception:
        pass

def convert_corr_to_dict(sitecode, wateryear):
    """ Converts a correction table to a dictionary

    **Inputs**
    :sitecode: ex. GSWS01
    :wateryear: ex. 2010

    **Internal Variables**
    :dateformat_ideal: is what the db has
    :dateformat_old: is what craig enters
    :dateformat_13char: is the 13 character date

    ..Example:

    >>> corr_od[datetime.datetime(2014, 9, 29, 14, 50)]['duration']
    >>> 8910
    >>> corr_od[datetime.datetime(2014, 9, 29, 14, 50)]['bgn_dt']
    >>> datetime.datetime(2014, 9, 23, 10, 20)
    >>> datetime.datetime(2014, 9,23,10,20) + datetime.timedelta(minutes=8910)
    >>> datetime.datetime(2014, 9, 29, 14, 50)
    """

    # note: did not have the wy explicitly in here before 09-30-2015, may have caused namespace errors?
    corr_name = "corr_table_" + sitecode.lower() + "_" + str(wateryear) + ".csv"
    corr = os.path.join('corr_table', corr_name)

    # three possible date formats!
    dateformat_ideal = '%Y-%m-%d %H:%M:%S'
    dateformat_old = '%m/%d/%Y %H:%M'
    dateformat_13char = '%Y%m%d %H%M'

    # output dictionary - starts empty
    od = {}


    # if it's python 3, use the 'r' read versus the 'rb' read.
    if sys.version_info >=(3,0):
        mode = 'r'
    else:
        mode = 'rb'

    # 'r' and 'rb' methods for reading in file. see line above
    with open(corr, mode) as readfile:
        reader = csv.reader(readfile)

        # no need to bring in any values that begin after this water year
        test_value = datetime.datetime(wateryear,10,1,0,5)

        for row in reader:

            # skip header lines
            if str(row[2]) != sitecode:
                continue

            try:
                # first date format
                dt = datetime.datetime.strptime(str(row[3]), dateformat_ideal)

                if dt.minute % 5 != 0:
                    new_minute = dt.minute // 5 * 5
                    dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                    dt += datetime.timedelta(minutes = 5)

                    # if the beginning date time from the corr table is bigger than the last day of the water year, we won't ever use this correction, so don't bother to import it.
                    if dt >= test_value:
                        return od

            except Exception:

                try:
                    # second date format
                    dt = datetime.datetime.strptime(str(row[3]), dateformat_old)
                    if dt.minute % 5 != 0:
                        new_minute = dt.minute // 5 * 5
                        dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                        dt += datetime.timedelta(minutes = 5)

                        # see note above
                        if dt >= test_value:
                            return od

                except Exception:
                    try:
                        # third date format
                        dt = datetime.datetime.strptime(str(row[3]), dateformat_13char)
                        # set the correction to occur on the last five minute interval
                        if dt.minute % 5 != 0:
                            new_minute = dt.minute // 5 * 5
                            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                            dt += datetime.timedelta(minutes = 5)
                            if dt >= test_value:
                                return od

                    except Exception:

                        #print "error importing corr table due to incompatible date on the begin date. check the format and try again"
                        dt = None

            bgncr = float(row[4])
            bgnhg = float(row[5])

            bgnratio = bgnhg/bgncr
            bgn_diff = bgnhg - bgncr


            try:
                # first date format
                enddt = datetime.datetime.strptime(str(row[6]), dateformat_old)

                if enddt.minute % 5 != 0:
                    new_minute = enddt.minute // 5 * 5
                    enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                    enddt += datetime.timedelta(minutes = 5)

            except Exception as exc:

                try:
                    # second date format
                    enddt = datetime.datetime.strptime(str(row[6]), dateformat_ideal)
                    if enddt.minute % 5 != 0:
                        new_minute = enddt.minute // 5 * 5
                        enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                        enddt += datetime.timedelta(minutes = 5)
                except Exception:
                    try:
                        # third date format
                        enddt = datetime.datetime.strptime(str(row[6]), dateformat_13char)

                        if enddt.minute % 5 != 0:
                            new_minute = enddt.minute // 5 * 5
                            enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                            enddt += datetime.timedelta(minutes = 5)

                    except Exception:

                        #print "There is an error importing corr table due to incompatible date on end date - can you bring over an extra record from the subsequent table? In the meantime, I\'ll pass in a None and your adjustments will stop on the last known good correction date"
                        enddt = None

            try:
                endcr = float(row[7])
                endhg = float(row[8])
                endratio = endhg/endcr
                end_diff = endhg - endcr

            except Exception as exc:
                endcr = None
                endhg = None
                endratio = None
                end_diff = None

            try:
                # compute the duration of the interval from that beginning time to its follower in minutes
                duration = (enddt - dt).days*1440 + (enddt - dt).seconds//60

            except Exception:
                duration = None


            # if the key is already in the dictionary, skip it
            if enddt not in od:
                # populate it
                od[enddt] = {'sitecode': sitecode, 'bgn_cr': bgncr, 'bgn_hg': bgnhg, 'bgn_rat': bgnratio, 'bgn_dt' : dt, 'end_cr': endcr, 'end_hg': endhg, 'end_rat': endratio, 'duration':duration, 'end_diff': end_diff, 'bgn_diff': bgn_diff}

            elif enddt in od:
                pass

    # return the correction table as dictioanry
    return od

def drange(start, stop, step):
    """
    Creates a generator for walking over just about any kind of iterable you can imagne
    """
    r = start
    while r < stop:
        yield r
        r += step

def wrapper(walk, stop):
    """
    Holds a drange (calling it a walk here) and it's stop value, and returns the stop value rather than StopIteration
    """
    try:
        return next(walk)
    except StopIteration:
        return stop
    finally:
        del(walk)

def parameterize_first(sitecode, wateryear, filename):
    """ from the raw input figure out which column has the dates and what its format is. assume that the data is in the column which is to the right of the dates. """

    # "output dictionary" --> anytime I use od in a program this is what it is -- Fox 09/10/2015
    od = {}
    date_column = ""

    # figure out which column contains the date and what its type is
    date_type, column = test_csv_structure(filename)
    date_column = column

    # if you are on python 3, use 'r', otherwise, use 'rb'
    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'

    # open the file and process
    with open(filename, mode) as readfile:
        reader = csv.reader(readfile)

        for row in reader:

            # get the date time and call it dt.
            dt = datetime.datetime.strptime(str(row[column]), date_type)

            try:
                data_value = round(float(row[column + 1]),3)

                if str(data_value) == "nan":
                    data_value = None

                # if you forgot and put ESTIMATED DATA IN THE RAW DATA FOLDER

                if len(row) > 4 and row[column + 4] == "E":
                    data_value = round(float(row[column + 2]),3)
                else:
                    pass

            except Exception:
                data_value = None

            # break out of the loop if you have done more than the water year
            if dt > datetime.datetime(wateryear, 10, 1, 0, 0):
                return od
            else:
                pass

            if dt not in od:
                od[dt] = data_value
            elif dt in od:
                pass


    return od, date_column

def generate_first(od, sitecode, wateryear, partial, sparse=False):
    """ Generates the outputs with estimations if sparse is set to false and without estimations if sparse is set to True

    The "first" output will not show the adjustments, just the site code, date, data, and estimated data if you set sparse to false

    ** new feature : if an extra arguement of 'partial' exists, the date will start on a more recent day.
    """

    output_filename = sitecode + "_" + str(wateryear) + "_" + "first.csv"

    if sparse == False:

        # this section just deals with the partial method
        if partial != True:
            # generator to make iterator of a perfect wateryear at 5 minute intervals going from before your data started to after it completes by 5 minutes. The StopIteration gets thrown on the last one, so you wend at 10-01-wateryear.
            compare_range = drange(datetime.datetime(wateryear-1, 10, 1, 0, 0), datetime.datetime(wateryear, 10, 1, 0, 5), datetime.timedelta(minutes=5))

        elif partial == True:

            output_filename = sitecode + "_" + str(wateryear) + "_" + "partial.csv"

            start_date = min(list(od.keys()))
            print(" You are processing a partial water year. Your data will start on " + datetime.datetime.strftime(start_date, '%Y-%m-%d %H:%M:%S'))

            #  generator to make iterator of a perfect wateryear at 5 minute intervals going from before your data started to after it completes by 5 minutes. The StopIteration gets thrown on the last one, so you wend at 10-01-wateryear.
            compare_range = drange(start_date, datetime.datetime(wateryear, 10, 1, 0, 5), datetime.timedelta(minutes=5))

        # Create a blank dictionary with 5 minute spacing. Last value will be on 10-01-wateryear
        blank_dict = dict.fromkeys(compare_range)

        # update your blank dictionary it with existing values from the raw data
        # anything that doesn't have a value will be None
        blank_dict.update(od)

        # create another dictionary to contain flags associated with those estimations
        flag_dict = {}

        # first fill it with blanks and accepteds based on the blanks!
        for each_date in sorted(list(blank_dict.keys())):

            # if data is missing from the raw data, fill in with 'M'
            if blank_dict[each_date] == None:
                flag_dict.update({each_date:'M'})
            else:
                flag_dict.update({each_date:'A'})

        # create a dictionary to contain estimations (remember this is not the sparse method!)
        estim_dict = {}

        # a list of all of the observed dates in the raw data
        list_obs_1 = sorted(list(od.keys()))

        # a list of the dates which come in via the raw data and have either an observation or a non-conventional missing value (i.e. anything but numeric none)
        list_possible_data = [key for (key,value) in sorted(list(od.items())) if value != None]

        # if the observational data has any gaps in it, it will be smaller than the filled up data and we will want our observations array to be smaller than the filled up array, otherwise, they can be the same size
        if len(list_possible_data) < len(list_obs_1):
            list_obs = list_possible_data
        else:
            list_obs = list_obs_1

        # iterate over the observed dates in the raw data
        #(note that it will only go up to the second to last thing, so that when we add 1 index to it, the last thing will be ok)
        for index, each_obs in enumerate(list_obs[:-1]):

            # compute the difference between subsequent observations and test if it is 5 minutes. Subtracting these should result in datetime.timedelta(0, 300)
            compute_obs = list_obs[index+1] - list_obs[index]

            # if the obsevations computed are five minutes from one another, store them in the estimated dictionary, otherwise, use the drange function to do a linear interpolation between them
            if compute_obs == datetime.timedelta(minutes=5):

                # in the estimation dictionary, we store({the datetime : the measured value at that date time})
                estim_dict.update({list_obs[index]:od[list_obs[index]]})

            else:
                # generate a small range of dates for the missing dates and listify
                mini_dates = drange(list_obs[index], list_obs[index+1], datetime.timedelta(minutes=5))
                dl = [x for x in mini_dates]

                # if the current value and the next one are the same
                if od[list_obs[index]] == od[list_obs[index+1]]:
                    vl = [od[list_obs[index]]]*len(dl)
                    el = 'E'*len(vl)
                    # update the estimations dictionary with these new values
                    newd = dict(zip(dl,vl))
                    # update the flags with "E"
                    newd2 = dict(zip(dl,el))
                    # update the estimations dictionary
                    estim_dict.update(newd)
                    flag_dict.update(newd2)

                else:
                    # a numpy array for the number of missing
                    indices_missing = np.arange(len(dl))
                    knownx = [indices_missing[0], indices_missing[-1]]
                    knowny = [od[list_obs[index]], od[list_obs[index+1]]]

                    # interpolation function
                    fx = interp1d(knownx, knowny)

                    # apply to the indices
                    vl = fx(indices_missing)

                    # estimate code for the length of vl
                    el = 'E'*len(vl)

                    # update the estimations dictionary with these new values
                    newd = dict(zip(dl,vl))

                    # update the flags with "E"
                    newd2 = dict(zip(dl,el))
                    estim_dict.update(newd)
                    flag_dict.update(newd2)

                    newd={}
                    newd2={}

        # if the maximum from the blank is not in the estimated we need to add it
        if max(blank_dict.keys()) not in estim_dict.keys():
            estim_dict.update({max(blank_dict.keys()):blank_dict[max(blank_dict.keys())]})

        # writing modes for python3
        if sys.version_info >= (3,0):
            mode = 'w'
        else:
            mode = 'wb'


        # write it to a csv file for subsequent generation
        with open(output_filename, mode) as writefile:
            writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

            try:
                # blank dict has been gap filled
                for each_date in sorted(list(blank_dict.keys())):

                    dt = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')

                    writer.writerow([sitecode, dt, blank_dict[each_date], estim_dict[each_date], flag_dict[each_date]])

            except Exception:
                pass

    elif sparse == True:

        # a list of the observed dates in the raw data
        list_obs = sorted(list(od.keys()))

        # if the final date time in the sparse method is before the end of the water year notify user
        if max(od.keys()) < datetime.datetime(wateryear, 10, 1, 0, 0):
            print("In this sparse analysis, your final data occurs BEFORE the end of the water year, on :" + datetime.datetime.strftime(max(od.keys()), '%Y-%m-%d %H:%M:%S'))

        if sys.version_info >= (3,0):
            mode = 'w'
        else:
            mode = 'wb'


        # write it to a csv file for subsequent generation
        with open(output_filename, mode) as writefile:
            writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

            try:
                # blank dict has NOT  been gap filled
                for each_date in list_obs:
                    dt = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')

                    try:
                        writer.writerow([sitecode, dt, od[each_date], od[each_date], 'A'])
                    except Exception:
                        pass

            except Exception:
                pass

    return output_filename


def do_adjustments(sitecode, wateryear, filename, corr_od, method, partial, date_column):
    """ Performs adjustments on the outputs - ALWAYS pulls from column 3!

    :sitecode: ex. GSWS01
    :wateryear: ex. 2010
    :filename: csv file containing the input (program assigns in main loop for first and sparse)
    :corr_od: dictionary of corrections
    :method: 're' in most cases, 'first', 'sparse' also possible
    :partial: True or False
    :date_column: in which column of the data is the date
    """

    # on the first go round
    if method == "first" or method == "sparse":


        # new output location
        dirname = str(sitecode) + "_" + str(wateryear) + "_" + "working"
        filename_list = find_files(sitecode, wateryear, dirname)

        # make sure working directory is blank
        if filename_list == []:

            if partial == True:

                output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re_partial.csv")

                print("Creating " + output_filename + " to \'working\'. Running \'re\' \'partial\'")

            elif partial == False:

                output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re.csv")

                print("Creating " + output_filename + " to \'working\'. Running \'re\'")

        # if it's not blank, check for existing file you don't want to over write
        elif filename_list != []:

            if partial == True:

                for name in filename_list:
                    if 'partial' in name:

                        sys.exit("You have a file in the working directory called " + name + ". Please save this file elsewhere and remove it from working before continuing")
                    else:
                        output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re_partial.csv")

            elif partial == False:

                for name in filename_list:
                    if 'partial' not in name:

                        sys.exit("You have a file in the working directory called " + name + ". Please save this file elsewhere and remove it from working before continuing")
                    else:
                        output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re.csv")

            else:
                possibly_dangerous = ", ".join(filename_list)

                sys.exit("You have a files in the working directory called " + possibly_dangerous + ". Please save these files elsewhere and remove them from working before continuing")

    # if the re adjustmet, check the working directory for the files with your sitecode and wateryear
    elif method == "re":

        dirname = str(sitecode) + "_" + str(wateryear) + "_" + "working"
        filename_list = find_files(sitecode, wateryear, dirname)

        if len(filename_list) > 1:

            if partial == True:

                # if we find a partial and we're doing partial, we want to break out
                for name in filename_list:

                    # if you have a partial already and you are running partial, you'll want to use it
                    if 'partial' in name:

                        # you will overwriate the old one -- i.e. you write to "re_partial" and you input from "re_partial"
                        output_filename = name

                        # if you already have the re, you want to copy it
                        shutil.copy(output_filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups",sitecode + "_" + str(wateryear) + "_" + "re_partial.csv"))

                        print("saved a copy of " + name + " to \'backups\'. Running \'re\' \'partial\' on " + name + " and outputs go to \'working\'")

                        # write over the existing file with your new one
                        #filename = os.path.join(dirname,name)
                        break

                    # if we don't find a partial and we're doing partial we must make one
                    elif 'partial' not in name:

                        # create a name for your output files which are of the partial method
                        output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re_partial.csv")

                        print("creating " + name + " to \'backups\'. Running \'re\' \'partial\' on " + name + " and outputs go to \'working\'")

            elif partial == False:

                # if we find a partial and we're not doing partial, we want to skip it
                for name in filename_list:

                    # if partial is in the name, skip it
                    if 'partial' in name:
                        continue

                    else:
                        output_filename = name

                        # if you already have the re, you want to copy it
                        shutil.copy(output_filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups",sitecode + "_" + str(wateryear) + "_" + "re.csv"))
                        print("saved a copy of " + name + " to \'backups\'. Running \'re\' on " + name + " and outputs go to \'working\'")

                        #filename = os.path.join(dirname,name)
                        break

        elif len(filename_list) == 1:

            if partial==True and 'partial' in filename_list[0]:
                output_filename = filename_list[0]

                print("saved a copy of " + filename_list[0] + " to \'backups\'. Running \'re\' \'partial\' on " + filename_list[0] + " and outputs go to \'working\'")

                shutil.copy(output_filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups",sitecode + "_" + str(wateryear) + "_" + "re_partial.csv"))

            elif partial== False and 'partial' not in filename_list[0]:
                output_filename = filename_list[0]

                print("saved a copy of " + filename_list[0] + " to \'backups\'. Running \'re\' on " + filename_list[0] + " and outputs go to \'working\'")
                shutil.copy(output_filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups",sitecode + "_" + str(wateryear) + "_" + "re.csv"))

    # a blank output dictionary structure
    od = {}

    # check date type by using the first column
    try:
        date_type = test_csv_date(filename, date_column)

    except Exception:
        try:
            date_type = test_csv_date(filename, 1)
        except Exception:
            # raw data of ws3 it's on the 0th column!
            date_type = test_csv_date(filename, 0)

    if date_type == False:
        date_type = '%Y-%m-%d %H:%M:%S'

    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'


    # open the input file and process
    with open(filename, mode) as readfile:
        reader = csv.reader(readfile)
        for row in reader:

            # don't bother carrying site code, we'll have it in the function
            # we know that this file is either a 'first' or a 're' file and therefore the date column is always column 1.
            try:
                dt = datetime.datetime.strptime(str(row[1]), date_type)
            except Exception:
                # just in case, reference column
                dt = datetime.datetime.strptime(str(row[date_column]), date_type)

            # in both the first and "re", the data on which the computation is done is in column 3 (4th column). Raw data is always in column 2 (3rd column)
            try:
                data_value = round(float(row[3]),3)
            except Exception:
                data_value = None


            # raw values brought across, but don't do anything with them in times other than the first time, store in column 2 (third column)
            try:
                raw_value = round(float(row[2]),3)
            except Exception:
                raw_value = None

            if method != "re":
                # flag values are just assigned as "A" or "M" or "E" in first and sparse modes; we do anything with them; in column 4 (fifth column)
                flag_value = str(row[4])

            elif method == "re":
                # flag values are carried across from subsequent runs using re - now in column 5 (6th column) because the new adjustments are in column 4
                flag_value = str(row[5])

            # generate a dictionary of all the values in the inputs - datetime : raw, adjustable, flag, event
            if dt not in od:
                # assign 'NA' for events beforehand, update after adjusting
                od[dt] = {'raw' : raw_value, 'val': data_value, 'fval': flag_value, 'event':'NA'}

            elif dt in od:
                pass

        # the key function is "determine weights" -- this is where the adjustment happens
        wd = determine_weights(sitecode, wateryear, corr_od, od, partial)

    # setting mode to be python 3 friendly
    if sys.version_info >=(3,0):
        mode = 'w'
    else:
        mode = 'wb'

    # the difference method does resolve correctly, as far as I can see from testing on ws1 alone
    with open(output_filename, mode) as writefile:
        writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

        valid_dates = sorted(list(wd.keys()))

        for each_date in valid_dates:

            try:
                writer.writerow([sitecode, datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S'), wd[each_date]['raw'], wd[each_date]['val'], round(wd[each_date]['adj_diff'],3), wd[each_date]['fval'], wd[each_date]['event']])

            except Exception:
                if wd[each_date]['raw'] == None:
                    writer.writerow([sitecode, datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S'), wd[each_date]['raw'], wd[each_date]['val'], None, 'M', wd[each_date]['event']])

        # add on one extra date stamp to buffer the output. Make the event 'NA'
        #last_date = valid_dates[-1] + datetime.timedelta(minutes = 5)
        #writer.writerow([sitecode, datetime.datetime.strftime(last_date, '%Y-%m-%d %H:%M:%S'), wd[valid_dates[-1]]['raw'], wd[valid_dates[-1]]['val'], round(wd[valid_dates[-1]]['adj_diff'],3), wd[valid_dates[-1]]['fval'], 'NA'])

    return wd, output_filename

def determine_weights(sitecode, wateryear, corr_od, od, partial):
    """ Determines the adjustment for each given observation and applies it.

    The corr dates prior to the start of the data set can be disregarded except for the one just prior to the start

    """

    # these are the sorted "ending dates"
    try:
        corr_dates_as_list = sorted(list(corr_od.keys()))

    except Exception:
        # in the most recent year the end date is missing so we need to not use that one, it is "None"
        corr_dates_1 = [x for x in corr_od.keys() if x != None]
        corr_dates_as_list = sorted(list(corr_dates_1))

    # generate a list of observed dates
    observed_dates_as_list = sorted(list(od.keys()))

    # filter the correction table to only include things that are indexed on an enddate which is in our water year - nothing after this year.

    if partial == True:

        first_index_preceding_data = [index for index,x in enumerate(corr_dates_as_list) if x>=min(od.keys())][0] - 1

        # remove corr dates you don't need to look at if doing a partial year.
        if first_index_preceding_data >= 0:
            relevant_corr_dates = corr_dates_as_list[first_index_preceding_data:]
        else:
            relevant_corr_dates = [x for x in corr_dates_as_list if x >= datetime.datetime(wateryear-1, 10,1,0,0)]

    else:
        # if not processing a partial year.
        relevant_corr_dates = [x for x in corr_dates_as_list if x >= datetime.datetime(wateryear-1, 10,1,0,0)]

    # working dictionary
    wd = {}

    # we'll use the same "correction" until we pass that time, at which point, we'll move to the next correction factor, by calling the iterator.next() method
    # by indexing on the final date we don't have to worry that we'll run over the boundary of the iterator
    iterator_for_correction = iter(relevant_corr_dates)

    # the first correction to be applied - first date-time in your list
    if sys.version_info >=(3,0):
        this_correction = next(iterator_for_correction)
    else:
        this_correction = iterator_for_correction.next()

    for each_date in observed_dates_as_list:

        # as long as the date is less than the correction factor or equal to it
        if each_date <= this_correction:

            # the number of minutes left until the end of the interval, in minutes - for example, if the interval is 9000 minutes long and we are 7000 minutes in, this is 2000
            time_difference = float((this_correction-each_date).days*1440 + (this_correction - each_date).seconds//60)

            # the number of minutes elapsed from the starting time
            time_from_start = corr_od[this_correction]['duration'] - time_difference

            # let's say we are 8130/9050 minutes into the interval.
            # the weight of the beginning of the interval would be 1 if we were 0/9050, and 0 if we were 9050 of 9050
            # as it stands, the weight of the beginning is (1 - (minutes_in/total_minutes))
            beginning_weight = (1-(time_from_start/corr_od[this_correction]['duration']))

            # for the end, the weight of the end of the interval would be 1 if we were at 9050/9050 and 0 if we are at 0/9050.
            # as it stands, the weight of the end is (minutes_in/total_minutes)
            # in this case, that is 0.89
            ending_weight = (time_from_start/corr_od[this_correction]['duration'])

            # in this case, that is 1-0.89 = 0.11
            # the ratio at the beginning ('bgn_rat') carries a 0.11 weight.
            weighted_begin_ratio = corr_od[this_correction]['bgn_rat']*beginning_weight

            # if we use the diff method, we need that the weight of the beginning of the interval has an offset of 1 of its own weight and 0 of the ends weight - for example a value of 0.211 should go up to .214 with the largest offset as represented by the beginning
            weighted_begin_diff = corr_od[this_correction]['bgn_diff']*beginning_weight

            # the ratio at the end ('end_rat') carries a 0.89 weight
            weighted_end_ratio = corr_od[this_correction]['end_rat']*ending_weight

            # if we use the end method, then the adjustment at the end time should be the whole weight of that adjustment and should peak at the exact time that the time from start is = the duration
            weighted_end_diff = corr_od[this_correction]['end_diff']*ending_weight

            # now we will take the weighted beginning ratio and multiply the value by it, and add that to the weighted ending ratio, also multiplied by the value, to generate the adjustment.
            try:
                adjusted_value_rat = round(weighted_begin_ratio*od[each_date]['val'] + weighted_end_ratio*od[each_date]['val'],3)

            except Exception:
                adjusted_value_rat = None

            try:
                # adjusted by difference method
                # ex, if the beginning is 50% of the weight and the adj is + 3 and the end is 50% of the weight and the adj is -5, then the middle is + 1.5 - 2.5, which is -1, plus whatever the actual value on the cr logger is
                adjusted_value_diff = round(weighted_begin_diff, 3) + round(weighted_end_diff,3) + od[each_date]['val']

            except Exception:
                adjusted_value_diff= None

            # try:
            #     print "ratio adjusted: " + str(adjusted_value_rat) + " | diff adjusted: " + str(adjusted_value_diff) + " | cr value: " + str(od[each_date]['val']) + " | correction ends: " + datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')

            # except Exception:
            #     pass


            # if the date incoming is not the same as the correction date, the event is a nonevent.
            if each_date != this_correction:
                event = 'NA'
            # if the date incoming is a correction event, mark it
            else:
                event = "MAINTE"

        # if the event is more than the correction date, we need to move to the next correction factor.
        elif each_date > this_correction:


            # assign the event
            event = 'NA'

            if sys.version_info >= (3,0):
                try:
                    this_correction = next(iterator_for_correction)
                except StopIteration:
                    return wd
            else:
                # step to the next correction factor
                try:
                    this_correction = iterator_for_correction.next()
                except StopIteration:
                    return wd

            # you still need to compute this value here! because the correction has moved on it should fall into the less than pool on the next loop

            # the number of minutes left until the end of the interval, in minutes - for example, if the interval is 9000 minutes long and we are 7000 minutes in, this is 2000
            time_difference = float((this_correction-each_date).days*1440 + (this_correction - each_date).seconds//60)

            # the number of minutes elapsed from the starting time
            time_from_start = corr_od[this_correction]['duration'] - time_difference

            # let's say we are 8130/9050 minutes into the interval.
            # the weight of the beginning of the interval would be 1 if we were 0/9050, and 0 if we were 9050 of 9050
            # as it stands, the weight of the beginning is (1 - (minutes_in/total_minutes))
            beginning_weight = (1-(time_from_start/corr_od[this_correction]['duration']))

            # for the end, the weight of the end of the interval would be 1 if we were at 9050/9050 and 0 if we are at 0/9050.
            # as it stands, the weight of the end is (minutes_in/total_minutes)
            # in this case, that is 0.89
            ending_weight = (time_from_start/corr_od[this_correction]['duration'])

            # in this case, that is 1-0.89 = 0.11
            # the ratio at the beginning ('bgn_rat') carries a 0.11 weight.
            weighted_begin_ratio = corr_od[this_correction]['bgn_rat']*beginning_weight

            # if we use the diff method, we need that the weight of the beginning of the interval has an offset of 1 of its own weight and 0 of the ends weight - for example a value of 0.211 should go up to .214 with the largest offset as represented by the beginning
            weighted_begin_diff = corr_od[this_correction]['bgn_diff']*beginning_weight

            # the ratio at the end ('end_rat') carries a 0.89 weight
            weighted_end_ratio = corr_od[this_correction]['end_rat']*ending_weight

            # if we use the end method, then the adjustment at the end time should be the whole weight of that adjustment and should peak at the exact time that the time from start is = the duration
            weighted_end_diff = corr_od[this_correction]['end_diff']*ending_weight

            # now we will take the weighted beginning ratio and multiply the value by it, and add that to the weighted ending ratio, also multiplied by the value, to generate the adjustment.
            try:
                adjusted_value_rat = round(weighted_begin_ratio*od[each_date]['val'] + weighted_end_ratio*od[each_date]['val'],3)
            except Exception:
                adjusted_value_rat = None

            try:
                # adjusted by difference method
                # ex, if the beginning is 50% of the weight and the adj is + 3 and the end is 50% of the weight and the adj is -5, then the middle is + 1.5 - 2.5, which is -1, plus whatever the actual value on the cr logger is
                adjusted_value_diff = round(weighted_begin_diff, 3) + round(weighted_end_diff,3) + od[each_date]['val']
            except Exception:
                adjusted_value_diff = None

            # try:
            #     print "ratio adjusted: " + str(adjusted_value_rat) + " | diff adjusted: " + str(adjusted_value_diff) + " | cr value: " + str(od[each_date]['val']) + " | correction ends: " + datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')

            # except Exception:
            #     adjusted_value_di = None

        # if we aren't writing it out already
        if each_date not in wd:

            wd[each_date] = {'val': od[each_date]['val'], 'adj_diff': adjusted_value_diff, 'adj_rat': adjusted_value_rat, 'wt_bgn': round(time_difference/corr_od[this_correction]['duration'],3), 'wt_end': round((1-time_difference/corr_od[this_correction]['duration']),3), 'wt_bgn_ratio': round(weighted_begin_ratio,3), 'wt_end_ratio': round(weighted_end_ratio,3), 'raw' : od[each_date]['raw'], 'fval': od[each_date]['fval'], 'event': event}

        elif each_date in wd:
            print("this date has already been put in")

    return wd


def test_csv_date(filename, date_column):
    """ figure out what date format to use """

    # one of these date formats will match, we hope.
    dateformat_ideal = '%Y-%m-%d %H:%M:%S'
    dateformat_older = '%m/%d/%y %H:%M'
    dateformat_13char = '%Y%m%d %H%M'
    dateformat_old = '%m/%d/%Y %H:%M'

    if sys.version_info >= (3,0):
        mode = 'r'
    else:
        mode = 'rb'

    with open(filename, mode) as readfile:
        reader = csv.reader(readfile)
        #import pdb; pdb.set_trace()
        try:
            testline = reader.next()
        except Exception:
            try:
                testline = next(reader)
            except StopIteration:
                return False

        try:
            # YYYY-mm-dd HH:MM:SS
            is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_ideal)
            return dateformat_ideal

        except Exception:
            try:
                # mm/dd/YY HH:MM
                is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_old)
                return dateformat_old
            except Exception:
                try:
                    # YYYYmmdd HHMM
                    is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_13char)
                    return dateformat_13char
                except Exception:

                    try:
                        #mm/dd/YYYY HH:MM (note that this has 4 character year vs. 2)
                        is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_older)
                        return dateformat_older

                    except Exception:
                        #print("None of the known date types seem to fit your raw data. Please modify your dates to fit a nice date structure, such as YYYY-mm-dd HH:MM:SS")
                        return False

def test_csv_structure(filename):
    """ try to find the date column in about 7 columns"""

    for column in [0,1,2,3,4,5,6,7]:

        date_type = test_csv_date(filename, column)

        if date_type != False:
            return date_type, column
        else:
            continue

def make_graphs(sitecode, wateryear, adjusted_dictionary):
    """ make the graphs as you did before"""

    # directory of images; path to images with a slash in case
    dir_images = str(sitecode) + "_" + str(wateryear) + "_" + "images"

    # no sense in sorting this a million times
    sorted_dates = sorted(adjusted_dictionary.keys())

    if sys.version_info >= (3,0):
        param_set = range(1,13)
    else:
        param_set = xrange(1,13)

    for each_month in param_set:

        # generate graphs for months with the wateryear as the year (vs. those year before)
        if each_month not in [10, 11, 12]:
            dates = [x for x in sorted_dates if x.month == each_month and x.year==wateryear]

            prior_values = [adjusted_dictionary[x]['val'] for x in dates if adjusted_dictionary[x]['val'] != None]
            pvd = [x for x in dates if adjusted_dictionary[x]['val'] != None]

            adjusted_values = [adjusted_dictionary[x]['adj_diff'] for x in dates if adjusted_dictionary[x]['adj_diff'] != None]
            avd = [x for x in dates if adjusted_dictionary[x]['adj_diff'] != None]

            # image name for png
            image_name = str(wateryear) + "_" + str(each_month) + "_wy_" + sitecode + ".png"
            name1 = os.path.join(dir_images, image_name)

            # image name for html
            #html_image_name = str(wateryear) + "_" + str(each_month) + "_wy_" + sitecode + ".html"
            #name2 = os.path.join(dir_images, html_image_name)

            fig, ax = plt.subplots()
            fig.autofmt_xdate()
            ax.fmt_xdata = mdates.DateFormatter('%Y-%m')
            ax.plot(pvd, prior_values, color = 'blue', linewidth= 1.2, alpha = 0.5, label = 'corrected cr logger')
            ax.plot(avd, adjusted_values, color = 'red', linewidth= 0.7, label = 'adjusted to hg')
            #ax.legend(loc = 1)
            plt.savefig(name1)

            #html = mpld3.fig_to_html(fig)
            #mpld3.save_html(fig, name2)

            plt.close()

        # generate graphs for the year before (ie wy 2014 these have year 2013)
        elif each_month in [10,11,12]:
            dates = [x for x in sorted_dates if x.month == each_month and x.year == (wateryear -1)]
            prior_values = [adjusted_dictionary[x]['val'] for x in dates if adjusted_dictionary[x]['val'] != None]
            pvd = [x for x in dates if adjusted_dictionary[x]['val'] != None]

            adjusted_values = [adjusted_dictionary[x]['adj_diff'] for x in dates if adjusted_dictionary[x]['adj_diff'] != None]
            avd = [x for x in dates if adjusted_dictionary[x]['adj_diff'] != None]

            image_name = str(wateryear-1) + "_" + str(each_month) + "_wy_" + sitecode + ".png"
            name1 = os.path.join(dir_images, image_name)

            #html_image_name = str(wateryear-1) + "_" + str(each_month) + "_wy_" + sitecode + ".html"
            #name2 = os.path.join(dir_images, html_image_name)

            fig, ax = plt.subplots()
            fig.autofmt_xdate()
            ax.fmt_xdata = mdates.DateFormatter('%Y-%m')
            ax.plot(pvd, prior_values, color = 'blue', linewidth= 1.2, alpha = 0.5, label = 'corrected cr logger')
            ax.plot(avd, adjusted_values, color = 'red', linewidth= 0.7, label = 'adjusted to hg')
            #ax.legend(loc = 1)
            plt.savefig(name1)

            #html = mpld3.fig_to_html(fig)
            #mpld3.save_html(fig, name2)

            plt.close()

if __name__ == "__main__":
    """ This is the code to run the "main" loop.

    :sitecode: - on command line, "GSWS01"
    :year: - on command line 2014
    :mode: - on command line 'first', 'sparse'', 're'
    :partial: - optional fourth argument of 'partial'.


    ..Example:
    python weir2k.py "GSWS01" 2014 "first"
    python weir3k.py "GSWS03" 2015 "re" "partial"

    """
    sitecode_raw = sys.argv[1]
    wateryear_raw = sys.argv[2]
    method= sys.argv[3]

    # partial is either false or true, but must be given after the mode of "first", "sparse", or "re"
    partial = False

    # for partial, if the sys.argv > 4 (script name + 3) run partial if the fourth arguement is partial
    if len(sys.argv) > 4 and str(sys.argv[4]).lower() == 'partial':
        partial = True

    # checks for upper and lower case things
    sitecode, wateryear = string_correct(sitecode_raw, wateryear_raw)

    # get the corr table and put it into a dictionary
    corr_od = convert_corr_to_dict(sitecode, wateryear)

    # create subfolders for images and working data
    create_subfolders(sitecode, wateryear)

    # for the "first" and "sparse" methods, we'll generate only the four column format
    if method == "first":

        # return a list of files in raw data that contain your site code and water year.
        filename_list = find_files(sitecode, wateryear, 'raw_data')

        if len(filename_list) > 1:

            # warn don there are more than one first file in the raw data directory, and ask him to copy and paste the name of the right file on the line, or to type 'NO'
            string_name = ", ".join(filename_list)

            # Python 3
            if sys.version_info >= (3,0):
                valid = input("Found multiple potential files -- " + string_name + " -- for the \'first\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path) of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                    sys.exit("Exiting! Try cleaning your \'raw_data\' directory to only have 1 file for the given site and year")

                print("File found for the " + method + " method : " + filename)

            # Python 2
            elif sys.version_info < (3,0):
                valid = raw_input("Found multiple potential files -- " + string_name + " -- for the \'first\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path) of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                     sys.exit("Exiting! Try cleaning your \'raw_data\' directory to only have 1 file for the given site and year")

                print("File found for the " + method + " method : " + filename)

        # if only one file is in the raw_data folder with that site and water year, print its name and use it
        elif len(filename_list) == 1:

            filename = filename_list[0]
            print("File found for the " + method + " method : " + filename)

        # if no files are found, then throw an exit
        elif filename_list == []:
            sys.exit("You don\'t have any files to process in your \'raw_data\' directory containing the sitecode and water year you desire. Please add the files.")

        # if it screws up, then throw an exit
        else:
            sys.exit("Please check the program, you are getting an error trying to import from directory \'raw_data\'. No files are being found or called.")

        # find files in the root containing the word "first"
        scary_files = find_root_files(sitecode, wateryear)

        # if there are files in root containing 'first', copy them to 'backups'
        if scary_files != []:

            scary_string = ", ".join(scary_files)
            print("There are file(s) in your root that contain the string \'first\'. These are -- " + scary_string + "-- For your safety, I am copying this/these file(s) to your \'backups\' directory.")

            for each_file in scary_files:
                shutil.copy(each_file, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups", each_file))

        # figure out what columns contain the dates (date_column) and raw values and read in from csv
        # note, if you started after the beginning of the water year, you will see the first day here as after he beginning of the water year.
        od, date_column = parameterize_first(sitecode, wateryear, filename)

        print("The first day and time in your raw data is " + datetime.datetime.strftime(min(od.keys()), '%Y-%m-%d %H:%M:%S'))
        print("The final day and time in your raw data is " + datetime.datetime.strftime(max(od.keys()), '%Y-%m-%d %H:%M:%S'))

        # generate a first data with or without estimations
        output_filename_first = generate_first(od, sitecode, wateryear, partial, sparse=False)


        print("Generating \'re\' file from " + output_filename_first + " for the method: " + method + ". Recall that the file named " + output_filename_first + " contains merely a replicate of the raw data, although possibly gapfilled, in the second data column. However, this column is necessary so as not to overwrite the raw data.")

        if partial == True:
            print("Remeber that you used the partial method!")

        # generate the adjustments data with the extra column
        adjusted_dictionary, output_filename_re = do_adjustments(sitecode, wateryear, output_filename_first, corr_od, method, partial, date_column)

        print("Generated \'re\'' file named " + output_filename_re + " and put it in the working directory!")


        #make_optioal_graphs(adjusted_dictionary) <--- do not run this! not for use!!
        make_graphs(sitecode, wateryear, adjusted_dictionary)

    elif method == "sparse":

        # return a list of files in raw data that contain your site code and water year.
        filename_list = find_files(sitecode, wateryear, 'raw_data')

        if len(filename_list) > 1:

            string_name = ", ".join(filename_list)

            if sys.version_info >= (3,0):
                valid = input("Found multiple potential files -- " + string_name + " -- for the \'sparse\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path)  of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                    sys.exit("Exiting! Try cleaning your \'raw_data\' directory to only have 1 file for the given site and year")
                # tell you what file you found
                print("File found for the " + method + " method : " + filename)


            elif sys.version_info < (3,0):
                valid = raw_input("Found multiple potential files -- " + string_name + " -- for the \'sparse\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path)  of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                     sys.exit("Exiting! Try cleaning your \'raw_data\' directory to only have 1 file for the given site and year")
                # tell you what file you found
                print("File found for the " + method + " method : " + filename)


        elif len(filename_list) == 1:

            filename = filename_list[0]

            # tell you what file you found
            print("File found for the " + method + " method : " + filename)


        elif filename_list == []:
            sys.exit("You don\'t have any files to process in your \'raw_data\' directory containing the sitecode and water year you desire. Please add the files.")

        else:
            sys.exit("Please check the program, you are getting an error trying to import from directory \'raw_data\'. No files are being found or called.")

        # find files in the root containing the word "first"
        scary_files = find_root_files(sitecode, wateryear)

        if scary_files != []:

            scary_string = ", ".join(scary_files)
            print("There are file(s) in your root that contain the string \'first\'. These are -- " + scary_string + "-- For your safety, I am copying this/these file(s) to your \'backups\' directory.")

            for each_file in scary_files:
                shutil.copy(each_file, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups", each_file))

        # figure out what columns contain the dates and raw values and read in from csv
        # note, if you start after the beginning of the water year you will see the first day as after the beginning of the water year
        od, date_column = parameterize_first(sitecode, wateryear, filename)

        print("The first day and time in your raw data is " + datetime.datetime.strftime(sorted(list(od.keys()))[0], '%Y-%m-%d %H:%M:%S'))

        # generate a first data with or without estimations
        output_filename_first = generate_first(od, sitecode, wateryear, partial, sparse=True)


        print("Generating \'re\' file from " + output_filename_first + " for the method: " + method + ". Recall that the file named " + output_filename_first + " contains merely a replicate of the raw data, and not gapfilled in the " + method + " method, located in the second data column. The leftmost column is the raw data - it is never over written.")

        # generate the adjustments data with the extra column
        adjusted_dictionary, output_filename_re = do_adjustments(sitecode, wateryear, output_filename_first, corr_od, method, partial, date_column)

        print("Generated re file named " + output_filename_re + " !")

        make_graphs(sitecode, wateryear, adjusted_dictionary)

    elif method == "re":

        # new directory
        dirname = sitecode + "_" + str(wateryear) + "_" + "working"
        filename_list = find_files(sitecode, wateryear, dirname)

        if len(filename_list) > 1:

            string_name = ", ".join(filename_list)

            if sys.version_info >= (3,0):
                valid = input("Found multiple potential files -- " + string_name + " -- for the \'re\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path) of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                    sys.exit("Exiting! Try cleaning your \'working\' directory to only have 1 file for the given site and year")
                # tell you what file you found
                print("File found for the " + method + " method : " + filename)


            elif sys.version_info < (3,0):
                valid = raw_input("Found multiple potential files -- " + string_name + " -- for the \'first\' analysis. If you don't want to proceed, please type \'NO\'. Otherwise, please copy and paste the FULL name (including the path)  of the desired file at the prompt: ~>")

                if valid != 'NO':
                    filename = valid
                else:
                     sys.exit("Exiting! Try cleaning your \'raw_data\' directory to only have 1 file for the given site and year")
                # tell you what file you found
                print("File found for the " + method + " method : " + filename)


        elif len(filename_list) == 1:

            filename = filename_list[0]

            # tell you what file you found
            print("File found for the " + method + " method : " + filename)


        elif filename_list == []:
            sys.exit("You don\'t have any files to process in your \'working\' directory containing the sitecode and water year you desire. Please add the files.")

        else:
            sys.exit("Please check the program, you are getting an error trying to import from directory \'working\'. No files are being found or called.")


        if "first" in filename:
            print("The file in your \'raw_data\' contains the string \'first\'. For your safety, I am copying this file to your \'backups\' directory.")
            shutil.copy(filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups", sitecode + "_" + str(wateryear) + "_" + "first.csv"))

        # try to find re file or re_partial file!
        try:
            if partial != True:
                output_filename_re = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re.csv")
            elif partial == True:
                output_filename_re = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re_partial.csv")

        #     try:
        #         output_filename_re = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode + "_" + str(wateryear) + "_" + "re_partial.csv")
        #         print("It appears you have a partial file in the working directory. \'re\' will try to process it.")
        #         partial = True
        #     except Exception:
        #         output_filename_re = find_files(sitecode, wateryear, 'working')
        #         if 'partial' in output_filename_re or 'PARTIAL' in output_filename_re:
        #             print("It appears you have a partial file in the working directory. \'re\' will try to process it")
        #             partial = True
        #         elif 're' in output_filename_re or 'RE' in output_filename_re:
        #             print("Looks like there is a re file here to process, but the name is unconventional : " + output_filename_re)

            print("You are running the \'re\' method, using the file named " + output_filename_re + " which is located in the working directory. A backup has been saved in the backups directory.")

            od, date_column = parameterize_first(sitecode, wateryear, output_filename_re)

            adjusted_dictionary, output_filename = do_adjustments(sitecode, wateryear, output_filename_re, corr_od, method, partial, date_column)


        except Exception:
            # if for some reason you make it with the sitecode in lower case.
            output_filename_re_lower = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode.lower() + "_" + str(wateryear) + "_" + "re.csv")

            if partial == True:
                output_filename_re_lower = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working", sitecode.lower() + "_" + str(wateryear) + "_" + "re_partial.csv")

            od, date_column = parameterize_first(sitecode, wateryear, output_filename_re_lower)

            adjusted_dictionary, output_filename = do_adjustments(sitecode, wateryear, output_filename_re_lower, corr_od, method, partial, date_column)

        make_graphs(sitecode, wateryear, adjusted_dictionary)
