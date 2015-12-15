from pyflow import *
from nose import with_setup


# This file contains a series of tests that do the workflow of GSWSMA in 2015.

def test_pymssql_connection():
	""" Tests that the SQL connection can be formed"""
	conn, cur = fc()
	assert conn.__class__.__name__ == 'Connection'

def test_eqn_sets():
	""" Tests that the right equation set can be gathered from the database"""
	conn, cur = fc()
	returns = get_equation_sets(cur, 'GSWSMA', 2015)
	mocked = {'A3': {'eqn_set': ['32', '35'], 'tuple_date': [(datetime.datetime(1979, 10, 1, 0, 1), datetime.datetime(1995, 10, 1, 0, 0)), (datetime.datetime(1995, 10, 1, 0, 1), datetime.datetime(2051, 1, 1, 0, 0))]}}
	assert returns == mocked

def test_eqns_with_numbers():
	conn, cur = fc()
	gswsma_returns_2015 = get_equation_sets(cur, 'GSWSMA', 2015)
	returns = get_equations_by_value(cur,'GSWSMA',gswsma_returns_2015)
	mocked = {'A3': {'eqns': {0.509: [3.568, 1.741562], 2.54: [3.856196, 2.168731]}, 'eqn_set': ['32', '35'], 'acres': '1436.0', 'tuple_date': [(datetime.datetime(1979, 10, 1, 0, 1), datetime.datetime(1995, 10, 1, 0, 0)), (datetime.datetime(1995, 10, 1, 0, 1), datetime.datetime(2051, 1, 1, 0, 0))]}}
	assert returns == mocked

def test_csv_imports():
	""" Gets data from 2015 GSWSMA for testing purposes"""
	sitecode = "GSWSMA"
	wateryear = 2015
	csvfilename = os.path.join(sitecode.upper() + "_" + str(wateryear) + "_working", sitecode.upper() + "_" + str(wateryear) + "_re.csv")
	raw,bfav = get_data_from_csv(csvfilename)
	assert raw != {}

def test_sample_dates():
	""" Gets the sample dates from generic GSMACK"""
	conn, cur = fc()
	sitecode = 'GSWSMA'
	wateryear = 2015
	sd = get_samples_dates(cur, sitecode, wateryear)
	mocked = [datetime.datetime(2014, 10, 1, 0, 0), datetime.datetime(2014, 10, 15, 11, 5), datetime.datetime(2014, 11, 5, 14, 0), datetime.datetime(2014, 11, 24, 14, 0), datetime.datetime(2014, 12, 16, 9, 0), datetime.datetime(2015, 1, 6, 8, 53), datetime.datetime(2015, 1, 26, 11, 35), datetime.datetime(2015, 2, 18, 16, 25), datetime.datetime(2015, 3, 11, 10, 25), datetime.datetime(2015, 4, 1, 8, 45), datetime.datetime(2015, 4, 22, 8, 5), datetime.datetime(2015, 5, 13, 7, 55), datetime.datetime(2015, 6, 3, 8, 5), datetime.datetime(2015, 6, 22, 15, 50), datetime.datetime(2015, 7, 14, 9, 15), datetime.datetime(2015, 8, 4, 19, 15), datetime.datetime(2015, 8, 25, 18, 10), datetime.datetime(2015, 9, 15, 9, 25), datetime.datetime(2015, 10, 1, 0, 0)]
	assert sd == mocked