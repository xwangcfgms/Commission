import datetime
from dateutil.relativedelta import relativedelta
from decimal import Decimal
import argparse

import psycopg2
import psycopg2.extras as pgextras
from utils import config_logging
from sql import *

db_prod = {
  'host': 'reporting-pg-database-1.cfrrmsfprykj.us-east-1.rds.amazonaws.com',
  'database': 'postgres',
  'user': 'postgres',
  'password': 'JmD87ZanLr0YFImyLGlnrp5UCjYTeeiBmG792hhT'
}

db_local = {
  'host': 'localhost',
  'database': 'rpt_test',
  'user': 'postgres',
  'password': 'root'
}

db = db_prod

class MerchantCollection:
  def __init__(self, rs):
    self.merchant_id = rs[0]
    self.id = rs[1]
    self.amount = rs[2]
    self.name = None
    self.overcollections = None
    self.agency_type = None
    self.cohort = None
    self.merchant_name = None


external_agencies = [1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,19,21]
inetrnal_agencies = [8,25,20,22,23,26,27,28,29,31]
merchant_list=[]

def agency_data(agency_cur, agency_sql, sec_cur, overcollect_sql, date_range, data):
  agency_cur.execute(agency_sql, (date_range,))
  for rs in agency_cur:
    mc = MerchantCollection(rs)
    merchant_list.append(mc.merchant_id)

    sec_cur.execute(overcollect_sql, (mc.merchant_id, date_range))
    r = sec_cur.fetchone()
    if r[0]:
      mc.overcollections = r[0]
    else:
      mc.overcollections = Decimal(0.0)

    sec_cur.execute(agency_name_sql, (mc.id,))
    mc.name = sec_cur.fetchone()[0]  # this will explode if the agency is not in the DB

    if mc.id in external_agencies:
      mc.agency_type = "External"
    elif mc.id in inetrnal_agencies:
      mc.agency_type = "Internal"
    else:
      mc.agency_type = "Unknown"

    sec_cur.execute(cohort_sql, (mc.merchant_id,))
    r = sec_cur.fetchone()
    if r:
      mc.merchant_name = r[0]
      mc.cohort = r[1]
    else:
      print(f'could not get cohort! {mc.merchant_id}')
      sec_cur.execute(merchant_name_sql, (mc.merchant_id,))
      r = sec_cur.fetchone()
      mc.merchant_name = r[0]
      mc.cohort = 'Unknown'

    data.append(mc)

## adding merchants with overcollections only
def overcollection_data(agency_cur, sec_cur, totalovercollection_sql, date_range, data):
  agency_cur.execute(totalovercollection_sql, (date_range,))
  # print(merchant_list)
  for records in agency_cur:
    # print(records[0])
    if records[0] not in merchant_list:
      mco = MerchantCollection(records)
      mco.overcollections = records[3]

      sec_cur.execute(agency_name_sql, (mco.id,))
      mco.name = sec_cur.fetchone()[0]  # this will explode if the agency is not in the DB

      if mco.id in external_agencies:
        mco.agency_type = "External"
      elif mco.id in inetrnal_agencies:
        mco.agency_type = "Internal"
      else:
        mco.agency_type = "Unknown"

      sec_cur.execute(cohort_sql, (mco.merchant_id,))
      r = sec_cur.fetchone()
      if r:
        mco.merchant_name = r[0]
        mco.cohort = r[1]
      else:
        print(f'could not get cohort! {mco.merchant_id}')
        sec_cur.execute(merchant_name_sql, (mco.merchant_id,))
        r = sec_cur.fetchone()
        mco.merchant_name = r[0]
        mco.cohort = 'Unknown'
      data.append(mco)

agency_sqls = [
  [agency1_sql, overcollections1_sql],
  [agency2_sql, overcollections2_sql],
  [agency3_sql, overcollections3_sql],
  [agency4_sql, overcollections4_sql]
]

def get_agency_data(db_params, sd, ed):
  date_range = psycopg2.extras.DateRange(lower=sd, upper=ed, bounds='[)')
  merchant_collections = []
  with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as agency_cur:
      with conn.cursor() as cur:
        # with conn.cursor() as over_cur:
          for sqls in agency_sqls:
            # agency_data(agency_cur, sqls[0], cur, sqls[1], over_cur, totalovercollection_sql, date_range,  merchant_collections)
            agency_data(agency_cur, sqls[0], cur, sqls[1], date_range, merchant_collections)
          overcollection_data(agency_cur, cur, totalovercollection_sql, date_range, merchant_collections)
  return merchant_collections

def store_merchant_collections(db_params, data):
  with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as cur:
      for mc in data:
        gross = mc.amount + mc.overcollections
        cur.execute(merchant_collection_insert, (mc.merchant_id, mc.merchant_name,
                        mc.cohort, mc.name, mc.agency_type, mc.amount, mc.overcollections, gross))

reset_table_sql = """delete from ns_import.rpt_merchant_collections"""
def reset_data(db_params):
  with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as cur:
      cur.execute(reset_table_sql)

def run(sd, ed):
  log = config_logging('commission-rpt')
  log.info(f'starting commission report {datetime.datetime.now()}')
  log.info(f'start date: {sd} end date: {ed}')

  #delete existing data
  log.info('resetting data...')
  reset_data(db)

  log.info('getting agency data...')
  merchant_collections = get_agency_data(db, sd, ed)

  print(len(merchant_collections))

  log.info('storing merchant collections...')
  store_merchant_collections(db, merchant_collections)

  log.info(f'ending commission report {datetime.datetime.now()}')

def calc_start_end(last_month=False):
  today = datetime.date.today()
  first_this_month = datetime.date(today.year, today.month, 1)

  if last_month:
    # subtract a month from first_this_month
    sd = first_this_month - relativedelta(months=1)
    ed = first_this_month
  else:
    #add a month to first_this_month
    sd = first_this_month
    ed = first_this_month + relativedelta(months=1)
  return sd, ed

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Collections Commission Data Extraction')
  parser.add_argument('--last', action='store_true', default=False,
                      help='add if the timeperiod to use is for last month; defaults to the current month')
  args = parser.parse_args()
  start, end = calc_start_end(args.last)
  run(start, end)
