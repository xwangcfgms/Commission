import logging
import datetime
import os.path

def create_logs_dir():
  p = './logs'
  if not os.path.exists(p):
    os.mkdir('./logs')

def config_logging(name):

  create_logs_dir()

  now = datetime.datetime.now()

  logger = logging.getLogger(name)
  logger.setLevel(logging.DEBUG)

  logfname = './logs/{}-{:%Y%m%d%H%M%S}.log'.format(name, now)
  fh = logging.FileHandler(logfname)
  fh.setLevel(logging.DEBUG)

  ch = logging.StreamHandler()
  ch.setLevel(logging.INFO)

  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fh.setFormatter(formatter)
  ch.setFormatter(formatter)

  logger.addHandler(fh)
  logger.addHandler(ch)

  return logger
