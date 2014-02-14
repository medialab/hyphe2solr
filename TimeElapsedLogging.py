import logging
from time import time
 
# based on proposition found here :http://www.developpez.net/forums/d987446/autres-langages/python-zope/general-python/module-logging-relativecreated/
# by  http://www.developpez.net/forums/u246147/wiztricks/
class TimeElapsedFilter(logging.Filter):
    def __init__(self):
        self._start = time()
        super(TimeElapsedFilter, self).__init__()
 
    def filter(self, record):
        record.seconds = '%.2f s' % ((time() - self._start))
        self._start = time()
        return True

# TimeElapsed enabled logging factory
def create_log(name,filename,format="%(asctime)-15s %(name)-5s %(levelname)-8s [ %(seconds)-4s ] %(message)s") :
  logging.basicConfig(level=logging.DEBUG,
                       format=format,
                       filename=filename)
  log = logging.getLogger(name)
  timer_filter = TimeElapsedFilter()
  log.addFilter(timer_filter)
  return log

if __name__ == "__main__":
  from random import choice
  from time import sleep
  levels = (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)
 
  def log_messages(count):
       for x in range(count):
           lvl = choice(levels)
           sleep(0.1)
           lvlname = logging.getLevelName(lvl)
           log.log(lvl, "A message at %s level" % lvlname)
 
  log = create_log("myLog","my.log")
  log_messages(5)
  with open("my.log") as lf:
    for line in lf:
      print line