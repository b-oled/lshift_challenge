#!/usr/bin/python

import time
import urllib
import pytz

from datetime import date, datetime, timedelta
from time import gmtime, strftime
from apscheduler.schedulers.background import BackgroundScheduler
from xml.dom import minidom
from lxml import etree
from collections import namedtuple
from apscheduler.schedulers.background import BackgroundScheduler

# :/ need to use this for proper datetime parsing, including timezone
from django.utils.dateparse import parse_datetime
from django.utils import timezone

URLS = ["http://www.bbc.co.uk/radio2/programmes/schedules.xml", "http://www.bbc.co.uk/5livesportsextra/programmes/schedules.xml"]

NOTIFICATION_TIMES = [10,5,3,2,1]
sched = BackgroundScheduler() # should switch to blocking later

def send_onairnow(pid, text):
    f = open('onairnow', 'a')
    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    output = "{0} - PID: {1} - text: {2}".format( timestamp, pid, text )
    print output
    f.write(output + "\n")
    f.close()

def send_onairnext(pid, text):
    f = open('onairnext', 'a')
    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    output = "{0} - PID: {1} - text: {2}".format( timestamp, pid, text )
    print output
    f.write(output + "\n")
    f.close()

# scans bbc document at given 'url' and creates scheduled tasks for each
def schedule_all(url):
    xml = minidom.parse(urllib.urlopen(url))
    broadcasts = xml.getElementsByTagName("broadcast")
    for bc in broadcasts:
        pid = bc.getElementsByTagName("pid")[0].firstChild.data

        start_time_string = bc.getElementsByTagName("start")[0].firstChild.data
        start_time = parse_datetime(start_time_string)

        # need to use UTC, otherwise python can't compare offset-naive and offset-aware timestamps
        now = datetime.utcnow().replace(tzinfo = pytz.utc)

        # only act if programme hasn't started yet
        if start_time > now:
            for programme in bc.getElementsByTagName("programme"):
                if programme.getAttribute("type") == "episode":
                    
                    # found correct programme
                    title = programme.getElementsByTagName("display_titles")[0].firstChild.firstChild.data
                    subtitle = programme.getElementsByTagName("display_titles")[0].lastChild.firstChild.data
                    text = title + subtitle

                    # only add a new job if it is not already in (might happen due to overlapping programmes from day to next day)
                    if not sched.get_job(pid):
                        print "adding 'onairnow' job {0}".format(pid)
                        sched.add_job(send_onairnow, 'date', run_date=start_time, args=[pid, text], id=pid )
                    else:
                        print "job {0} already scheduled!".format(pid)
                        
                    # schedule a job for each onairnext time
                    for notification_time in NOTIFICATION_TIMES:

                    # generate unique job id
                        unique_id = "{0}_{1}".format( pid, notification_time)
                        if not sched.get_job(unique_id):
                            print "adding 'onairnext' job {0}".format(unique_id)
                            sched.add_job(send_onairnext, 'date', run_date=start_time - timedelta(minutes=notification_time), args=[pid, text], id=unique_id )
                        else:
                            print "job {0} already scheduled!".format(unique_id)
        else:
            print "Programme has already started at {0} PID: {1}. Nothing to do.".format( start_time_string, pid )
            
if __name__ == "__main__":
    sched.start()
    try:
        while True:
                for url in URLS:
                    # actually this should only happen once per day - didn't finish
                    schedule_all(url)
                sched.print_jobs()
                time.sleep(3600)

    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()


"""
TODO / nice things that could be added:
- lots of error handling instead of just accessing array elements directly
- tests :)
- currently runs schedule_all() every hour - as I wasn't sure when BBC updates XML
  - it would have been much better to regularly poll the xmls and check for changes in element day, attribute date of the xml

USAGE:
- just start to background via "./schedule_populator.py &"
"""
