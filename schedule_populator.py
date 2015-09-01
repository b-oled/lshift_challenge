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
# :/ need to use this for proper datetime parsing, including timezone
from django.utils.dateparse import parse_datetime
from django.utils import timezone
from apscheduler.schedulers.background import BackgroundScheduler

URLS = ["http://www.bbc.co.uk/radio2/programmes/schedules.xml", "http://www.bbc.co.uk/5livesportsextra/programmes/schedules.xml"]

NOTIFICATION_TIMES = [10,5,3,2,1]
sched = BackgroundScheduler() # should switch to blocking later
today = ''

def send_onairnow(pid, text):
    f = open('onairnow', 'a')
    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    output = "{0} - PID: {1} - text: {2}".format( timestamp, pid, text )
    print output
    f.write(output)
    f.close()

def send_onairnext(pid, text):
    f = open('onairnext', 'a')
    timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    output = "{0} - PID: {1} - text: {2}".format( timestamp, pid, text )
    print output
    f.write(output)
    f.close()

def schedule_all():
    for url in URLS:
        xml = minidom.parse(urllib.urlopen(url))
        broadcasts = xml.getElementsByTagName("broadcast")
        for bc in broadcasts:
            pid = bc.getElementsByTagName("pid")[0].firstChild.data

            start_time_string = bc.getElementsByTagName("start")[0].firstChild.data
            start_time = parse_datetime(start_time_string)
            # end_time_string = bc.getElementsByTagName("end")[0].firstChild.data
            # end_time = parse_datetime(end_time_string)

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

                        print text
                        
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
    # first create jobs for all needed programmes
    schedule_all()

    # restart scheduler every day at midnight (?)
    sched.add_job(schedule_all, )
    
    sched.print_jobs()
    sched.start()
    try:
        while True:
            time.sleep(60)
            
            
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()

"""
TODO:
- restart on next day: need to analyze day date
"""
