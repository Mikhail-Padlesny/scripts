
import sys
from datetime import  datetime, timedelta
import pytz
import re
from urllib import parse
import psycopg2
from twilio.rest.monitor import TwilioMonitorClient

#test creds
dbname = 'twilio_audit'
user = 'vagrant'
host = 'localhost'
password = '123456'
port = '15432'
connect = "dbname='" + dbname + "' user='" + user+ "' host='" + host + "' password='" + password + "' port=" + port


#from twilio.rest import TwilioRestClient
# To find these visit https://www.twilio.com/user/account

ACCOUNT_SID = ""
AUTH_TOKEN = ""

#CREATE TABLE Alerts (
#       sid              varchar(40) NOT NULL UNIQUE,
#       alert_text       text  NULL,
#       error_code       varchar(8)  NULL,
#       log_level        varchar(8)  NULL,
#       request_method   text DEFAULT NULL,
#       request_url      text DEFAULT NULL,
#       response_body    text DEFAULT NULL,
#       response_headers  text DEFAULT NULL,
#       from_number      VARCHAR(14)  NULL,
##       to_number        VARCHAR(14)  NULL,
 #      date_created     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 #      date_generated   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 #      date_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP, request_variables      text DEFAULT NULL
#);

# CREATE TABLE Events (
#        event_sid         varchar(40) NOT NULL UNIQUE,
#        event_description text  NULL,
#        event_base_uri    text  NULL,
#        event_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP  ,
#        event_data        text NULL,
#        event_type        varchar(20)  DEFAULT NULL,
#        actor_sid         varchar(40) DEFAULT NULL,
#        actor_type        varchar(20) DEFAULT NULL,
#        event_source      VARCHAR(14)  DEFAULT NULL,
#        event_source_ip   VARCHAR(14)  NULL
# );

monitor = TwilioMonitorClient(ACCOUNT_SID, AUTH_TOKEN)
#client = TwilioRestClient(ACCOUNT_SID, AUTH_TOKEN)

try:
    con = psycopg2.connect(connect)
    cursor = con.cursor()

    #check latest entry for alerts

    cursor.execute('SELECT max(date_created) from alerts;')
    time_now = datetime.now()
    alerts_time_from = cursor.fetchone()
    if alerts_time_from[0] is not None:
        alerts_time_from = alerts_time_from[0]
    else:
        alerts_time_from = time_now - timedelta(days=2)
    print(alerts_time_from)

    # check latest entry for events

    cursor.execute('SELECT max(event_date) from events;')
    events_time_from = cursor.fetchone()
    if events_time_from[0] is not None:
        events_time_from = events_time_from[0]
    else:
        events_time_from = time_now - timedelta(days=2)
    print(events_time_from)


    #get events

    events = monitor.events.list(
        start_date=events_time_from.replace(tzinfo=pytz.UTC).isoformat(),
        end_date=time_now.replace(tzinfo=pytz.UTC).isoformat()
    )
    #get alerts

    alerts = monitor.alerts.iter(
        start_date=alerts_time_from.replace(tzinfo=pytz.UTC).isoformat(),
        end_date=time_now.replace(tzinfo=pytz.UTC).isoformat(),
    )
    events_data = ()

    for e in events:
        event_description = str(e.description)
        event_sid = str(e.sid)
        event_base_uri = str(e.base_uri)
        event_data=str(e.event_data)
        event_date=str(e.event_date)
        event_type=str(e.event_type)

        if e.actor_sid is not None:
            actor_sid=str(e.actor_sid)
            actor_type = str(e.actor_type)
        else:
            actor_sid = None
            actor_type = None

        event_source=str(e.source)
        event_source_ip=str(e.source_ip_address)

        event_data = (event_sid,event_description,event_base_uri,event_data,event_date,event_type,actor_sid,actor_type,event_source,event_source_ip)

        events_data += (event_data,)
    print(events_data)


    alerts_data=()

    for alert in alerts:

        sid = str(alert.sid)
        alert_text = parse.unquote(alert.alert_text)
        error_code = str(alert.error_code)
        log_level = str(alert.log_level)
        date_created = str(alert.date_created)
        date_updated = str(alert.date_updated)
        date_generated = str(alert.date_generated)
        if alert.request_method != None:
            #get additional info on alert
            alert_full= monitor.alerts.get(alert.sid)
            #parse numbers
            from_number = re.search('(?<=From=)\+\d+', parse.unquote(alert_full.request_variables))
            str_to_number = ''
            str_from_number = ''
            if from_number != None:
                str_from_number = from_number.group()
            to_number = re.search('(?<=To=)\+\d+', parse.unquote(alert_full.request_variables))
            if to_number != None:
                str_to_number = to_number.group()
            request_method = str(alert_full.request_method),
            request_url = str(alert_full.request_url),
            response_body = str(alert_full.response_body),
            response_headers = str(alert_full.response_headers),
            request_variables = str(alert_full.request_variables)
        else:
            request_method = None
            request_url = None
            response_body = None
            response_headers = None
            request_variables = None

        alert_data = (sid,
                      alert_text,
                      error_code,
                      log_level,
                      date_created,
                      date_updated,
                      date_generated,
                      request_method,
                      request_url,
                      response_body,
                      response_headers,
                      request_variables,
                      str_from_number,
                      str_to_number)
    #skip notice level
        if log_level != 'notice':
            alerts_data += (alert_data,)

    alerts_query = """INSERT INTO alerts (sid,
            alert_text,
            error_code,
            log_level,
            date_created,
            date_updated,
            date_generated,
            request_method,
            request_url,
            response_body,
            response_headers,
            request_variables,
            from_number,
            to_number)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            on conflict do nothing;"""

    events_query = """INSERT INTO events (event_sid,
        event_description,
        event_base_uri,
        event_data,
        event_date,
        event_type,
        actor_sid,
        actor_type,
        event_source,
        event_source_ip)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict do nothing;
    """

    cursor.executemany(alerts_query, alerts_data)
    cursor.executemany(events_query, events_data)
    con.commit()




except psycopg2.DatabaseError as e:
    if con:
        con.rollback()

    print ('Error %s' % e)
    sys.exit(1)


finally:
    if con:
        con.close()
