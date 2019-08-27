import requests
import gzip
import argparse
import csv
import datetime
from pytz import timezone

from influxdb import InfluxDBClient

epoch_naive = datetime.datetime.utcfromtimestamp(0)
epoch = timezone('UTC').localize(epoch_naive)

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

##
## Check if data type of field is float
##
def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

def isbool(value):
    try:
        return value.lower() in ('true', 'false')
    except:
        return False

def str2bool(value):
    return value.lower() == 'true'

##
## Check if data type of field is int
##
def isinteger(value):
        try:
            if(float(value).is_integer()):
                return True
            else:
                return False
        except:
            return False


def loadCsv(inputfilename, servername, user, password, dbname, metric, 
    timecolumn, timeformat, tagcolumns, fieldcolumns, usegzip, 
    delimiter, batchsize, create, datatimezone):

    host = servername[0:servername.rfind(':')]
    port = int(servername[servername.rfind(':')+1:])
    client = InfluxDBClient(host, port, user, password, dbname)

    if(create == True):
        print('Deleting database %s'%dbname)
        client.drop_database(dbname)
        print('Creating database %s'%dbname)
        client.create_database(dbname)

    client.switch_user(user, password)

    # format tags and fields
    if tagcolumns:
        tagcolumns = tagcolumns.split(',')
    if fieldcolumns:
        fieldcolumns = fieldcolumns.split(',')

    # open csv
    datapoints = []
    inputfile = open(inputfilename, 'r')
    count = 0
    with inputfile as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in reader:
            datetime_naive = datetime.datetime.strptime(row[timecolumn],timeformat)

            if datetime_naive.tzinfo is None:
                datetime_local = timezone(datatimezone).localize(datetime_naive)
            else:
                datetime_local = datetime_naive

            timestamp = unix_time_millis(datetime_local) * 1000000 # in nanoseconds

            tags = {}
            for t in tagcolumns:
                v = 0
                if t in row:
                    v = row[t]
                tags[t] = v

            fields = {}
            for f in fieldcolumns:
                v = 0
                if f in row:
                    if (isfloat(row[f])):
                        v = float(row[f])
                    elif (isbool(row[f])):
                        v = str2bool(row[f])
                    else:
                        v = row[f]
                fields[f] = v


            point = {"measurement": metric, "time": timestamp, "fields": fields, "tags": tags}

            datapoints.append(point)
            count+=1
            
            if len(datapoints) % batchsize == 0:
                print('Read %d lines'%count)
                print('Inserting %d datapoints...'%(len(datapoints)))
                response = client.write_points(datapoints)

                if not response:
                    print('Problem inserting points, exiting...')
                    exit(1)

                print("Wrote %d points, up to %s, response: %s" % (len(datapoints), datetime_local, response))

                datapoints = []
            

    # write rest
    if len(datapoints) > 0:
        print('Read %d lines'%count)
        print('Inserting %d datapoints...'%(len(datapoints)))
        response = client.write_points(datapoints)

        if response == False:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(datapoints), response))

    print('Done')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Csv to influxdb.')

    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='Input csv file.')

    parser.add_argument('-d', '--delimiter', nargs='?', required=False, default=',',
                        help='Csv delimiter. Default: \',\'.')

    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086',
                        help='Server address. Default: localhost:8086')

    parser.add_argument('-u', '--user', nargs='?', default='root',
                        help='User name.')

    parser.add_argument('-p', '--password', nargs='?', default='root',
                        help='Password.')

    parser.add_argument('--dbname', nargs='?', required=True,
                        help='Database name.')

    parser.add_argument('--create', action='store_true', default=False,
                        help='Drop database and create a new one.')

    parser.add_argument('-m', '--metricname', nargs='?', default='value',
                        help='Metric column name. Default: value')

    parser.add_argument('-tc', '--timecolumn', nargs='?', default='timestamp',
                        help='Timestamp column name. Default: timestamp.')

    parser.add_argument('-tf', '--timeformat', nargs='?', default='%Y-%m-%d %H:%M:%S',
                        help='Timestamp format. Default: \'%%Y-%%m-%%d %%H:%%M:%%S\' e.g.: 1970-01-01 00:00:00')

    parser.add_argument('-tz', '--timezone', default='UTC',
                        help='Timezone of supplied data. Default: UTC')

    parser.add_argument('--fieldcolumns', nargs='?', default='value',
                        help='List of csv columns to use as fields, separated by comma, e.g.: value1,value2. Default: value')

    parser.add_argument('--tagcolumns', nargs='?', default='host',
                        help='List of csv columns to use as tags, separated by comma, e.g.: host,data_center. Default: host')

    parser.add_argument('-g', '--gzip', action='store_true', default=False,
                        help='Compress before sending to influxdb.')

    parser.add_argument('-b', '--batchsize', type=int, default=5000,
                        help='Batch size. Default: 5000.')

    args = parser.parse_args()
    loadCsv(args.input, args.server, args.user, args.password, args.dbname, 
        args.metricname, args.timecolumn, args.timeformat, args.tagcolumns, 
        args.fieldcolumns, args.gzip, args.delimiter, args.batchsize, args.create, 
        args.timezone)
