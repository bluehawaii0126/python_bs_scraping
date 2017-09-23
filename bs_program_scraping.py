import urllib.request
import sys
import bs4
import re
import psycopg2
import datetime

def convertChannels(channel):
    result = 0
    if channel == 1:
        result = 15
    elif channel == 3:
        result = 16
    elif channel == 41:
        result = 17
    elif channel == 51:
        result = 18
    elif channel == 61:
        result = 19
    elif channel == 71:
        result = 20
    elif channel == 81:
        result = 21
    return result

today = datetime.date.today()
tomorrow = today + datetime.timedelta(1)
baseUrl = "https://tv.so-net.ne.jp/chart/bs1.action?head=%s0500&span=%s&descriptive=false"
captureDateUrl = baseUrl % (today.strftime("%Y%m%d"), 24)
nextDateUrl = baseUrl % (tomorrow.strftime("%Y%m%d"), 1)

rawPrograms = bs4.BeautifulSoup(urllib.request.urlopen(captureDateUrl), "html.parser")
rawProgramsByNextDate = bs4.BeautifulSoup(urllib.request.urlopen(nextDateUrl), "html.parser")

programs = rawPrograms.find_all('a', class_="schedule-link", href=re.compile(".action"))
tomorrowPrograms = rawProgramsByNextDate.find_all('a', class_="schedule-link", href=re.compile(".action"))
valueFormat = "('%s', '%s', '%s', '%s', '%s'),"
values = []
channels = []

for program in programs:
    channel = convertChannels(int(program.attrs['href'][14:16]))
    values.append({'started_at' : datetime.datetime.strptime(program.attrs['href'][16:28], "%Y%m%d%H%M"), 'channel' : channel, 'title' : program.text})
    channels.append(channel)

tomorrowValues = []
for tomorrowProgram in tomorrowPrograms:
    channel = convertChannels(int(tomorrowProgram.attrs['href'][14:16]))
    tomorrowValues.append({'started_at' : datetime.datetime.strptime(tomorrowProgram.attrs['href'][16:28], "%Y%m%d%H%M"), 'channel' : channel})

uniqueChannels = sorted(list(set(channels)))
channelPrograms = []
tomorrowChannelPrograms = []
arrayIndex = 0
for uniqueChannel in uniqueChannels:
    channelPrograms.append(list(filter(lambda x:x['channel'] == int(uniqueChannel), values)))
    tomorrowChannelPrograms.append(list(filter(lambda x:x['channel'] == int(uniqueChannel), tomorrowValues)))

insertQuery = ''
tomorrowIndex = 0
for channelProgram in channelPrograms:
    sortedTimeDatas = sorted(channelProgram, key=lambda x:x['started_at'])
    sortedTomorrowTimeDatas = sorted(tomorrowChannelPrograms[tomorrowIndex], key=lambda x:x['started_at'])

    for i in range(len(sortedTimeDatas)):
        ended_at = ''
        try:
            ended_at = sortedTimeDatas[i + 1]['started_at']
        except IndexError:
            ended_at = sortedTomorrowTimeDatas[0]['started_at']

        insertQuery = insertQuery + valueFormat % (
                                         sortedTimeDatas[i]['started_at'],
                                         sortedTimeDatas[i]['started_at'],
                                         ended_at,
                                         sortedTimeDatas[i]['channel'],
                                         sortedTimeDatas[i]['title'].strip())
    tomorrowIndex += 1

query = 'insert into bs_programs (date, started_at, ended_at, channel_id, title) values %s' % insertQuery[:-1]
connection = psycopg2.connect("host=192.168.33.11 port=5432 dbname=traindb user=kenichi password=password")
cur = connection.cursor()
cur.execute(query)
query = 'delete from bs_programs where started_at = ended_at'
cur.execute(query)
connection.commit()
