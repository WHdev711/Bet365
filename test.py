import datetime, locale
locale.setlocale(locale.LC_TIME, ('it_IT', 'UTF-8'))
data_ora = "08 Dic 10:00"
now = datetime.datetime.now()
year = now.year
day = now.day
month = now.month
print(now)
#try:
#    dux = datetime.datetime.strptime(data_ora, '%d %b %H:%M')
#    dux = dux.replace(year=year)
#except:
#    dux = datetime.datetime.strptime(data_ora, '%H:%M')
#    dux = dux.replace(year=year, month=month, day=day)
spazi = data_ora.count(" ")
print("*****",data_ora)
if spazi == 0:
    dux = datetime.datetime.strptime(data_ora, '%H:%M')
    dux = dux.replace(year=year, month=month, day=day)
elif spazi == 2:
    print("===================\n", data_ora)
    dux = datetime.datetime.strptime(data_ora, '%d %b %H:%M')
    dux = dux.replace(year=year)
    print(dux)