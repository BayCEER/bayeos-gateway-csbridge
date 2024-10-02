from datetime import datetime, timezone
from zoneinfo import ZoneInfo

tz = 'Etc/GMT-2'
d = datetime.strptime("2022-07-08T09:13:01","%Y-%m-%dT%H:%M:%S")
d = d.replace(tzinfo=ZoneInfo(tz))
print(tz,d.timestamp())
