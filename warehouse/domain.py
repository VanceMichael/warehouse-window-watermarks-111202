from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT event, device, instant, units FROM readings ORDER BY event')]
    totals = defaultdict(int)
    for record in rows:
        instant = datetime.fromisoformat(record['instant'])
        if instant.tzinfo is None:
            raise ValueError("时间缺少偏移")
        day = instant.astimezone(timezone.utc).date().isoformat()
        totals[day] += record['units']
    return [{"day": day, 'units': value} for day, value in sorted(totals.items())]
