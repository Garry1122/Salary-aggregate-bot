from datetime import datetime, timedelta, timezone
from pymongo import ASCENDING

async def aggregate_salaries(db, dt_from, dt_upto, group_type):
    collection = db.salary_collection

    dt_from = datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    dt_upto = datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    match_stage = {
        "$match": {
            "dt": {"$gte": dt_from, "$lt": dt_upto}
        }
    }

    if group_type == "hour":
        group_id = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
            "hour": {"$hour": "$dt"}
        }
    elif group_type == "day":
        group_id = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"}
        }
    elif group_type == "month":
        group_id = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"}
        }

    group_stage = {
        "$group": {
            "_id": group_id,
            "total": {"$sum": "$value"}
        }
    }

    sort_stage = {
        "$sort": {"_id": ASCENDING}
    }

    pipeline = [match_stage, group_stage, sort_stage]
    results = await collection.aggregate(pipeline).to_list(None)

    dataset = []
    labels = []

    full_dates = []
    current_date = dt_from
    while current_date <= dt_upto:
        full_dates.append(current_date)
        if group_type == "hour":
            current_date += timedelta(hours=1)
        elif group_type == "day":
            current_date += timedelta(days=1)
        elif group_type == "month":
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year+1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month+1)

    for date in full_dates:
        if group_type == "hour":
            label = date.strftime("%Y-%m-%dT%H:00:00")
        elif group_type == "day":
            label = date.strftime("%Y-%m-%dT00:00:00")
        elif group_type == "month":
            label = date.strftime("%Y-%m-01T00:00:00")

        labels.append(label)

        for item in results:
            item_date = datetime(item['_id']['year'], item['_id']['month'], item['_id'].get('day', 1), item['_id'].get('hour', 0), tzinfo=timezone.utc)
            if item_date == date:
                dataset.append(item['total'])
                break
        else:
            dataset.append(0)

    return {"dataset": dataset, "labels": labels}
