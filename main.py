import json
import os
import requests
from datetime import datetime
from influxdb import InfluxDBClient

database_name = "covid_data"
testing_data_url = "https://opendata.ecdc.europa.eu/covid19/testing/json"
deaths_data_url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json"
norway_url = "https://redutv-api.vg.no/corona/v1/areas/country/key"

def load_data_from_url(url):
    return requests.get(url).json()

def load_data_from_file(file_name):
    with open(file_name) as f:
        return list(json.load(f))

# gives a datetime object based on strings like '2020-W14', where 2020 is the
# year and 14 is the week number, starting on Monday
def parse_date(datestr):
    arr = datestr.split('-W')
    return datetime.fromisocalendar(int(arr[0]), int(arr[1]), 1)

def new_dp(name, date, tags={}, fields={}):
    return {
        "measurement": name,
        "tags": tags,
        "time": datetime.strptime(date, '%Y-%m-%d').isoformat(),
        "fields": fields
    }

def write_testing_data(client, data):
    payload = []
    for l in data:
        if "positivity_rate" not in l:
            continue
        payload.append({
            "measurement": "testing",
            "tags": {
                "country": l['country']
            },
            "time": parse_date(l['year_week']).isoformat(),
            "fields": {
                "tests_done": l['tests_done'],
                "new_cases": l['new_cases'],
                "population": l['population'],
                "testing_rate": float(l['testing_rate']),
                "positivity_rate": float(l['positivity_rate']),
            }
        })
        if len(payload) > 100:
            client.write_points(payload)
            payload = []
    client.write_points(payload)

def write_deaths_data(client, data):
    payload = []
    for l in deaths_data:
        # calculate deaths per million
        dpm = float(l['daily_count']) / float(l['population']) * 1000000
        payload.append(new_dp("deaths", l["date"], tags={"country": l["country"]},
            fields={"death_rate": dpm, "death_count": l["daily_count"]}))
        if len(payload) > 100:
            client.write_points(payload)
            payload = []
    client.write_points(payload)

def write_norway_data(client, data):
    payload = []
    pop = data["meta"]["area"]["population"]
    for l in data["items"]:
        if l["id"] == "deaths":
            for d in l["data"]:
                v = float(d["value"])
                a = float(d["movingAverage"])
                payload.append(new_dp("norway_deaths", d["date"], fields={"count": v, "movingAverage": a}))
        elif l["id"] == "cases":
            for d in l["data"]:
                v = float(d["value"])
                a = float(d["movingAverage"])
                payload.append(new_dp("norway_cases", d["date"], fields={"count": v, "movingAverage": a}))
        elif l["id"] == "tested":
            for d in l["data"]:
                v = float(d["value"])
                a = float(d["movingAverage"])
                payload.append(new_dp("norway_tested", d["date"], fields={"count": v, "movingAverage": a}))
        elif l["id"] == "positive-share":
            for d in l["data"]:
                v = float(d["value"])
                a = float(d["movingAverage"])
                payload.append(new_dp("norway_positive_share", d["date"], fields={"count": v, "movingAverage": a}))
        elif l["id"] == "hospitalized":
            for d in l["data"]:
                if d["value"]:
                    v = float(d["value"])
                else:
                    v = float(0)
                payload.append(new_dp("norway_hospitalized", d["date"], fields={"count": v}))
        elif l["id"] == "intensiveCare":
            for d in l["data"]:
                if d["value"]:
                    v = float(d["value"])
                else:
                    v = float(0)
                payload.append(new_dp("norway_intensive_care", d["date"], fields={"count": v}))
        elif l["id"] == "respiratory":
            for d in l["data"]:
                if d["value"]:
                    v = float(d["value"])
                else:
                    v = float(0)
                payload.append(new_dp("norway_respirator", d["date"], fields={"count": v}))
        if len(payload) > 100:
            l = len(payload)
            client.write_points(payload)
            payload = []
    if len(payload) > 0:
        client.write_points(payload)


if __name__ == "__main__":
    influx_addr = os.environ["INFLUX_ADDR"]
    print("connecting to influxdb at", influx_addr)
    client = InfluxDBClient(host=influx_addr.split(":")[0], port=influx_addr.split(":")[1])
    client.switch_database(database_name)

    print("loading testing data")
    write_testing_data(client, load_data_from_url(testing_data_url))

    print("loading norway data")
    write_norway_data(client, load_data_from_url(norway_url))

    print("loading deaths data (this might take some time)")
    deaths_data = []
    for point in load_data_from_url(deaths_data_url):
        if point['continent'] == 'Europe':
            deaths_data.append(point)
    write_deaths_data(client, deaths_data)

    print("all data successfully loaded")
