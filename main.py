import json
import requests
import time
from terminaltables import AsciiTable
from datetime import datetime
import csv
from pprint import pprint

import threading

# outbound information
outbound_airport = ['GVA']
outbound_date = '2019-11-08'
outbound_depart_time = '15:00'
outbound_arrival_time = '23:59'

# inbound information
inbound_date = '2019-11-10'
inbound_depart_time = '14:00'
inbound_arrival_time = '23:59'

rapid_API_Key = ''


def main(origins, outbound_date, outbound_depart_time, outbound_arrival_time, inbound_date, inbound_depart_time,
         inbound_arrival_time):
    places = []

    for origin in origins:
        places += get_suggested_destinations(origin, outbound_date, inbound_date)

    places = [dict(t) for t in {tuple(d.items()) for d in places}]
    places = sorted(places, key=lambda x: x['code'])

    print_places_table(places)
    print("Number of destinations:" + str(len(places)))

    time.sleep(30)

    threads = []
    for origin in origins:
        thread = threading.Thread(target=get_flights, args=(origin, places, outbound_date, outbound_depart_time, outbound_arrival_time,
                                   inbound_date, inbound_depart_time, inbound_arrival_time,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def get_flights(origin, destinations, outbound_date, outbound_depart_time, outbound_arrival_time, inbound_date,
                inbound_depart_time, inbound_arrival_time):
    flight_paths = []
    i = 0
    for destination in destinations:
        i += 1
        if origin == destination['code']:
            continue
        flight_paths += get_flight_paths(origin, destination['code'], outbound_date, outbound_depart_time,
                                         outbound_arrival_time, inbound_date, inbound_depart_time, inbound_arrival_time)
        # if i == 1:
        #     break

        flight_paths = sorted(flight_paths, key=lambda x: x['price'])

        print_routes_table(origin, destinations, flight_paths, outbound_date, inbound_date)

    return flight_paths


def get_flight_paths(origin, destination,
                     outbound_date, outbound_depart_time, outbound_arrival_time,
                     inbound_date, inbound_depart_time, inbound_arrival_time):

    url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"
    payload = "country=US&currency=USD&locale=en-US&adults=1&" + \
              "originPlace=" + origin + "-sky&outboundDate=" + outbound_date + "&" + \
              "destinationPlace=" + destination + "-sky&inboundDate=" + inbound_date
    headers = {
        'content-type': "application/x-www-form-urlencoded"
    }
    response = request_skyscanner("POST", url, headers, None, payload, 201)
    location_pieces = response.headers['Location'].split('/')
    session_key = location_pieces[len(location_pieces) - 1]

    print("Origin: " + origin + "|Destination: " + destination + "|Session Key: " + session_key)

    while True:
        time.sleep(10)

        url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/uk2/v1.0/" + \
              session_key

        querystring = {"sortType": "price", "sortOrder": "asc",
                       "outboundDepartStartTime": outbound_depart_time, "outboundArriveEndTime": outbound_arrival_time,
                       "inboundDepartStartTime": inbound_depart_time, "inboundArriveEndTime": inbound_arrival_time,
                       "pageIndex": "0", "pageSize": "1000"}

        response = request_skyscanner("GET", url, None, querystring, None, 200)
        response = response.json()

        if response['Status'] == 'UpdatesComplete':
            return standardize_skyscanner_response(response, inbound_date, inbound_arrival_time, outbound_date,
                                                   outbound_arrival_time)
        else:
            print("Not update complete yet")
            time.sleep(30)


def standardize_skyscanner_response(response, inbound_date, inbound_arrival_time, outbound_date, outbound_arrival_time):
    data = []

    for itinerary in response['Itineraries']:
        outbound_leg = find_in_array(response['Legs'], 'Id', itinerary['OutboundLegId'])
        outbound_departure_airport = find_in_array(response['Places'], 'Id', outbound_leg['OriginStation'])
        outbound_arrival_airport = find_in_array(response['Places'], 'Id', outbound_leg['DestinationStation'])
        inbound_leg = find_in_array(response['Legs'], 'Id', itinerary['InboundLegId'])
        inbound_departure_airport = find_in_array(response['Places'], 'Id', inbound_leg['OriginStation'])
        inbound_arrival_airport = find_in_array(response['Places'], 'Id', inbound_leg['DestinationStation'])
        price = int(itinerary['PricingOptions'][0]['Price'])
        booking_link = itinerary['PricingOptions'][0]['DeeplinkUrl']

        # Some filters
        # Inbound Arrival End Time
        inbound_flight_arrival_time = datetime.strptime(inbound_leg['Arrival'], '%Y-%m-%dT%H:%M:%S')
        inbound_max_time = datetime.strptime(inbound_date + "T" + inbound_arrival_time + ":00", '%Y-%m-%dT%H:%M:%S')
        if inbound_flight_arrival_time > inbound_max_time:
            continue

        # Outbound Arrival End Time
        outbound_flight_arrival_time = datetime.strptime(outbound_leg['Arrival'], '%Y-%m-%dT%H:%M:%S')
        outbound_max_time = datetime.strptime(outbound_date + "T" + outbound_arrival_time + ":00", '%Y-%m-%dT%H:%M:%S')
        if outbound_flight_arrival_time > outbound_max_time:
            continue

        data.append({
            "outbound_departure_time": outbound_leg['Departure'],
            "outbound_departure_airport_code": outbound_departure_airport['Code'],
            'outbound_arrival_time': outbound_leg['Arrival'],
            "outbound_arrival_airport_code": outbound_arrival_airport['Code'],
            "inbound_departure_time": inbound_leg['Departure'],
            "inbound_departure_airport_code": inbound_departure_airport['Code'],
            "inbound_arrival_time": inbound_leg['Arrival'],
            "inbound_arrival_airport_code": inbound_arrival_airport['Code'],
            "price": price
        })

    return data


def find_in_array(array, key, value):
    for item in array:
        if item[key] == value:
            return item


def get_suggested_destinations(origin, outbound_date, inbound_date):
    url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/browsequotes/v1.0/US/USD/en-US/" + \
          origin + "-sky/anywhere/" + outbound_date + "/" + inbound_date

    response = request_skyscanner("GET", url, None, None, None, 200)

    places = []

    for place in response.json()['Places']:
        if place['Type'] == "Station":
            places.append(
                {"code": place['SkyscannerCode'], "location": place['CityName'] + ", " + place['CountryName']})

    return places


def request_skyscanner(method, url, extra_headers, params, payload, success_code):
    headers = {
        'x-rapidapi-host': "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
        'x-rapidapi-key': rapid_API_Key
    }

    if extra_headers is not None:
        headers.update(extra_headers)

    response = requests.request(method, url, headers=headers, data=payload, params=params)

    while response.status_code != success_code:
        print("Failed")
        pprint(url + ":" + str(response.status_code))
        pprint(response.text)
        time.sleep(30)
        response = requests.request(method, url, headers=headers, data=payload, params=params)

    return response


# Printing
def print_places_table(places):
    header = ['Airport Code', 'Location']
    data = [header]

    with open('places.csv', mode='w') as places_csv:
        places_writer = csv.writer(places_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        places_writer.writerow(header)

        for place in places:
            data.append([place['code'], place['location']])
            places_writer.writerow([place['code'], place['location']])

    table = AsciiTable(data)

    print(table.table)


def print_routes_table(origin, places, flight_paths, outbound_date, inbound_date):
    header = ['Origin', 'Destination Airport', 'Destination', 'Outbound Departure', 'Outbound Arrival', 'Inbound Departure',
         'Inbound Arrival', 'Price']

    data = [header]

    with open(origin + '_' + outbound_date + '_' + inbound_date + '_flights.csv', mode='w') as flights:
        flights_writer = csv.writer(flights, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        flights_writer.writerow(header)

        for flight_path in flight_paths:
            row = [flight_path['outbound_departure_airport_code'], flight_path['outbound_arrival_airport_code'],
                         find_in_array(places, 'code', flight_path['outbound_arrival_airport_code'])['location'],
                         flight_path['outbound_departure_time'], flight_path['outbound_arrival_time'],
                         flight_path['inbound_departure_time'], flight_path['inbound_arrival_time'],
                         flight_path['price']]

            data.append(row)
            flights_writer.writerow(row)

    table = AsciiTable(data)
    print(table.table)


if __name__ == '__main__':
    main(outbound_airport, outbound_date, outbound_depart_time, outbound_arrival_time, inbound_date,
         inbound_depart_time, inbound_arrival_time)
