import requests
import csv
import time

collectedStores = []
zipcodes = [line.split(',')[0] for line in open('FootLockerAddZip.txt').readlines()]

with open('FootLockerBusinessIDs_Add_.csv','wb') as LocatedBusinesses: #successful
    for zip in zipcodes:
        try:
        #Get id for each Foot Locker page on yelp
            writer = csv.writer(LocatedBusinesses, delimiter=',')
            country = 'US'
            category = 'Shoe Stores (shoes, All)'

            api_key = 'xyz'
            url = 'https://api.yelp.com/v3/businesses/search'

            headers = {'Authorization': 'bearer %s' % api_key}
            # Yelp v3 API: https://nz.yelp.com/developers/documentation/v3
            params = {'cc': country,
                      'location': zip,
                      'term': 'Foot Locker',
                      'categories': category}
            response = requests.get(url=url, params=params, headers=headers)
            results = response.json()['businesses']
            for business in results:
                name = business['name']
                id = business['id']
                address = business['location']['address1']
                city = business['location']['city']
                state = business['location']['state']
                zipcode = business['location']['zip_code']
                lat = business['coordinates']['latitude']
                long = business['coordinates']['longitude']
                if address not in collectedStores:
                    collectedStores.append(address)
                    writer.writerow([id,name,address,city,state,zipcode,lat,long])
                    print id,name,address,city,state,zipcode,lat,long
                    LocatedBusinesses.flush()
            print 'Foot Locker Stores retrieved: ' , len(results)
            time.sleep(4)
        except Exception as e:
            print e.message, e.args
            continue
LocatedBusinesses.close()
