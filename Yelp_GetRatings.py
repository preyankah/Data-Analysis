import requests
import csv
import time

collectedStores = []
ids = [line.split(',')[0] for line in open('FootLockerBusinessIDs_Add.csv').readlines()]

with open('FootLockerReviews.csv','ab') as LocatedBusinesses: #successful
    for id in ids:
        try:
        #Get id for each Foot Locker page on yelp
            writer = csv.writer(LocatedBusinesses, delimiter=',')
            app_id = 'XYZ'
            app_secret = 'xyz'
            data = {'grant_type': 'client_credentials',
                    'client_id': app_id,
                    'client_secret': app_secret}
            token = requests.post('https://api.yelp.com/oauth2/token', data=data)
            access_token = token.json()['access_token']
            url = 'https://api.yelp.com/v3/businesses/'+ id

            headers = {'Authorization': 'bearer %s' % access_token}
            # Yelp v3 API: https://nz.yelp.com/developers/documentation/v3
            params = {'id': id,
                    }
            response = requests.get(url=url, headers=headers)
            business = response.json()
            rating = business['rating']
            id = business['id']
            writer.writerow([id,rating])
            print id,rating
            LocatedBusinesses.flush()
            time.sleep(2)
        except:
            writer.writerow([id, "NA"])
            print id, "NA"
            LocatedBusinesses.flush()
            continue
LocatedBusinesses.close()
