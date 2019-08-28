import csv
import requests
import time

Name = [line.split(',')[0] for line in open('Addresses.txt').readlines()]
Address = [line.split(',')[1] for line in open('Addresses.txt').readlines()]
City = [line.split(',')[2] for line in open('Addresses.txt').readlines()]
State = [line.split(',')[3] for line in open('Addresses.txt').readlines()]
Zip = [line.split(',')[4] for line in open('Addresses.txt').readlines()]

with open('GeocodedStores.csv','ab') as CollectionFile: #successful
    with open('ErrorStores.csv', 'wb') as ErrorFile:  # successful
        writer = csv.writer(CollectionFile, delimiter=',')
        ewriter = csv.writer(ErrorFile, delimiter=',')
        for n,a,c,st,z in zip(Name,Address,City,State,Zip):
            try:
                a = a.replace('\n','')
                c = c.replace('\n','')
                st = st.replace('\n', '')
                n = n.replace('\n', '')
                z = z.replace('\n', '')
                fullAddress = "%s %s %s %s" % (a,c,st,z)
                time.sleep(20)
                maps_url = "https://maps.googleapis.com/maps/api/geocode/json"
                is_sensor = "false"
                payload = {'address': fullAddress, 'sensor': is_sensor,'Verify':False}
                r = requests.get(maps_url, params=payload)
                print r
                # store the json object output
                maps_output = r.json()
                # store the top level dictionary
                results_list = maps_output['results']
                result_status = maps_output['status']
                formatted_address = results_list[0]['formatted_address']
                result_geo_lat = results_list[0]['geometry']['location']['lat']
                result_geo_lng = results_list[0]['geometry']['location']['lng']
                writer.writerow([n,a,c,st,z,result_geo_lat,result_geo_lng])
                print n,a,c,st,z,result_geo_lat,result_geo_lng
                CollectionFile.flush()

            except Exception as e:
                print (str(e))
                print 'Error in Geocoding: ',fullAddress
                ewriter.writerow([n,a,c,st,z,'Error', 'Error','Error'])
                ErrorFile.flush()
    ErrorFile.close()
    CollectionFile.close()