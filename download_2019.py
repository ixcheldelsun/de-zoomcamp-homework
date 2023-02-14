import wget



if __name__ == '__main__':
    path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'

    for i in range(1,13):
        if i < 10:
            file = 'fhv_tripdata_2019-0' + str(i) + '.csv.gz'
        else:
            file = 'fhv_tripdata_2019-' + str(i) + '.csv.gz'
        
        wget.download(path + file, out='data/')