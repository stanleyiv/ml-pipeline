import time

INTERVAL_SEC = 60
FILENAME = './outputs/data.npz'

def retrieve():
    print('Simulating DB retrieval...')
    time.sleep(5)
    f = open(FILENAME, "w")
    print('OUTPUT: ' + FILENAME)

def main():
    print('Starting Data Ingestion')
    time.sleep(2)

    retrieve()



if __name__ == '__main__':
    main()