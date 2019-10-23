import sys, gzip, os, re, uuid
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


LINE_RE = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
BATCH_SIZE = 250


# drop table if exists, and re-create it
def drop_create_table(table):
    session.execute("DROP TABLE IF EXISTS " + table)
    session.execute("CREATE TABLE " + table + " ( host TEXT, id UUID, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id) )")


# change input line to an structured tuple
def separate_columns(line):
    columns = LINE_RE.split(line)

    if (len(columns) == 6):
        HOST = columns[1]
        ID = uuid.uuid4()
        DATETIME = datetime.timestamp(datetime.strptime(columns[2], "%d/%b/%Y:%H:%M:%S"))
        PATH = columns[3]
        BYTE = int(columns[4])
        return [HOST, ID, DATETIME, PATH, BYTE]


def read_files(inputs, table):
    record_counter = 0
    batch_counter = 0

    batch_insert = BatchStatement()
    insert_statement = session.prepare("INSERT INTO " + table + " (host, id, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")

    # get all files in input folder
    for file in os.listdir(inputs):

        # unzip files
        with gzip.open(os.path.join(inputs, file), 'rt', encoding='utf-8') as logfile:

            # read file line by line
            for line in logfile:
                # create a tuple of requried fields
                log_object = separate_columns(line)

                # if log object is valid
                if (log_object is not None):
                    record_counter += 1
                    batch_insert.add(insert_statement, (log_object[0], log_object[1], log_object[2], log_object[3], log_object[4]))

                # insert records when reached to declared batch size
                if (record_counter >= BATCH_SIZE):
                    print("writing batch " + str(batch_counter))

                    session.execute(batch_insert)
                    batch_insert.clear()

                    record_counter = 0
                    batch_counter += 1

    # to insert the final part with number of rows less than batch size
    if (record_counter > 0):

        print("writing final batch " + str((batch_counter + 1)))
        session.execute(batch_insert)


# main function
def main(inputs, table):
    drop_create_table(table)

    read_files(inputs, table)


# start point
if (__name__ == '__main__'):
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    cluster = Cluster(["199.60.17.32"])
    session = cluster.connect(keyspace)

    main(inputs, table)
