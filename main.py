import paramiko
import psycopg2
import pandas as pd
from io import StringIO
import create_tables
from datetime import datetime

# sftp server connection details
sftp_hostname = "localhost"
sftp_username = "data_engineer"
sftp_private_key = "./keys/id_key_test"
sftp_private_key_password = ""  # "private_key_file_passphrase_if_it_encrypted"
sftp_public_key = "./keys/id_key_test.pub"
sftp_port = 2222
remote_path = "."
target_local_path = "./uploads"

# connect to SSH client
ssh = paramiko.SSHClient()

# Load target host public cert for host key verification
ssh.load_host_keys(sftp_public_key)

ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Load encrypted private key and ssh connect
key = paramiko.RSAKey.from_private_key_file(sftp_private_key, sftp_private_key_password)
ssh.connect(hostname=sftp_hostname, port=sftp_port, username=sftp_username, pkey=key)

# Get the sftp connection
sftp = ssh.open_sftp()

# print channel status
print(sftp.get_channel())

# list of files in remote server
files = sftp.listdir(remote_path)

# Connect to PostgreSQL database
conn = psycopg2.connect(dbname='test_db', user='postgres', password='pass', host='localhost', port=5432)
cur = conn.cursor()

# create Postgres tables if they do not exist
create_tables.create_tables(conn)

# iterate through files name starting with TRX or POS
for file in files:
    if file[:3] in ('TRX', 'POS') :
        local_path = './uploads/' + file
        sftp.get(file, local_path)

        # Open the CSV file and insert its contents into dataframe
        df = pd.read_csv(local_path, index_col=False)

        # get day_id from, filename
        df['day_id'] = file[4:12]

        # add timestamp in column inserted
        df['inserted'] = datetime.now()

        # create StringIO file like object
        file_like_obj = StringIO()

        # remove index column, remove header
        df.to_csv(file_like_obj, index=False, header=False)

        buffer = file_like_obj.getvalue()

        # insert TRX data into fct_transactions table
        if file[:3] == 'TRX':
            cur.copy_from(StringIO(buffer), 'fct_transactions', sep=',',
                          columns=['trx_id', 'pos_id', 'description', 'trx_type',
                                   'quantity', 'price', 'value', 'currency', 'day_id', 'inserted'])
            print(file, 'transactions loaded to DB.')

        # insert POS data into fct_positions table
        else:
            cur.copy_from(StringIO(buffer), 'fct_positions', sep=',',
                          columns=['pos_id', 'pos_name', 'current_price',
                                   'quantity', 'currency', 'value', 'instrument_type',
                                   'fx_eur', 'value_eur', 'day_id', 'inserted'])
            print(file, 'positions loaded to DB.')

        # commit insert in Postgre db
        conn.commit()


# Close the cursor and connection
cur.close()
conn.close()

# closing connections to sftp and ssh if exist
if sftp: sftp.close()
if ssh: ssh.close()

# print channel status
print(sftp.get_channel())
