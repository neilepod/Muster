import pandas as pd
from sqlalchemy import create_engine

# create column according to json file
column_names = ['payment_date','panchayat_code' ,'total_dues' , 'tot_persondays' , 'msr_no', 'muster_roll_period_from', 'muster_roll_period_to']
# read json file
df = pd.read_csv('/home/user/data/data.csv', header = None, names = column_names)
print(df)

df = pd.read_csv('/home/user/data/data.csv', header = 0)
print(df)
engine = create_engine('mysql://root:@localhost/test')
with engine.connect() as conn, conn.begin():
    df.to_sql('csv', conn, if_exists='append', index=False)















