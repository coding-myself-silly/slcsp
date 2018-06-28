#!/usr/bin/python

#
# Run as python process.py in the directory containing the .csv files
#

import csv, sqlite3, shutil, os

ZIPS_FILE = 'zips.csv'
PLANS_FILE = 'plans.csv'
SLCSP_FILE = 'slcsp.csv'
TEMP_FILE = 'temp.csv'

def getSecondLowestSilverPlan(con, zipcode):
    return_plan = None
    cur = con.cursor()
    cur.execute("select rate from (select max(rate) as rate, count(*) as row_count from (SELECT p.rate from plans p join zips z on p.state = z.state and p.rate_area = z.rate_area " + 
        "where p.metal_level = 'Silver' and zipcode = ? group by p.rate order by p.rate asc limit 2)) where row_count = 2", (zipcode,))
    row = cur.fetchone()
    if row != None:
        return_plan = row[0]
    cur.close()
    return return_plan

def loadZipTable(con):
    cur = con.cursor()
    cur.execute("CREATE TABLE zips (zipcode,state,county_code,name,rate_area);")
    with open(ZIPS_FILE,'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['zipcode'], i['state'],i['county_code'],i['name'],i['rate_area']) for i in dr]

    cur.executemany("INSERT INTO zips (zipcode,state,county_code,name,rate_area) VALUES (?, ?, ?, ?, ?);", to_db)
    con.commit()
    cur.close()
    fin.close()

def loadPlansTable(con):
    cur = con.cursor()
    cur.execute("CREATE TABLE plans (plan_id,state,metal_level,rate REAL,rate_area);")
    with open(PLANS_FILE,'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['plan_id'], i['state'],i['metal_level'],i['rate'],i['rate_area']) for i in dr]

    cur.executemany("INSERT INTO plans (plan_id,state,metal_level,rate ,rate_area) VALUES (?, ?, ?, ?, ?);", to_db)
    con.commit()
    cur.close()
    fin.close()

def processInputFile(con):
    cur = con.cursor()
    with open(SLCSP_FILE,'r') as fin:
        reader = csv.DictReader(fin)
        with open(TEMP_FILE,'w') as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames) 
            writer.writeheader()
            for row in reader:
                zip = row['zipcode']
                rate = getSecondLowestSilverPlan(con, zip)
                if rate == None:
                    writer.writerow({'zipcode': zip})
                else:
                    writer.writerow({'zipcode': zip, 'rate': rate})
        fout.close();
    fin.close()

def postProcessFiles():
    if os.path.exists(SLCSP_FILE):
        os.remove(SLCSP_FILE)
    shutil.move(TEMP_FILE, SLCSP_FILE)

con = sqlite3.connect(":memory:")

loadZipTable(con)
loadPlansTable(con)
processInputFile(con)
postProcessFiles()
print('Success!')

con.close()