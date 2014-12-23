
# coding: utf-8
# Description: retries failed scraping attempts from NREGA dashboard
# Date Updated: 12/23/2014


import requests
import pandas as pd
import ast
import time
import datetime

#read in errors.
df = pd.read_csv('./Intermediate/errors.csv', skiprows=[0], names=['state_code', 'district_code', 'block_code', 'year', 'url', 'error_code'])

#read in set of blocks
base_dir = './Intermediate/'
blocks = {"2012-2013" : pd.read_csv(base_dir + "Blocks1213.csv", index_col=False),
          "2013-2014" : pd.read_csv(base_dir + "Blocks1314.csv", index_col=False),
          "2014-2015" : pd.read_csv(base_dir + "Blocks1415.csv", index_col=False)}

states = {"2012-2013" : pd.read_csv(base_dir + "States1213.csv", index_col=False),
          "2013-2014" : pd.read_csv(base_dir + "States1314.csv", index_col=False),
          "2014-2015" : pd.read_csv(base_dir + "States1415.csv", index_col=False)}
		  
#retry URLs
url_stub = 'http://164.100.129.6/netnrega/nrega-reportdashboard/api/dashboard_report_monthly.aspx?'

rows = []
errors = []
fail_count = 0

for i, row in df.iterrows(): 
    year = row.year
    er_state = row.state_code
    er_district = row.district_code
    er_block = row.block_code
    
    if row.error_code == "STATE_FAILURE": # if full state failure.
        fail_count = 0
        for block in blocks[year][blocks[year]["state"] == er_state]["block"]:
            #add leading zeroes
            state_code =    str(blocks[year][blocks[year]["block"] == block]["state"].values[0]).zfill(2)
            district_code = str(blocks[year][blocks[year]["block"] == block]["district"].values[0]).zfill(4)
            block_code = str(blocks[year][blocks[year]["block"] == block]["block"].values[0]).zfill(7)

            #create URL
            url = url_stub + 'state_code='                 + state_code + '&district_code='                 + district_code + '&block_code='                 + block_code + '&fin_year='                 + year + '&type=b'
            
            #add timestamp
            ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

            #parse page returns
            try:
                if fail_count <=3:
                    site_text = requests.get(url, timeout=(5.0, 10.0)).text #query and get the text on the site
                    data = ast.literal_eval(site_text)[0] #evaluate the text as python code
                    if data == [{}]:
                            print "Empty dictionary returned for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
                            errors.append([state_code, district_code, block_code, year, url, "EMPTY"])
                    else:                    	
                        data['state_code'] = state_code
                        data['district_code'] = district_code
                        data['block_code'] = block_code
                        data['year'] = year
                        data['timestamp'] = ts
                        rows.append(data)
                        time.sleep(0.5)
                else:
                    print "State %s is being skipped for year %s for too many failures" % (state_code, year)
                    del errors[-4:]
                    errors.append([state_code, "ALL", "ALL", year, "ALL","STATE_FAILURE", ts])
                    break

            except requests.exceptions.ConnectTimeout as e:
                print "Server too slow to connect for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
                errors.append([state_code, district_code, block_code, year, url, "TIMEOUT", ts])
                fail_count += 1
            except requests.exceptions.ReadTimeout as e:
                print "Server too slow to read for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
                errors.append([state_code, district_code, block_code, year, url, "TIMEOUT", ts])
                fail_count += 1
            except:
                print "Could not access page for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
                errors.append([state_code, district_code, block_code, year, url, "NO_ACCESS", ts])
                fail_count += 1
    
		
    else: #if particular blocks fails
        state_code = er_state
        district_code = er_district
        block_code = er_block
        url = row.url

        #add timestamp
        ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        #parse page returns
        try:
            if fail_count <=3:
                site_text = requests.get(url, timeout=(5.0, 10.0)).text #query and get the text on the site
                data = ast.literal_eval(site_text)[0] #evaluate the text as python code
                if data == [{}]:
                        print "Empty dictionary returned for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
                        errors.append([state_code, district_code, block_code, year, url, "EMPTY"])
                else:                    	
                    data['state_code'] = state_code
                    data['district_code'] = district_code
                    data['block_code'] = block_code
                    data['year'] = year
                    data['timestamp'] = ts
                    rows.append(data)
                    time.sleep(0.5)
            else:
                print "State %s is being skipped for year %s for too many failures" % (state_code, year)
                del errors[-4:]
                errors.append([state_code, "ALL", "ALL", year, "ALL","STATE_FAILURE", ts])
                break

        except requests.exceptions.ConnectTimeout as e:
            print "Server too slow to connect for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
            errors.append([state_code, district_code, block_code, year, url, "TIMEOUT", ts])
            fail_count += 1
        except requests.exceptions.ReadTimeout as e:
            print "Server too slow to read for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
            errors.append([state_code, district_code, block_code, year, url, "TIMEOUT", ts])
            fail_count += 1
        except:
            print "Could not access page for state %s, district %s, and block %s for year %s" % (state_code, district_code, block_code, year)
            errors.append([state_code, district_code, block_code, year, url, "NO_ACCESS", ts])
            fail_count += 1
    
pd.DataFrame(rows).to_csv('./Intermediate/retry_successes.csv', index=False)     
pd.DataFrame(errors).to_csv('./Intermediate/errors.csv', index=False) #should overwrite as we have a new list of errors now!


