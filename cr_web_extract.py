import requests
import pandas as pd
import ast
import time
import datetime

#note: due to a bug in python, i had to manually edit the files below to add column headers and convert to "UTF-8 without BOM"
#years = ['2012-2013', '2013-2014', '2014-2015'] # something is wrong with 2012-2013 requests
years = ['2012-2013', '2013-2014', '2014-2015']
base_dir = './Intermediate/'
blocks = {"2012-2013" : pd.read_csv(base_dir + "Blocks1213.csv", index_col=False),
          "2013-2014" : pd.read_csv(base_dir + "Blocks1314.csv", index_col=False),
          "2014-2015" : pd.read_csv(base_dir + "Blocks1415.csv", index_col=False)}

states = {"2012-2013" : pd.read_csv(base_dir + "States1213.csv", index_col=False),
          "2013-2014" : pd.read_csv(base_dir + "States1314.csv", index_col=False),
          "2014-2015" : pd.read_csv(base_dir + "States1415.csv", index_col=False)}

url_stub = 'http://164.100.129.6/netnrega/nrega-reportdashboard/api/dashboard_report_monthly.aspx?'

errors = [] #create list of troublesome state/district/block codes.
rows = []
    
for year in years:
    for state in states[year]["state_code"]: #note that it's not entirely necessary to loop through states
                                             #but doing so allows us to skip states if they are being problematic.
        fail_count = 0
        for block in blocks[year][blocks[year]["state"] == state]["block"]:
            #add leading zeroes
            state_code =    str(blocks[year][blocks[year]["block"] == block]["state"].values[0]).zfill(2)
            district_code = str(blocks[year][blocks[year]["block"] == block]["district"].values[0]).zfill(4)
            block_code = str(blocks[year][blocks[year]["block"] == block]["block"].values[0]).zfill(7)
			
			#add timestamp
			ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

            #create URL
            url = url_stub + 'state_code=' \
                + state_code + '&district_code=' \
                + district_code + '&block_code=' \
                + block_code + '&fin_year=' \
                + year + '&type=b'

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
     
pd.DataFrame(rows).to_csv('./Intermediate/successes.csv', index=False)
pd.DataFrame(errors).to_csv('./Intermediate/errors.csv', index=False)           

