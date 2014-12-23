
# coding: utf-8

### Data portal web scraping

### December 12, 2014


###import and set options
import pandas as pd
import numpy as np
import datetime as DT
from time import sleep

import matplotlib
from matplotlib import rcParams
from matplotlib import pyplot as plt

from statsmodels.formula.api import ols
from statsmodels.stats.api import anova_lm

pd.set_option('use_inf_as_null', True) # IMPORTANT!

#colorbrewer2 Dark2 qualitative color table
dark2_colors = [(0.10588235294117647, 0.6196078431372549, 0.4666666666666667),
                (0.8509803921568627, 0.37254901960784315, 0.00784313725490196),
                (0.4588235294117647, 0.4392156862745098, 0.7019607843137254),
                (0.9058823529411765, 0.1607843137254902, 0.5411764705882353),
                (0.4, 0.6509803921568628, 0.11764705882352941),
                (0.9019607843137255, 0.6705882352941176, 0.00784313725490196),
                (0.6509803921568628, 0.4627450980392157, 0.11372549019607843)]

rcParams['figure.figsize'] = (10, 6)
rcParams['figure.dpi'] = 150
rcParams['axes.color_cycle'] = dark2_colors
rcParams['lines.linewidth'] = 2
rcParams['axes.facecolor'] = 'white'
rcParams['font.size'] = 14
rcParams['patch.edgecolor'] = 'white'
rcParams['patch.facecolor'] = dark2_colors[0]
rcParams['font.family'] = 'StixGeneral'


def remove_border(axes=None, top=False, right=False, left=True, bottom=True):
    """
    Minimize chartjunk by stripping out unnecesasry plot borders and axis ticks
    
    The top/right/left/bottom keywords toggle whether the corresponding plot border is drawn
    """
    ax = axes or plt.gca()
    ax.spines['top'].set_visible(top)
    ax.spines['right'].set_visible(right)
    ax.spines['left'].set_visible(left)
    ax.spines['bottom'].set_visible(bottom)
    
    #turn off all ticks
    ax.yaxis.set_ticks_position('none')
    ax.xaxis.set_ticks_position('none')
    

    #now re-enable visibles
    if top:
        ax.xaxis.tick_top()
    if bottom:
        ax.xaxis.tick_bottom()
    if left:
        ax.yaxis.tick_left()
    if right:
        ax.yaxis.tick_right()
        
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 100)


### Reading in scraped data

df = pd.read_csv('./Intermediate/successes.csv')

#fixing months
months = {   'jan_' :  '01',
             'feb_' :  '02',
             'mar_' :  '03',
             'march_' :'03',
             'apr_' :  '04',
             'april_': '04',
             'may_' :  '05',
             'jun_' :  '06',
             'june_' : '06',
             'jul_' :  '07',
             'july_' : '07',
             'aug_' :  '08',
             'sep_' :  '09',
             'oct_' :  '10',
             'nov_' :  '11',
             'dec_' :  '12'}

#need to have months be suffixes, rather than prefixes to reshape easily.
for m_old, m_new in months.iteritems():
    df = df.rename(columns=lambda x: x.replace(m_old, "")+m_new if m_old in x else x)
    

#now transposing the variables which include months
df['ind'] = df.index

# need to add prefix to variables with similar names.
rename_dict = {'PD_per_hh_preyr': 'XX_PD_per_hh_preyr', 'delay_amt': 'XX_delay_amt', 
               'hh_P_emp_pre' : 'XX_hh_P_emp_pre', 'lab_pre': 'XX_lab_pre',
               'unemp_amt': 'XX_unemp_amt', 'unpaid_delay_amt' : 'XX_unpaid_delay_amt' }

#now reshape
for k,v in rename_dict.iteritems():
    df.rename(columns=lambda x: x.replace(k, v) if x.find(k) ==0 else x, inplace=True)

for m in range(1,13):
    reshape_cols = [col.replace(str(m).zfill(2), "") for col in df.columns.values if str(m).zfill(2) in col]

dfT = pd.wide_to_long(df, reshape_cols, i='ind', j='month')
dfT.columns = [x.replace("XX_", "") for x in dfT.columns]

dfT.reset_index(level=1, inplace=True) # get rid of month as the index for now.

# create a calendar year variable.
dfT['cal_year'] = np.where((dfT['month'] >=4), dfT.year.str[0:4], dfT.year.str[-4:]) 

#create pandas date object
dfT['cal_date'] = pd.to_datetime(dfT.month.astype(str) + dfT.cal_year.astype(str), format="%m%Y") 

"""
Analysis/graph making begins below
"""

### Delay days (per household employed)

#### National level delay days (per household employed) by year


#make national level dataframe of sums.
dfdelay_py=dfT.groupby('year').agg({'hh_P_emp':'sum', 'delay':'sum'})
dfdelay_py['delay per HH'] = dfdelay_py.delay / dfdelay_py.hh_P_emp
dfdelay_py.to_csv('./Output/national_delay_stats.csv')


#### Line chart of national level delay days (per household worked) by month.

#create dataset collapsed to months
ts = dfT.groupby(pd.to_datetime(dfT.month.astype(str) + dfT.cal_year.astype(str), format="%m%Y")).agg({'hh_P_emp':'sum', 'delay':'sum'})
ts['delay per HH'] = ts.delay / ts.hh_P_emp

"""
something changed in pandas 0.15 that broke the normal plotting that I am used to. I wasted a lot of time on this
apparently matplotlib does not accept datetime64 objects for date variables. This does not interact well with Pandas 0.15.
adding ".to_pydatetime()" fixes it.
Read here for more details: http://stackoverflow.com/questions/26526230/plotting-datetimeindex-on-x-axis-with-matplotlib-creates-wrong-ticks-in-pandas-0
"""
#plot line chart of delay per Household
plt.figure(figsize=(15,6))
plt.title("Average Delay Days per Household Employed (National)")
plt.ylabel("Delay per Household (Days)")
plt.plot(ts.index.to_pydatetime(), ts['delay per HH'])
remove_border()
plt.savefig('./Output/NREGA_ntnl_delayperHH.png', bbox_inches='tight')

#plot line chart of delay per HH
fig = plt.figure(figsize=(15,6))
plt.title("Average Delay Days (National)")
plt.ylabel("Delay in Days (100,000s)")
plt.plot(ts.index.to_pydatetime(), ts['delay'])
plt.axes().get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int((x*1e-6)), ",")))
remove_border()
plt.savefig('./Output/NREGA_ntnl_delay.png', bbox_inches='tight')

### Monthwise histograms of delay days for blocks (including median line)

dfT['delayHH'] = dfT.delay / dfT.hh_P_emp
dfT['display_month'] = dfT.month.astype(str) + "/" +  dfT.cal_year.astype(str)
dfT = dfT.sort('cal_date') #must be sorted for small multiples to work

#define small multiple function
def small_mult_hist(df, var, group, year, title, filename, rows=4, cols=3, bins=10, log=False, thou =False, ):
    """
    function to make small multiple histograms based on fiscal years

    input
    -----
    
    df: dataframe of interest (NOTE, MUST SORT)
    var: variable to get the distribution of
    group: groups to draw histograms of
    year: fiscal year (ex. '2014-2015')
    title: Super-title of small multiple chart
	filename: filename to output graph to
    rows, cols: how many rows/cols of charts
    bins: how many bins used for histogram
    log: log y axis
    thou: x axis in thousands
    
    output
    -----
    rows * cols number of hisograms.
    
    """
    
    num_plots = rows * cols  # number of subplots

    figwidth = 20
    figheight = 20

    fig = plt.figure(figsize=(figwidth, figheight))
    fig.text(0.09, 0.5, 'Frequency', ha='center', va='center', rotation='vertical', fontsize=25)
    fig.suptitle(title + " (" + year +")", fontsize=30, x=0.5, y=0.95)
    axes = [plt.subplot(rows,cols,i) for i in range(1,num_plots+1)]

    categories = df[(df.year == year)][group].unique()
    xr = (df[df.year == year][var].min(), df[df.year == year][var].max())
    
    for i,x in enumerate(categories):
		#if the number of categories > number of spaces allocated in the plot
        if i <= len(axes)-1:
            ax = axes[i] 
        else:
            break 
              
        
        chart_data = df[(df.year == year) & (df[group] == x) & (df[var] != 0)][var].dropna().values
        zero_count=  df[(df.year == year) & (df[group] == x) & (df[var] == 0)][var].count()
        total_count = df[(df.year == year) & (df[group] == x)][var].count()
        
        if len(chart_data) >0:
            ax.hist(chart_data, bins=bins, log=log, range=xr)
            ax.axvline(x=np.median(chart_data),c='red')
            ax.set_title(x)
            ax.text(1,.75, ("%d zero values dropped\nout of %d observations" % (zero_count, total_count)),transform=ax.transAxes, horizontalalignment='right',verticalalignment='top')
            if thou: ax.get_xaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int((x*1e-3)), ",") if x>1000 else format(int(x), ",")))
            remove_border(ax)
        else: #if no data for a specific month
            ax.plot([])
            remove_border(ax, top=False, left=False, right=False, bottom=False)
            ax.xaxis.set_visible(False)
            ax.yaxis.set_visible(False)
   
    plt.savefig('./Output/' + filename + "_" + year + ".png", bbox_inches='tight')


#now graph the small multiples
years = dfT.year.unique()
for year in years:
    small_mult_hist(dfT, 'delayHH', 'display_month', 
                    year, 'Distribution of Delay days per Household Employed among Blocks', filename= 'Dist_delay_HH',
                    rows=4, log=False, bins=50, thou=False)
    small_mult_hist(dfT, 'delay', 'display_month', year, 'Distribution of Delay Days (thousands) among Blocks', 
                    log=False, bins=50, thou=True, filename= "Dist_delay")

    


### Variance analysis using ANOVA

anova_lm(ols('delay ~ display_month', dfT.dropna(subset=['delay'])).fit()).to_csv('./Output/delay_ANOVA.csv')
anova_lm(ols('delayHH ~ display_month', dfT.dropna(subset=['delayHH'])).fit()).to_csv('./Output/delayHH_ANOVA.csv')

