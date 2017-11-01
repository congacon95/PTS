#REGION: IMPORT MODULES
import requests
from bs4 import BeautifulSoup as bs
import string
import os, sys
import pandas
import time, datetime
import threading
from math import ceil
import smtplib
#import gc
#gc.enable()
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
if sys.platform=='win32' or sys.platform=='win64':
    IS_WINDOWS=True
else:
    IS_WINDOWS=False
    from xvfbwrapper import Xvfb

#REGION: GLOBAL FIELDS
ERRORS=[]
LOGS=[]
START_TIME=''
ROOT_PATH=''
SCRIPT_PATH=''
PROXIES=[]
PROXY_TYPE='http'
USE_PROXIES=False
TIME_OUT=60
DEBUG=False
N_DRIVERS=15
DR=None
VDP=None
WDW=None
WDW_DELAY=30
#REGION: FUNCTION DEFENITION
#HELPER METHODS
def exception_string():
    _type, _obj, _tb=sys.exc_info()
    return '> '+str(_type.__name__)+' at line '+str(_tb.tb_lineno)+': '+str(_obj)
def exception(message=None):
    '''get exception line number, type and obj'''
    error=exception_string()
    if message:
        error+=', '+message
    ERRORS.append(error)
    log(error)
def log(text):
    LOGS.append(text)
    print(text)
def format_integer(i, length=2):
    # given a number and return a string represent it
    # i.e: fi(25, 4) -> "0025"
    s=str(i)
    while len(s) < length:
        s='0'+s
    return s
def date_now():
    # get date.
    # i.e: () -> "20171024"
    return time.strftime("%Y%m%d")
def time_now():
    # get time.
    # i.e: () -> "2230"            
    return time.strftime("%H%M")
def date_time_now():
    return time.strftime("%Y%m%d_%H%M")
def root_domain(url):
    return url[0:url[8:].index('/')+8]
def domain_name(url):
    t=url.split('//')[1].split('/')[0].split('.')
    if t[-2]=='com': 
        return t[-3]
    else:
        return t[-2]
def format_name(row, name):
    name=name.split(',')[0]    
    row['Full Name']=name
    row['First Name']=name.split()[0]
    row['Last Name']=name.split()[-1]
    row['Middle Name']=' '.join(name.split()[1:-1])

def format_phone(text):
    if text:                
        for c in ['\t', '\n', ' ', '.', 'P', 'T', 'F', ':']:
            text=text.replace(c, '')
        return text
    else:
        return ''

_CASES=[{'char':'K', 'val':1000},
        {'char':'M', 'val':1000000}, 
        {'char':'B', 'val':1000000000}, 
        {'char':'G', 'val':1000000000000}, ]
def convert(string):
    if string.isdigit():
        return int(string)
    if '$' in string:
        string=string.replace('$', '')
    if len(string)==0 or not string[0].isdigit():
        return -1
    string=string.replace(',','')
    for case in _CASES:
        if case['char'].lower() in string.lower():
            string=string.lower().split(case['char'].lower())[0]
            return float(string)*case['val']
    return float(string)

def save_logs(source):
    global LOGS, ERRORS
    log('> '+date_time_now()+' : Save logs from '+source)
    save(LOGS, 'logs/'+START_TIME+'/'+source+'_LOGS')
    LOGS=[]
    save(ERRORS, 'logs/'+START_TIME+'/'+source+'_ERRORS')
    ERRORS=[]
    
def quit():
    save_logs('quit()')
    driver_close()

#MAKE REQUESTS
def load_proxies(path):
    USE_PROXIES=True
    pass
def rand_proxy():
    proxy=PROXIES[random.randint(0, len(PROXIES)-1)] 
    proxy=PROXY_TYPE+"://"+proxy
    return { PROXY_TYPE : proxy}
def make_request(url):        
    if USE_PROXIES:
        return requests.get(url, timeout=TIME_OUT, proxies=rand_proxy())
    else:
        return requests.get(url, timeout=TIME_OUT)
def request_content(url):
    # give a url and request its content, try atleast 5 times if false.
    try:
        respone=make_request(url)
    except:
        exception()
        for i in range(1, 6):
            log('> Get '+url+' failed '+str(i)+' times')
            time.sleep(1)
            try:
                respone=make_request(url)
                if '200' in str(respone):
                    break
            except:
                exception()
    return respone.content

#FILE IO
def isdir(path):
    # check if folder exist, if not create it.
    folders=path.split('/')
    path=''
    for folder in folders[:-1]:
        path +=folder+'\\'
        if not os.path.isdir(path):
            log('> Create folder : '+ROOT_PATH+'\\'+path)
            os.mkdir(os.path.dirname(ROOT_PATH+'\\'+path))
def save(data, path, drop=[]):
    # save a list or dataframe as csv and xlsx file
    isdir(path)
    if type(data) is list:
        data=pandas.DataFrame(data)
    data=data.reset_index(drop=True)
    if len(drop)>0:
        columns=data.columns.drop(drop)
        data.to_excel(path+'.xlsx', encoding='utf8', index=False, columns=columns)
        data.to_csv(path+'.csv', encoding='utf8', index=False, columns=columns)
    else:
        data.to_excel(path+'.xlsx', encoding='utf8', index=False)
        data.to_csv(path+'.csv', encoding='utf8', index=False)
def fast_save(data, path):
    _df=pandas.DataFrame(data).reset_index(drop=True)
    _df.to_excel(path+'.xlsx', encoding='utf8', index=False)
    _df.to_csv(path+'.csv', encoding='utf8', index=False)
def dataframe(path):
    # load file from path and return it as a dataframe.
    if '.xlsx' in path:
        data=pandas.read_excel(open(path, 'rb'), encoding='utf8')
    elif '.csv' in path:
        data=pandas.DataFrame.from_csv(path, encoding='utf8')
    return data.reset_index()
def read(folder):
    # given a folder path that contain multiple file named [0,1,2,3...].csv
    # load all the files into a dataframe and return it.
    data=pandas.DataFrame()
    i=0
    if folder[-1]!='/':
        folder+='/'
    fpath=folder+str(i)+'.csv'
    while os.path.isfile(fpath):
        try:
            data=pandas.concat([data, dataframe(fpath)])
        except:
            exception()
        i +=1
        fpath=folder+str(i)+'.csv'
    return data.reset_index(drop=True)
def split(data, size):   
    # split the dataframe into multiple smaller size list      
    if type(data) is not list:
        rows=[]
        for key, val in data.iterrows():
            row={}
            for k in val.keys():
                row[k]=val[k]
            rows.append(row)
        data=rows
    return [data[i*size: (i+1)*size] for i in range(ceil(len(data)/ size))]

#BEAUTIFUL SOUP
__SOUP__=''

def soup(url, driver=None, delay=None, waits=None, waitv=None, get=False):
    global __SOUP__
    if driver is None:
        if get:
            return bs(request_content(url), 'html.parser').find('body')
        __SOUP__=bs(request_content(url), 'html.parser').find('body')
    else:
        driver.get(url)
        if delay:             
            time.sleep(delay)
        if waits and waitv:
            wait(waits, waitv, driver)
        if get:
            return bs(driver.page_source, "html.parser").find('body')
        __SOUP__=bs(driver.page_source, "html.parser").find('body') 
def soups(rows, path, _id):
    try:
        if path=='driver':
            dr=driver()
        else:
            dr=None
        for row in rows:
            try:
                row['SOUP']=soup(row['URL'], dr, get=True)
            except:
                pe()
    except:
        pe()
    if dr:
        dr.quit()
def download_soups(batches, method='nodriver'):    
    log('> Found '+str(len(batches))+' row entries')
    log('> Download the data') 
    run_threads(split(batches, ceil(len(batches)/N_DRIVERS)), soups, path=method)
def ftext(e):
    if e:
        return e.text.strip()
    else:
        return ''
ft=ftext
def bs_elements(val, tag='div', selector='class', parent=None):
    try:
        if parent:
            #print(parent)
            return parent.findAll(tag,{selector:val})
        return __SOUP__.findAll(tag,{selector:val})
    except:
        pe()
bes=bs_elements
def bs_element(val, tag='div', selector='class', index=0, parent=None):
    try:
        _elements=bs_elements(val, tag, selector, parent)
        if _elements and len(_elements)>0:
            return _elements[index]
    except:
        pe()
be=bs_element
def bs_element_text(val, tag='div', selector='class', parent=None):
    try:
        if parent:
            return ft(parent.find(tag,{selector:val}))
        return ft(__SOUP__.find(tag,{selector:val}))
    except:
        pe()
bet=bs_element_text
def bs_element_float(val, tag='div', selector='class', parent=None):
    return convert(bs_element_text(val, tag, selector, parent))
bef=bs_element_float
def bs_element_phone(val, tag='div', selector='class', parent=None):
    return format_phone(bs_element_text(val, tag, selector, parent))
bep=bs_element_phone
def children(element):
    return element.findAll(True, recursive=False)
#REGION: SETUP
def setup(user_agent=None, proxy=None, 
    no_sandbox=True, incognito=True,
    extension=None, debug=False, new_driver=True):
    global DR, START_TIME, WDW_DELAY, ROOT_PATH, SCRIPT_PATH, DEBUG
    try:
        if debug:
            DEBUG=True
            WDW_DELAY=5
        else:
            DEBUG=False
            WDW_DELAY=30
        ROOT_PATH=os.getcwd()
        SCRIPT_PATH=os.path.realpath(__file__)[:-6]
        START_TIME=dtn()
        
        log('> DEBUG:\t'+str(DEBUG))
        log('> SCRIPT_PATH:\t'+SCRIPT_PATH)
        log('> ROOT_PATH:\t'+ROOT_PATH)
        log('> START_TIME:\t'+START_TIME)
        if new_driver:
            DR=driver(user_agent, proxy, no_sandbox, incognito, extension, debug)      
    except:
        exception()

#REGION: MULTI THREADING IMPLEMENTATION
class thread(threading.Thread):
    def __init__(self, threadID, links, func, path):
        threading.Thread.__init__(self)
        self.threadID=threadID
        self.func=func
        self.links=links
        self.path=path
    def run(self):
        log('> Thread '+str(self.threadID)+' started with '+str(len(self.links))+' data entries.')
        try:
            self.func(self.links, self.path, self.threadID)
        except:
            exception()
        log('> Thread '+str(self.threadID)+' ended.')
def run_threads(batches, func=None, path='', callback=None):
    log('> Started multi threading:\t'+date_time_now())
    log('> Number of threads:    \t'+str(len(batches)))
    threads=[thread(i, batches[i], func, path) for i in range(len(batches))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for t in threads:
        del t
    log('> Finished multi threading:\t'+date_time_now())
    if callback:
        return callback(path)       
#SELENIUM
def driver(user_agent=None, proxy=None, no_sandbox=True, incognito=True, extension=None, debug=False):
    global VDP
    try:      
        chrome_options=webdriver.ChromeOptions()
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--position=0,0")

        #We need this argument ortherwise it won't work on VPS
        if no_sandbox:
            chrome_options.add_argument('--no-sandbox')
        #Set the proxy for chrome
        if proxy:
            chrome_options.add_argument('--proxy-server='+proxy)
        #Set user agent
        if user_agent:
            chrome_options.add_argument('--user-agent='+user_agent)

        #Open chrome under incognito mode
        if incognito and not extension:
            chrome_options.add_argument("--incognito")
        if extension:
            chrome_options.add_extension(extension)
        if sys.platform=='win32' or sys.platform=='win64':
            if debug:
                wdr=webdriver.Chrome(SCRIPT_PATH+'chromedriver.exe', chrome_options=chrome_options)
            else:
                wdr=webdriver.PhantomJS(SCRIPT_PATH+'phantomjs.exe')
            wdr.set_window_size(1920, 1080)
            wdr.set_window_position(0, 0)
            return wdr
        else:
            if not VDP and not debug:
                VDP=Xvfb()
                VDP.start()
            return webdriver.Chrome(chrome_options=chrome_options)
    except:
        exception()
def driver_close():
    global DR, VDP
    try:
        if DR:
            DR.quit()
            del DR
        if VDP:
            VDP.stop()
            del VDP
    except:
        exception()
def wait(selector, val, driver=None):
    try:
        log('> Wait for element:\t'+selector+'='+val)
        if selector=='id': 
            selector=By.ID
        elif selector=='name':
            selector=By.NAME
        elif selector=='class':
            selector=By.CLASS_NAME
        elif selector=='tag':
            selector=By.TAG_NAME
        if not driver:
            driver=DR
        WebDriverWait(driver, WDW_DELAY).until(EC.presence_of_element_located((selector, val)))
    except:
        exception()
def site(url, driver=None):
    try:
        if 'http' not in url:            
            if 'www.' not in url:
                url='www.'+url
            url='http://'+url
        else:
            if 'www.' not in url:
                _url=url.split('//')
                url=_url[0]+'www.'+_url[1]
        log('> Go to site: '+url)
        if not driver:
            driver=DR
        driver.get(url)
    except:
        exception()
def elements(selector, val, driver=None):
    try:
        wait(selector, val, driver=driver)
        if not driver:
            driver=DR
        return driver.find_elements_by_xpath('//*[@'+selector+'=\"'+val+'\"]')
    except:
        exception()
        return []
def element(selector, val, index=0, driver=None):
    try:
        _elements=elements(selector, val, driver=driver)
        if len(_elements)==0:
            log('> No element:\t\t'+selector+'='+val)
            return None
        if len(_elements)-1 <index:
            log('> Index out of bound:\t'+selector+'='+val)
            return None
        return _elements[index]
    except:
        exception()
        return None
def send_keys(selector, val, text, index=0, driver=None):
    try:
        _element=element(selector, val, index=index, driver=driver)
        try:
            _element.clear()
        except:
            exception()
        _element.send_keys(text)
    except:
        exception()
def click_element(selector, val, index=0, driver=None):
    try:
        element(selector, val, index=index, driver=driver).click()
    except:
        exception()
def click_option_normal(selector, val, option_text, driver=None):
    try:
        wait(selector, val, driver=driver)
        xpath='//*[@'+selector+'="'+val+'"]/option[text()="'+option_text+'"]'
        if not driver:
            driver=DR
        driver.find_element_by_xpath(xpath).click()
    except:
        exception()
def login(email, password,
        es='name', ev='email', eidx=0,
        ps='name', pv='pass', pidx=0,
        ss='type', sv='submit', sidx=0, driver=None):
    # es, ev, eidx=       email selector, value, index
    # ps, pv, pidx=    password selector, value, index
    # ss, sv, sidx=submit button selector, value, index
    try:
        send_keys(es, ev, email, index=e_id, driver=driver)
        send_keys(ps, pv, password, index=e_id, driver=driver)
        click_element(ss, sv, index=s_id, driver=driver) 
        log('> Logged in.')
    except:
        exception()
def click_option(select, ul_name, val,_id=0):
    try:
        click_element('class', select)        
        wait('class', ul_name)
        lis=DR.find_elements_by_class_name(ul_name)[_id].find_elements_by_tag_name('li')
        for li in lis:        
            if val in li.text: 
                li.click()
                break    
    except:
        exception()
def get_text(selector, val, index=0, driver=None):
    try:
        return ' '.join(element(selector, val, index=index, driver=driver).text.strip().split())
    except:
        exception()
        return ''
    
#REGION: FUNCTION SHORTHAND NOTATION
pe=exception
fi=format_integer
dn=date_now
tn=time_now
dtn=date_time_now
df=dataframe
dmn=domain_name
dmr=root_domain
lp=load_proxies
rp=rand_proxy
mr=make_request
ct=request_content
name=format_name
st=setup
dr=driver
es=elements   
e=element    
sk=send_keys
ce=click_element
con=click_option_normal
txt=get_text
fp=format_phone