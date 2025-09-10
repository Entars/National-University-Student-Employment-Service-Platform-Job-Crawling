import queue
import random
import threading
import time
from multiprocessing import Manager
from bs4 import BeautifulSoup
import urllib3
import json
from tqdm.auto import tqdm
import sys
from  fake_useragent import UserAgent
from selenium import  webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


#抑制认证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def platform_choose():
    print("请选择平台(数字)：\n 1.BOSS \n 2.国家大学生就业服务平台 \n 3.国聘 \n 4.智联招聘 \n 5.省市及高校 \n 6.拉钩 \n 7.退出")
    platform_num = input("请输入：")
    if platform_num.isdigit():
        int_num = int(platform_num)
        if int_num <= 6 and int_num >= 1:
            return int_num
        elif int_num == 7:
            print("------------成功退出-----------")
            sys.exit()
    print("------------输入错误-----------")
    platform_choose()

def sourcetype(code):
    if code == 5:
        return "1"
    else:
        return ""

def keep_connect(chrome_location,username,password):
    """使用selenium模拟登录，获取Cookie，并维持登录状态防止服务器端将Cookie注销"""
    options = webdriver.ChromeOptions()
    options.binary_location = chrome_location
    options.add_argument("ignore-certificate-errors")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=options,executable_path='../chromedriver.exe')

    login_url = 'https://account.chsi.com.cn/passport/login?service=https://www.ncss.cn/student/connect/chsi&entrytype=stu'
    driver.get(login_url)
    time.sleep(1)

    acc_input = driver.find_element(By.XPATH, r'/html/body/div/div[2]/div[2]/div/div[2]/form/div[1]/input')
    acc_input.send_keys(username)

    pwd_input = driver.find_element(By.XPATH, f'/html/body/div/div[2]/div[2]/div/div[2]/form/div[2]/input')
    pwd_input.send_keys(password)
    driver.find_element(By.XPATH, r'/html/body/div/div[2]/div[2]/div/div[2]/form/div[4]').click()

    element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, r"/html/body/div/span/span/a[1]")))
    element.click()

    dic = {}

    for i in driver.get_cookies():
        dic[i['name']] = i['value']

    return dic


def Cookie_concat(got_property):
    """将从keep_connect函数中获取到的Cookie数据进行重新包装"""
    changed_property_list = ['SESSION', 'Hm_lpvt_378ff17a1ac046691cf78376632d1ed4', '_ga','_ga_6CXWRD3K0D']

    no_change = {
        '_ga_1ESVLDHDYL': 'GS1.1.1726503021.3.0.1726503021.0.0.0',
        '_abfpc': 'd7760a7e8d00953eaa18b111273b00d6ad83f1cc_2.0',
        'cna': 'eac1e178d67b00529df2b287b5842f2c',
        '_gid': 'GA1.2.673435621.1755336171',
        'aliyungf_tc': 'bfc4565f794c9e6ac253a5c09f4a937f2afb69e01fc7b413d486aaeaadf06249',
        'XSRF-CCKTOKEN': 'a12b3436b08549ba2f33a4aefa9a1454',
        'CHSICC_CLIENTFLAGNCSS': 'aba5b818eaa8bc94d6fb1ddf17d1df4f',
        'CHSICC01': '!DzrVNB/pHD1H78bzYxYLahOzddj6Y4XQ6NJ5RnOPIOyzHzKixC+5X5WINIjztT+S4x5PGaf/cowaI/Q=',
        'CHSICC_CLIENTFLAGSTUDENT': '5d0ab9cce044f18a699886e7d6705555',
        'Hm_lvt_378ff17a1ac046691cf78376632d1ed4': '1754926580,1754968150,1755336169,1755411478',
        'HMACCOUNT': 'CEB955474E107530',
        'acw_tc': 'ac11000117554230396983911ee7b259a823321b52a20a878c0b42ee677273',
        '_gat_gtag_UA_105074615_1': '1'
    }

    property_all = no_change.copy()

    for i in changed_property_list:
        property_all[i] = got_property[i]
    property_rank = ['SESSION','_ga_1ESVLDHDYL','_abfpc','cna','_gid','aliyungf_tc','acw_tc','XSRF-CCKTOKEN','CHSICC_CLIENTFLAGNCSS','CHSICC01','CHSICC_CLIENTFLAGSTUDENT','Hm_lvt_378ff17a1ac046691cf78376632d1ed4','HMACCOUNT','_gat_gtag_UA_105074615_1','Hm_lpvt_378ff17a1ac046691cf78376632d1ed4','_ga','_ga_6CXWRD3K0D']

    Cookie=''

    for i in property_rank:
        Cookie = Cookie + i + '=' + property_all[i] + '; '
    print("------------------完成Cookie创建------------------")

    return Cookie


def simulate_login_get_cookie(chrome_location,username,password):
    """将模拟登录部分和包装Cookie部分整合在一起"""
    dic = keep_connect(chrome_location=chrome_location,username=username,password=password)
    return Cookie_concat(dic)


ua = UserAgent()


def generate_timestamp():
    """生成13位时间戳"""
    return str(int(time.time() * 1000))



def get_job_data(http,Cookie,source_name,source_type,page=1,page_size=10):
    """获取岗位数据"""
    baseurl = "https://www.ncss.cn/student/jobs/jobslist/ajax/"
    #构造请求头
    headers = {
        'User-Agent':ua.random,
        'Connection':'keep-alive',
        'Cookie':Cookie,
        "Accept": "application/json,*/*",
        'Referer':"https://account.chsi.com.cn/passport/login?service=https://job.ncss.cn/student/connect/chsi&entrytype=stu"
    }
    
    # 查询参数
    params = {
        "jobType": "",
        "areaCode": "",
        "jobName": "",
        "monthPay": "",
        "industrySectors": "",
        "property": "",
        "categoryCode": "",
        "memberLevel": "",
        "recruitType": "",
        "offset": page,
        "limit": page_size,
        "keyUnits": "",
        "degreeCode": "",
        "sourcesName": source_name,
        "sourcesType": source_type,
        "_": generate_timestamp()  # 动态时间戳
    }

    try:
        resp = http.request(
            method='GET',
            url=baseurl,
            fields=params,
            headers=headers
        )
        if resp.status in [403,401]:
            tqdm.write(f"请求第{page}页时登录过期")
            return None
        elif resp.status == 200:
            data = json.loads(resp.data.decode('UTF-8'))
            return data['data']['list']
        else:
            tqdm.write(f"请求第{page}页时错误，状态码{resp.status}")
            return None

    except Exception as e:
        tqdm.write(f"请求第{page}页时发生错误:{e}")
        return None




def fetch_detail_page(job_id,http,Cookie):
    """获取详情页数据"""

    headers = {
        'User-Agent':ua.random,
        'Cookie':Cookie,
        'Referer':'https://job.ncss.cn/student/jobs/index.html'
    }

    detail_url = f"https://www.ncss.cn/student/jobs/{job_id}/detail.html"
    try:
        resp = http.request(
            'GET',
            detail_url,
            headers = headers
        )
        if resp.status == 200:
            return resp.data.decode('utf-8')
        elif resp.status in [401,403]:
            tqdm.write(f"获取{job_id}详情页时登录过期")
            return None
        else:
            tqdm.write(f"请求失败，状态码{resp.status}")
            return None
    except Exception as e:
        tqdm.write(f"请求失败，{str(e)}")
        return None



def pares_detail_job_info(html,job_info):
    """将爬取到的详情页进行解析，获取“岗位介绍”部分的数据"""
    soup = BeautifulSoup(html,'html.parser')

    job_detail_describe_div =soup.find(name='div',attrs={'class':"details"})
    if job_detail_describe_div is not None:
        job_detail_describe = job_detail_describe_div.getText()
    else:
        tqdm.write(f"未找到岗位描述元素，岗位ID:{job_info.get('岗位ID')}")
        job_detail_describe = "未知"

    job_info.update({
        "岗位介绍":job_detail_describe
    })

    return job_info



def parse_job_info(job):
    """解析岗位信息"""
    id = job.get("jobId")
    timeStamp = job.get("updateDate")
    timeArray = time.localtime(float(timeStamp)/1000)

    return {
        "岗位ID":id,
       "职位名称":job.get("jobName"),
        "薪资水平":str(job.get("lowMonthPay"))+'k-'+str(job.get('highMonthPay'))+'k',
        "招聘人数":job.get("headCount"),
        "学历要求":job.get("degreeName"),
        "招聘方":job.get("recName"),
        "公司规模":job.get("recScale"),
        "地区":job.get("areaCodeName"),
        "福利":job.get("recTags"),
        "专业要求":job.get("major"),
        "岗位更新时间":time.strftime("%Y-%m-%d %H:%M:%S",timeArray),
        "详情网址":f"https://www.ncss.cn/student/jobs/{id}/detail.html"
    }





def list_page_worker(list_page_queue, http, Cookie, detail_task_queue, list_pbar,source_name,source_type):
    """获取岗位列表的工作流程"""
    global list_get_wrong, list_pages_done, enqueued_jobs_count

    while True:
        page = None
        try:
            page = list_page_queue.get(timeout=30)

            if page is None:
                list_page_queue.task_done()   # 对应主线程 put(None)
                break

            # 爬取列表页
            jobs = get_job_data(http=http, Cookie=Cookie, page=page,source_name=source_name,source_type=source_type, page_size=10)
            if jobs:
                for job in jobs:
                    job_info = parse_job_info(job)
                    detail_task_queue.put(job_info)
                    with progress_lock:
                        enqueued_jobs_count += 1
            time.sleep(random.uniform(0.5, 1.5))

        except queue.Empty:
            thread_name = threading.current_thread().name
            tqdm.write(f"列表页任务队列空，线程 {thread_name} 准备退出")
            continue

        except Exception as e:
            tqdm.write(f"列表页工作线程异常: {str(e)}")
            if page:
                list_get_wrong.append(page)
            time.sleep(5)

        finally:
            if page is not None:  # 只在真实任务时更新
                list_pages_done += 1
                list_pbar.update(1)
                list_page_queue.task_done()




def detail_page_worker(detail_task_queue,result_queue,http,Cookie,detail_pbar):
    """详情页工作流程"""

    global detail_get_wrong,detail_jobs_done

    while True:
        job_info = None
        job_id =None
        try:
            job_info = detail_task_queue.get(timeout=30)

            if job_info is None:
                detail_task_queue.task_done()
                break

            job_id = job_info.get("岗位ID")

            # print(f"开始爬取详情页{job_id}")

            html = fetch_detail_page(job_id, http, Cookie)

            if html:
                new_job_info = pares_detail_job_info(html, job_info)
                result_queue.put(new_job_info)
            # print(f"详情页{job_id}解析完成")
            else:
                tqdm.write(f"详情页{job_id}爬取失败")

            time.sleep(random.uniform(0.3, 0.8))

        except queue.Empty:
            thread_name = threading.current_thread().name
            tqdm.write(f"详情页任务队列空，线程{thread_name}准备退出")
            continue

        except Exception as e:
            tqdm.write(f"详情页工作线程异常: {str(e)}")
            if job_id:
                detail_get_wrong.append(job_id)
            time.sleep(3)
        finally:
            if job_info is not None:
                detail_jobs_done += 1
                detail_pbar.update(1)
                detail_task_queue.task_done()


def result_writer(result_queue, output_address,platform):
    """结果写入线程"""
    struc_time = time.localtime()
    time_year = struc_time.tm_year
    time_month = struc_time.tm_mon
    time_day = struc_time.tm_mday

    output_file = f"{output_address}{platform}_{time_year}_{time_month}_{time_day}_jobs.json"

    count = 0
    with open(output_file, "a", encoding='utf-8', newline='') as f:
        while True:
            try:
                # 获取结果
                result = result_queue.get(timeout=120)
                if result is None:
                    break

                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                f.flush()

                count += 1

                if count % 100 == 0:
                    tqdm.write(f"已写入 {count} 条结果")

                result_queue.task_done()
            except queue.Empty:
                tqdm.write("结果写入线程超时退出")
                break
            except Exception as e:
                tqdm.write(f"结果写入异常: {str(e)}")

    tqdm.write(f"结果写入完成，共写入 {count} 条数据")



def process_manager(total_pages,http,Cookie,source_name,source_type,platform,num_list_workers=2,num_detail_workers=8,output_address=""):
    """进程管理器（主进程）"""
    global  enqueued_jobs_count
    start_time = time.time()
    tqdm.write(f"开始爬取任务，总页数: {total_pages}")

    #创建Manager
    with (Manager() as manager):
        #创建队列
        list_page_queue = manager.Queue()
        detail_task_queue = manager.Queue(maxsize=5000)
        result_queue = manager.Queue()

        #添加列表页任务
        for page in range(1,total_pages+1):
            list_page_queue.put(page)

        expected_total_list_pages = total_pages
        expected_total_detail_jobs = total_pages * 10

        #进度条
        list_pbar = tqdm(total=expected_total_list_pages,desc='列表页',unit='页',dynamic_ncols=True,file=sys.stdout)
        detail_pbar = tqdm(total=expected_total_detail_jobs,desc='详情页',unit='项',dynamic_ncols=True,file=sys.stdout)

        #创建并启动线程
        threads = []

        #列表页工作线程
        for i in range(num_list_workers):
            t = threading.Thread(
                target=list_page_worker,
                args=(list_page_queue,http,Cookie,detail_task_queue,list_pbar,source_name,source_type),
                name=f"ListWorker-{i+1}",
                daemon=True
            )
            t.start()
            threads.append(t)

        #详情页工作线程
        for i in range(num_detail_workers):
            t = threading.Thread(
                target=detail_page_worker,
                args=(detail_task_queue,result_queue,http,Cookie,detail_pbar),
                name=f"DetailWorker-{i+1}",
                daemon=True
            )
            t.start()
            threads.append(t)

        #结果写入线程
        writer_thread = threading.Thread(
            target=result_writer,
            args=(result_queue,output_address,platform),
            name="ResultWriter",
            daemon=True
        )
        writer_thread.start()

        #等待列表页队列为空（等待完成列表页任务）
        list_page_queue.join()

        #通知列表页任务线程退出
        for _ in range(num_list_workers):
            list_page_queue.put(None)
        tqdm.write(f"所有列表页任务已完成,{num_list_workers}个工作线程已退出")


        #调整详情任务总数
        with progress_lock:
            actual_enqueued = enqueued_jobs_count
        if actual_enqueued > 0 and actual_enqueued < detail_pbar.total:
            detail_pbar.total = actual_enqueued
            detail_pbar.refresh()


        #等待详情页任务队列为空（详情页任务完成）
        detail_task_queue.join()

        #通知详情页工作线程退出
        for _ in range(num_detail_workers):
            detail_task_queue.put(None)
        tqdm.write(f"所有详情页任务已完成,{num_detail_workers}个工作线程已退出")


        #通知结果写入线程退出
        result_queue.put(None)

        for t in threads:
            t.join(timeout=10)

        writer_thread.join(timeout=10)

        elapsed_time = time.time() - start_time

        list_pbar.close()
        detail_pbar.close()

        print(f"爬取完成! 总耗时: {elapsed_time:.2f} 秒")


if __name__ == "__main__":
    #全局变量
    progress_lock = threading.Lock()
    list_pages_done = 0
    detail_jobs_done =0
    enqueued_jobs_count = 0
    platform_code = {1:"2000846399",2:"0",3:"gn5yn9s9wjydmrlh5kre1rzkma8mmjbu",4:"fawzqa3wo3hy2x4uqe1b1bwy4met2wpb",5:"",6:"kw1l0wxnmrjublxvvuny363bct3z11eo"}
    platform_name = {1:"BOSS",2:"国大",3:"国聘",4:"智联",5:"省校",6:"拉钩"}
    # 配置爬取参数
    TOTAL_PAGES = 400  # 要爬取的列表页总数
    NUM_LIST_WORKERS = 4  # 列表页工作线程数
    NUM_DETAIL_WORKERS = 10  # 详情页工作线程数
    OUTPUT_ADDRESS = "data\\"  # 输出文件的地址，默认为项目地址
    #配置模拟登录参数
    chrome_location = r'D:\python_project\Google\Chrome\Application\chrome.exe' #chrome启动器的位置
    username = "15897310548" #模拟登录的账号
    password = "2453829998Hu"#模拟登陆的密码

    detail_get_wrong = [] #获取详情失败的job_id
    list_get_wrong = [] #获取岗位失败的页码

    code = platform_choose()
    source_name = platform_code.get(code)
    source_type = sourcetype(code)
    platform = platform_name.get(code)

    #构建连接池
    print("------------------开始构建连接池------------------")
    http = urllib3.PoolManager(
        num_pools=50,
        maxsize=50,
        cert_reqs='CERT_NONE',
        assert_hostname=False,
        block=True,
        timeout=urllib3.Timeout(connect=5.0, read=10.0),
        retries=urllib3.Retry(total=3, backoff_factor=0.5),
    )
    print("------------------完成构建连接池------------------")

    #模拟登录，维持登录状态，获取组成Cookie的必要的参数
    print("-------------------开始模拟登录-------------------")
    Cookie = simulate_login_get_cookie(chrome_location=chrome_location,username=username,password=password)
    print("-------------------完成模拟登录-------------------")

    # 启动爬虫
    print("-------------------开始网络爬取-------------------")
    process_manager(
        total_pages=TOTAL_PAGES,
        num_list_workers=NUM_LIST_WORKERS,
        num_detail_workers=NUM_DETAIL_WORKERS,
        output_address=OUTPUT_ADDRESS,
        http = http,
        Cookie = Cookie,
        source_name = source_name,
        source_type = source_type,
        platform = platform
    )
    print("-------------------完成网络爬取-------------------")






