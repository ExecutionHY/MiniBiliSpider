#!pyana
# -*- coding: utf-8 -*-

import json, math, queue
from time import sleep
from threading import Thread

import requests
import csv


import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt

# *******************************************************
# 扫描参数
# *******************************************************
#MAX_USER_ID = 449999999
MIN_USER_ID = 6269
MAX_USER_ID = 10000
DELAY_SEC = 0.1
DEBUG = 0
tag_query = [
    'JOJO',
]
video_sum = 0
countout_list = [
    33, 32, 39, 96, 98, 176,
    153,168,169,195,170,
    95,189,190,191,127,
    158, #服饰手动剔除
    164,159,192,
    166,131,
    37,178,179,180,
    147,145,146,83,
    185,187,
]
type_list = [
    24,25,47,27,
    51,152,
    28,31,30,194,59,193,29,130,
    20,154,156,
    17,171,172,65,173,121,136,19,
    124,122,
    138,21,76,75,161,162,163,174,
    22,26,126,
    #157,
    #71,137,
    #182,183,85,184,
]

# *******************************************************
# 封装 HTTP 请求
# *******************************************************
tasks = queue.Queue()
is_closing = False

# logger
import logging

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)

fh = logging.FileHandler("monitor.log")
fh.setLevel(logging.INFO)

fh.setFormatter(formatter)
logger.addHandler(fh)


# *******************************************************
# 后台进程
# *******************************************************
def deamon():

    print('<daemon> 运行中 ...')

    while True:

        sleep(0.1)

        if DEBUG:
            print('<daemon> 循环中 ...')

        try:
            data = tasks.get()
            name = data['name']
            url = data['url']
            hdlr = data['hdlr']
            cb = data['cb']
            # name, url, hdlr, cb = tasks.get()
            if DEBUG:
                print('<daemon> 处理消息', name, url)

        except queue.Empty:
            continue

        else:
            req = None
            txt = None
            res = None

            try:
                req = requests.get(url, timeout=(3,7))

            except requests.exceptions.Timeout:
                tasks.put_nowait({'name': name, 'url': url, 'hdlr': hdlr, 'cb': cb})
                if DEBUG:
                    print('<daemon> 请求超时 !')
                continue

            try:
                txt = hdlr(req.text)

            except:
                # TODO logging
                print('<daemon> 失败！未能成功处理消息文本！')
                continue

            try:
                res = json.loads(txt)

            except:
                # TODO logging
                print('<daemon> 失败！未能成功解析 JSON！')
                continue

            try:
                code = res['code']

            except KeyError:

                try:
                    status = res['status']

                except KeyError:
                    cb(None)
                    # TODO logging
                    print('<daemon> 找不到 `code` 或 `status`！')

                else:

                    if status is True:
                        cb(res)

                    else:
                        cb(None)
                        # TODO logging
                        print('<daemon> 非法 `status` [{}] !'.format(status))

            else:

                if code == 0:
                    #res['aid'] = url.split('=')[1]
                    cb(res)

                else:
                    #message = res['message']
                    #ttl = res['ttl']
                    cb(None)
                    # TODO logging
                    print('<daemon> 非法 `code` [{}] !'.format(code))

    print('<daemon> 循环结束！')

# *******************************************************
# 将 HTTP 请求压栈
# *******************************************************
def get(url, name = '', handler = (lambda text: text), callback = None):

    if DEBUG:
        print('请求 [{}] 压栈'.format(name))

    if callback is None:
        raise TypeError('`callback` 空指针！')

    global tasks
    tasks.put_nowait({'name': name, 'url': url, 'hdlr': handler, 'cb': callback})

# *******************************************************
# 处理 __jp5 回调
# *******************************************************
def handle_jp5(text):
    if len(text) <= 7:
        raise AssertionError('handle_jp5 内容具有错误长度 [{}]！'.format(len(text)))

    prefix, suffix = text[:6], text[-1]
    if prefix == '__jp5(' and suffix == ')':
        return text[6:-1]

# *******************************************************
# 处理用户关注关系数据
# *******************************************************
def handle_relation_data(data):
    buf = []

    for user in data['list']:
        entry = {
            'mid': user['mid'],
            #'attribute': user['attribute'],
            'mtime': user['mtime'],
            #'tag': user['tag'],
            #'special': user['special'],
            #'uname': user['uname'],
            #'face': user['face'],
            #'sign': user['sign'],
            #'official_verify': {
            #    'type': user['official_verify']['type'],
            #    'desc': user['official_verify']['desc'],
            #},
            #'vip': {
            #    'vipType': user['vip']['vipType'],
            #    'vipDueDate': user['vip']['vipDueDate'],
            #    'dueRemark': user['vip']['dueRemark'],
            #    'accessStatus': user['vip']['accessStatus'],
            #    'vipStatus': user['vip']['vipStatus'],
            #    'vipStatusWarn': user['vip']['vipStatusWarn'],
            #},
        }
        buf.append(entry)

    return buf

# *******************************************************
# 获取该用户粉丝列表
# *******************************************************
def get_followers(user_id, count_followers):
    step = 50
    followers = []

    for page in range(1, min(5, 1 + math.ceil(count_followers / step))):
        url = 'https://api.bilibili.com/x/relation/followers?vmid={}&pn={}&ps={}&order=desc&jsonp=jsonp&callback=__jp5'.format(user_id, page, step)
        get(url, name='get_followers', handler=handle_jp5, callback=lambda res: followers.extend(handle_relation_data(res['data'])))

    return followers

# *******************************************************
# 获取该用户关注列表
# *******************************************************
def get_followings(user_id, count_followings):
    step = 50
    followings = []

    for page in range(1, min(5, 1 + math.ceil(count_followings / step))):
        url = 'https://api.bilibili.com/x/relation/followings?vmid={}&pn={}&ps={}&order=desc&jsonp=jsonp&callback=__jp5'.format(user_id, page, step)
        get(url, name='get_followings', handler=handle_jp5, callback=lambda res: followings.extend(handle_relation_data(res['data'])))

    return followings

# *******************************************************
# 获取该用户基础信息
# *******************************************************
def get_user_info(user_id):
    #url01 = 'https://api.bilibili.com/x/relation/stat?vmid={}'.format(user_id)
    url02 = 'https://api.bilibili.com/x/space/navnum?mid={}'.format(user_id)
    step = 100
    following = None
    follower = None
    list_followings = None
    list_followers = None
    video = None
    videos = []
    videos_signal = None
    tags = []
    tags_signal = None
    tag_list = []



    def handle_information(res):
        nonlocal video

        if len(res) == 0:
            video = 0
            return
        data = res['data']
        # 该用户上传的视频 - 数量
        video = int(data['video'])
        if DEBUG:
            print('_视频数量：', video)
        # 该用户订阅的番剧 - 数量
        #bangumi = int(data['bangumi'])
        # 该用户创建的频道 - 数量
        #channel = {'master': int(data['channel']['master']), 'guest': int(data['channel']['guest'])}
        # 该用户创建的收藏夹 - 数量
        #favourite = {'master': int(data['favourite']['master']), 'guest': int(data['favourite']['guest'])}
        # 该用户订阅的标签 - 数量
        #tag = int(data['tag'])
        # 该用户撰写的文章 - 数量
        #article = int(data['article'])
        #playlist = data['playlist']
        #album = data['album']

    def handle_video_list(res):
        nonlocal videos

        if len(res) == 0:
            return
        data = res['data']
        vlist = data['vlist']

        for video_info in vlist:
            if video_info['play'] != "--" and video_info["aid"] != 0:
                # count out some videos in useless type
                if int(video_info['typeid']) in type_list and (video_info['play'] > 5000 or
                    'JO' in video_info['title'] or 'jo' in video_info['title'] or 'Jo' in video_info['title']):
                    videos.append({
                        # 视频av号
                        'aid': int(video_info['aid']),
                        # 视频分类
                        'typeid': video_info['typeid'],
                        # 播放量
                        'play': int(video_info['play']),
                        # 版权
                        'copyright': video_info['copyright'],
                        # 视频标题
                        'title': video_info['title'],
                        # 作者昵称
                        #'author': video_info['author'],
                        # 作者ID
                        'mid': int(video_info['mid']),
                        # 发布时间
                        'created': int(video_info['created']),
                        # 时间长度
                        'length': video_info['length'],
                    })
        nonlocal videos_signal
        videos_signal = 1

    def handle_tag(res):
        nonlocal tags
        nonlocal tags_signal

        
        if res == None:
            tags_signal = 0
            return
        if len(res) == 0:
            tags_signal = 0
            return
        data = res['data']
        if len(data):
            for tag in data:
                if 'JOJO' in tag['tag_name']:
                    tags_signal = 1
                    return

        tags_signal = 0


    #####################################################
    # 关系网
    #####################################################
    #get(url01, name='get_user_info', callback=handle_relation)

    #####################################################
    # 稿件信息
    #####################################################
    get(url02, name='get_user_info', callback=handle_information)
    while video == None:
        sleep(DELAY_SEC)
    if DEBUG:
        print('视频数量：', video)
    if video == 0:
        return None

    #####################################################
    # 遍历视频
    #
    # 参考：https://space.bilibili.com/ajax/member/getSubmitVideos?mid=6290510&pagesize=50&page=1
    #####################################################
    for page in range(1, min(5, 1 + math.ceil(video / step))):
        url = 'http://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize={}&page={}'.format(user_id, step, page)
        get(url, name='get_video_list_info', callback=handle_video_list)
    while videos_signal == None:
        sleep(DELAY_SEC)

    # tag 
    global video_cnt
    video_cnt = 0

    jojo_list = []
    for video in videos:
        tags = []
        tags_signal = None
        if 'JOJO' in video['title']:
            jojo_list.append(video)
            continue
        
        url_tag = "https://api.bilibili.com/x/tag/archive/tags?aid={}".format(video['aid'])
        get(url_tag, name='get_tag_info', callback=handle_tag)
        
        video_cnt += 1
        if video_cnt % 1000 == 0:
            logger.info('vcnt='+str(video_cnt)+'/'+str(len(videos)))
        while tags_signal == None:
            sleep(DELAY_SEC)
        
        if tags_signal == 1:
            jojo_list.append(video)
                
    # print(len(tags))
    # if len(tags) > 0:
    #     print(info)

    return jojo_list

'''
def get_video_info(video_id):
    url = 'https://api.bilibili.com/x/web-interface/archive/stat?aid={}'.format(video_id)
    res = get(url, name='get_video_info', callback=None)

def get_comments(video_id):
    url = 'https://api.bilibili.cn/feedback?aid={}'.format(video_id)
    res = get(url, name="get_comments", callback=None)
'''



def main():
    mid_list = []
    with open('out/mid_list.csv') as csvfile:
        csv_reader = csv.reader(csvfile)  # 使用csv.reader读取csvfile中的文件
        for row in csv_reader:  # 将csv 文件中的数据保存到birth_data中
            mid_list.append(int(row[1]))

    new_list = sorted(list(set(mid_list)))
    #print(new_list)

    cnt = 0
    for mid in new_list:
        cnt += 1
        print('正在扫描用户 [{}] ... {}/{}'.format(mid, cnt, len(new_list)))
        if cnt % 10 == 0:
            logger.info('id_cnt = {}/{}'.format(cnt, len(new_list)))

        data = get_user_info(mid)

        pd.DataFrame([[v['aid'], v['typeid'], v['play'], v['copyright'], v['title'], v['mid'], v['created'], v['length']] for v in data]
            ).to_csv('out/jojo_list.csv', mode='a', encoding='utf-8', header=None)

    # for type in type_list:
    #     info = get_type_videos(type)


    #     new_list = list(set(info))
        
    #     pd.DataFrame([[mid, type] for mid in new_list]
    #         ).to_csv('out/mid_list.csv', mode='a', encoding='utf-8', header=None)

class DaemonThread(Thread):

    def __init__(self):
            Thread.__init__(self)
            self.daemon = True
            self.name = 'Bilibili 爬虫 后台线程'

    def run(self):
        print('线程 `{}` 正在运行 ...'.format(self.name))
        deamon()

class MainThread(Thread):

    def __init__(self):
            Thread.__init__(self)
            self.name = 'Bilibili 爬虫 主线程'

    def run(self):
        print('线程 `{}` 正在运行 ...'.format(self.name))
        main()

DaemonThread().start()
MainThread().start()
