# -*- coding: utf-8 -*-

import time
import datetime
import csv
import pandas as pd

def datestr2unix(dt):
    return time.mktime(time.strptime(dt, '%Y-%m-%d'))

def datetime2unix(dt):
    format = '%Y-%m-%d'
    dt = dt.strftime(format)
    return time.mktime(time.strptime(dt, format))

def unix2date(val):
    format = '%Y-%m-%d'
    val = time.localtime(val)
    return time.strftime(format, val)

type_list = [
	24,
	25,47,27,
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
typename_list = [
    '动画',
    '动画','动画','动画',
    '番剧','番剧',
    '音乐','音乐','音乐','音乐','音乐','音乐','音乐','音乐',
    '舞蹈','舞蹈','舞蹈',
    '游戏','游戏','游戏','游戏','游戏','游戏','游戏','游戏',
    '科技','科技',
    '生活','生活','生活','生活','生活','生活','生活','生活',
    '鬼畜','鬼畜','鬼畜',
]

name_list = [
    'MAD·AMV',
    'MMD·3D','短片·手书·配音','综合',
    '资讯','官方延伸',
    '原创音乐','翻唱','VOCALOID·UTAU','电音','演奏','MV','音乐现场','音乐综合',
    '宅舞','三次元舞蹈','舞蹈教程',
    '单机游戏','电子竞技','手机游戏','网络游戏','桌游棋牌','GMV','音游','Mugen',
    '趣味科普人文','野生技术协会',
    '搞笑','日常','美食圈','动物圈','手工','绘画','运动','其他',
    '鬼畜调教','音MAD','人力VOCALOID',
]

all_list = []

for i in range(1, 39):
    new_list = []
    cnt = 0

    with open('out/all_list{}.csv'.format(i), "r") as f:
        for line in f:
            try:
                item = [int(line.split(',')[1]), float(line.split(',')[4]), line.split(',')[5]]
            except :
                continue

            cnt += 1
            new_list.append(item)
            if cnt % 50000 == 0:
                print('read {}={}'.format(i, cnt))

    new_list = list(set([tuple(t) for t in new_list]))
    new_list = [list(v) for v in new_list]

    new_list.sort(key=lambda x:x[1])

    time_small = unix2date(float(new_list[0][1]))
    begin = datetime.date(int(time_small.split('-')[0]), int(time_small.split('-')[1]), int(time_small.split('-')[2]))
    end = datetime.date(2019, 8, 27)
    t = begin
    if datetime2unix(t) < datetime2unix(datetime.date(2009, 7, 16)):
        t = datetime.date(2011, 1, 1)
    cnt = 0
    jojo_cnt = 0
    time_list = []
    for row in new_list:
        if 'JOJO' in row[2] or 'jojo' in row[2] or 'JoJo' in row[2]:
            jojo_cnt += 1
        if float(row[1]) <= datetime2unix(t):
            cnt += 1
        else:
            while float(row[1]) > datetime2unix(t):
                t += datetime.timedelta(days=1)
                num = 0
                if cnt > 0:
                    num = jojo_cnt/cnt*1000
                time_list.append([typename_list[i-1]+'-'+name_list[i-1], 'JOJO投稿数量', jojo_cnt, t])
            cnt += 1
        if cnt % 50000 == 0:
            print('calc={}/{}'.format(cnt, len(new_list)))
        if datetime2unix(t) > datetime2unix(end):
            break
    
    pd.DataFrame([row for row in time_list]
        ).to_csv('out2/jojo_list3.csv', columns=[0,1,2,3], mode='a', encoding='utf-8', header=None, index=False)

