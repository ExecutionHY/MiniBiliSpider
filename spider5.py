#!pyana
# -*- coding: utf-8 -*-

import json, math, queue
from time import sleep
from threading import Thread

import requests

import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt

# *******************************************************
# 扫描参数
# *******************************************************
#MAX_USER_ID = 449999999
MIN_USER_ID = 6269
MAX_USER_ID = 10000
DELAY_SEC = 0.01
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

	global handle_signal

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
				handle_signal = 1
				print('<daemon> 失败！未能成功处理消息文本！')
				continue

			try:
				res = json.loads(txt)

			except:
				# TODO logging
				handle_signal = 1
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
			#	'type': user['official_verify']['type'],
			#	'desc': user['official_verify']['desc'],
			#},
			#'vip': {
			#	'vipType': user['vip']['vipType'],
			#	'vipDueDate': user['vip']['vipDueDate'],
			#	'dueRemark': user['vip']['dueRemark'],
			#	'accessStatus': user['vip']['accessStatus'],
			#	'vipStatus': user['vip']['vipStatus'],
			#	'vipStatusWarn': user['vip']['vipStatusWarn'],
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
	step = 50
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

	print('正在扫描用户 [{}] ...'.format(user_id))

	def handle_relation(res01):
		nonlocal following
		nonlocal follower
		nonlocal list_followings
		nonlocal list_followers

		data = res01['data']
		# 该用户关注的人 - 数量
		following = int(data['following'])
		# 该用户的悄悄话 - 数量
		#whisper = int(data['whisper'])
		# 该用户的黑名单 - 数量
		#black = int(data['black'])
		# 关注该用户的人 - 数量
		follower = int(data['follower'])

		# 该用户的关注列表
		list_followings = get_followings(user_id, following)
		# 该用户的粉丝列表
		list_followers = get_followers(user_id, follower)

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
				if int(video_info['typeid']) in countout_list:
					continue 
				videos.append({
					# 评论数量
					'comment': int(video_info['comment']),
					# 视频分类
					'typeid': video_info['typeid'],
					# 播放量
					'play': int(video_info['play']),
					# 视频封面图片
					'pic': video_info['pic'],
					# 子标题？
					#'subtitle': video_info['subtitle'],
					# 视频简介
					#'description': video_info['description'],
					# 版权
					#'copyright': video_info['copyright'],
					# 视频标题
					'title': video_info['title'],
					#'review': video_info['review'],
					# 作者昵称
					#'author': video_info['author'],
					# 作者ID
					'mid': int(video_info['mid']),
					# 发布时间
					'created': int(video_info['created']),
					# 时间长度
					'length': video_info['length'],
					#'video_review': video_info['video_review'],
					# 视频av号
					'aid': int(video_info['aid']),
					#'hide_click': video_info['hide_click'],
				})
		nonlocal videos_signal
		videos_signal = 1

	def handle_tag(res):
		nonlocal tags
		nonlocal tags_signal

		if len(res) == 0:
			tags_signal = 1
			return
		data = res['data']
		if len(data):
			for tag in data:
				for tq in tag_query:
					if tq in tag['tag_name']:
						tags.append(tq)

		tags_signal = 1


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
	global video_sum
	for video in videos:
		tags = []
		tags_signal = None
		for tq in tag_query:
			if tq in video['title']:
				tags.append(tq)
		
		url_tag = "https://api.bilibili.com/x/tag/archive/tags?aid={}".format(video['aid'])
		get(url_tag, name='get_tag_info', callback=handle_tag)
		
		video_sum += 1
		if video_sum % 1000 == 0:
			logger.info('vcnt='+str(video_sum))
		while tags_signal == None:
			sleep(DELAY_SEC)
		# Duplicate removal 
		new_tags = list(set(tags))

		for tag in new_tags:
			tag_list.append({'tag':tag, 'created':video['created'], 'typeid':video['typeid']})
		
	
	if len(tag_list) > 0:
		print(tag_list)
	#####################################################
	# 存储数据
	#####################################################
	info = {
		#'followings': {
		#	'count': following,
		#	'users': list_followings,
		#},
		#'followers': {
		#	'count': follower,
		#	'users': list_followers,
		#},
		'videos': {
			'count': video,
			'videos': videos,
		},
		'tags': tag_list
	}
				
	# print(len(tags))
	# if len(tags) > 0:
	# 	print(info)

	return info

'''
def get_video_info(video_id):
	url = 'https://api.bilibili.com/x/web-interface/archive/stat?aid={}'.format(video_id)
	res = get(url, name='get_video_info', callback=None)

def get_comments(video_id):
	url = 'https://api.bilibili.cn/feedback?aid={}'.format(video_id)
	res = get(url, name="get_comments", callback=None)
'''

def get_type_videos(type_id, left, right):
	global nonstop
	global pn_sum
	pn = 0
	global handle_signal
	handle_signal = None
	videos = []

	def handle_video_list(res):
		global handle_signal
		global nonstop
		global pn_sum
		nonlocal videos

		if res == None:
			handle_signal = 1
			return
		if len(res) == 0:
			handle_signal = 1
			return

		data = res['data']
		pn_sum = int(data['page']['count'])/50
		for video_info in data['archives']:
			if video_info['stat']['view'] != '--' and video_info['aid'] != 0:
				# videos.append({
				# 	# 视频av号
				# 	'aid': int(video_info['aid']),
				# 	# 视频分类
				# 	'typeid': video_info['typeid'],
				# 	# 播放量
				# 	'play': int(video_info['play']),
				# 	# 版权
				# 	'copyright': video_info['copyright'],
				# 	# 视频标题
				# 	'title': video_info['title'],
				# 	# 作者昵称
				# 	#'author': video_info['author'],
				# 	# 作者ID
				# 	'mid': int(video_info['mid']),
				# 	# 发布时间
				# 	'created': int(video_info['created']),
				# 	# 时间长度
				# 	'length': video_info['length'],
				# })
				videos.append([
					video_info['aid'],
					video_info['tid'],
					video_info['copyright'],
					video_info['pubdate'],
					video_info['title'],
					video_info['duration'],
					video_info['owner']['mid'],
					video_info['owner']['name'],
					video_info['stat']['view']
				])

		handle_signal = 1
		if len(data['archives']) == 0:
			nonstop = False


	for pn in range(left, right):
		handle_signal = None
		url = 'http://api.bilibili.com/x/web-interface/newlist?rid={}&pn={}&ps=0'.format(type_id, pn)
		get(url, name='get_type_videos', callback=handle_video_list)
		if pn % 100 == 0:
			logger.info('type='+str(type_id)+', pn='+str(pn)+"/"+str(pn_sum))
		if pn % 10 == 0:
			print('type='+str(type_id)+', pn='+str(pn)+"/"+str(pn_sum))
		while handle_signal == None:
			sleep(DELAY_SEC)
		if nonstop == False or pn > pn_sum:
			break

	return videos




def main():
	
	idx = 0
	global nonstop
	global pn_sum

	fromidx = 30
	frompn = 1800

	for tid in type_list:
		idx += 1
		if idx < fromidx:
			continue
		nonstop = True
		pn_sum = 0
		for i in range(0, 1000):
			left = i * 100
			right = (i+1) * 100
			
			if idx == fromidx and left < frompn:
				continue

			info = get_type_videos(tid, left, right)

			pd.DataFrame([video for video in info]
				).to_csv('out/all_list{}.csv'.format(idx), mode='a', encoding='utf-8', header=None)

			if nonstop == False:
				break

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
