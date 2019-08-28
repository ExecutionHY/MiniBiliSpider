# MiniBiliSpider

reference [BiliSpider](https://github.com/uupers/BiliSpider)

- spider.py
  - 遍历所有uid，遍历每个人的空间里的视频，再次访问api获取tag
  - 存在问题：需要遍历4.5亿个人+6000万个视频tag页，算力不够
- spider2.py
  - 遍历二级分区页中的所有视频（剔除一些分区），筛选播放>1w的视频，再次访问tag页
  - 存在问题：精度不够，大量的播放少的JOJO视频被排除。如果降低筛选力度，则总视频量太多算力不够。二级分页访问数3000w/50，tag页500w
- spider3.py
  - 遍历二级分区内所有视频的标题，记录包含‘JO’字样的up主id
  - 不知道怎么回事，居然跑的很慢，比 spider5 慢多了，如果是这样我宁可先跑5，再直接用整个的数据来计算
- spider4.py
  - 根据‘JOJO相关up主列表’中的数据，遍历约5w个uid，每个人的空间页最多可以包含100个视频。
  - 筛选标题中带‘JO’的视频，和播放数超过5k的视频，速度还蛮快的
- spider5.py
  - 遍历二级分区内所有视频，导出成数据集，便于后期处理