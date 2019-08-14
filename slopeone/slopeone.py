# -*- coding=utf8 -*-

import pandas as pd
import heapq
import time
import json
import datetime


class recommender(object):

    def __init__(self, data, k=10, metric='pearson', n=5):
        # 以下变量将用于Slope One算法
        self.data = data
        self.frequencies = {}
        self.deviations = {}
        self.items = {}
        self.k = k

    def computeDeviations(self):
        # 获取每位用户的评分数据
        for ratings in self.data.values():
            # 对于该用户的每个评分项
            for item, rating in ratings.items():
                self.frequencies.setdefault(item, {})
                self.deviations.setdefault(item, {})
                # 再次遍历该用户的每个评分项
                for item2, rating2 in ratings.items():
                    if item != item2:
                        # 将评分的差异保存到变量中
                        self.frequencies[item].setdefault(item2, 0)
                        self.deviations[item].setdefault(item2, 0.0)
                        try:
                            self.frequencies[item][item2] += 1
                            self.deviations[item][item2] += rating - rating2
                        except KeyError:
                            continue
        del_keys_out = []
        del_keys_inner = []
        print("before del count: %d" % len(self.deviations))
        for item, ratings in self.deviations.items():
            # 删除值为空的key
            if len(ratings) < 1:
                del_keys_out.append(item)
            for item2 in ratings:
                ratings[item2] /= self.frequencies[item][item2]

                # 删除值为 0.0 的key，值为 0.0 的key对于推荐值累加时可以省略，为了减少遍历长度，删除
                if self.deviations[item][item2] == 0.0:
                    del_keys_inner.append((item, item2))
        for (item, item2) in del_keys_inner:
            del self.deviations[item][item2]
        for key in del_keys_out:
            del self.deviations[key]
        print("after del count: %d" % len(self.deviations))
        self.items = set(self.deviations.keys())

    def slopeOneRecommendations(self, userRatings):
        recommendations = {}
        frequencies = {}
        # 遍历目标用户的评分项
        for userItem, userRating in userRatings.items():
            # 对目标用户未评价的物品进行计算
            #for diffItem, diffRatings in self.deviations.items():
            for diffItem in self.items:
                diffRatings = self.deviations[diffItem]
                if diffItem not in userRatings and userItem in diffRatings:
                    freq = self.frequencies[diffItem][userItem]
                    recommendations.setdefault(diffItem, 0.0)
                    frequencies.setdefault(diffItem, 0)
                    # 分子
                    recommendations[diffItem] += (diffRatings[userItem] + userRating) * freq
                    # 分母
                    frequencies[diffItem] += freq
                    #freq = self.frequencies[diffItem][userItem]
                    #recommendations.setdefault(diffItem, 0.0)
                    #frequencies.setdefault(diffItem, 0)

        recommendations = [(k, v / frequencies[k] if not frequencies[k] == 0 else 0.0) for k, v in recommendations.items()]

        # 排序并返回，用topk获取值最大的k个
        # recommendations.sort(key=lambda artistTuple: artistTuple[1], reverse=True)
        results = heapq.nlargest(self.k, recommendations, key=lambda item: item[1])

        return results


def run():
    rating_path = 'data/etl/train_implicit/part-00000'  # (user_id, item_id, rating)
    result_path = 'data/recall/u_slopeone_result'  # (user_id, item_id, rating)

    df = pd.read_csv(rating_path, header=None, sep='|')
    df.columns = ['user_id', 'item_id', 'rating']
    print(df.shape)
    print(df)
    # 按用户id分组
    data = dict(list(df.groupby('user_id')))
    # 训练数据格式化成字典
    train_data = get_formated_data(data)
    print("count: %d" %len(train_data))

    start_time = time.time()
    # 计算推荐结果, 取top10
    k = 10
    slopeone_recom(train_data, data, diff2_path, result_path, k)

    end_time = time.time()
    print("cost %.2f seconds" % (end_time - start_time))


def slopeone_recom(train_data, data, diff_path, result_path, k):
    r = recommender(train_data, k)
    # 计算差异矩阵
    r.computeDeviations()

    results = {}
    user_list = data.keys()
    print("user count:", len(user_list))
    # 对每个用户计算推荐结果
    for i in range(len(user_list)):
        user_id = user_list[i]
        user_rating = train_data[user_id]
        # print("user:", user_id)
        # print(user_rating)
        # 对用户根据slopeone推荐物品，计算评分值
        user_result = r.slopeOneRecommendations(user_rating)
        completed_count = len(results)

        results[user_id] = user_result
        # 打印计算进度（完成的用户比例）
        if completed_count % 100 == 0:
            print("%.2f%s (%d users) completed!" % (completed_count * 100 / len(train_data), '%', completed_count))
        # print("result:")
        # print(result)
    print("results:")
    print(len(results))
    # 将所有用户的推荐结果由dict转化为dataframe，并根据user_id flatmap 出来
    result_df = pd.DataFrame.from_dict(results, orient='index').stack().apply(pd.Series).reset_index().drop(['level_1'],
                                                                                                            axis=1)
    print("df count:", result_df.shape)
    print(result_df.head())
    # 保存最终结果
    result_df.to_csv(result_path, sep='|', index=False, header=False)


def get_formated_data(data):
    formatted_data = {}
    for user_id in data.keys():
        user_rating = {}
        group_user_rating = data[user_id]

        item_list = list(group_user_rating['item_id'])
        rating_list = list(group_user_rating['rating'])

        for i in range(len(item_list)):
            item_id = item_list[i]
            rating = rating_list[i]

            user_rating[item_id] = rating
        formatted_data[user_id] = user_rating

    return formatted_data


if __name__ == '__main__':
    run()
