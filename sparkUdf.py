# encoding: utf-8

"""
@Author liaoxiangdang
@Date 2018-09-08 23:02

封装开发过程中常用的spark操作
"""

import simplejson as json
import os
import sys
import socket


def _itemgetter(dimension):
    """
    >>> a = {'k1': 'v1', 'k2': 'v2'}
    >>> f = _itemgetter(['k1', 'k2'])
    >>> f(a)
    ('v1', 'v2')
    """

    def f(item):
        return tuple([item.get(i) for i in dimension]) if dimension else None

    return f


def _distinct(lines, dimension=None, numPartitions=None):
    """
    根据dimmension指定的key值去重

    :param lines: spark rdd 实例, 每行数据必须为dict类型
    :param numPartitions: 去重后的分区数
    :param dimension: 根据哪些维度进行去重
    :return:

    >>> lines = sc.parallelize([{'k1': '1', 'k2': '2'}, {'k1': '1', 'k2': '2'}, {'k1': '1', 'k2': '3'}])
    >>> lines = _distinct(lines, numPartitions=10, dimension=['k1', 'k2'])
    >>> for line in lines.collect():
    >>>     print(line)

    {'k2': '3', 'k1': '1'}
    {'k2': '2', 'k1': '1'}

    """
    if numPartitions is None:
        numPartitions = lines.getNumPartitions()
    if dimension is None:
        raise Exception("must setup dimension")
    lines = lines.keyBy(_itemgetter(dimension))
    lines = lines.reduceByKey(lambda a, b: a, numPartitions=numPartitions)
    lines = lines.map(lambda _: _[1])
    return lines


def _fullOuterJoin(lines1, lines2, dimension1, dimension2, distinct=True, numPartitions=None):
    """

    :param lines1:
    :param lines2:
    :param dimension1:
    :param dimension2:
    :param distinct: 在join之前是否需要去重
    :param numPartitions: join之后的分区数
    :return:

    >>> lines1 = sc.parallelize([{'userid': '1', 'os': 'ios'}, {'userid': '1', 'os': 'android'}, {'userid': '2', 'os': 'android'}])
    >>> lines2 = sc.parallelize([{'mt_userid': '1', 'os': 'ios'}, {'mt_userid': '3', 'os': 'android'}])
    >>> joined_lines = _fullOuterJoin(lines1, lines2, ['userid', 'os'], ['mt_userid', 'os'], distinct=True, numPartitions=1)
    >>> print ('')
    >>> for line in joined_lines.collect():
    >>>     print (line)

    (('2', 'android'), ({'os': 'android', 'userid': '2'}, None))
    (('1', 'ios'), ({'os': 'ios', 'userid': '1'}, {'mt_userid': '1', 'os': 'ios'}))
    (('1', 'android'), ({'os': 'android', 'userid': '1'}, None))
    (('3', 'android'), (None, {'mt_userid': '3', 'os': 'android'}))

    """
    lines1 = lines1.keyBy(_itemgetter(dimension1))
    lines2 = lines2.keyBy(_itemgetter(dimension2))
    if not numPartitions:
        numPartitions = lines1.getNumPartitions() + lines2.getNumPartitions()

    # 去重，考虑去重随机保留一条记录是否对结果影响
    if distinct:
        lines1 = lines1.reduceByKey(lambda a, b: a, numPartitions=numPartitions)
        lines2 = lines2.reduceByKey(lambda a, b: a, numPartitions=numPartitions)

    joined_lines = lines1.fullOuterJoin(lines2, numPartitions=numPartitions)
    return joined_lines


def _leftOuterJoin(lines1, lines2, dimension1, dimension2, distinct=True, numPartitions=None):
    """

    :param lines1:
    :param lines2:
    :param dimension1:
    :param dimension2:
    :param distinct: 在join之前是否需要去重
    :param numPartitions: join之后的分区数
    :return:

    >>> lines1 = sc.parallelize([{'userid': '1', 'os': 'ios'}, {'userid': '1', 'os': 'android'}, {'userid': '2', 'os': 'android'}])
    >>> lines2 = sc.parallelize([{'mt_userid': '1', 'os': 'ios'}, {'mt_userid': '3', 'os': 'android'}])
    >>> joined_lines = _leftOuterJoin(lines1, lines2, ['userid', 'os'], ['mt_userid', 'os'], distinct=True, numPartitions=1)
    >>> for line in joined_lines.collect():
    >>>     print (line)

    (('2', 'android'), ({'os': 'android', 'userid': '2'}, None))
    (('1', 'ios'), ({'os': 'ios', 'userid': '1'}, {'mt_userid': '1', 'os': 'ios'}))
    (('1', 'android'), ({'os': 'android', 'userid': '1'}, None))
    """
    if not numPartitions:
        numPartitions = max(lines1.getNumPartitions(), lines2.getNumPartitions())
    lines1 = lines1.keyBy(_itemgetter(dimension1))
    lines2 = lines2.keyBy(_itemgetter(dimension2))

    # 去重，考虑去重随机保留一条记录是否对结果影响
    if distinct:
        lines1 = lines1.reduceByKey(lambda a, b: a, numPartitions=numPartitions)
        lines2 = lines2.reduceByKey(lambda a, b: a, numPartitions=numPartitions)

    joined_lines = lines1.leftOuterJoin(lines2, numPartitions=numPartitions)
    return joined_lines


def _join(lines1, lines2, dimension1, dimension2, distinct=True, numPartitions=None):
    """

    :param distinct: 在join之前是否需要去重
    :param numPartitions: join之后的分区数

    >>> lines1 = sc.parallelize([{'userid': '1', 'os': 'ios'}, {'userid': '1', 'os': 'ios'}, {'userid': '1', 'os': 'android'}])
    >>> lines2 = sc.parallelize([{'mt_userid': '1', 'os': 'ios'}, {'mt_userid': '1', 'os': 'ios'}, {'mt_userid': '1', 'os': 'android'}])
    >>> joined_lines = _join(lines1, lines2, ['userid', 'os'], ['mt_userid', 'os'], distinct=True, numPartitions=1)
    >>> for line in joined_lines.collect():
    >>>     print (line)

    (('1', 'android'), ({'os': 'android', 'userid': '1'}, {'mt_userid': '1', 'os': 'android'}))
    (('1', 'ios'), ({'os': 'ios', 'userid': '1'}, {'mt_userid': '1', 'os': 'ios'}))
    """
    if not numPartitions:
        numPartitions = max(lines1.getNumPartitions(), lines2.getNumPartitions())
    lines1 = lines1.keyBy(_itemgetter(dimension1))
    lines2 = lines2.keyBy(_itemgetter(dimension2))

    # 去重，考虑去重随机保留一条记录是否对结果影响
    if distinct:
        lines1 = lines1.reduceByKey(lambda a, b: a, numPartitions=numPartitions)
        lines2 = lines2.reduceByKey(lambda a, b: a, numPartitions=numPartitions)

    joined_lines = lines1.join(lines2, numPartitions=numPartitions)
    return joined_lines


def _count(lines, dimension=None):
    """
    统计每个维度下的数量

    :param lines:
    :param dimension:
    :return:
    """
    if dimension is None:
        dimension = []
    lines = lines.map(_itemgetter(dimension))
    return dict(lines.countByValue())


def sortMergeJoin(rdd1, rdd2, numPartitions=5000):
    rdd1 = rdd1.repartitionAndSortWithinPartitions(numPartitions)
    rdd2 = rdd2.repartitionAndSortWithinPartitions(numPartitions)
    rdd = rdd1.join(rdd2)
    return rdd


def groupByKeyWithLimit(lines, limit=1000, numPartitions=5000):
    """
    多个rdd需要join时，传统的做法下n个rdd 关联到一起需要n-1次join, 大量数据会被重复传输，严重影响性能
    如果首先把所有数据通过union算子关联起来，用这个方法进行关联，所有数据只需要传输一次，可以大幅度提升性能

    :param lines: spark rdd实例，要求类型为（k,v)
    :param limit:
    :param numPartitions:
    :return:
    """

    def reducer(lines, limit=limit):
        key = None
        res = []
        for k, v in lines:
            if k == key:
                if len(res) > limit:
                    continue
                res.append(v)
            else:
                if key is not None:
                    yield (key, res)
                key = k
                res = [v]
        if res:
            yield (key, res)

    lines = lines.repartitionAndSortWithinPartitions(numPartitions)
    lines = lines.mapPartitions(reducer)
    return lines


def _rdd2hdfs(rdd, output_path, overwrite=True, codec="org.apache.hadoop.io.compress.GzipCodec", f=json.dumps):
    """
    保存文件到hdfs, 默认覆盖已经存在的文件，压缩并转json格式后保存

    :param rdd: 需要保存到hdfs的的spark RDD
    :param output_path: hdfs输出路径
    :param overwrite: 是否覆盖已有文件, overwrite=True时覆盖已有文件
    :param codec: 压缩编码,codec=None时不进行文件压缩
    :param f: 文件写入前对每行数据做操作
    :return:
    """
    print(output_path)
    if overwrite:
        _rmhdfs(output_path)
    if f:
        rdd = rdd.map(f)
    if codec:
        rdd.saveAsTextFile(output_path, codec)
    else:
        rdd.saveAsTextFile(output_path)

def _getfsAndConf():
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(conf)
    return fs, conf

def _mvhdfs(src_path, des_path, overwrite=True):
    fs, conf = _getfsAndConf()

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    src_path_obj = Path(src_path)
    des_path_obj = Path(des_path)


    if not overwrite and fs.exists(des_path_obj):
        return False
    _rmhdfs(des_path)
    return fs.rename(src_path_obj, des_path_obj)

def _existshdfs(path):
    fs, conf = _getfsAndConf()

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    path_obj = Path(path)

    return fs.exists(path_obj)

def _rmhdfs(output_path):
    """
    删除hdfs目录或文件

    :param output_path:
    :return:
    """

    fs, conf = _getfsAndConf()

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    path_obj = Path(output_path)

    Trash = sc._gateway.jvm.org.apache.hadoop.fs.Trash
    return fs.exists(path_obj) and Trash.moveToAppropriateTrash(fs, path_obj, conf)

def _hdfs2rdd(input_dir, f=json.loads):
    """
    读取文件

    :param input_dir:
    :param f:
    :return:
    """
    lines = spark.sparkContext.textFile(input_dir)
    if f:
        lines = lines.map(f)
    return lines


def _hdfs2df(input_dir, filetype='json'):
    """
    读取hdfs文件为DataFrame，只支持json和parquet格式的文件

    :param input_dir:
    :param filetype: 支持json, parquet
    :return:
    """
    if filetype == 'json':
        lines_df = spark.read.json(input_dir)
    elif filetype == 'parquet':
        lines_df = spark.read.parquet(input_dir)
    else:
        raise Exception('illegal filepyte %s' % filetype)
    return lines_df


def _hive2rdd(sql, f=json.loads):
    """

    :return:
    """
    lines = spark.sql(sql).toJSON()
    if f:
        lines = lines.map(f)
    return lines


def _isSql(path_or_sql):
    """
    以select开头的任务是sql语句
    """
    return path_or_sql.lower().strip().startswith('select')

def _top(lines, key_func, top_num, del_top=0.0):
    total_num = lines.count()
    sample_list = _sample(lines, pow(10, 6), total_num).map(key_func).collect()
    sample_list.sort(reverse=True)
    sample_len = len(sample_list)

    start_pos = int(sample_len * 1.0 * top_num / total_num + sample_len * del_top)
    start_pos = min(start_pos, sample_len - 1)

    end_score = sample_list[int(sample_len * del_top)]
    start_score = sample_list[start_pos]

    return lines.filter(lambda _: key_func(_) >= start_score and key_func(_) <= end_score)

def _sample(lines, num=pow(10, 5), total_num=None):
     if total_num is None:
         total_num = lines.count()
     fraction = num * 1.0 /  total_num
     lines = lines.sample(False, fraction, num)
     return lines

def _read(path_or_sql, f=json.loads):
    """
    读取sql或hdfs为rdd

    :param path_or_sql: hdfs路径或sql语句
    :param f:
    :return:
    """
    if _isSql(path_or_sql):
        lines = spark.sql(path_or_sql).toJSON()
    else:
        lines = spark.sparkContext.textFile(path_or_sql)

    if f:
        lines = lines.map(f)
    return lines

def writeToEs(data, alias_name, index_name, es_cluster_name='giant_prod_marketing', mode="insert", owner=None, daxiang_push=False, verbose=True, id_key=None):
    client = sjs.Client()
    params = {
        'appName': 'qlservice',
        'classPath': 'es.WriteEs',
        'context': 'pySparkSession_esService_300x8_gaoyajun02@meituan.com'
    }
    data = {
        "mode": mode,
        "data" : data,
        "alias_name": alias_name,
        "index_name": index_name,
        "daxiang_push": daxiang_push,
        "es_cluster_name": es_cluster_name,
        "shard_num": 10,
        "parallelism": 20,
        "batch_lines": 100
    }
    if id_key:
        data['id_key'] = id_key
    if owner:
        data['owner'] = owner
    if verbose:
        print('请求参数:')
        print(json.dumps(data, ensure_ascii=True, indent=4).encode('utf8'))
    res = client.submit(json.dumps(data), params, verbose=verbose)
    if verbose:
        print('请求结果:')
        print(json.dumps(res, ensure_ascii=True, indent=4).encode('utf8'))
