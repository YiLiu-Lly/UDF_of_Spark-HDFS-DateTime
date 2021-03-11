'''
在python中调用java对象来操作hdfs文件:
中文文档：https://blog.csdn.net/qq_36027670/article/details/79912206
英文文档：https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#rename-org.apache.hadoop.fs.Path-org.apache.hadoop.fs.Path-
'''
def get_hdfs(sc):
    """
    创建FileSystem
    :param sc SparkContext
    :return FileSystem对象
    """
    hadoopConf = sc._jsc.hadoopConfiguration()
    hdfs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    return hdfs

def hadoop_path(sc, filepath):
    """
    创建hadoop path对象
    :param sc SparkContext
    :param filepath 绝对路径
    """
    path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
    return path_class(filepath)

def delete_hdfsfile(sc, filepath, is_directory=True):
    """
    删除hdfs文件
    :param sc SparkContext
    :param filepath 绝对路径
    :param is_directory 是否为文件夹，单个文件取False，默认为True
    """
    hdfs = get_hdfs(sc)
    delete_file = hadoop_path(sc, filepath)
    if hdfs.exists(delete_file):
        return hdfs.delete(delete_file, is_directory)
    
def get_hdfsfile(sc, filepath):
    """
    读取所有匹配格式的文件
    常用通配符：？(匹配单个字符)、*(匹配多个字符)、[a-c]、{ab,cd}、{ab,c{de,fh}}
    :param sc SparkContext
    :param filepath 绝对路径
    """
    hdfs = get_hdfs(sc)
    files = []
    status = hdfs.globStatus(hadoop_path(sc, filepath)) # 通配符方式
    for fileStatus in status:
        files.append(str(fileStatus.getPath()))
    return files

def rename_hdfsfile(sc, filename_origin, filename_new):
    """
    重命名文件，相当于路径相同的mv
    :param sc SparkContext
    :param filename_origin path to be renamed
    :param filename_new new path after rename
    """
    hdfs = get_hdfs(sc)
    return hdfs.rename(hadoop_path(sc, filename_origin), hadoop_path(sc, filename_new))

def get_last_modified_time(sc, filepath):
    """
    读取文件最后一次修改时间，返回格式为13位时间戳
    :param sc SparkContext
    :param filepath 绝对路径
    """
    hdfs = get_hdfs(sc)
    return hdfs.getFileStatus(hadoop_path(sc, filepath)).getModificationTime()

def hdfs_mkdir(sc, filepath):
    """
    创建文件夹
    :param sc SparkContext
    :param filepath 绝对路径
    """
    hdfs = get_hdfs(sc)
    return hdfs.mkdirs(hadoop_path(sc,filepath))

def CopyFromHDFStoLocal(sc, hdfs_path, local_path):
    """
    从HDFS下载到本地，注意第一个参数是HDFS_path
    :param sc SparkContext
    :param hdfs_path 远端HDFS路径，填写完整文件名
    :param local_path 本地路径，填写完整文件名
    """
    hdfs = get_hdfs(sc)
    hdfs.copyToLocalFile(hadoop_path(sc,hdfs_path), hadoop_path(sc,local_path))

def CopyFromLocaltoHDFS(sc, local_path, hdfs_path):
    """
    从本地下载到HDFS，注意第一个参数是本地_path
    :param sc SparkContext
    :param local_path 本地路径，填写完整文件名
    :param hdfs_path 远端HDFS路径，填写完整文件名
    """
    hdfs = get_hdfs(sc)
    hdfs.copyFromLocalFile(hadoop_path(sc,local_path), hadoop_path(sc,hdfs_path))

if __name__=="__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

    hdfs_path = 'gh/wangbin18/ysera/bundle_model/featmap.train.txt'
    local_path = 'featmap.train.txt'
    delete_hdfsfile(sc, local_path, is_directory=False)
    CopyFromHDFStoLocal(sc, hdfs_path, local_path)

    def getFeatmap(biz_list):
        featmap = open('featmap.train.txt', 'r')
        fmap = {}
        for line in featmap:
            index, key, _ = line.strip().split('\t')
            fmap[key] = int(index)
        # 动态添加 label_biz 特征
        for biz in biz_list:
            label = 'label_biz=%s' % biz
            fmap[label] = len(fmap)
        featmap.close()
        return fmap

    BIZ_RATIO_LIST = [
        ('train', 0.029),
        ('zhenguo',0.041),
        ]
    biz_list = list(map(lambda _: _[0], BIZ_RATIO_LIST))
    a=getFeatmap(biz_list)
    print(a)