import os
import datetime


def delete_old_files(directory):
    # 获取当前时间
    current_time = datetime.datetime.now()

    # 定义时间差为10分钟
    time_difference = datetime.timedelta(minutes=10)

    # 遍历目录中的所有文件
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # 判断是否为文件而非目录
        if os.path.isfile(file_path):
            # 获取文件的最后修改时间
            modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

            # 如果文件日期早于20分钟前，就删除文件
            if current_time - modified_time > time_difference:
                os.remove(file_path)
