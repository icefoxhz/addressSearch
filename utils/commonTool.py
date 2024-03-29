import re
import os
import datetime


class CommonTool:

    @staticmethod
    def has_chinese_characters(s):
        """
        判断是否包含中文
        :param s:
        :return:
        """
        pattern = re.compile(r'[\u4e00-\u9fff]')
        return bool(pattern.search(s))

    @staticmethod
    def contains_letter_or_digit(s):
        """
        判断是否包含数字或字母
        :param s:
        :return:
        """
        pattern = re.compile(r'[a-zA-Z0-9]')
        if pattern.search(s):
            return True
        else:
            return False

    @staticmethod
    def is_last_char_number(s):
        if len(s) > 0:
            first_char = s[-1]
            return first_char.isalpha() or first_char.isdigit()
        else:
            # 如果字符串为空，首字母不存在，可返回False或自定义逻辑
            return False

    @staticmethod
    def is_first_char_number(s):
        if len(s) > 0:
            first_char = s[0]
            return first_char.isalpha() or first_char.isdigit()
        else:
            # 如果字符串为空，首字母不存在，可返回False或自定义逻辑
            return False

    @staticmethod
    def is_first_char_chinese(s):
        if len(s) > 0 and '\u4e00' <= s[0] <= '\u9fff':
            return True
        else:
            return False

    @staticmethod
    def count_chinese_characters(text):
        # 定义匹配中文字符的正则表达式
        pattern = re.compile(r'[\u4e00-\u9fa5]')

        # 使用findall查找所有中文字符
        chinese_characters = re.findall(pattern, text)

        # 返回中文字符的数量
        return len(chinese_characters)

    @staticmethod
    def chinese_to_arabic(chinese_num):
        """
        中文数字转阿拉伯数字
        :param chinese_num:
        :return:
        """
        # 没有中文直接返回
        if not CommonTool.has_chinese_characters(chinese_num):
            return chinese_num

        chinese_nums = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
            '〇': 0, '零': 0, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9,
        }
        chinese_units = {
            '十': 10, '拾': 10, '百': 100, '佰': 100, '千': 1000, '仟': 1000, '万': 10000, '亿': 100000000,
            "东": 10000, "南": 20000, "西": 30000, "北": 40000,
        }

        num = 0
        temp = 1
        for char in chinese_num:
            if char in chinese_nums:
                temp = chinese_nums[char]
            elif char in chinese_units:
                unit = chinese_units[char]
                num += temp * unit
                temp = 0
        num += temp
        return num

    @staticmethod
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

