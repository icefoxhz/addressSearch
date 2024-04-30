import re
import os
import datetime
import signal
import sys

import numpy as np
import pandas as pd


class CommonTool:
    ES_LONG_MAX = 9223372036854775807

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
        chinese_nums = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
            '〇': 0, '零': 0, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9,
        }
        if len(s) > 0:
            first_char = s[-1]
            return first_char.isdigit() or first_char in chinese_nums.keys()
        else:
            # 如果字符串为空，首字母不存在，可返回False或自定义逻辑
            return False

    @staticmethod
    def is_first_char_number(s):
        chinese_nums = {
            '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
            '〇': 0, '零': 0, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9,
        }
        if len(s) > 0:
            first_char = s[0]
            return first_char.isdigit() or first_char in chinese_nums.keys()
        else:
            # 如果字符串为空，首字母不存在，可返回False或自定义逻辑
            return False

    @staticmethod
    def is_first_char_letter(s):
        pattern = r'^[a-zA-Z]'
        return bool(re.match(pattern, s))

    @staticmethod
    def is_first_char_number_or_letter(s):
        return CommonTool.is_first_char_letter(s) or CommonTool.is_first_char_number(s)

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

    @staticmethod
    def remove_chinese_chars(input_string):
        pattern = "[\u4e00-\u9fa5]"
        result = re.sub(pattern, "", input_string)
        return result

    @staticmethod
    def replace_chinese_to_symbol(input_string, symbol="-"):
        pattern = "[\u4e00-\u9fa5]"
        result = re.sub(pattern, symbol, input_string)
        return result

    @staticmethod
    def hash_to_int(number, max_val):
        # 使用模运算将任何数字映射到 0 - max_val-1 之间
        return abs(hash(number)) % max_val

    @staticmethod
    def convert_building_num(building_num):
        try:
            building_num = str(building_num)
            # 如果是数字
            pattern = re.compile(r'^-?\d+(\.\d+)?$')
            if bool(pattern.match(building_num)):
                return int(building_num)

            # 带字母的
            pattern = re.compile('^[a-zA-Z0-9]*$')
            if bool(pattern.match(building_num)):
                en_count = 0
                for char in building_num:
                    if char.isalpha():  # 检查字符是否为英文字母
                        en_count += 1

                ascii_values = []
                for char in building_num:
                    if char.isalpha():  # 检查字符是否为英文字母
                        if en_count == 1:
                            # 如果带字母，且只有1个字母。把字母转成 ascII码 + 1000 ，这里加1000是为了避免和正的楼栋号一样，正的楼栋号不可能超过1000
                            ascii_values.append(str(ord(char) + 1000))
                        else:
                            ascii_values.append(str(ord(char)))
                    else:
                        ascii_values.append(char)
                building_num = "".join(ascii_values)
                building_num = int(building_num)
                # hash，最大值为es中Long类型的最大值
                if building_num > CommonTool.ES_LONG_MAX:
                    building_num = CommonTool.hash_to_int(building_num, CommonTool.ES_LONG_MAX)
                return building_num
        except:
            pass
        return 0

    @staticmethod
    def split_dataframe(df, n):
        # 首先确保n不会导致分割结果的小数部分
        if n < 2 or n > df.shape[0]:
            return [df]

        # 计算每份数据的行数
        chunks = np.array_split(df.values, n, axis=0)

        # 将numpy数组转换回DataFrame
        return [pd.DataFrame(chunk, columns=df.columns) for chunk in chunks]

    @staticmethod
    def write_pid(file_name, pid):
        with open(file_name, mode="w") as file:
            file.write(str(pid))

    @staticmethod
    def full_to_half(s):
        """
        全角字符转半角字符
        :param s: 输入的全角字符串
        :return: 转换后的半角字符串
        """
        # 全角空格转换为半角空格
        s = s.replace('　', ' ')
        # 转换其他全角字符（根据Unicode编码范围进行转换）
        res = ''
        for char in s:
            if '\uFF01' <= char <= '\uFF5E':  # 全角字符范围
                # 半角字符 = 全角字符 - 0xfee0
                char = chr(ord(char) - 0xfee0)
            res += char
        return res

    @staticmethod
    def remove_spaces(s):
        """
        移除字符串中的所有空格字符
        :param s: 输入的字符串
        :return: 所有空格移除后的字符串
        """
        # 使用正则表达式匹配并替换掉所有空格字符
        return re.sub(r'\s+', '', s)

