# -*- coding: utf-8 -*-
"""
:copyright: (c) 2016 by Huang Dong 
:mail: bjdhuang@cn.ibm.com.
:license: Apache 2.0, see LICENSE for more details.
"""
import random
from PIL import Image, ImageDraw, ImageFont, ImageFilter

_letter_cases = "abcdefghjkmnpqrstuvwxy"  # 小写字母，去除可能干扰的 i，l，o，z
_upper_cases = _letter_cases.upper()  # 大写字母
_numbers = ''.join(map(str, range(3, 10)))  # 数字
init_chars = ''.join((_letter_cases, _upper_cases, _numbers))

import random
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

CONTENT = "我二十一岁时正在云南插队队有一天她从山上下来和我讨论她不是破鞋的问题那时我还不大认识她因为她觉得穿什么不穿什么无所谓想像我和陈清扬讨论破鞋问题时的情景队长家的母狗正好跑到山上叫我看见"


class RandomChar():
    """用于随机生成汉字对应的Unicode字符"""

    @staticmethod
    def GB2312():
        head = random.choice(list(CONTENT))
        return head


def randRGB():
    return (random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255))


class ImageChar():
    def __init__(self, fontColor=(0, 0, 0),
                 size=(100, 40),
                 fontPath='YaHeiConsolas.ttf',
                 bgColor=(255, 255, 255, 255),
                 fontSize=25):
        self.size = size
        self.fontPath = fontPath
        self.bgColor = bgColor
        self.fontSize = fontSize
        self.fontColor = fontColor
        self.font = ImageFont.truetype(self.fontPath, self.fontSize)
        self.image = Image.new('RGBA', size, bgColor)

    def rotate(self):
        img1 = self.image.rotate(random.randint(-5, 5), expand=0)  # 默认为0，表示剪裁掉伸到画板外面的部分
        img = Image.new('RGBA', img1.size, (255,) * 4)
        self.image = Image.composite(img1, img, img1)

    def drawText(self, pos, txt, fill):
        draw = ImageDraw.Draw(self.image)
        draw.text(pos, txt, font=self.font, fill=fill)
        del draw

    def randPoint(self):
        (width, height) = self.size
        return (random.randint(0, width), random.randint(0, height))

    def randLine(self, num):
        draw = ImageDraw.Draw(self.image)
        for i in range(0, num):
            draw.line([self.randPoint(), self.randPoint()], randRGB())
        del draw

    def randChinese(self, num):
        gap = 0
        start = 0
        strRes = ''
        for i in range(0, num):
            char = RandomChar().GB2312()
            strRes += char
            x = start + self.fontSize * i + random.randint(0, gap) + gap * i
            self.drawText((x, random.randint(-5, 5)), char, (0, 0, 0))
            self.rotate()
        self.randLine(8)
        return strRes, self.image


if __name__ == "__main__":
    ic = ImageChar(fontColor=(100, 211, 90))
    strs, code_img = ic.randChinese(6)
    print(strs)
    buf = BytesIO()
    print(type(buf))
    code_img.save('xxx.jpg', 'JPEG')
    code_img.save(buf, 'JPEG', quality=70)

    # buf_str = buf.getvalue()
    # response = app.make_response(buf_str)
    # response.headers['Content-Type'] = 'image/jpeg'
    # return response
