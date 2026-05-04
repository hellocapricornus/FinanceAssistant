# accounting.py - 完整修改版（适配物理隔离）
import re
import time
import sqlite3
import logging
import aiohttp
import math
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler, CallbackQueryHandler
from auth import is_authorized, get_user_admin_id, add_temp_operator, remove_temp_operator
from config import OWNER_ID

# 导入新的连接管理器
from db_manager import get_conn

logger = logging.getLogger(__name__)

# 状态定义 (保持不变)
ACCOUNTING_DATE_SELECT = 1
ACCOUNTING_CONFIRM_CLEAR = 2
ACCOUNTING_CONFIRM_CLEAR_ALL = 3
ACCOUNTING_VIEW_PAGE = 4
ACCOUNTING_YEAR_SELECT = 5
ACCOUNTING_MONTH_SELECT = 6
ACCOUNTING_DATE_SELECT_PAGE = 7
ACCOUNTING_EXPORT_YEAR_SELECT = 8
ACCOUNTING_EXPORT_MONTH_SELECT = 9
ACCOUNTING_EXPORT_DATE_RANGE_SELECT = 10

MAX_DISPLAY_RECORDS = 8
PAGE_SIZE = 10
DB_TIMEOUT = 10
DAYS_PER_PAGE = 10

# USDT 合约地址 (不变)
USDT_CONTRACTS = {
    'TRC20': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
    'ERC20': '0xdAC17F958D2ee523a2206206994597C13D831ec7'
}
TRONGRID_API = "https://api.trongrid.io"
ETHERSCAN_API = "https://api.etherscan.io/api"
ETHERSCAN_API_KEY = "MVYZTUF89KQ117USY6WH8CT2M6W7TK3PUD"

BEIJING_TZ = timezone(timedelta(hours=8))
def beijing_time(timestamp: int) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=BEIJING_TZ)
def get_today_beijing() -> str:
    return beijing_time(int(time.time())).strftime('%Y-%m-%d')

# 国家代码和名称映射（带国旗）
COUNTRY_FLAGS = {
    # 中文名称
    '中国': '🇨🇳', '美国': '🇺🇸', '日本': '🇯🇵', '韩国': '🇰🇷',
    '英国': '🇬🇧', '法国': '🇫🇷', '德国': '🇩🇪', '意大利': '🇮🇹',
    '西班牙': '🇪🇸', '葡萄牙': '🇵🇹', '荷兰': '🇳🇱', '瑞士': '🇨🇭',
    '瑞典': '🇸🇪', '挪威': '🇳🇴', '丹麦': '🇩🇰', '芬兰': '🇫🇮',
    '俄罗斯': '🇷🇺', '澳大利亚': '🇦🇺', '新西兰': '🇳🇿', '加拿大': '🇨🇦',
    '巴西': '🇧🇷', '阿根廷': '🇦🇷', '墨西哥': '🇲🇽', '印度': '🇮🇳',
    '泰国': '🇹🇭', '越南': '🇻🇳', '新加坡': '🇸🇬', '马来西亚': '🇲🇾',
    '印度尼西亚': '🇮🇩', '菲律宾': '🇵🇭', '土耳其': '🇹🇷', '阿联酋': '🇦🇪',
    '沙特': '🇸🇦', '南非': '🇿🇦', '埃及': '🇪🇬', '希腊': '🇬🇷',
    '爱尔兰': '🇮🇪', '波兰': '🇵🇱', '捷克': '🇨🇿', '奥地利': '🇦🇹',
    '比利时': '🇧🇪', '匈牙利': '🇭🇺',
    '尼日利亚': '🇳🇬', '乌克兰': '🇺🇦', '波兰': '🇵🇱', '捷克': '🇨🇿',
    '斯洛伐克': '🇸🇰', '匈牙利': '🇭🇺', '罗马尼亚': '🇷🇴', '保加利亚': '🇧🇬',
    '塞尔维亚': '🇷🇸', '克罗地亚': '🇭🇷', '斯洛文尼亚': '🇸🇮', '爱沙尼亚': '🇪🇪',
    '拉脱维亚': '🇱🇻', '立陶宛': '🇱🇹', '白俄罗斯': '🇧🇾', '摩尔多瓦': '🇲🇩',
    '格鲁吉亚': '🇬🇪', '亚美尼亚': '🇦🇲', '阿塞拜疆': '🇦🇿', '哈萨克斯坦': '🇰🇿',
    '乌兹别克斯坦': '🇺🇿', '土库曼斯坦': '🇹🇲', '吉尔吉斯斯坦': '🇰🇬', '塔吉克斯坦': '🇹🇯',
    '蒙古': '🇲🇳', '朝鲜': '🇰🇵', '柬埔寨': '🇰🇭', '老挝': '🇱🇦',
    '缅甸': '🇲🇲', '斯里兰卡': '🇱🇰', '巴基斯坦': '🇵🇰', '孟加拉国': '🇧🇩',
    '尼泊尔': '🇳🇵', '不丹': '🇧🇹', '马尔代夫': '🇲🇻', '伊朗': '🇮🇷',
    '伊拉克': '🇮🇶', '科威特': '🇰🇼', '卡塔尔': '🇶🇦', '巴林': '🇧🇭',
    '阿曼': '🇴🇲', '也门': '🇾🇪', '约旦': '🇯🇴', '黎巴嫩': '🇱🇧',
    '叙利亚': '🇸🇾', '以色列': '🇮🇱', '巴勒斯坦': '🇵🇸', '塞浦路斯': '🇨🇾',
    '阿尔及利亚': '🇩🇿', '摩洛哥': '🇲🇦', '突尼斯': '🇹🇳', '利比亚': '🇱🇾',
    '苏丹': '🇸🇩', '埃塞俄比亚': '🇪🇹', '肯尼亚': '🇰🇪', '坦桑尼亚': '🇹🇿',
    '乌干达': '🇺🇬', '卢旺达': '🇷🇼', '刚果': '🇨🇩', '安哥拉': '🇦🇴',
    '纳米比亚': '🇳🇦', '博茨瓦纳': '🇧🇼', '赞比亚': '🇿🇲', '津巴布韦': '🇿🇼',
    '莫桑比克': '🇲🇿', '马达加斯加': '🇲🇬', '毛里求斯': '🇲🇺', '塞舌尔': '🇸🇨',
    '加纳': '🇬🇭', '科特迪瓦': '🇨🇮', '喀麦隆': '🇨🇲', '塞内加尔': '🇸🇳',
    '马里': '🇲🇱', '布基纳法索': '🇧🇫', '尼日尔': '🇳🇪', '乍得': '🇹🇩',
    '中非': '🇨🇫', '加蓬': '🇬🇦', '赤道几内亚': '🇬🇶', '吉布提': '🇩🇯',
    '索马里': '🇸🇴', '厄立特里亚': '🇪🇷', '毛里塔尼亚': '🇲🇷', '冈比亚': '🇬🇲',
    '几内亚': '🇬🇳', '几内亚比绍': '🇬🇼', '塞拉利昂': '🇸🇱', '利比里亚': '🇱🇷',
    '冰岛': '🇮🇸', '马耳他': '🇲🇹', '卢森堡': '🇱🇺', '摩纳哥': '🇲🇨',
    '列支敦士登': '🇱🇮', '安道尔': '🇦🇩', '圣马力诺': '🇸🇲', '梵蒂冈': '🇻🇦',
    '古巴': '🇨🇺', '牙买加': '🇯🇲', '海地': '🇭🇹', '多米尼加': '🇩🇴',
    '波多黎各': '🇵🇷', '巴哈马': '🇧🇸', '特立尼达和多巴哥': '🇹🇹', '巴巴多斯': '🇧🇧',
    '圣卢西亚': '🇱🇨', '格林纳达': '🇬🇩', '安提瓜和巴布达': '🇦🇬', '圣基茨和尼维斯': '🇰🇳',
    '哥伦比亚': '🇨🇴', '委内瑞拉': '🇻🇪', '厄瓜多尔': '🇪🇨', '秘鲁': '🇵🇪',
    '玻利维亚': '🇧🇴', '巴拉圭': '🇵🇾', '乌拉圭': '🇺🇾', '圭亚那': '🇬🇾',
    '苏里南': '🇸🇷', '法属圭亚那': '🇬🇫', '斐济': '🇫🇯', '巴布亚新几内亚': '🇵🇬',
    '所罗门群岛': '🇸🇧', '瓦努阿图': '🇻🇺', '萨摩亚': '🇼🇸', '汤加': '🇹🇴',
    '密克罗尼西亚': '🇫🇲', '马绍尔群岛': '🇲🇭', '帕劳': '🇵🇼', '瑙鲁': '🇳🇷',
    '图瓦卢': '🇹🇻', '基里巴斯': '🇰🇮',

    # 英文名称
    'china': '🇨🇳', 'usa': '🇺🇸', 'japan': '🇯🇵', 'korea': '🇰🇷',
    'uk': '🇬🇧', 'france': '🇫🇷', 'germany': '🇩🇪', 'italy': '🇮🇹',
    'spain': '🇪🇸', 'portugal': '🇵🇹', 'netherlands': '🇳🇱', 'switzerland': '🇨🇭',
    'sweden': '🇸🇪', 'norway': '🇳🇴', 'denmark': '🇩🇰', 'finland': '🇫🇮',
    'russia': '🇷🇺', 'australia': '🇦🇺', 'new zealand': '🇳🇿', 'canada': '🇨🇦',
    'brazil': '🇧🇷', 'argentina': '🇦🇷', 'mexico': '🇲🇽', 'india': '🇮🇳',
    'thailand': '🇹🇭', 'vietnam': '🇻🇳', 'singapore': '🇸🇬', 'malaysia': '🇲🇾',
    'indonesia': '🇮🇩', 'philippines': '🇵🇭', 'turkey': '🇹🇷', 'uae': '🇦🇪',
    'saudi': '🇸🇦', 'south africa': '🇿🇦', 'egypt': '🇪🇬', 'greece': '🇬🇷',
    'ireland': '🇮🇪', 'poland': '🇵🇱', 'czech': '🇨🇿', 'austria': '🇦🇹',
    'belgium': '🇧🇪', 'hungary': '🇭🇺',
    'nigeria': '🇳🇬', 'ukraine': '🇺🇦', 'poland': '🇵🇱', 'czech': '🇨🇿',
    'slovakia': '🇸🇰', 'hungary': '🇭🇺', 'romania': '🇷🇴', 'bulgaria': '🇧🇬',
    'serbia': '🇷🇸', 'croatia': '🇭🇷', 'slovenia': '🇸🇮', 'estonia': '🇪🇪',
    'latvia': '🇱🇻', 'lithuania': '🇱🇹', 'belarus': '🇧🇾', 'moldova': '🇲🇩',
    'georgia': '🇬🇪', 'armenia': '🇦🇲', 'azerbaijan': '🇦🇿', 'kazakhstan': '🇰🇿',
    'uzbekistan': '🇺🇿', 'turkmenistan': '🇹🇲', 'kyrgyzstan': '🇰🇬', 'tajikistan': '🇹🇯',
    'mongolia': '🇲🇳', 'cambodia': '🇰🇭', 'laos': '🇱🇦', 'myanmar': '🇲🇲',
    'sri lanka': '🇱🇰', 'pakistan': '🇵🇰', 'bangladesh': '🇧🇩', 'nepal': '🇳🇵',
    'bhutan': '🇧🇹', 'maldives': '🇲🇻', 'iran': '🇮🇷', 'iraq': '🇮🇶',
    'kuwait': '🇰🇼', 'qatar': '🇶🇦', 'bahrain': '🇧🇭', 'oman': '🇴🇲',
    'yemen': '🇾🇪', 'jordan': '🇯🇴', 'lebanon': '🇱🇧', 'syria': '🇸🇾',
    'israel': '🇮🇱', 'palestine': '🇵🇸', 'cyprus': '🇨🇾', 'algeria': '🇩🇿',
    'morocco': '🇲🇦', 'tunisia': '🇹🇳', 'libya': '🇱🇾', 'sudan': '🇸🇩',
    'ethiopia': '🇪🇹', 'kenya': '🇰🇪', 'tanzania': '🇹🇿', 'uganda': '🇺🇬',
    'rwanda': '🇷🇼', 'congo': '🇨🇩', 'angola': '🇦🇴', 'namibia': '🇳🇦',
    'botswana': '🇧🇼', 'zambia': '🇿🇲', 'zimbabwe': '🇿🇼', 'mozambique': '🇲🇿',
    'madagascar': '🇲🇬', 'mauritius': '🇲🇺', 'seychelles': '🇸🇨', 'ghana': '🇬🇭',
    'ivory coast': '🇨🇮', 'cameroon': '🇨🇲', 'senegal': '🇸🇳', 'mali': '🇲🇱',
    'burkina faso': '🇧🇫', 'niger': '🇳🇪', 'chad': '🇹🇩', 'central african': '🇨🇫',
    'gabon': '🇬🇦', 'equatorial guinea': '🇬🇶', 'djibouti': '🇩🇯', 'somalia': '🇸🇴',
    'eritrea': '🇪🇷', 'mauritania': '🇲🇷', 'gambia': '🇬🇲', 'guinea': '🇬🇳',
    'guinea-bissau': '🇬🇼', 'sierra leone': '🇸🇱', 'liberia': '🇱🇷', 'iceland': '🇮🇸',
    'malta': '🇲🇹', 'luxembourg': '🇱🇺', 'monaco': '🇲🇨', 'liechtenstein': '🇱🇮',
    'andorra': '🇦🇩', 'san marino': '🇸🇲', 'vatican': '🇻🇦', 'cuba': '🇨🇺',
    'jamaica': '🇯🇲', 'haiti': '🇭🇹', 'dominican': '🇩🇴', 'puerto rico': '🇵🇷',
    'bahamas': '🇧🇸', 'trinidad': '🇹🇹', 'barbados': '🇧🇧', 'saint lucia': '🇱🇨',
    'grenada': '🇬🇩', 'antigua': '🇦🇬', 'colombia': '🇨🇴', 'venezuela': '🇻🇪',
    'ecuador': '🇪🇨', 'peru': '🇵🇪', 'bolivia': '🇧🇴', 'paraguay': '🇵🇾',
    'uruguay': '🇺🇾', 'guyana': '🇬🇾', 'suriname': '🇸🇷', 'fiji': '🇫🇯',
    'papua new guinea': '🇵🇬', 'solomon islands': '🇸🇧', 'vanuatu': '🇻🇺',
    'samoa': '🇼🇸', 'tonga': '🇹🇴', 'micronesia': '🇫🇲', 'marshall islands': '🇲🇭',
    'palau': '🇵🇼', 'nauru': '🇳🇷', 'tuvalu': '🇹🇻', 'kiribati': '🇰🇮',

    # 常用缩写
    'cn': '🇨🇳', 'us': '🇺🇸', 'jp': '🇯🇵', 'kr': '🇰🇷',
    'gb': '🇬🇧', 'fr': '🇫🇷', 'de': '🇩🇪', 'it': '🇮🇹',
    'es': '🇪🇸', 'pt': '🇵🇹', 'nl': '🇳🇱', 'ch': '🇨🇭',
    'se': '🇸🇪', 'no': '🇳🇴', 'dk': '🇩🇰', 'fi': '🇫🇮',
    'ru': '🇷🇺', 'au': '🇦🇺', 'nz': '🇳🇿', 'ca': '🇨🇦',
    'br': '🇧🇷', 'ar': '🇦🇷', 'mx': '🇲🇽', 'in': '🇮🇳',
    'th': '🇹🇭', 'vn': '🇻🇳', 'sg': '🇸🇬', 'my': '🇲🇾',
    'id': '🇮🇩', 'ph': '🇵🇭', 'tr': '🇹🇷', 'ae': '🇦🇪',
    'sa': '🇸🇦', 'za': '🇿🇦', 'eg': '🇪🇬', 'gr': '🇬🇷',
    'ie': '🇮🇪', 'pl': '🇵🇱', 'cz': '🇨🇿', 'at': '🇦🇹',
    'be': '🇧🇪', 'hu': '🇭🇺',
    'ng': '🇳🇬', 'ua': '🇺🇦', 'pl': '🇵🇱', 'cz': '🇨🇿',
    'sk': '🇸🇰', 'hu': '🇭🇺', 'ro': '🇷🇴', 'bg': '🇧🇬',
    'rs': '🇷🇸', 'hr': '🇭🇷', 'si': '🇸🇮', 'ee': '🇪🇪',
    'lv': '🇱🇻', 'lt': '🇱🇹', 'by': '🇧🇾', 'md': '🇲🇩',
    'ge': '🇬🇪', 'am': '🇦🇲', 'az': '🇦🇿', 'kz': '🇰🇿',
    'uz': '🇺🇿', 'tm': '🇹🇲', 'kg': '🇰🇬', 'tj': '🇹🇯',
    'mn': '🇲🇳', 'kh': '🇰🇭', 'la': '🇱🇦', 'mm': '🇲🇲',
    'lk': '🇱🇰', 'pk': '🇵🇰', 'bd': '🇧🇩', 'np': '🇳🇵',
    'bt': '🇧🇹', 'mv': '🇲🇻', 'ir': '🇮🇷', 'iq': '🇮🇶',
    'kw': '🇰🇼', 'qa': '🇶🇦', 'bh': '🇧🇭', 'om': '🇴🇲',
    'ye': '🇾🇪', 'jo': '🇯🇴', 'lb': '🇱🇧', 'sy': '🇸🇾',
    'il': '🇮🇱', 'ps': '🇵🇸', 'cy': '🇨🇾', 'dz': '🇩🇿',
    'ma': '🇲🇦', 'tn': '🇹🇳', 'ly': '🇱🇾', 'sd': '🇸🇩',
    'et': '🇪🇹', 'ke': '🇰🇪', 'tz': '🇹🇿', 'ug': '🇺🇬',
    'rw': '🇷🇼', 'cd': '🇨🇩', 'ao': '🇦🇴', 'na': '🇳🇦',
    'bw': '🇧🇼', 'zm': '🇿🇲', 'zw': '🇿🇼', 'mz': '🇲🇿',
    'mg': '🇲🇬', 'mu': '🇲🇺', 'sc': '🇸🇨', 'gh': '🇬🇭',
    'ci': '🇨🇮', 'cm': '🇨🇲', 'sn': '🇸🇳', 'ml': '🇲🇱',
    'bf': '🇧🇫', 'ne': '🇳🇪', 'td': '🇹🇩', 'cf': '🇨🇫',
    'ga': '🇬🇦', 'gq': '🇬🇶', 'dj': '🇩🇯', 'so': '🇸🇴',
    'er': '🇪🇷', 'mr': '🇲🇷', 'gm': '🇬🇲', 'gn': '🇬🇳',
    'gw': '🇬🇼', 'sl': '🇸🇱', 'lr': '🇱🇷', 'is': '🇮🇸',
    'mt': '🇲🇹', 'lu': '🇱🇺', 'mc': '🇲🇨', 'li': '🇱🇮',
    'ad': '🇦🇩', 'sm': '🇸🇲', 'va': '🇻🇦', 'cu': '🇨🇺',
    'jm': '🇯🇲', 'ht': '🇭🇹', 'do': '🇩🇴', 'pr': '🇵🇷',
    'bs': '🇧🇸', 'tt': '🇹🇹', 'bb': '🇧🇧', 'lc': '🇱🇨',
    'gd': '🇬🇩', 'ag': '🇦🇬', 'co': '🇨🇴', 've': '🇻🇪',
    'ec': '🇪🇨', 'pe': '🇵🇪', 'bo': '🇧🇴', 'py': '🇵🇾',
    'uy': '🇺🇾', 'gy': '🇬🇾', 'sr': '🇸🇷', 'fj': '🇫🇯',
    'pg': '🇵🇬', 'sb': '🇸🇧', 'vu': '🇻🇺', 'ws': '🇼🇸',
    'to': '🇹🇴', 'fm': '🇫🇲', 'mh': '🇲🇭', 'pw': '🇵🇼',
    'nr': '🇳🇷', 'tv': '🇹🇻', 'ki': '🇰🇮',
}

def get_category_with_flag(category: str) -> str:
    """获取带国旗的类别名称"""
    if not category:
        return category

    category_lower = category.lower().strip()

    # 直接匹配
    if category_lower in COUNTRY_FLAGS:
        return f"{COUNTRY_FLAGS[category_lower]} {category}"

    # 尝试匹配部分（如"德国柏林" -> "德国"）
    for country, flag in COUNTRY_FLAGS.items():
        if country in category_lower or category_lower in country:
            return f"{flag} {category}"

    # 没有匹配到，返回原名称
    return category

def safe_escape_markdown(text: str) -> str:
    """转义 Markdown 特殊字符，防止报错"""
    if not text:
        return text
    # 需要转义的字符：_ * [ ] ( ) ~ ` > # + - = | { } . !
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

class Calculator:
    """安全计算器类"""

    # 允许的数学函数
    MATH_FUNCTIONS = {
        'sqrt': math.sqrt,
        'sin': math.sin,
        'cos': math.cos,
        'tan': math.tan,
        'asin': math.asin,
        'acos': math.acos,
        'atan': math.atan,
        'log': math.log,
        'log10': math.log10,
        'exp': math.exp,
        'abs': abs,
        'round': round,
        'floor': math.floor,
        'ceil': math.ceil,
        'pi': math.pi,
        'e': math.e,
    }

    @staticmethod
    def safe_eval(expr: str):
        """安全计算表达式"""
        try:
            # 预处理：替换 ^ 为 **
            expr = expr.replace('^', '**')

            # 创建安全的命名空间
            safe_dict = Calculator.MATH_FUNCTIONS.copy()
            safe_dict['__builtins__'] = {}

            # 计算
            result = eval(expr, {"__builtins__": {}}, safe_dict)

            return float(result)
        except:
            return None

    @staticmethod
    def format_result(result: float) -> str:
        """格式化结果"""
        if result.is_integer():
            return str(int(result))
        else:
            # 保留适当的小数位数
            s = f"{result:.6f}".rstrip('0').rstrip('.')
            return s


# 🔥 添加上标转换函数
def superscript_number(n) -> str:
    """将数字转换为上标形式，支持整数、小数和负数"""
    superscripts = {
        '0': '⁰', '1': '¹', '2': '²', '3': '³', '4': '⁴',
        '5': '⁵', '6': '⁶', '7': '⁷', '8': '⁸', '9': '⁹',
        '.': '·',   # 小数点用中点代替
        '-': '⁻'    # 负号用上标负号
    }

    s = str(n)
    result = []
    for c in s:
        if c in superscripts:
            result.append(superscripts[c])
        else:
            result.append(c)
    return ''.join(result)


def format_fee_info(fee_rate: float, exchange_rate: float) -> str:
    """格式化手续费和汇率显示，如 ⁵/7.2 或 ¹¹/7.2"""
    # 处理整数：5.0 显示为 5，不显示 .0
    if fee_rate == int(fee_rate):
        fee_display = int(fee_rate)
    else:
        fee_display = fee_rate

    fee_sup = superscript_number(fee_display)
    return f"{fee_sup}/{exchange_rate:.2f}"

def generate_export_html(records: List[Dict], group_name: str, start_date: str, end_date: str) -> str:
    """生成导出账单的 HTML"""

    per_transaction_fee = 0
    for r in records:
        if r['type'] == 'income':
            if r.get('per_transaction_fee', 0) != 0:
                per_transaction_fee = r.get('per_transaction_fee', 0)
            break

    # 按日期分组
    records_by_date = {}
    for r in records:
        date = r.get('date', '')
        if date not in records_by_date:
            records_by_date[date] = {'income': [], 'expense': [], 'stats': {'income_cny': 0, 'income_usdt': 0, 'expense_usdt': 0}}

        if r['type'] == 'income':
            records_by_date[date]['income'].append(r)
            records_by_date[date]['stats']['income_cny'] += r['amount']
            records_by_date[date]['stats']['income_usdt'] += r['amount_usdt']
        else:
            records_by_date[date]['expense'].append(r)
            records_by_date[date]['stats']['expense_usdt'] += r['amount_usdt']

    # 计算总计
    total_income_cny = sum(r['amount'] for r in records if r['type'] == 'income')
    total_income_usdt = sum(r['amount_usdt'] for r in records if r['type'] == 'income')
    total_expense_usdt = sum(r['amount_usdt'] for r in records if r['type'] == 'expense')
    total_pending = total_income_usdt - total_expense_usdt

    # 获取费率、汇率和单笔费用
    fee_rate = 0
    exchange_rate = 1
    per_transaction_fee = 0
    for r in records:
        if r['type'] == 'income':
            if r.get('fee_rate', 0) != 0:
                fee_rate = r.get('fee_rate', 0)
            if r.get('rate', 0) != 0:
                exchange_rate = r.get('rate', 1)
            if r.get('per_transaction_fee', 0) != 0:
                per_transaction_fee = r.get('per_transaction_fee', 0)
            if fee_rate != 0 and exchange_rate != 1:
                break

    def get_flag(cat):
        if not cat:
            return ''
        cat_lower = cat.lower().strip()
        if cat_lower in COUNTRY_FLAGS:
            return COUNTRY_FLAGS[cat_lower]
        for country, flag in COUNTRY_FLAGS.items():
            if country in cat_lower or cat_lower in country:
                return flag
        return ''

    def fmt(num, decimals=2):
        if num == int(num):
            return str(int(num))
        return f"{num:.{decimals}f}"

    # ========== 按操作人分组 ==========
    operator_income = {}
    for r in records:
        if r['type'] == 'income':
            operator = r.get('display_name', r.get('username', '未知'))
            user_id = r.get('user_id', 0)
            key = f"op_{user_id}_{hash(operator) % 10000}"
            if key not in operator_income:
                operator_income[key] = {
                    'name': operator,
                    'records': [],
                    'total_cny': 0.0,
                    'total_usdt': 0.0,
                }
            operator_income[key]['records'].append(r)
            operator_income[key]['total_cny'] += r['amount']
            operator_income[key]['total_usdt'] += r['amount_usdt']

    operator_expense = {}
    for r in records:
        if r['type'] == 'expense':
            operator = r.get('display_name', r.get('username', '未知'))
            user_id = r.get('user_id', 0)
            key = f"op_{user_id}_{hash(operator) % 10000}"
            if key not in operator_expense:
                operator_expense[key] = {
                    'name': operator,
                    'records': [],
                    'total_usdt': 0.0,
                }
            operator_expense[key]['records'].append(r)
            operator_expense[key]['total_usdt'] += r['amount_usdt']

        # ========== 生成每个操作人的可折叠记录（按日期分组） ==========
        operator_sections = ""
        all_operator_keys = set(list(operator_income.keys()) + list(operator_expense.keys()))

        if all_operator_keys:
            for idx, key in enumerate(sorted(all_operator_keys, key=lambda k: operator_income.get(k, {}).get('total_cny', 0), reverse=True)):
                safe_key = key.replace('.', '_').replace('-', '_')
                op_income = operator_income.get(key, {})
                op_expense = operator_expense.get(key, {})

                name = op_income.get('name') or op_expense.get('name', '未知')
                income_count = len(op_income.get('records', []))
                income_cny = op_income.get('total_cny', 0)
                income_usdt = op_income.get('total_usdt', 0)
                expense_count = len(op_expense.get('records', []))
                expense_usdt = op_expense.get('total_usdt', 0)

                # ✅ 按日期分组
                op_income_by_date = {}
                for r in op_income.get('records', []):
                    date = r.get('date', '未知日期')
                    if date not in op_income_by_date:
                        op_income_by_date[date] = []
                    op_income_by_date[date].append(r)

                op_expense_by_date = {}
                for r in op_expense.get('records', []):
                    date = r.get('date', '未知日期')
                    if date not in op_expense_by_date:
                        op_expense_by_date[date] = []
                    op_expense_by_date[date].append(r)

                operator_sections += f'''
            <div class="date-group operator-group">
                <div class="date-header" onclick="toggleSection(this)">
                    <span>👤 {name} · 入款{income_count}笔 {fmt(income_cny)}元 ≈ {fmt(income_usdt)}USDT · 出款{expense_count}笔 {fmt(expense_usdt)}USDT</span>
                    <span class="toggle-icon">▼</span>
                </div>
                <div class="date-content">
    '''

                # ✅ 按日期显示入款
                for date, date_records in sorted(op_income_by_date.items()):
                    date_cny = sum(r['amount'] for r in date_records)
                    date_usdt = sum(r['amount_usdt'] for r in date_records)
                    operator_sections += f'''
                    <div class="section-title">📈 {date} · 入款{len(date_records)}笔 · {fmt(date_cny)}元 ≈ {fmt(date_usdt)}USDT</div>
                    <table>
                        <thead>
                            <tr><th>时间</th><th>金额(元)</th><th>手续费</th><th>汇率</th><th>单笔费用</th><th>USDT</th><th>分类</th></tr>
                        </thead>
                        <tbody>
    '''
                    for r in date_records:
                        dt = beijing_time(r['created_at'])
                        time_str = dt.strftime('%H:%M')
                        cat = r.get('category', '') or ''
                        flag = get_flag(cat)
                        cat_display = f"{flag} {cat}" if flag else (cat or '无')
                        fr = r.get('fee_rate', 0)
                        rate = r.get('rate', 0)
                        per_fee = r.get('per_transaction_fee', 0)
                        fee_display = f"{fmt(fr)}%" if fr == int(fr) else f"{fr}%"
                        rate_display = fmt(rate) if rate == int(rate) else f"{rate:.2f}"
                        per_fee_display = f"{fmt(per_fee)}元" if per_fee > 0 else "-"
                        operator_sections += f'''
                            <tr>
                                <td class="record-time">{time_str}</td>
                                <td class="record-amount income">+{fmt(r['amount'])}</td>
                                <td class="record-fee">{fee_display}</td>
                                <td>{rate_display}</td>
                                <td>{per_fee_display}</td>
                                <td>{fmt(r['amount_usdt'])}</td>
                                <td>{cat_display}</td>
                            </tr>'''
                    operator_sections += '''
                        </tbody>
                    </table>
    '''

                # ✅ 按日期显示出款
                for date, date_records in sorted(op_expense_by_date.items()):
                    date_usdt = sum(r['amount_usdt'] for r in date_records)
                    operator_sections += f'''
                    <div class="section-title expense">📉 {date} · 出款{len(date_records)}笔 · {fmt(date_usdt)}USDT</div>
                    <table>
                        <thead>
                            <tr><th>时间</th><th>金额(USDT)</th></tr>
                        </thead>
                        <tbody>
    '''
                    for r in date_records:
                        dt = beijing_time(r['created_at'])
                        time_str = dt.strftime('%H:%M')
                        operator_sections += f'''
                            <tr>
                                <td class="record-time">{time_str}</td>
                                <td class="record-amount" style="color:#ef4444;">-{fmt(r['amount_usdt'])}</td>
                            </tr>'''
                    operator_sections += '''
                        </tbody>
                    </table>
    '''

                operator_sections += '''
                </div>
            </div>
    '''

    # 生成 HTML
    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>账单导出 - {group_name}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh; padding: 20px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .header {{
            background: white; border-radius: 16px; padding: 24px 32px;
            margin-bottom: 24px; box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }}
        .header h1 {{ color: #333; font-size: 28px; margin-bottom: 8px; }}
        .header .subtitle {{ color: #666; font-size: 14px; }}
        .summary {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px; margin-bottom: 24px;
        }}
        .summary-card {{
            background: white; border-radius: 12px; padding: 20px;
            text-align: center; box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }}
        .summary-card.income {{ border-bottom: 4px solid #10b981; }}
        .summary-card.expense {{ border-bottom: 4px solid #ef4444; }}
        .summary-card.pending {{ border-bottom: 4px solid #f59e0b; }}
        .summary-card .label {{ font-size: 14px; color: #666; margin-bottom: 8px; }}
        .summary-card .value {{ font-size: 28px; font-weight: bold; }}
        .summary-card.income .value {{ color: #10b981; }}
        .summary-card.expense .value {{ color: #ef4444; }}
        .summary-card.pending .value {{ color: #f59e0b; }}
        .config-card {{
            background: white; border-radius: 12px; padding: 16px 20px;
            margin-bottom: 24px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            display: flex; justify-content: space-around; flex-wrap: wrap; gap: 16px;
        }}
        .config-item {{ text-align: center; }}
        .config-item .label {{ font-size: 12px; color: #666; margin-bottom: 4px; }}
        .config-item .value {{ font-size: 18px; font-weight: 600; color: #333; }}
        .date-group {{
            background: white; border-radius: 12px; margin-bottom: 20px;
            overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .operator-group {{
            border-left: 4px solid #f59e0b;
        }}
        .date-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; padding: 12px 20px; font-size: 18px; font-weight: bold;
            cursor: pointer; display: flex; justify-content: space-between; align-items: center;
        }}
        .date-header:hover {{ opacity: 0.95; }}
        .date-header .toggle-icon {{ transition: transform 0.3s; }}
        .date-header.collapsed .toggle-icon {{ transform: rotate(-90deg); }}
        .date-content {{ padding: 20px; }}
        .date-content.collapsed {{ display: none; }}
        .section-title {{
            font-size: 16px; font-weight: 600; margin: 16px 0 12px 0;
            padding-left: 8px; border-left: 4px solid #10b981;
        }}
        .section-title.expense {{ border-left-color: #ef4444; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
        th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }}
        th {{ background: #f9fafb; font-weight: 600; color: #374151; font-size: 13px; }}
        td {{ font-size: 14px; }}
        .record-time {{ color: #6b7280; font-family: monospace; font-size: 12px; }}
        .record-amount {{ font-weight: 600; }}
        .record-amount.income {{ color: #10b981; }}
        .record-fee {{ color: #8b5cf6; font-family: monospace; font-size: 12px; }}
        .subtotal {{
            text-align: right; padding: 8px 12px; background: #f9fafb;
            border-radius: 8px; font-size: 14px; font-weight: 500;
        }}
        .footer {{ text-align: center; padding: 24px; color: rgba(255,255,255,0.8); font-size: 12px; }}
        @media (max-width: 768px) {{
            .summary-card .value {{ font-size: 20px; }}
            th, td {{ padding: 6px 8px; font-size: 12px; }}
            .config-item .value {{ font-size: 14px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 {group_name} 账单导出</h1>
            <div class="subtitle">日期范围：{start_date} 至 {end_date} | 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>

        <div class="config-card">
            <div class="config-item">
                <div class="label">💰 手续费率</div>
                <div class="value">{fee_rate}%</div>
            </div>
            <div class="config-item">
                <div class="label">💱 汇率</div>
                <div class="value">{exchange_rate}</div>
            </div>
            <div class="config-item">
                <div class="label">📝 单笔费用</div>
                <div class="value">{per_transaction_fee} 元</div>
            </div>
        </div>

        <div class="summary">
            <div class="summary-card income">
                <div class="label">💰 总入款</div>
                <div class="value">{total_income_cny:.2f} 元</div>
                <div class="value" style="font-size: 18px;">≈ {total_income_usdt:.2f} USDT</div>
            </div>
            <div class="summary-card expense">
                <div class="label">📤 总下发</div>
                <div class="value">{total_expense_usdt:.2f} USDT</div>
            </div>
            <div class="summary-card pending">
                <div class="label">⏳ 待下发</div>
                <div class="value">{total_pending:.2f} USDT</div>
            </div>
        </div>
'''

    # 按日期显示详细账单
    for date, data in sorted(records_by_date.items()):
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        date_display = date_obj.strftime('%Y年%m月%d日')
        income_count = len(data['income'])
        expense_count = len(data['expense'])
        income_usdt = data['stats']['income_usdt']
        expense_usdt = data['stats']['expense_usdt']
        day_pending = income_usdt - expense_usdt

        html += f'''
        <div class="date-group">
            <div class="date-header" onclick="toggleSection(this)">
                <span>📅 {date_display} ({income_count}笔入款 / {expense_count}笔出款)</span>
                <span class="toggle-icon">▼</span>
            </div>
            <div class="date-content">
'''

        if data['income']:
            html += f'''
                <div class="section-title">📈 入款记录</div>
                <table>
                    <thead>
                        <tr><th>日期时间</th><th>金额(元)</th><th>手续费</th><th>汇率</th><th>单笔费用</th><th>USDT</th><th>分类</th><th>操作人</th></tr>
                    </thead>
                    <tbody>
'''
            for r in data['income']:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%m-%d %H:%M')
                fee_rate_r = r.get('fee_rate', 0)
                rate_r = r.get('rate', 0)
                per_fee = r.get('per_transaction_fee', 0)
                fee_display = f"{fmt(fee_rate_r)}%" if fee_rate_r == int(fee_rate_r) else f"{fee_rate_r}%"
                rate_display = fmt(rate_r) if rate_r == int(rate_r) else f"{rate_r:.2f}"
                per_fee_display = f"{fmt(per_fee)}元" if per_fee > 0 else "-"
                category = get_category_with_flag(r.get('category', '')) or '无'
                operator = r.get('display_name', '未知')

                html += f'''
                        <tr>
                            <td class="record-time">{time_str}</td>
                            <td class="record-amount income">+{fmt(r['amount'])}</td>
                            <td class="record-fee">{fee_display}</td>
                            <td>{rate_display}</td>
                            <td>{per_fee_display}</td>
                            <td>{fmt(r['amount_usdt'])}</td>
                            <td>{category}</td>
                            <td>{operator}</td>
                        </tr>
'''
            html += '''
                    </tbody>
                </table>
'''

        if data['expense']:
            html += f'''
                <div class="section-title expense">📉 出款记录</div>
                <table>
                    <thead>
                        <tr><th>日期时间</th><th>USDT</th><th>操作人</th></tr>
                    </thead>
                    <tbody>
'''
            for r in data['expense']:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%m-%d %H:%M')
                operator = r.get('display_name', '未知')
                html += f'''
                        <tr>
                            <td class="record-time">{time_str}</td>
                            <td class="record-amount" style="color:#ef4444;">-{fmt(r['amount_usdt'])}</td>
                            <td>{operator}</td>
                        </tr>
'''
            html += '''
                    </tbody>
                </table>
'''

        html += f'''
                <div class="subtotal">
                    本日小计：入款 {income_usdt:.2f} USDT / 出款 {expense_usdt:.2f} USDT
                    {f' / 结余 {day_pending:.2f} USDT' if day_pending != 0 else ''}
                </div>
            </div>
        </div>
'''

    # ========== 各操作人详细记录 ==========
    if operator_sections:
        html += f'''
        <h2 style="color:white; margin: 24px 0 16px 0; font-size: 20px;">👤 各操作人详细记录</h2>
        {operator_sections}
'''

    html += f'''
        <div class="footer">
            本账单由 Telegram 记账机器人自动生成 | 共 {len(records)} 条记录
        </div>
    </div>
    <script>
        function toggleSection(element) {{
            element.classList.toggle('collapsed');
            var content = element.nextElementSibling;
            if (content) {{
                content.classList.toggle('collapsed');
            }}
        }}
    </script>
</body>
</html>
'''
    return html

# ==================== AccountingManager 类 ====================

class AccountingManager:
    """记账管理器"""

    def __init__(self):
        """不再需要 db_path，所有连接通过 admin_id 获取"""
        pass

    @contextmanager
    def _get_conn(self, admin_id: int):
        """获取数据库连接，自动创建缺失的表"""
        conn = get_conn(admin_id)
        try:
            self._ensure_tables(conn)
            yield conn
            conn.commit()  # ✅ 加 commit
        except Exception:
            conn.rollback()  # ✅ 加 rollback
            raise

    def _ensure_tables(self, conn):
        """确保所有必需的记账表都存在"""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS group_accounting_config (
                group_id TEXT PRIMARY KEY,
                fee_rate REAL DEFAULT 0.0,
                exchange_rate REAL DEFAULT 1.0,
                per_transaction_fee REAL DEFAULT 0.0,
                session_id TEXT,
                session_start_time INTEGER DEFAULT 0,
                session_end_time INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                updated_at INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS accounting_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                record_type TEXT NOT NULL,
                amount REAL NOT NULL,
                amount_usdt REAL NOT NULL,
                description TEXT,
                category TEXT DEFAULT '',
                rate REAL DEFAULT 0,
                fee_rate REAL DEFAULT 0,
                per_transaction_fee REAL DEFAULT 0,
                message_id INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                date TEXT NOT NULL,
                admin_id INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS accounting_sessions (
                session_id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                date TEXT NOT NULL,
                fee_rate REAL DEFAULT 0.0,
                exchange_rate REAL DEFAULT 1.0,
                per_transaction_fee REAL DEFAULT 0.0
            );

            CREATE TABLE IF NOT EXISTS group_users (
                group_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen INTEGER NOT NULL,
                PRIMARY KEY (group_id, user_id)
            );
            
            -- ✅ 新增：用户个性化配置表
            CREATE TABLE IF NOT EXISTS user_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                fee_rate REAL,
                exchange_rate REAL,
                per_transaction_fee REAL,
                updated_at INTEGER DEFAULT 0,
                UNIQUE(group_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_records_group_id ON accounting_records(group_id);
            CREATE INDEX IF NOT EXISTS idx_records_session_id ON accounting_records(session_id);
            CREATE INDEX IF NOT EXISTS idx_records_date ON accounting_records(date);
            CREATE INDEX IF NOT EXISTS idx_records_group_date ON accounting_records(group_id, date);
            CREATE INDEX IF NOT EXISTS idx_records_created ON accounting_records(created_at);
            CREATE INDEX IF NOT EXISTS idx_users_group_id ON group_users(group_id);
        """)

    # ========== 会话和配置管理 ==========
    def get_or_create_session(self, group_id: str, admin_id: int) -> Dict:
        """获取或创建当前会话"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT fee_rate, exchange_rate, per_transaction_fee, session_id, session_start_time, is_active
                    FROM group_accounting_config 
                    WHERE group_id = ? AND is_active = 1
                """, (group_id,))
                row = c.fetchone()
                if row:
                    return {
                        'session_id': row[3],
                        'fee_rate': row[0],
                        'exchange_rate': row[1],
                        'per_transaction_fee': row[2] if row[2] is not None else 0.0,
                        'start_time': row[4],
                        'is_active': True
                    }
                now = int(time.time())
                session_id = f"{group_id}_{now}"
                c.execute("DELETE FROM group_accounting_config WHERE group_id = ?", (group_id,))
                c.execute("""
                    INSERT INTO group_accounting_config 
                    (group_id, fee_rate, exchange_rate, per_transaction_fee, session_id, session_start_time, is_active, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                """, (group_id, 0.0, 1.0, 0.0, session_id, now, now))
                conn.commit()
                return {
                    'session_id': session_id,
                    'fee_rate': 0.0,
                    'exchange_rate': 1.0,
                    'start_time': now,
                    'is_active': True
                }
        except Exception as e:
            logger.error(f"获取/创建会话失败: {e}")
            return {
                'session_id': f"{group_id}_{int(time.time())}",
                'fee_rate': 0.0,
                'exchange_rate': 1.0,
                'start_time': int(time.time()),
                'is_active': True
            }

    def end_session(self, group_id: str, admin_id: int) -> Optional[Dict]:
        """结束当前会话，并按自然日分割账单"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())
                c.execute("""
                    SELECT session_id, fee_rate, exchange_rate, per_transaction_fee, session_start_time
                    FROM group_accounting_config 
                    WHERE group_id = ? AND is_active = 1
                """, (group_id,))
                row = c.fetchone()
                if not row:
                    return None
                old_session_id, fee_rate, exchange_rate, per_transaction_fee, start_time = row
                c.execute("""
                    SELECT date, COUNT(*), 
                           SUM(CASE WHEN record_type = 'income' THEN amount_usdt ELSE 0 END) as income_usdt,
                           SUM(CASE WHEN record_type = 'expense' THEN amount_usdt ELSE 0 END) as expense_usdt
                    FROM accounting_records
                    WHERE group_id = ? AND session_id = ?
                    GROUP BY date
                    ORDER BY date
                """, (group_id, old_session_id))
                date_groups = c.fetchall()
                if not date_groups:
                    c.execute("""
                        UPDATE group_accounting_config 
                        SET is_active = 0, session_end_time = ?, updated_at = ?
                        WHERE group_id = ? AND is_active = 1
                    """, (now, now, group_id))
                    conn.commit()
                    return None
                for date_group in date_groups:
                    date_str = date_group[0]
                    income_usdt = date_group[2] or 0
                    expense_usdt = date_group[3] or 0
                    new_session_id = f"{old_session_id}_{date_str}"
                    c.execute("""
                        SELECT MIN(created_at), MAX(created_at)
                        FROM accounting_records
                        WHERE group_id = ? AND session_id = ? AND date = ?
                    """, (group_id, old_session_id, date_str))
                    time_range = c.fetchone()
                    day_start = time_range[0] or start_time
                    day_end = time_range[1] or now
                    c.execute("""
                        INSERT INTO accounting_sessions 
                        (session_id, group_id, start_time, end_time, date, fee_rate, exchange_rate, per_transaction_fee)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (new_session_id, group_id, day_start, day_end, date_str, fee_rate, exchange_rate, per_transaction_fee))
                c.execute("""
                    UPDATE group_accounting_config 
                    SET is_active = 0, session_end_time = ?, updated_at = ?
                    WHERE group_id = ? AND is_active = 1
                """, (now, now, group_id))
                conn.commit()
                total_income_usdt = sum(g[2] for g in date_groups)
                total_expense_usdt = sum(g[3] for g in date_groups)
                return {
                    'session_id': old_session_id,
                    'fee_rate': fee_rate,
                    'exchange_rate': exchange_rate,
                    'income_usdt': total_income_usdt,
                    'expense_usdt': total_expense_usdt
                }
        except Exception as e:
            logger.error(f"结束会话失败: {e}")
            return None

    def get_records_paginated(self, group_id: str, admin_id: int = None, 
                              page: int = 0, page_size: int = 10,
                              date: str = None, session_id: str = None) -> Tuple[List[Dict], int]:
        """分页获取记账记录，返回 (记录列表, 总记录数)"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                # ✅ 构建 where 条件，使用表别名 r.
                where = ["r.group_id = ?"]
                params = [group_id]
                if admin_id is not None and admin_id != 0:
                    where.append("r.admin_id = ?")
                    params.append(admin_id)
                if date:
                    where.append("r.date = ?")
                    params.append(date)
                if session_id:
                    where.append("r.session_id = ?")
                    params.append(session_id)

                where_clause = " AND ".join(where)

                # 查询总数
                count_sql = f"SELECT COUNT(*) FROM accounting_records r WHERE {where_clause}" 
                c.execute(count_sql, params)
                total = c.fetchone()[0]

                # 查询分页数据
                offset = page * page_size
                query = f"""
                    SELECT r.record_type, r.amount, r.amount_usdt, r.description, r.created_at,
                           r.username, r.category, r.user_id, u.first_name, r.rate, r.fee_rate, r.date, r.per_transaction_fee
                    FROM accounting_records r
                    LEFT JOIN group_users u ON r.group_id = u.group_id AND r.user_id = u.user_id
                    WHERE {where_clause}
                    ORDER BY r.created_at DESC
                    LIMIT ? OFFSET ?
                """
                c.execute(query, params + [page_size, offset])
                rows = c.fetchall()

                records = []
                for row in rows:
                    record = {
                        'type': row[0], 'amount': row[1], 'amount_usdt': row[2],
                        'description': row[3], 'created_at': row[4], 'username': row[5],
                        'category': row[6], 'user_id': row[7], 'rate': row[9] or 0,
                        'fee_rate': row[10] or 0, 'date': row[11] or '',
                        'per_transaction_fee': row[12] or 0,
                    }
                    first_name = row[8] if len(row) > 8 else None
                    record['display_name'] = first_name or row[5] or f"用户{row[7]}"
                    records.append(record)
                return records, total
        except Exception as e:
            logger.error(f"分页获取记录失败: {e}")
            return [], 0


    # ========== 记录查询 ==========
    def _get_records_by_condition(self, group_id: str, session_id: str = None,
                                    date: str = None, date_range: tuple = None,
                                    admin_id: int = None) -> List[Dict]:
        """通用记录查询方法（支持日期范围）"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT r.record_type, r.amount, r.amount_usdt, r.description, r.created_at, 
                           r.username, r.category, r.user_id, u.first_name, r.rate, r.fee_rate, r.date, r.per_transaction_fee
                    FROM accounting_records r
                    LEFT JOIN group_users u ON r.group_id = u.group_id AND r.user_id = u.user_id
                    WHERE r.group_id = ?
                """
                params = [group_id]
                if session_id:
                    query += " AND r.session_id = ?"
                    params.append(session_id)
                if date:
                    query += " AND r.date = ?"
                    params.append(date)
                if date_range:
                    start_date, end_date = date_range
                    query += " AND r.date >= ? AND r.date <= ?"
                    params.extend([start_date, end_date])
                if admin_id is not None and admin_id != 0:
                    query += " AND r.admin_id = ?"
                    params.append(admin_id)
                query += " ORDER BY r.date ASC, r.created_at ASC"
                c.execute(query, params)
                rows = c.fetchall()
                records = []
                for row in rows:
                    record = {
                        'type': row[0],
                        'amount': row[1],
                        'amount_usdt': row[2],
                        'description': row[3],
                        'created_at': row[4],
                        'username': row[5],
                        'category': row[6],
                        'user_id': row[7],
                        'rate': row[9] if len(row) > 9 else 0,
                        'fee_rate': row[10] if len(row) > 10 else 0,
                        'date': row[11] if len(row) > 11 else '',
                        'per_transaction_fee': row[12] if len(row) > 12 else 0,
                    }
                    first_name = row[8] if len(row) > 8 else None
                    if first_name:
                        record['display_name'] = first_name
                    elif row[5]:
                        record['display_name'] = row[5]
                    else:
                        record['display_name'] = f"用户{row[7]}"
                    records.append(record)
                return records
        except Exception as e:
            logger.error(f"获取记录失败: {e}")
            return []

    def get_current_records(self, group_id: str, admin_id: int = None) -> List[Dict]:
        session = self.get_or_create_session(group_id, admin_id)
        return self._get_records_by_condition(group_id, session_id=session['session_id'], admin_id=admin_id)

    def get_records_by_date_range(self, group_id: str, start_date: str, end_date: str, admin_id: int = None) -> List[Dict]:
        return self._get_records_by_condition(group_id, date_range=(start_date, end_date), admin_id=admin_id)

    def get_today_records(self, group_id: str, admin_id: int = None) -> List[Dict]:
        today = get_today_beijing()
        return self._get_records_by_condition(group_id, date=today, admin_id=admin_id)

    def get_total_records(self, group_id: str, admin_id: int = None) -> List[Dict]:
        return self._get_records_by_condition(group_id, admin_id=admin_id)

    def get_records_by_date(self, group_id: str, date_str: str, admin_id: int = None) -> List[Dict]:
        return self._get_records_by_condition(group_id, date=date_str, admin_id=admin_id)

    def get_available_dates(self, group_id: str, admin_id: int) -> List[str]:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT DISTINCT date FROM accounting_records
                    WHERE group_id = ?
                    ORDER BY date ASC
                """, (group_id,))
                rows = c.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"获取可用日期失败: {e}")
            return []

    # ========== 统计信息 ==========
    def get_current_stats(self, group_id: str, admin_id: int = None) -> Dict:
        try:
            session = self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT record_type, SUM(amount), SUM(amount_usdt), COUNT(*)
                    FROM accounting_records
                    WHERE group_id = ? AND session_id = ?
                """
                params = [group_id, session['session_id']]
                if admin_id is not None and admin_id != 0:
                    query += " AND admin_id = ?"
                    params.append(admin_id)
                query += " GROUP BY record_type"
                c.execute(query, params)
                rows = c.fetchall()
            income_total = 0; income_usdt = 0; income_count = 0
            expense_total = 0; expense_usdt = 0; expense_count = 0
            for row in rows:
                if row[0] == 'income':
                    income_total = row[1] or 0
                    income_usdt = row[2] or 0
                    income_count = row[3] or 0
                else:
                    expense_total = row[1] or 0
                    expense_usdt = row[2] or 0
                    expense_count = row[3] or 0
            return {
                'fee_rate': session['fee_rate'],
                'exchange_rate': session['exchange_rate'],
                'per_transaction_fee': session.get('per_transaction_fee', 0),
                'income_total': income_total,
                'income_usdt': income_usdt,
                'income_count': income_count,
                'expense_total': expense_total,
                'expense_usdt': expense_usdt,
                'expense_count': expense_count,
                'pending_usdt': income_usdt - expense_usdt
            }
        except Exception as e:
            logger.error(f"获取当前统计失败: {e}")
            return {
                'fee_rate': 0, 'exchange_rate': 1, 'income_total': 0,
                'income_usdt': 0, 'income_count': 0, 'expense_total': 0,
                'expense_usdt': 0, 'expense_count': 0, 'pending_usdt': 0
            }

    def get_today_stats(self, group_id: str, admin_id: int = None) -> Dict:
        today = get_today_beijing()
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT record_type, SUM(amount), SUM(amount_usdt), COUNT(*)
                    FROM accounting_records
                    WHERE group_id = ? AND date = ?
                """
                params = [group_id, today]
                if admin_id is not None and admin_id != 0:
                    query += " AND admin_id = ?"
                    params.append(admin_id)
                query += " GROUP BY record_type"
                c.execute(query, params)
                rows = c.fetchall()
            income_total = 0; income_usdt = 0; income_count = 0
            expense_total = 0; expense_usdt = 0; expense_count = 0
            for row in rows:
                if row[0] == 'income':
                    income_total = row[1] or 0
                    income_usdt = row[2] or 0
                    income_count = row[3] or 0
                else:
                    expense_total = row[1] or 0
                    expense_usdt = row[2] or 0
                    expense_count = row[3] or 0
            session = self.get_or_create_session(group_id, admin_id)
            return {
                'fee_rate': session['fee_rate'],
                'exchange_rate': session['exchange_rate'],
                'per_transaction_fee': session.get('per_transaction_fee', 0),
                'income_total': income_total,
                'income_usdt': income_usdt,
                'income_count': income_count,
                'expense_total': expense_total,
                'expense_usdt': expense_usdt,
                'expense_count': expense_count,
                'pending_usdt': income_usdt - expense_usdt
            }
        except Exception as e:
            logger.error(f"获取今日统计失败: {e}")
            return self.get_current_stats(group_id, admin_id)

    def get_total_stats(self, group_id: str, admin_id: int = None) -> Dict:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT record_type, SUM(amount), SUM(amount_usdt), COUNT(*)
                    FROM accounting_records
                    WHERE group_id = ?
                """
                params = [group_id]
                if admin_id is not None and admin_id != 0:
                    query += " AND admin_id = ?"
                    params.append(admin_id)
                query += " GROUP BY record_type"
                c.execute(query, params)
                rows = c.fetchall()
            income_total = 0; income_usdt = 0; income_count = 0
            expense_total = 0; expense_usdt = 0; expense_count = 0
            for row in rows:
                if row[0] == 'income':
                    income_total = row[1] or 0
                    income_usdt = row[2] or 0
                    income_count = row[3] or 0
                else:
                    expense_total = row[1] or 0
                    expense_usdt = row[2] or 0
                    expense_count = row[3] or 0
            session = self.get_or_create_session(group_id, admin_id)
            return {
                'fee_rate': session['fee_rate'],
                'exchange_rate': session['exchange_rate'],
                'per_transaction_fee': session.get('per_transaction_fee', 0),
                'income_total': income_total,
                'income_usdt': income_usdt,
                'income_count': income_count,
                'expense_total': expense_total,
                'expense_usdt': expense_usdt,
                'expense_count': expense_count,
                'pending_usdt': income_usdt - expense_usdt
            }
        except Exception as e:
            logger.error(f"获取总计统计失败: {e}")
            return self.get_current_stats(group_id, admin_id)

    def get_stats_by_date(self, group_id: str, date_str: str, admin_id: int = None) -> Dict:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT record_type, SUM(amount), SUM(amount_usdt), COUNT(*)
                    FROM accounting_records
                    WHERE group_id = ? AND date = ?
                """
                params = [group_id, date_str]
                if admin_id is not None and admin_id != 0:
                    query += " AND admin_id = ?"
                    params.append(admin_id)
                query += " GROUP BY record_type"
                c.execute(query, params)
                rows = c.fetchall()
            income_total = 0; income_usdt = 0; income_count = 0
            expense_total = 0; expense_usdt = 0; expense_count = 0
            for row in rows:
                if row[0] == 'income':
                    income_total = row[1] or 0
                    income_usdt = row[2] or 0
                    income_count = row[3] or 0
                else:
                    expense_total = row[1] or 0
                    expense_usdt = row[2] or 0
                    expense_count = row[3] or 0
            fee_rate = 0; exchange_rate = 1; per_transaction_fee = 0
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT rate, fee_rate FROM accounting_records
                    WHERE group_id = ? AND date = ? AND record_type = 'income'
                    LIMIT 1
                """, (group_id, date_str))
                row = c.fetchone()
                if row:
                    exchange_rate = row[0] if row[0] else 1
                    fee_rate = row[1] if row[1] else 0
                c.execute("""
                    SELECT per_transaction_fee FROM accounting_sessions
                    WHERE group_id = ? AND date = ?
                    LIMIT 1
                """, (group_id, date_str))
                row = c.fetchone()
                if row:
                    per_transaction_fee = row[0] if row[0] else 0
            return {
                'income_total': income_total,
                'income_usdt': income_usdt,
                'income_count': income_count,
                'expense_total': expense_total,
                'expense_usdt': expense_usdt,
                'expense_count': expense_count,
                'pending_usdt': income_usdt - expense_usdt,
                'fee_rate': fee_rate,
                'exchange_rate': exchange_rate,
                'per_transaction_fee': per_transaction_fee
            }
        except Exception as e:
            logger.error(f"获取日期统计失败: {e}")
            return {
                'income_total': 0,
                'income_usdt': 0,
                'income_count': 0,
                'expense_total': 0,
                'expense_usdt': 0,
                'expense_count': 0,
                'pending_usdt': 0,
                'fee_rate': 0,
                'exchange_rate': 1,
                'per_transaction_fee': 0
            }

    def get_total_pending_stats(self, group_id: str, admin_id: int = None) -> Dict:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                query = """
                    SELECT 
                        SUM(CASE WHEN record_type = 'income' THEN amount_usdt ELSE 0 END) as total_income,
                        SUM(CASE WHEN record_type = 'expense' THEN amount_usdt ELSE 0 END) as total_expense
                    FROM accounting_records
                    WHERE group_id = ?
                """
                params = [group_id]
                if admin_id is not None and admin_id != 0:
                    query += " AND admin_id = ?"
                    params.append(admin_id)
                c.execute(query, params)
                row = c.fetchone()
                total_income_usdt = row[0] or 0
                total_expense_usdt = row[1] or 0
                pending_usdt = total_income_usdt - total_expense_usdt
                return {
                    'income_usdt': total_income_usdt,
                    'expense_usdt': total_expense_usdt,
                    'pending_usdt': pending_usdt if pending_usdt > 0 else 0
                }
        except Exception as e:
            logger.error(f"获取总待下发统计失败: {e}")
            return {'income_usdt': 0, 'expense_usdt': 0, 'pending_usdt': 0}

    # ========== 添加/修改记录 ==========
    def add_record(self, group_id: str, user_id: int, username: str,
                   record_type: str, amount: float, description: str = "",
                   category: str = "", temp_rate: float = None,
                   message_id: int = 0, temp_fee: float = None,
                   admin_id: int = 0) -> Tuple[bool, int]:
        try:
            session = self.get_or_create_session(group_id, admin_id)
            if record_type == 'income':
                actual_rate = temp_rate if temp_rate is not None else session['exchange_rate']
                actual_fee_rate = temp_fee if temp_fee is not None else session['fee_rate']
                after_fee = amount * (1 - actual_fee_rate / 100)
                per_fee = session.get('per_transaction_fee', 0)
                with_fee = after_fee + per_fee
                amount_usdt = with_fee / actual_rate
                current_fee_rate = actual_fee_rate
                current_per_fee = per_fee
            else:
                amount_usdt = amount
                actual_rate = 0
                current_fee_rate = 0
                current_per_fee = 0
            now = int(time.time())
            date_str = beijing_time(now).strftime('%Y-%m-%d')
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO accounting_records
                    (group_id, session_id, user_id, username, record_type, amount, amount_usdt,
                     description, category, rate, fee_rate, per_transaction_fee, message_id, created_at, date, admin_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (group_id, session['session_id'], user_id, username, record_type, amount,
                      amount_usdt, description, category, actual_rate, current_fee_rate,
                      current_per_fee, message_id, now, date_str, admin_id))
                record_id = c.lastrowid
                conn.commit()
            return True, record_id
        except Exception as e:
            logger.error(f"添加记录失败: {e}")
            return False, 0

    def set_fee_rate(self, group_id: str, rate: float, admin_id: int) -> bool:
        try:
            self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())
                c.execute("""
                    UPDATE group_accounting_config 
                    SET fee_rate = ?, updated_at = ?
                    WHERE group_id = ? AND is_active = 1
                """, (rate, now, group_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"设置费率失败: {e}")
            return False

    def set_exchange_rate(self, group_id: str, rate: float, admin_id: int) -> bool:
        try:
            self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())
                c.execute("""
                    UPDATE group_accounting_config 
                    SET exchange_rate = ?, updated_at = ?
                    WHERE group_id = ? AND is_active = 1
                """, (rate, now, group_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"设置汇率失败: {e}")
            return False

    def set_per_transaction_fee(self, group_id: str, fee: float, admin_id: int) -> bool:
        try:
            self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())
                c.execute("""
                    UPDATE group_accounting_config 
                    SET per_transaction_fee = ?, updated_at = ?
                    WHERE group_id = ? AND is_active = 1
                """, (fee, now, group_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"设置单笔费用失败: {e}")
            return False

    # ========== 用户个性化配置管理 ==========

    def set_user_config(self, group_id: str, user_id: int, fee_rate: float = None, 
                        exchange_rate: float = None, per_transaction_fee: float = None, 
                        admin_id: int = 0) -> bool:
        """设置用户在指定群组的个性化配置"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())

                # 先检查是否已有配置
                c.execute(
                    "SELECT id FROM user_config WHERE group_id = ? AND user_id = ?",
                    (group_id, user_id)
                )
                existing = c.fetchone()

                if existing:
                    # 更新现有配置
                    updates = []
                    params = []
                    if fee_rate is not None:
                        updates.append("fee_rate = ?")
                        params.append(fee_rate)
                    if exchange_rate is not None:
                        updates.append("exchange_rate = ?")
                        params.append(exchange_rate)
                    if per_transaction_fee is not None:
                        updates.append("per_transaction_fee = ?")
                        params.append(per_transaction_fee)

                    if updates:
                        updates.append("updated_at = ?")
                        params.append(now)
                        params.extend([group_id, user_id])
                        c.execute(
                            f"UPDATE user_config SET {', '.join(updates)} WHERE group_id = ? AND user_id = ?",
                            params
                        )
                else:
                    # 插入新配置
                    c.execute("""
                        INSERT INTO user_config (group_id, user_id, fee_rate, exchange_rate, per_transaction_fee, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (group_id, user_id, fee_rate, exchange_rate, per_transaction_fee, now))

                conn.commit()
            return True
        except Exception as e:
            logger.error(f"设置用户配置失败: {e}")
            return False

    def get_user_config(self, group_id: str, user_id: int, admin_id: int = 0) -> dict:
        """获取用户在指定群组的个性化配置"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT fee_rate, exchange_rate, per_transaction_fee FROM user_config WHERE group_id = ? AND user_id = ?",
                    (group_id, user_id)
                )
                row = c.fetchone()
                if row:
                    return {
                        "fee_rate": row[0],
                        "exchange_rate": row[1],
                        "per_transaction_fee": row[2],
                        "has_config": True
                    }
                return {"has_config": False}
        except Exception as e:
            logger.error(f"获取用户配置失败: {e}")
            return {"has_config": False}

    def delete_user_config(self, group_id: str, user_id: int, admin_id: int = 0) -> bool:
        """删除用户在指定群组的个性化配置"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute(
                    "DELETE FROM user_config WHERE group_id = ? AND user_id = ?",
                    (group_id, user_id)
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"删除用户配置失败: {e}")
            return False

    def get_all_user_configs(self, group_id: str, admin_id: int = 0) -> list:
        """获取群组中所有用户的个性化配置"""
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT uc.user_id, uc.fee_rate, uc.exchange_rate, uc.per_transaction_fee, 
                           gu.first_name, gu.username
                    FROM user_config uc
                    LEFT JOIN group_users gu ON uc.group_id = gu.group_id AND uc.user_id = gu.user_id
                    WHERE uc.group_id = ?
                    ORDER BY uc.updated_at DESC
                """, (group_id,))
                rows = c.fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"获取所有用户配置失败: {e}")
            return []

    def get_effective_config(self, group_id: str, user_id: int, admin_id: int = 0) -> dict:
        """
        获取用户的有效配置（优先使用个性化配置，否则使用群组默认配置）
        """
        session = self.get_or_create_session(group_id, admin_id)

        # 先获取用户个性化配置
        user_config = self.get_user_config(group_id, user_id, admin_id)

        if user_config.get("has_config"):
            return {
                "fee_rate": user_config["fee_rate"] if user_config["fee_rate"] is not None else session["fee_rate"],
                "exchange_rate": user_config["exchange_rate"] if user_config["exchange_rate"] is not None else session["exchange_rate"],
                "per_transaction_fee": user_config["per_transaction_fee"] if user_config["per_transaction_fee"] is not None else session.get("per_transaction_fee", 0),
                "is_personalized": True
            }

        return {
            "fee_rate": session["fee_rate"],
            "exchange_rate": session["exchange_rate"],
            "per_transaction_fee": session.get("per_transaction_fee", 0),
            "is_personalized": False
        }

    # ========== 清理和撤销 ==========
    def clear_current_session(self, group_id: str, admin_id: int) -> bool:
        try:
            session = self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("DELETE FROM accounting_records WHERE group_id = ? AND session_id = ?", 
                          (group_id, session['session_id']))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"清空记录失败: {e}")
            return False

    def clear_all_records(self, group_id: str, admin_id: int) -> bool:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("DELETE FROM accounting_records WHERE group_id = ?", (group_id,))
                c.execute("DELETE FROM accounting_sessions WHERE group_id = ?", (group_id,))
                c.execute("""
                    UPDATE group_accounting_config 
                    SET fee_rate = 0, exchange_rate = 1, per_transaction_fee = 0, updated_at = ?
                    WHERE group_id = ?
                """, (int(time.time()), group_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"清空所有记录失败: {e}")
            return False

    def remove_last_record(self, group_id: str, admin_id: int) -> Tuple[bool, Dict]:
        try:
            session = self.get_or_create_session(group_id, admin_id)
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT id, record_type, amount, amount_usdt, description, category, rate, created_at, username, user_id
                    FROM accounting_records
                    WHERE group_id = ? AND session_id = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                """, (group_id, session['session_id']))
                row = c.fetchone()
                if not row:
                    return False, {"error": "没有可以移除的记录"}
                removed_record = {
                    'id': row[0],
                    'type': row[1],
                    'amount': row[2],
                    'amount_usdt': row[3],
                    'description': row[4],
                    'category': row[5],
                    'rate': row[6],
                    'created_at': row[7],
                    'username': row[8],
                    'user_id': row[9]
                }
                c.execute("DELETE FROM accounting_records WHERE id = ?", (row[0],))
                conn.commit()
                return True, removed_record
        except Exception as e:
            logger.error(f"移除最后记录失败: {e}")
            return False, {"error": str(e)}

    def get_record_by_message_id(self, group_id: str, message_id: int, admin_id: int) -> Optional[Dict]:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT id, record_type, amount, amount_usdt, description, category, rate, fee_rate, per_transaction_fee, created_at, username, user_id
                    FROM accounting_records
                    WHERE group_id = ? AND message_id = ?
                """, (group_id, message_id))
                row = c.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'type': row[1],
                        'amount': row[2],
                        'amount_usdt': row[3],
                        'description': row[4],
                        'category': row[5],
                        'rate': row[6],
                        'fee_rate': row[7],
                        'per_transaction_fee': row[8],
                        'created_at': row[9],
                        'username': row[10],
                        'user_id': row[11]
                    }
                return None
        except Exception as e:
            logger.error(f"根据消息ID获取记录失败: {e}")
            return None

    def delete_record_by_id(self, record_id: int, admin_id: int) -> bool:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("DELETE FROM accounting_records WHERE id = ?", (record_id,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"删除记录失败: {e}")
            return False

    # ========== 用户信息追踪 ==========
    def update_user_info(self, group_id: str, user_id: int, username: str, 
             first_name: str, last_name: str = "", admin_id: int = 0) -> Tuple[bool, str, str, str]:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                now = int(time.time())
                c.execute("""
                    SELECT username, first_name, last_name
                    FROM group_users
                    WHERE group_id = ? AND user_id = ?
                """, (group_id, user_id))
                old = c.fetchone()
                old_display_name = None
                change_type = None
                if old:
                    old_username = old[0] or ""
                    old_first_name = old[1] or ""
                    if old_username:
                        old_display_name = f"{old_first_name} (@{old_username})"
                    else:
                        old_display_name = old_first_name
                if username:
                    new_display_name = f"{first_name} (@{username})"
                else:
                    new_display_name = first_name
                c.execute("""
                    INSERT OR REPLACE INTO group_users 
                    (group_id, user_id, username, first_name, last_name, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (group_id, user_id, username or "", first_name or "", last_name or "", now))
                conn.commit()
                if old:
                    old_username = old[0] or ""
                    old_first_name = old[1] or ""
                    if old_username != (username or "") and old_first_name != (first_name or ""):
                        change_type = "昵称和用户名"
                    elif old_username != (username or ""):
                        change_type = "用户名"
                    elif old_first_name != (first_name or ""):
                        change_type = "昵称"
                if change_type:
                    return True, old_display_name or first_name, new_display_name, change_type
                return False, "", "", ""
        except Exception as e:
            logger.error(f"更新用户信息失败: {e}")
            return False, "", "", ""

    # ========== 地址查询记录 ==========
    def record_address_query(self, group_id: str, address: str, chain_type: str, 
                              user_id: int, username: str, balance: float, admin_id: int):
        try:
            now = int(time.time())
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO address_queries (group_id, address, chain_type, query_time, user_id, username, balance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(group_id, address) DO UPDATE SET
                        query_time = excluded.query_time,
                        user_id = excluded.user_id,
                        username = excluded.username,
                        balance = excluded.balance
                """, (group_id, address, chain_type, now, user_id, username, balance))
                c.execute("""
                    INSERT INTO address_query_log (address, query_time, balance)
                    VALUES (?, ?, ?)
                """, (address, now, balance))
                conn.commit()
        except Exception as e:
            logger.error(f"记录地址查询失败: {e}")

    def get_address_stats(self, address: str, admin_id: int) -> dict:
        try:
            with self._get_conn(admin_id) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM address_query_log WHERE address = ?", (address,))
                total_queries = c.fetchone()[0]
                c.execute("SELECT MIN(query_time) FROM address_query_log WHERE address = ?", (address,))
                first_query = c.fetchone()[0]
                c.execute("SELECT MAX(query_time) FROM address_query_log WHERE address = ?", (address,))
                last_query = c.fetchone()[0]
                return {
                    'total_queries': total_queries,
                    'first_query': first_query,
                    'last_query': last_query
                }
        except Exception as e:
            logger.error(f"获取地址统计失败: {e}")
            return {'total_queries': 0, 'first_query': None, 'last_query': None}


# 全局变量：缓存每个管理员独立的 AccountingManager 实例
_accounting_managers = {}

def get_accounting_manager(admin_id: int) -> AccountingManager:
    if admin_id not in _accounting_managers:
        _accounting_managers[admin_id] = AccountingManager()
    return _accounting_managers[admin_id]


# ==================== USDT 地址查询函数 ====================

async def query_trc20_balance(address: str) -> dict:
    """查询 TRC20 USDT 余额"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{TRONGRID_API}/v1/accounts/{address}"
            async with session.get(url) as resp:
                data = await resp.json()
            if data.get('data') and len(data['data']) > 0:
                account = data['data'][0]
                trc20_tokens = account.get('trc20', [])
                for token in trc20_tokens:
                    if USDT_CONTRACTS['TRC20'] in token:
                        balance = int(token[USDT_CONTRACTS['TRC20']]) / 10**6
                        return {'balance': balance, 'success': True, 'chain': 'TRC20'}
            return {'balance': 0, 'success': True, 'chain': 'TRC20'}
    except Exception as e:
        logger.error(f"TRC20 查询失败: {e}")
        return {'balance': None, 'success': False, 'error': str(e)}

async def query_erc20_balance(address: str) -> dict:
    """查询 ERC20 USDT 余额"""
    try:
        params = {
            'module': 'account',
            'action': 'tokenbalance',
            'contractaddress': USDT_CONTRACTS['ERC20'],
            'address': address,
            'tag': 'latest',
            'apikey': ETHERSCAN_API_KEY
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(ETHERSCAN_API, params=params) as resp:
                data = await resp.json()
            if data.get('status') == '1':
                balance = int(data.get('result', 0)) / 10**6
                return {'balance': balance, 'success': True, 'chain': 'ERC20'}
            return {'balance': 0, 'success': True, 'chain': 'ERC20'}
    except Exception as e:
        logger.error(f"ERC20 查询失败: {e}")
        return {'balance': None, 'success': False, 'error': str(e)}

def is_valid_address(text: str) -> tuple:
    """检测文本中的 USDT 地址，返回 (是否匹配, 地址, 链类型)"""
    trc20_pattern = r'T[0-9A-Za-z]{33}'
    erc20_pattern = r'0x[0-9a-fA-F]{40}'
    trc20_match = re.search(trc20_pattern, text)
    if trc20_match:
        return True, trc20_match.group(), 'TRC20'
    erc20_match = re.search(erc20_pattern, text)
    if erc20_match:
        return True, erc20_match.group(), 'ERC20'
    return False, None, None

# ==================== 格式化账单消息 ====================

def _format_record_line(record: Dict) -> str:
    """格式化单条记录"""
    dt = beijing_time(record['created_at'])
    time_str = dt.strftime('%H:%M')
    amount = record['amount']
    amount_usdt = record['amount_usdt']
    rate = record.get('rate', 0)
    record_type = record.get('type', '')
    rate_info = ""
    if record_type == 'income' and rate > 0:
        if rate == int(rate):
            rate_info = f" /{int(rate)}"
        else:
            rate_info = f" /{rate:.2f}"
    display_name = record.get('display_name', '未知用户')
    user_id = record.get('user_id')
    if user_id:
        mention = f" [{display_name}](tg://user?id={user_id})"
    else:
        mention = f" {display_name}"
    if amount < 0:
        return f"`{time_str} {amount:.2f}{rate_info} = {amount_usdt:.2f} USDT`{mention}"
    else:
        return f"`{time_str} +{amount:.2f}{rate_info} = {amount_usdt:.2f} USDT`{mention}"

def format_bill_message(stats: Dict, records: List[Dict], title: str = "当前账单") -> str:
    """格式化账单消息"""
    message = f"📊 **{title}**\n\n"
    income_records = [r for r in records if r['type'] == 'income']
    expense_records = [r for r in records if r['type'] == 'expense']

    if income_records:
        income_records_sorted = sorted(income_records, key=lambda x: x['created_at'], reverse=True)
        categories = {}
        no_category_records = []
        for r in income_records_sorted:
            category = r.get('category', '') or ''
            if category:
                if category not in categories:
                    categories[category] = []
                categories[category].append(r)
            else:
                no_category_records.append(r)
        total_income_count = len(income_records)
        message += f"📈 **入款 {total_income_count} 笔**\n"
        if no_category_records:
            for r in no_category_records[:MAX_DISPLAY_RECORDS]:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%H:%M')
                amount = r['amount']
                amount_usdt = r['amount_usdt']
                fee_rate = r.get('fee_rate', 0)
                rate = r.get('rate', 0)
                fee_info = format_fee_info(fee_rate, rate)
                operator = r.get('display_name', '未知用户')
                safe_operator = safe_escape_markdown(operator)
                user_id = r.get('user_id')
                if user_id:
                    mention = f" [{safe_operator}](tg://user?id={user_id})"
                else:
                    mention = f" {safe_operator}"
                amount_str = f"{amount:+.2f}"
                message += f"  {time_str} {amount_str} {fee_info} = {amount_usdt:.2f} USDT{mention}\n"
            if len(no_category_records) > MAX_DISPLAY_RECORDS:
                message += f"  `... 还有 {len(no_category_records) - MAX_DISPLAY_RECORDS} 条记录`\n"
        for category, group_records in categories.items():
            group_sorted = sorted(group_records, key=lambda x: x['created_at'], reverse=True)
            display_category = get_category_with_flag(category)
            message += f"\n{display_category} ({len(group_records)} 笔)\n"
            for r in group_sorted[:MAX_DISPLAY_RECORDS]:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%H:%M')
                amount = r['amount']
                amount_usdt = r['amount_usdt']
                fee_rate = r.get('fee_rate', 0)
                rate = r.get('rate', 0)
                fee_info = format_fee_info(fee_rate, rate)
                operator = r.get('display_name', '未知用户')
                safe_operator = safe_escape_markdown(operator)
                user_id = r.get('user_id')
                if user_id:
                    mention = f" [{safe_operator}](tg://user?id={user_id})"
                else:
                    mention = f" {safe_operator}"
                amount_str = f"{amount:+.2f}"
                message += f"  {time_str} {amount_str} {fee_info} = {amount_usdt:.2f} USDT{mention}\n"
            group_total_cny = sum(r['amount'] for r in group_records)
            group_total_usdt = sum(r['amount_usdt'] for r in group_records)
            message += f"  小计：{group_total_cny:.2f} = {group_total_usdt:.2f} USDT\n"
            if len(group_records) > MAX_DISPLAY_RECORDS:
                message += f"  `... 还有 {len(group_records) - MAX_DISPLAY_RECORDS} 条记录`\n"
        message += "\n"
    else:
        message += "📈 **入款 0 笔**\n\n"

    if expense_records:
        expense_records_sorted = sorted(expense_records, key=lambda x: x['created_at'], reverse=True)
        display_expense = expense_records_sorted[:MAX_DISPLAY_RECORDS]
        total_expense_count = len(expense_records)
        message += f"📉 **出款 {total_expense_count} 笔**\n"
        if total_expense_count > MAX_DISPLAY_RECORDS:
            message += f" (显示最新{MAX_DISPLAY_RECORDS}条)\n"
        for r in display_expense:
            dt = beijing_time(r['created_at'])
            time_str = dt.strftime('%H:%M')
            amount = r['amount']
            amount_usdt = r['amount_usdt']
            operator = r.get('display_name', '未知用户')
            safe_operator = safe_escape_markdown(operator)
            user_id = r.get('user_id')
            if user_id:
                mention = f" [{safe_operator}](tg://user?id={user_id})"
            else:
                mention = f" {safe_operator}"
            amount_str = f"{amount:+.2f}"
            message += f"  {time_str} {amount_str} = {amount_usdt:.2f} USDT{mention}\n"
        if total_expense_count > MAX_DISPLAY_RECORDS:
            message += f"  `... 还有 {total_expense_count - MAX_DISPLAY_RECORDS} 条记录`\n"
        message += "\n"
    else:
        message += "📉 **出款 0 笔**\n\n"

    if income_records:
        categories = {}
        for r in income_records:
            category = r.get('category', '') or ''
            if category:
                if category not in categories:
                    categories[category] = {'cny': 0, 'usdt': 0, 'count': 0}
                categories[category]['cny'] += r['amount']
                categories[category]['usdt'] += r['amount_usdt']
                categories[category]['count'] += 1
        if categories:
            message += f"📊 **入款分组统计**\n"
            for category, data in categories.items():
                display_category = get_category_with_flag(category)
                message += f"{display_category}：{data['cny']:.2f} = {data['usdt']:.2f} USDT ({data['count']}笔)\n"
            message += "\n"

    fee_rate = stats['fee_rate']
    exchange_rate = stats['exchange_rate']
    per_transaction_fee = stats.get('per_transaction_fee', 0)
    total_income_cny = stats['income_total']
    total_income_usdt = stats['income_usdt']
    total_expense_usdt = stats['expense_usdt']
    pending_usdt = total_income_usdt - total_expense_usdt

    message += f"💰 **费率**：{fee_rate}%\n"
    message += f"💱 **汇率**：{exchange_rate}\n"
    message += f"📝 **单笔费用**：{per_transaction_fee} 元\n\n"
    message += f"📊 **总入款**：{total_income_cny:.2f} = {total_income_usdt:.2f} USDT\n"
    message += f"📤 **已下发**：{total_expense_usdt:.2f} USDT\n"
    if title == "总计账单":
        message += f"📋 **总待出款**：{pending_usdt:.2f} USDT"
    else:
        message += f"⏳ **待下发**：{pending_usdt:.2f} USDT"

    return message

# ==================== 辅助函数 ====================

def _is_authorized_in_group(update: Update, full_access: bool = False) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or chat.type not in ['group', 'supergroup']:
        return False
    return is_authorized(user.id, require_full_access=full_access)

# ==================== 指令处理函数 ====================

async def handle_end_bill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    user_id = user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    stats = am.get_current_stats(group_id, admin_id=admin_id)
    records = am.get_current_records(group_id, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await update.message.reply_text("📭 当前没有账单记录，无需结束")
        return
    final_bill = format_bill_message(stats, records, "结束账单")
    result = am.end_session(group_id, admin_id)
    if result:
        await update.message.reply_text(
            f"✅ **账单已结束并保存！**\n\n{final_bill}\n\n"
            f"💡 提示：费率已重置为0%，汇率已重置为1 = 1 USDT\n"
            f"可使用「设置手续费」和「设置汇率」重新配置",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ 结束账单失败")

async def handle_set_fee(update: Update, context: ContextTypes.DEFAULT_TYPE, rate: float):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    group_id = str(update.effective_chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    if am.set_fee_rate(group_id, rate, admin_id):
        await update.message.reply_text(f"✅ 手续费率已设置为 {rate}%")
    else:
        await update.message.reply_text("❌ 设置失败，请稍后重试")

async def handle_set_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE, rate: float):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    group_id = str(update.effective_chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    if am.set_exchange_rate(group_id, rate, admin_id):
        await update.message.reply_text(f"✅ 汇率已设置为 {rate}")
    else:
        await update.message.reply_text("❌ 设置失败，请稍后重试")

async def handle_set_per_transaction_fee(update: Update, context: ContextTypes.DEFAULT_TYPE, fee: float):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    group_id = str(update.effective_chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    if am.set_per_transaction_fee(group_id, fee, admin_id):
        await update.message.reply_text(f"✅ 单笔费用已设置为 {fee} 元")
    else:
        await update.message.reply_text("❌ 设置失败，请稍后重试")

async def handle_add_income(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                            amount: float, is_correction: bool = False,
                            category: str = "", temp_rate: float = None,
                            temp_fee: float = None, admin_id: int = 0):
    if not is_authorized(update.effective_user.id, require_full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    group_id = str(update.effective_chat.id)
    user = update.effective_user
    username = user.username or user.first_name or str(user.id)
    record_amount = -abs(amount) if is_correction else abs(amount)
    desc = "修正入款" if is_correction else "入款"
    message_id = update.message.message_id
    cur_admin_id = get_user_admin_id(user.id)
    am = get_accounting_manager(cur_admin_id)

    # ✅ 获取有效配置（优先使用个性化配置）
    effective_config = am.get_effective_config(group_id, user.id, cur_admin_id)

    # 如果用户有临时设置的参数，优先使用临时参数
    actual_fee_rate = temp_fee if temp_fee is not None else effective_config["fee_rate"]
    actual_exchange_rate = temp_rate if temp_rate is not None else effective_config["exchange_rate"]
    actual_per_fee = effective_config["per_transaction_fee"]

    # 使用有效配置计算
    session = am.get_or_create_session(group_id, cur_admin_id)
    if session['exchange_rate'] > 0:
        after_fee = amount * (1 - actual_fee_rate / 100)
        with_fee = after_fee + actual_per_fee
        amount_usdt = with_fee / actual_exchange_rate
    else:
        amount_usdt = amount
        
    success, _ = am.add_record(
        group_id, user.id, username, 'income', record_amount, desc,
        category, actual_exchange_rate, message_id, actual_fee_rate, cur_admin_id
    )
    
    if success:
        stats = am.get_current_stats(group_id, admin_id=cur_admin_id)
        records = am.get_current_records(group_id, admin_id=cur_admin_id)
        message_text = format_bill_message(stats, records, "当前账单")
        
        # ✅ 构建反馈信息
        prefix = f"✅ 已记录修正入款：-{abs(amount):.2f}" if is_correction else f"✅ 已记录入款：{amount:.2f}"
        if category:
            prefix += f" (分类：{category})"

        # 显示使用的配置信息
        config_info = []
        if effective_config.get("is_personalized"):
            config_info.append("📌 使用个性化配置")
        if temp_fee is not None:
            config_info.append(f"临时手续费：{temp_fee}%")
        if temp_rate is not None:
            config_info.append(f"临时汇率：{temp_rate}")
        if config_info:
            prefix += f"\n{' | '.join(config_info)}"

        prefix += f"\n⚙️ 费率：{actual_fee_rate}% | 汇率：{actual_exchange_rate} | 单笔费用：{actual_per_fee}元"
        
        view_button = InlineKeyboardButton("📊 查看当前账单", callback_data="view_current_bill")
        reply_markup = InlineKeyboardMarkup([[view_button]])

        await update.message.reply_text(
            f"{prefix} \n\n{message_text}", 
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text("❌ 记录失败，请稍后重试")

async def handle_add_expense(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                             amount: float, is_correction: bool = False,
                             admin_id: int = 0):
    if not is_authorized(update.effective_user.id, require_full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    group_id = str(update.effective_chat.id)
    user = update.effective_user
    username = user.username or user.first_name or str(user.id)
    record_amount = -abs(amount) if is_correction else abs(amount)
    desc = "修正出款" if is_correction else "出款"
    message_id = update.message.message_id
    cur_admin_id = get_user_admin_id(user.id)
    am = get_accounting_manager(cur_admin_id)
    success, _ = am.add_record(
        group_id, user.id, username, 'expense', record_amount, desc,
        "", None, message_id, None, cur_admin_id
    )
    if success:
        stats = am.get_current_stats(group_id, admin_id=cur_admin_id)
        records = am.get_current_records(group_id, admin_id=cur_admin_id)
        message = format_bill_message(stats, records, "当前账单")
        prefix = f"✅ 已记录修正出款：-{abs(amount):.2f} USDT" if is_correction else f"✅ 已记录出款：{amount:.2f} USDT"
        # ✅ 添加内联按钮
        view_button = InlineKeyboardButton("📊 查看当前账单", callback_data="view_current_bill")
        reply_markup = InlineKeyboardMarkup([[view_button]])

        await update.message.reply_text(
            f"{prefix}\n\n{message}", 
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await update.message.reply_text("❌ 记录失败，请稍后重试")

async def handle_total_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int = None):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    user_id = update.effective_user.id
    if admin_id is None:
        admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    # ✅ 先获取统计信息
    stats = am.get_total_stats(group_id, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await update.message.reply_text("📭 暂无账单记录")
        return

    # ✅ 存储查询参数
    context.user_data["bill_group_id"] = group_id
    context.user_data["bill_admin_id"] = admin_id
    context.user_data["bill_stats"] = stats
    context.user_data["bill_page"] = 0
    context.user_data["bill_type"] = "total"
    context.user_data["bill_title"] = "总计账单"
    context.user_data["bill_page_size"] = PAGE_SIZE

    await send_bill_page(update, context)

async def handle_today_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int = None):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    user_id = update.effective_user.id
    if admin_id is None:
        admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    today = get_today_beijing()
    stats = am.get_today_stats(group_id, admin_id=admin_id)

    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await update.message.reply_text("📭 今日暂无账单记录")
        return

    # ✅ 存储查询参数
    context.user_data["bill_group_id"] = group_id
    context.user_data["bill_admin_id"] = admin_id
    context.user_data["bill_stats"] = stats
    context.user_data["bill_page"] = 0
    context.user_data["bill_type"] = "today"
    context.user_data["bill_title"] = "今日账单"
    context.user_data["bill_date"] = today
    context.user_data["bill_page_size"] = PAGE_SIZE

    await send_bill_page(update, context)

async def handle_current_bill(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int = None):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    user_id = update.effective_user.id
    if admin_id is None:
        admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    # ✅ 获取统计信息（stats 数据量小，一次性查询没问题）
    stats = am.get_current_stats(group_id, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await update.message.reply_text(
            "📭 当前账单为空\n\n"
            "💡 使用以下指令开始记账：\n"
            "  • +金额 - 添加入款\n"
            "  • +金额 备注 - 带分类的入款（如：+1000 德国）\n"
            "  • -金额 - 修正入款\n"
            "  • 下发金额u - 添加出款\n"
            "  • 下发-金额u - 修正出款\n"
            "  • 设置手续费 数字 - 设置手续费率\n"
            "  • 设置汇率 数字 - 设置汇率\n"
            "  • 结束账单 - 结束并保存当前账单"
        )
        return

    # ✅ 获取当前会话ID
    session = am.get_or_create_session(group_id, admin_id)

    # ✅ 存储查询参数，不存储记录列表（实现真正的分页）
    context.user_data["bill_group_id"] = group_id
    context.user_data["bill_admin_id"] = admin_id
    context.user_data["bill_stats"] = stats
    context.user_data["bill_page"] = 0
    context.user_data["bill_type"] = "current"
    context.user_data["bill_title"] = "当前账单"
    context.user_data["bill_session_id"] = session['session_id']
    context.user_data["bill_page_size"] = PAGE_SIZE

    await send_bill_page(update, context)

async def handle_query_bill(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_id: int = None):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    user_id = update.effective_user.id
    if admin_id is None:
        admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    records = am.get_total_records(group_id, admin_id=admin_id)
    years = set()
    for r in records:
        if r.get('created_at'):
            year = datetime.fromtimestamp(r['created_at'], tz=BEIJING_TZ).year
            years.add(year)
    if not years:
        await update.message.reply_text("📭 暂无历史账单记录")
        return
    years = sorted(list(years), reverse=True)
    context.user_data["bill_years"] = years
    context.user_data["query_group_id"] = group_id
    context.user_data["query_admin_id"] = admin_id   # 存储 admin_id 供后续使用
    keyboard = []
    row = []
    for i, year in enumerate(years):
        row.append(InlineKeyboardButton(f"{year}年", callback_data=f"bill_year_{year}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="acct_cancel")])
    await update.message.reply_text(
        "📅 **请选择年份：**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return ACCOUNTING_YEAR_SELECT

async def handle_year_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    year = int(query.data.replace("bill_year_", ""))
    group_id = context.user_data.get("query_group_id")
    admin_id = context.user_data.get("query_admin_id", 0)
    am = get_accounting_manager(admin_id)
    records = am.get_total_records(group_id, admin_id=admin_id)
    months = set()
    for r in records:
        if r.get('created_at'):
            dt = datetime.fromtimestamp(r['created_at'], tz=BEIJING_TZ)
            if dt.year == year:
                months.add(dt.month)
    months = sorted(list(months))
    context.user_data["selected_year"] = year
    context.user_data["bill_months"] = months
    keyboard = []
    row = []
    for i, month in enumerate(months):
        row.append(InlineKeyboardButton(f"{month}月", callback_data=f"bill_month_{month}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="bill_back_to_years")])
    await query.message.edit_text(
        f"📅 **{year}年 - 请选择月份：**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return ACCOUNTING_MONTH_SELECT

async def handle_month_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    month = int(query.data.replace("bill_month_", ""))
    year = context.user_data.get("selected_year")
    group_id = context.user_data.get("query_group_id")
    admin_id = context.user_data.get("query_admin_id", 0)
    am = get_accounting_manager(admin_id)
    records = am.get_total_records(group_id, admin_id=admin_id)
    days = set()
    for r in records:
        if r.get('created_at'):
            dt = datetime.fromtimestamp(r['created_at'], tz=BEIJING_TZ)
            if dt.year == year and dt.month == month:
                days.add(dt.day)
    days = sorted(list(days))
    context.user_data["selected_month"] = month
    context.user_data["bill_days"] = days
    context.user_data["bill_days_page"] = 0
    await send_days_page(update, context)
    return ACCOUNTING_DATE_SELECT_PAGE

async def send_days_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    days = context.user_data.get("bill_days", [])
    page = context.user_data.get("bill_days_page", 0)
    year = context.user_data.get("selected_year")
    month = context.user_data.get("selected_month")
    if not days:
        await update.callback_query.message.edit_text(f"📭 {year}年{month}月 没有账单记录")
        return
    page_size = DAYS_PER_PAGE
    total_pages = (len(days) + page_size - 1) // page_size
    start = page * page_size
    end = min(start + page_size, len(days))
    page_days = days[start:end]
    keyboard = []
    row = []
    for i, day in enumerate(page_days):
        row.append(InlineKeyboardButton(f"{day}日", callback_data=f"bill_day_{day}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data="bill_days_prev"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data="bill_days_next"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("◀️ 返回月份", callback_data="bill_back_to_months")])
    await update.callback_query.message.edit_text(
        f"📅 **{year}年{month}月 - 请选择日期：**\n第 {page+1}/{total_pages} 页",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_day_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理日期选择，显示账单"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    year = context.user_data.get("selected_year")
    month = context.user_data.get("selected_month")
    day = int(query.data.replace("bill_day_", ""))
    group_id = context.user_data.get("query_group_id")

    if year is None or month is None or group_id is None:
        await query.message.edit_text("❌ 会话已过期，请重新查询账单")
        return

    date_str = f"{year}-{month:02d}-{day:02d}"
    am = get_accounting_manager(admin_id)

    # ✅ 先获取统计信息
    stats = am.get_stats_by_date(group_id, date_str, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await query.message.edit_text(f"📭 {date_str} 暂无账单记录")
        return

    # ✅ 存储查询参数
    context.user_data["bill_group_id"] = group_id
    context.user_data["bill_admin_id"] = admin_id
    context.user_data["bill_stats"] = stats
    context.user_data["bill_date"] = date_str
    context.user_data["bill_page"] = 0
    context.user_data["bill_type"] = "date"
    context.user_data["bill_title"] = f"{date_str} 账单"
    context.user_data["bill_page_size"] = PAGE_SIZE

    await send_bill_page(update, context)
    return ACCOUNTING_VIEW_PAGE

async def handle_bill_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "bill_back_to_years":
        years = context.user_data.get("bill_years", [])
        keyboard = []
        row = []
        for i, year in enumerate(years):
            row.append(InlineKeyboardButton(f"{year}年", callback_data=f"bill_year_{year}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="acct_cancel")])
        await query.message.edit_text(
            "📅 **请选择年份：**",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return ACCOUNTING_YEAR_SELECT
    elif data == "bill_back_to_months":
        months = context.user_data.get("bill_months", [])
        year = context.user_data.get("selected_year")
        keyboard = []
        row = []
        for i, month in enumerate(months):
            row.append(InlineKeyboardButton(f"{month}月", callback_data=f"bill_month_{month}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("◀️ 返回年份", callback_data="bill_back_to_years")])
        await query.message.edit_text(
            f"📅 **{year}年 - 请选择月份：**",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return ACCOUNTING_MONTH_SELECT
    elif data == "bill_days_prev":
        page = context.user_data.get("bill_days_page", 0)
        context.user_data["bill_days_page"] = max(0, page - 1)
        await send_days_page(update, context)
        return ACCOUNTING_DATE_SELECT_PAGE
    elif data == "bill_days_next":
        page = context.user_data.get("bill_days_page", 0)
        context.user_data["bill_days_page"] = page + 1
        await send_days_page(update, context)
        return ACCOUNTING_DATE_SELECT_PAGE

async def handle_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    date_str = query.data.replace("acct_date_", "")
    group_id = str(query.message.chat.id)
    am = get_accounting_manager(admin_id)

    stats = am.get_stats_by_date(group_id, date_str, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await query.message.edit_text(f"📭 {date_str} 暂无账单记录")
        return ConversationHandler.END

    context.user_data["bill_group_id"] = group_id
    context.user_data["bill_admin_id"] = admin_id
    context.user_data["bill_stats"] = stats
    context.user_data["bill_date"] = date_str
    context.user_data["bill_page"] = 0
    context.user_data["bill_type"] = "date"
    context.user_data["bill_title"] = f"{date_str} 账单"
    context.user_data["bill_page_size"] = PAGE_SIZE

    await send_bill_page(update, context)
    return ACCOUNTING_VIEW_PAGE

async def handle_clear_bill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    chat = update.effective_chat
    group_id = str(chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    stats = am.get_current_stats(group_id, admin_id=admin_id)
    if stats['income_count'] == 0 and stats['expense_count'] == 0:
        await update.message.reply_text("📭 当前账单为空，无需清理")
        return
    keyboard = [
        [InlineKeyboardButton("✅ 确认清理", callback_data="clear_current_confirm")],
        [InlineKeyboardButton("❌ 取消", callback_data="clear_current_cancel")]
    ]
    await update.message.reply_text(
        f"⚠️ **警告：此操作将清空当前账单！**\n\n"
        f"📊 当前账单统计：\n"
        f"  总入款：{stats['income_total']:.2f} = {stats['income_usdt']:.2f} USDT\n"
        f"  已下发：{stats['expense_usdt']:.2f} USDT\n"
        f"  记录总数：{stats['income_count'] + stats['expense_count']} 笔\n\n"
        f"⚠️ **注意：当前账单未结束，清理后所有数据将永久丢失！**\n\n"
        f"确认要继续吗？",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return ACCOUNTING_CONFIRM_CLEAR

async def handle_clear_current_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = str(query.message.chat.id)
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    if am.clear_current_session(group_id, admin_id):
        await query.message.edit_text("✅ 已清空当前账单")
        stats = am.get_current_stats(group_id, admin_id=admin_id)
        records = am.get_current_records(group_id, admin_id=admin_id)
        message = format_bill_message(stats, records, "当前账单")
        await query.message.reply_text(message, parse_mode='Markdown')
    else:
        await query.message.edit_text("❌ 清空失败，请稍后重试")
    return ConversationHandler.END

async def handle_clear_current_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.edit_text("✅ 已取消清空操作")
    return ConversationHandler.END

async def handle_clear_all_bill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    chat = update.effective_chat
    group_id = str(chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    total_stats = am.get_total_stats(group_id, admin_id=admin_id)
    if total_stats['income_count'] == 0 and total_stats['expense_count'] == 0:
        await update.message.reply_text("📭 暂无任何账单记录")
        return
    keyboard = [
        [InlineKeyboardButton("✅ 确认清空所有账单", callback_data="clear_all_confirm")],
        [InlineKeyboardButton("❌ 取消", callback_data="clear_all_cancel")]
    ]
    await update.message.reply_text(
        f"⚠️ **警告：此操作将清空本群的所有账单记录！**\n\n"
        f"📊 当前统计：\n"
        f"  总入款：{total_stats['income_total']:.2f} = {total_stats['income_usdt']:.2f} USDT\n"
        f"  总下发：{total_stats['expense_usdt']:.2f} USDT\n"
        f"  记录总数：{total_stats['income_count'] + total_stats['expense_count']} 笔\n\n"
        f"确认要继续吗？此操作不可恢复！",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return ACCOUNTING_CONFIRM_CLEAR_ALL

async def handle_clear_all_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = str(query.message.chat.id)
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    if am.clear_all_records(group_id, admin_id):
        await query.message.edit_text("✅ 已清空本群所有账单记录（包括历史记录）")
        stats = am.get_current_stats(group_id, admin_id=admin_id)
        records = am.get_current_records(group_id, admin_id=admin_id)
        message = format_bill_message(stats, records, "当前账单")
        await query.message.reply_text(message, parse_mode='Markdown')
    else:
        await query.message.edit_text("❌ 清空失败，请稍后重试")
    return ConversationHandler.END

async def handle_clear_all_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.edit_text("✅ 已取消清空操作")
    return ConversationHandler.END

async def handle_remove_last_record(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_authorized_in_group(update, full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员权限")
        return
    chat = update.effective_chat
    group_id = str(chat.id)
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    success, result = am.remove_last_record(group_id, admin_id)
    if not success:
        await update.message.reply_text(f"❌ {result.get('error', '移除失败')}")
        return
    record = result
    record_type = "入款" if record['type'] == 'income' else "出款"
    if record['type'] == 'income':
        amount_info = f"{record['amount']:.2f} 元 = {record['amount_usdt']:.2f} USDT"
        if record.get('category'):
            amount_info += f" (分类：{record['category']})"
    else:
        amount_info = f"{record['amount_usdt']:.2f} USDT"
    dt = beijing_time(record['created_at'])
    time_str = dt.strftime('%H:%M:%S')
    operator = record.get('username') or f"用户{record.get('user_id', '未知')}"
    await update.message.reply_text(
        f"✅ 已移除上一笔记账\n\n"
        f"📝 记录信息：\n"
        f"  • 类型：{record_type}\n"
        f"  • 金额：{amount_info}\n"
        f"  • 时间：{time_str}\n"
        f"  • 操作人：{operator}\n\n"
        f"💡 使用「当前账单」查看最新账单"
    )
    stats = am.get_current_stats(group_id, admin_id=admin_id)
    records = am.get_current_records(group_id, admin_id=admin_id)
    message = format_bill_message(stats, records, "当前账单")
    await update.message.reply_text(message, parse_mode='Markdown')

async def handle_revoke_record(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    chat = update.effective_chat
    user = update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await message.reply_text("❌ 此操作需要管理员或操作员权限")
        return
    if chat.type not in ['group', 'supergroup']:
        await message.reply_text("❌ 此功能仅在群组中可用")
        return
    if not message.reply_to_message:
        await message.reply_text("❌ 请回复要撤销的记账消息\n\n使用方法：回复记账消息，然后发送「撤销账单」")
        return
    replied_msg = message.reply_to_message
    replied_msg_id = replied_msg.message_id
    group_id = str(chat.id)
    user_id = user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    record = am.get_record_by_message_id(group_id, replied_msg_id, admin_id)
    if not record:
        await message.reply_text("❌ 未找到对应的记账记录，可能已被撤销或不是记账消息")
        return
    success = am.delete_record_by_id(record['id'], admin_id)
    if success:
        record_type_name = "入款" if record['type'] == 'income' else "出款"
        if record['type'] == 'income':
            fee_rate = record.get('fee_rate', 0)
            rate = record.get('rate', 0)
            per_fee = record.get('per_transaction_fee', 0)
            amount_usdt = record.get('amount_usdt', 0)
            detail_info = f"""
📝 **撤销的入款记录详情：**
• 金额：{record['amount']:.2f} 元
• 手续费率：{fee_rate}%
• 汇率：{rate}
• 单笔费用：{per_fee} 元
• 到账 USDT：{amount_usdt:.2f} USDT
• 时间：{beijing_time(record['created_at']).strftime('%Y-%m-%d %H:%M:%S')}
"""
        else:
            detail_info = f"""
📝 **撤销的出款记录详情：**
• 金额：{record['amount_usdt']:.2f} USDT
• 时间：{beijing_time(record['created_at']).strftime('%Y-%m-%d %H:%M:%S')}
"""
        await message.reply_text(
            f"✅ 已撤销{record_type_name}记录\n{detail_info}",
            parse_mode='Markdown'
        )
        stats = am.get_current_stats(group_id, admin_id=admin_id)
        records = am.get_current_records(group_id, admin_id=admin_id)
        bill_message = format_bill_message(stats, records, "当前账单")
        await message.reply_text(bill_message, parse_mode='Markdown')
    else:
        await message.reply_text("❌ 撤销失败，请稍后重试")

async def send_bill_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送分页账单（真正分页，每次翻页重新查询数据库）"""
    group_id = context.user_data.get("bill_group_id")
    admin_id = context.user_data.get("bill_admin_id")
    page = context.user_data.get("bill_page", 0)
    page_size = context.user_data.get("bill_page_size", PAGE_SIZE)
    bill_type = context.user_data.get("bill_type", "")
    title = context.user_data.get("bill_title", "账单")
    date_str = context.user_data.get("bill_date", "")
    session_id = context.user_data.get("bill_session_id")
    stats = context.user_data.get("bill_stats", {})

    if not group_id or admin_id is None:
        if update.callback_query:
            await update.callback_query.message.edit_text("❌ 查询参数丢失，请重新查询")
        else:
            await update.message.reply_text("❌ 查询参数丢失，请重新查询")
        return

    am = get_accounting_manager(admin_id)

    # ✅ 根据类型调用分页查询
    if bill_type == "current" and session_id:
        records, total = am.get_records_paginated(
            group_id, admin_id=admin_id, page=page, page_size=page_size,
            session_id=session_id
        )
    elif bill_type == "today" and date_str:
        records, total = am.get_records_paginated(
            group_id, admin_id=admin_id, page=page, page_size=page_size,
            date=date_str
        )
    elif bill_type == "date" and date_str:
        records, total = am.get_records_paginated(
            group_id, admin_id=admin_id, page=page, page_size=page_size,
            date=date_str
        )
    else:
        # 总计：不加任何过滤
        records, total = am.get_records_paginated(
            group_id, admin_id=admin_id, page=page, page_size=page_size
        )

    total_pages = (total + page_size - 1) // page_size if total > 0 else 1

    # 分离入款和出款（仅当前页）
    income_records = [r for r in records if r['type'] == 'income']
    expense_records = [r for r in records if r['type'] == 'expense']

    # 开始构建消息
    if bill_type == "date":
        message = f"📅 **{title}** (第 {page+1}/{total_pages} 页)\n\n"
    else:
        message = f"📊 **{title}** (第 {page+1}/{total_pages} 页)\n\n"

    # ==================== 入款记录（按备注分组） ====================
    if income_records:
        categories = {}
        no_category_records = []

        for r in income_records:
            category = r.get('category', '') or ''
            if category:
                if category not in categories:
                    categories[category] = []
                categories[category].append(r)
            else:
                no_category_records.append(r)

        total_income_count = len(income_records)
        message += f"📈 **入款 {total_income_count} 笔**\n"

        if no_category_records:
            for r in no_category_records:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%H:%M')
                fee_rate = r.get('fee_rate', 0)
                rate = r.get('rate', 0)
                fee_info = format_fee_info(fee_rate, rate)
                display_name = r.get('display_name', '未知用户')
                user_id = r.get('user_id')
                mention = f" [{display_name}](tg://user?id={user_id})" if user_id else f" {display_name}"
                amount_str = f"{r['amount']:+.2f}"
                message += f"  `{time_str} {amount_str} {fee_info} = {r['amount_usdt']:.2f} USDT`{mention}\n"
            message += "\n"

        for category, group_records in categories.items():
            display_category = get_category_with_flag(category)
            message += f"{display_category} ({len(group_records)} 笔)\n"
            for r in group_records:
                dt = beijing_time(r['created_at'])
                time_str = dt.strftime('%H:%M')
                fee_rate = r.get('fee_rate', 0)
                rate = r.get('rate', 0)
                fee_info = format_fee_info(fee_rate, rate)
                display_name = r.get('display_name', '未知用户')
                user_id = r.get('user_id')
                mention = f" [{display_name}](tg://user?id={user_id})" if user_id else f" {display_name}"
                amount_str = f"{r['amount']:+.2f}"
                message += f"  `{time_str} {amount_str} {fee_info} = {r['amount_usdt']:.2f} USDT`{mention}\n"
            group_total_cny = sum(r['amount'] for r in group_records)
            group_total_usdt = sum(r['amount_usdt'] for r in group_records)
            message += f"  小计：{group_total_cny:.2f} = {group_total_usdt:.2f} USDT\n\n"
    else:
        message += "📈 **入款 0 笔**\n\n"

    # ==================== 出款记录 ====================
    if expense_records:
        message += f"📉 **出款 {len(expense_records)} 笔**\n"
        for r in expense_records:
            dt = beijing_time(r['created_at'])
            time_str = dt.strftime('%H:%M')
            amount_usdt = r['amount_usdt']
            display_name = r.get('display_name', '未知用户')
            user_id = r.get('user_id')
            mention = f" [{display_name}](tg://user?id={user_id})" if user_id else f" {display_name}"
            if amount_usdt > 0:
                message += f"  `{time_str} -{amount_usdt:.2f} USDT`{mention}\n"
            else:
                message += f"  `{time_str} +{abs(amount_usdt):.2f} USDT (修正)`{mention}\n"
        message += "\n"

    # ==================== 统计信息 ====================
    fee_rate = stats.get('fee_rate', 0)
    exchange_rate = stats.get('exchange_rate', 1)
    per_transaction_fee = stats.get('per_transaction_fee', 0)
    total_income_cny = stats.get('income_total', 0)
    total_income_usdt = stats.get('income_usdt', 0)
    total_expense_usdt = stats.get('expense_usdt', 0)
    pending_usdt = total_income_usdt - total_expense_usdt

    message += f"💰 **费率**：{fee_rate}%\n"
    message += f"💱 **汇率**：{exchange_rate}\n"
    message += f"📝 **单笔费用**：{per_transaction_fee} 元\n\n"
    message += f"📊 **总入款**：{total_income_cny:.2f} = {total_income_usdt:.2f} USDT\n"
    message += f"📤 **已下发**：{total_expense_usdt:.2f} USDT\n"
    if title == "总计账单":
        message += f"📋 **总待出款**：{pending_usdt:.2f} USDT"
    else:
        message += f"⏳ **待下发**：{pending_usdt:.2f} USDT"

    # 分页按钮
    keyboard = []
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data="bill_page_prev"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data="bill_page_next"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("❌ 关闭", callback_data="bill_close")])

    if update.callback_query:
        await update.callback_query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
    else:
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

async def handle_bill_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理账单分页按钮"""
    query = update.callback_query
    await query.answer()

    data = query.data
    current_page = context.user_data.get("bill_page", 0)

    if data == "bill_page_prev":
        context.user_data["bill_page"] = max(0, current_page - 1)
    elif data == "bill_page_next":
        context.user_data["bill_page"] = current_page + 1
    elif data == "bill_close":
        keys = ["bill_records", "bill_stats", "bill_date", "bill_page", "bill_type",
                "bill_title", "bill_group_id", "bill_admin_id", "bill_session_id", "bill_page_size"]
        for k in keys:
            context.user_data.pop(k, None)
        await query.message.delete()
        return

    await send_bill_page(update, context)

async def handle_export_bill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await update.message.reply_text("❌ 此操作需要管理员或操作员权限")
        return
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("❌ 此功能仅在群组中可用")
        return
    group_id = str(chat.id)
    context.user_data["export_group_id"] = group_id
    context.user_data["export_group_name"] = chat.title or chat.first_name or "群组"
    user_id = user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)
    dates = am.get_available_dates(group_id, admin_id)
    years = set()
    for d in dates:
        year = d.split('-')[0]
        years.add(year)
    if not years:
        await update.message.reply_text("📭 暂无账单记录可导出")
        return
    years = sorted(list(years), reverse=True)
    context.user_data["export_years"] = years
    context.user_data["export_admin_id"] = admin_id
    keyboard = []
    row = []
    for i, year in enumerate(years):
        row.append(InlineKeyboardButton(f"{year}年", callback_data=f"export_year_{year}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="export_cancel")])
    await update.message.reply_text(
        "📅 **请选择要导出的年份：**\n\n"
        "选择后可以选择具体月份或导出整年",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_export_year_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user or update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()
    year = int(query.data.replace("export_year_", ""))
    group_id = context.user_data.get("export_group_id")
    admin_id = context.user_data.get("export_admin_id", 0)
    am = get_accounting_manager(admin_id)
    dates = am.get_available_dates(group_id, admin_id)
    months = set()
    for d in dates:
        y, m, _ = d.split('-')
        if int(y) == year:
            months.add(int(m))
    months = sorted(list(months))
    context.user_data["export_year"] = year
    context.user_data["export_months"] = months
    keyboard = []
    keyboard.append([InlineKeyboardButton("📅 导出全年", callback_data=f"export_full_year_{year}")])
    row = []
    for i, month in enumerate(months):
        row.append(InlineKeyboardButton(f"{month}月", callback_data=f"export_month_{year}_{month}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="export_back_to_years")])
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="export_cancel")])
    await query.message.edit_text(
        f"📅 **{year}年 - 选择导出范围：**\n\n"
        f"• 点击「导出全年」导出该年所有账单\n"
        f"• 或选择具体月份导出该月账单",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def send_export_days_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    days = context.user_data.get("export_days", [])
    page = context.user_data.get("export_days_page", 0)
    year = context.user_data.get("export_selected_year")
    month = context.user_data.get("export_selected_month")
    if not days:
        await query.message.edit_text(f"📭 {year}年{month}月 没有账单记录")
        return
    page_size = 10
    total_pages = (len(days) + page_size - 1) // page_size
    start = page * page_size
    end = min(start + page_size, len(days))
    page_days = days[start:end]
    keyboard = []
    row = []
    for i, day in enumerate(page_days):
        row.append(InlineKeyboardButton(f"{day}日", callback_data=f"export_day_{year}_{month}_{day}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data="export_days_prev"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data="export_days_next"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("📅 导出整月", callback_data=f"export_full_month_{year}_{month}")])
    keyboard.append([InlineKeyboardButton("◀️ 返回月份", callback_data="export_back_to_months")])
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="export_cancel")])
    await query.message.edit_text(
        f"📅 **{year}年{month}月 - 请选择要导出的日期：**\n第 {page+1}/{total_pages} 页\n\n"
        f"💡 提示：\n"
        f"• 点击具体日期可导出该日账单\n"
        f"• 点击「导出整月」可导出整个月账单",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_export_day_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user or update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()
    data = query.data
    if data.startswith("export_day_"):
        parts = data.split("_")
        year = int(parts[2])
        month = int(parts[3])
        day = int(parts[4])
        date_str = f"{year}-{month:02d}-{day:02d}"
        await generate_and_send_export(update, context, date_str, date_str, f"{year}年{month}月{day}日")
    elif data.startswith("export_full_month_"):
        parts = data.split("_")
        year = int(parts[3])
        month = int(parts[4])
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year}-12-31"
        else:
            end_date = f"{year}-{month+1:02d}-01"
            end_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        await generate_and_send_export(update, context, start_date, end_date, f"{year}年{month}月")
    elif data == "export_days_prev":
        page = context.user_data.get("export_days_page", 0)
        context.user_data["export_days_page"] = max(0, page - 1)
        await send_export_days_page(update, context)
    elif data == "export_days_next":
        page = context.user_data.get("export_days_page", 0)
        context.user_data["export_days_page"] = page + 1
        await send_export_days_page(update, context)
    elif data == "export_back_to_months":
        year = context.user_data.get("export_selected_year")
        group_id = context.user_data.get("export_group_id")
        admin_id = context.user_data.get("export_admin_id", 0)
        am = get_accounting_manager(admin_id)
        dates = am.get_available_dates(group_id, admin_id)
        months = set()
        for d in dates:
            y, m, _ = d.split('-')
            if int(y) == year:
                months.add(int(m))
        months = sorted(list(months))
        context.user_data["export_months"] = months
        keyboard = []
        keyboard.append([InlineKeyboardButton("📅 导出全年", callback_data=f"export_full_year_{year}")])
        row = []
        for i, month in enumerate(months):
            row.append(InlineKeyboardButton(f"{month}月", callback_data=f"export_month_{year}_{month}"))
            if len(row) == 4:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="export_back_to_years")])
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="export_cancel")])
        await query.message.edit_text(
            f"📅 **{year}年 - 选择导出范围：**\n\n"
            f"• 点击「导出全年」导出该年所有账单\n"
            f"• 点击月份可查看该月具体日期",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

async def handle_export_month_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user or update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()
    data = query.data
    if data.startswith("export_full_year_"):
        year = int(data.replace("export_full_year_", ""))
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        await generate_and_send_export(update, context, start_date, end_date, f"{year}年全年")
    elif data.startswith("export_month_"):
        parts = data.split("_")
        year = int(parts[2])
        month = int(parts[3])
        context.user_data["export_selected_year"] = year
        context.user_data["export_selected_month"] = month
        group_id = context.user_data.get("export_group_id")
        admin_id = context.user_data.get("export_admin_id", 0)
        am = get_accounting_manager(admin_id)
        records = am.get_total_records(group_id, admin_id=admin_id)
        days = set()
        for r in records:
            if r.get('date'):
                y, m, d = r['date'].split('-')
                if int(y) == year and int(m) == month:
                    days.add(int(d))
        if not days:
            await query.message.edit_text(f"📭 {year}年{month}月 没有账单记录")
            return
        days = sorted(list(days))
        context.user_data["export_days"] = days
        context.user_data["export_days_page"] = 0
        await send_export_days_page(update, context)
    elif data == "export_back_to_years":
        years = context.user_data.get("export_years", [])
        if not years:
            # ✅ 如果上下文丢失，重新从数据库获取
            group_id = context.user_data.get("export_group_id")
            from auth import get_user_admin_id
            user_id = query.from_user.id
            admin_id = get_user_admin_id(user_id)
            am = get_accounting_manager(admin_id)
            dates = am.get_available_dates(group_id, admin_id)
            years_set = set()
            for d in dates:
                years_set.add(d.split('-')[0])
            years = sorted(list(years_set), reverse=True)
            context.user_data["export_years"] = years
        keyboard = []
        row = []
        for i, year in enumerate(years):
            row.append(InlineKeyboardButton(f"{year}年", callback_data=f"export_year_{year}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data="export_cancel")])
        await query.message.edit_text(
            "📅 **请选择要导出的年份：**",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    elif data == "export_cancel":
        await query.message.edit_text("✅ 已取消导出操作")
        context.user_data.pop("export_group_id", None)
        context.user_data.pop("export_group_name", None)
        context.user_data.pop("export_years", None)
        context.user_data.pop("export_year", None)
        context.user_data.pop("export_months", None)
        context.user_data.pop("export_selected_year", None)
        context.user_data.pop("export_selected_month", None)
        context.user_data.pop("export_days", None)
        context.user_data.pop("export_days_page", None)
        context.user_data.pop("export_admin_id", None)

async def generate_and_send_export(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                    start_date: str, end_date: str, title: str):
    query = update.callback_query
    user = query.from_user or update.effective_user
    if not is_authorized(user.id, require_full_access=False):
        await query.answer("❌ 无权限", show_alert=True)
        return
    group_id = context.user_data.get("export_group_id")
    group_name = context.user_data.get("export_group_name", "群组")
    admin_id = context.user_data.get("export_admin_id", 0)
    am = get_accounting_manager(admin_id)
    await query.message.edit_text(f"📊 正在生成 {title} 账单导出文件，请稍候...")
    records = am.get_records_by_date_range(group_id, start_date, end_date, admin_id=admin_id)
    if not records:
        await query.message.edit_text(f"📭 {title} 暂无账单记录")
        return
    html_content = generate_export_html(records, group_name, start_date, end_date)
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', encoding='utf-8', delete=False) as f:
        f.write(html_content)
        temp_path = f.name
    try:
        with open(temp_path, 'rb') as f:
            await query.message.reply_document(
                document=f,
                filename=f"账单_{group_name}_{start_date}_至_{end_date}.html",
                caption=f"✅ **账单已导出！**\n\n"
                        f"📊 {title} 账单导出\n"
                        f"📅 日期范围：{start_date} 至 {end_date}\n"
                        f"📝 共 {len(records)} 条记录\n\n"
                        f"💡 **提示：**\n"
                        f"• 点击文件即可在浏览器中查看\n"
                        f"• 支持手机和电脑查看\n"
                        f"• 可保存为网页文件或打印为PDF"
            )
    except Exception as e:
        logger.error(f"发送导出文件失败: {e}")
        await query.message.reply_text(f"❌ 导出失败：{str(e)}")
    finally:
        try:
            os.unlink(temp_path)
        except:
            pass
    context.user_data.pop("export_group_id", None)
    context.user_data.pop("export_group_name", None)
    context.user_data.pop("export_years", None)
    context.user_data.pop("export_year", None)
    context.user_data.pop("export_months", None)
    context.user_data.pop("export_selected_year", None)
    context.user_data.pop("export_selected_month", None)
    context.user_data.pop("export_days", None)
    context.user_data.pop("export_days_page", None)
    context.user_data.pop("export_admin_id", None)

async def handle_user_config_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理设置指定人的手续费/汇率/单笔费用"""
    message = update.message
    chat = update.effective_chat

    if chat.type not in ['group', 'supergroup']:
        await message.reply_text("❌ 此功能仅在群组中可用")
        return

    user_id = message.from_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_authorized(user_id, require_full_access=False):
        await message.reply_text("❌ 您没有权限进行此设置")
        return

    import re

    # 获取目标用户
    target_user = None
    target_username = None

    # 先检查是否是回复消息
    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
        display_name = target_user.first_name or target_user.username or str(target_user.id)
        display_name = f"用户 {display_name}"
    else:
        # 从 @ 中提取用户
        if message.entities:
            for entity in message.entities:
                if entity.type == "text_mention" and entity.user:
                    target_user = entity.user
                    break
                elif entity.type == "mention":
                    # 提取 @username
                    target_username = text[entity.offset:entity.offset + entity.length].lstrip('@')
                    break

    # 如果只有 username，在数据库中查找对应的 user_id
    if not target_user and target_username:
        # 在 group_users 表中查找
        group_id = str(chat.id)
        am = get_accounting_manager(admin_id)

        # 在当前管理员的 group_users 中查找
        with am._get_conn(admin_id) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT user_id, first_name FROM group_users WHERE group_id = ? AND username = ? LIMIT 1",
                (group_id, target_username)
            )
            row = c.fetchone()
            if row:
                target_user = type('User', (), {
                    'id': row[0],
                    'first_name': row[1] or target_username,
                    'username': target_username
                })()
            else:
                # 如果当前群组没找到，在其他群组中查找
                c.execute(
                    "SELECT user_id, first_name, username FROM group_users WHERE username = ? LIMIT 1",
                    (target_username,)
                )
                row = c.fetchone()
                if row:
                    target_user = type('User', (), {
                        'id': row[0],
                        'first_name': row[1] or target_username,
                        'username': target_username
                    })()

    if not target_user:
        if target_username:
            await message.reply_text(
                f"❌ 未找到用户 @{target_username}\n\n"
                "💡 请确认：\n"
                "• 用户名是否正确\n"
                "• 该用户是否在群组中发过言\n"
                "• 或尝试回复该用户的消息来设置"
            )
        else:
            await message.reply_text("❌ 请使用 @用户名 指定用户，或回复用户消息来设置")
        return

    # 使用 parse_batch_settings 解析设置参数
    fee_rate, exchange_rate, per_transaction_fee = parse_batch_settings(text)

    if fee_rate is None and exchange_rate is None and per_transaction_fee is None:
        await message.reply_text(
            "❌ 未识别到有效的设置参数\n\n"
            "💡 用法示例：\n"
            "• 设置 @用户名 手续费5\n"
            "• 设置 @用户名 手续费5 汇率10\n"
            "• 设置 @用户名 手续费5 汇率10 单笔费用12\n"
            "• 回复用户消息：设置手续费5 汇率10",
            parse_mode="Markdown"
        )
        return

    group_id = str(chat.id)
    am = get_accounting_manager(admin_id)

    # 更新用户信息（记录到 group_users 表）
    am.update_user_info(group_id, target_user.id, target_user.username or "", 
                        target_user.first_name or "", "", admin_id)

    # 执行设置
    display_name = target_user.first_name or target_user.username or str(target_user.id)

    success = am.set_user_config(
        group_id, target_user.id,
        fee_rate=fee_rate,
        exchange_rate=exchange_rate,
        per_transaction_fee=per_transaction_fee,
        admin_id=admin_id
    )

    if success:
        # 获取完整配置
        config = am.get_effective_config(group_id, target_user.id, admin_id)

        reply = f"✅ 已为 {display_name} 设置个性化配置\n\n"
        reply += f"💰 手续费：{config['fee_rate']}%\n"
        reply += f"💱 汇率：{config['exchange_rate']}\n"
        reply += f"📝 单笔费用：{config['per_transaction_fee']} 元\n\n"
        reply += "💡 该用户记账时将自动使用以上配置"
    else:
        reply = f"❌ 为 {display_name} 设置配置失败"

    await message.reply_text(reply, parse_mode="Markdown")


async def handle_delete_user_config(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理删除指定人的个性化配置"""
    message = update.message
    chat = update.effective_chat

    if chat.type not in ['group', 'supergroup']:
        await message.reply_text("❌ 此功能仅在群组中可用")
        return

    user_id = message.from_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_authorized(user_id, require_full_access=False):
        await message.reply_text("❌ 您没有权限进行此操作")
        return

    # 获取目标用户
    target_user = None

    if message.reply_to_message:
        target_user = message.reply_to_message.from_user
    else:
        if message.entities:
            for entity in message.entities:
                if entity.type == "text_mention" and entity.user:
                    target_user = entity.user
                    break
                elif entity.type == "mention":
                    username = text[entity.offset:entity.offset + entity.length].lstrip('@')
                    from auth import admins
                    for aid in admins.keys():
                        from db_manager import get_conn
                        conn = get_conn(aid)
                        row = conn.execute(
                            "SELECT user_id FROM group_users WHERE username = ? LIMIT 1",
                            (username,)
                        ).fetchone()
                        if row:
                            target_user = type('User', (), {
                                'id': row[0],
                                'first_name': username,
                                'username': username
                            })()
                            break
                    break

    if not target_user:
        await message.reply_text("❌ 请使用 @用户名 或回复用户消息来指定要删除配置的用户")
        return

    group_id = str(chat.id)
    am = get_accounting_manager(admin_id)

    # 先检查是否有配置
    config = am.get_user_config(group_id, target_user.id, admin_id)

    if not config.get("has_config"):
        display_name = target_user.first_name or target_user.username or str(target_user.id)
        await message.reply_text(f"❌ {display_name} 没有个性化配置")
        return

    # 删除配置
    success = am.delete_user_config(group_id, target_user.id, admin_id)

    if success:
        display_name = target_user.first_name or target_user.username or str(target_user.id)
        await message.reply_text(f"✅ 已删除 {display_name} 的个性化配置\n\n💡 该用户将使用群组默认配置")
    else:
        await message.reply_text("❌ 删除失败，请稍后重试")


async def handle_view_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看群组的配置信息（包括个性化配置）"""
    message = update.message
    chat = update.effective_chat

    if chat.type not in ['group', 'supergroup']:
        await message.reply_text("❌ 此功能仅在群组中可用")
        return

    group_id = str(chat.id)
    user_id = message.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    # 获取群组默认配置
    session = am.get_or_create_session(group_id, admin_id)

    reply = "⚙️ **群组配置信息**\n\n"
    reply += "📋 **默认配置**\n"
    reply += f"💰 手续费：{session['fee_rate']}%\n"
    reply += f"💱 汇率：{session['exchange_rate']}\n"
    reply += f"📝 单笔费用：{session.get('per_transaction_fee', 0)} 元\n"

    # 获取个性化配置列表
    user_configs = am.get_all_user_configs(group_id, admin_id)

    if user_configs:
        reply += f"\n👥 **个性化配置**（{len(user_configs)}人）\n"
        for config in user_configs[:20]:  # 最多显示20个
            display_name = config.get('first_name', '')
            username = config.get('username', '')
            if username and display_name:
                display_name = f"{display_name} (@{username})"
            elif username:
                display_name = f"@{username}"
            elif display_name:
                pass
            else:
                display_name = f"用户{config['user_id']}"

            reply += f"\n👤 {display_name}\n"
            if config['fee_rate'] is not None:
                reply += f"   手续费：{config['fee_rate']}%\n"
            if config['exchange_rate'] is not None:
                reply += f"   汇率：{config['exchange_rate']}\n"
            if config['per_transaction_fee'] is not None:
                reply += f"   单笔费用：{config['per_transaction_fee']} 元\n"

        if len(user_configs) > 20:
            reply += f"\n... 还有 {len(user_configs) - 20} 人有配置"
    else:
        reply += "\n👥 **个性化配置**：暂无"

    await message.reply_text(reply, parse_mode="Markdown")

async def handle_calculator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        return
    text = update.message.text.strip()
    exclude_prefixes = ['设置手续费', '设置汇率', '设置单笔费用', '结束账单', '今日总', '总', 
                        '当前账单', '查询账单', '清理账单', '清空账单', '清理总账单', 
                        '清空总账单', '清空所有账单', '下发', '+', '-']
    for prefix in exclude_prefixes:
        if text.startswith(prefix):
            return
    def is_valid_expression(expr: str) -> bool:
        expr = expr.replace(' ', '')
        if not expr:
            return False
        valid_starts = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '(', '-', '+', 
                       'sqrt', 'sin', 'cos', 'tan', 'log', 'abs', 'round', 'floor', 'ceil', 'pi', 'e')
        if not any(expr.startswith(s) for s in valid_starts):
            return False
        if not (expr[-1].isdigit() or expr[-1] == ')'):
            return False
        operators = ['+', '-', '*', '/', '%', '^']
        has_operator = any(op in expr for op in operators)
        if not has_operator:
            return False
        return True
    if not is_valid_expression(text):
        return
    simple_pattern = r'^(-?\d+(?:\.\d+)?)\s*([+\-*/%])\s*(-?\d+(?:\.\d+)?)$'
    simple_match = re.match(simple_pattern, text)
    if simple_match:
        a, op, b = simple_match.groups()
        a_num = float(a)
        b_num = float(b)
        try:
            if op == '+':
                result = a_num + b_num
            elif op == '-':
                result = a_num - b_num
            elif op == '*':
                result = a_num * b_num
            elif op == '/':
                if b_num == 0:
                    await update.message.reply_text("❌ 除数不能为0")
                    return
                result = a_num / b_num
            elif op == '%':
                result = a_num % b_num
            else:
                return
            result_str = str(int(result)) if result.is_integer() else f"{result:.2f}"
            await update.message.reply_text(f"🧮 {a}{op}{b} = {result_str}")
            return
        except Exception as e:
            await update.message.reply_text(f"❌ 计算错误：{str(e)[:50]}")
            return
    result = Calculator.safe_eval(text)
    if result is not None:
        result_str = Calculator.format_result(result)
        await update.message.reply_text(f"🧮 {text} = {result_str}")
    else:
        await update.message.reply_text(
            "❌ 请输入完整计算格式\n"
            "支持的运算符：+ - * / % ^ ( )\n"
            "支持的函数：sqrt, sin, cos, tan, log, abs, round, floor, ceil\n"
            "支持的常数：pi, e\n"
        )

async def handle_user_info_tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not user or chat.type not in ['group', 'supergroup']:
        return
    group_id = str(chat.id)
    username = user.username
    first_name = user.first_name
    last_name = user.last_name or ""
    user_id = user.id
    admin_id = get_user_admin_id(user_id)
    # ✅ 超级管理员也记录用户信息（用自己的ID作为admin_id）
    if admin_id == 0 and user_id == OWNER_ID:
        admin_id = OWNER_ID
    if admin_id == 0:
        return  # 真正的未知用户不记录
    am = get_accounting_manager(admin_id)
    has_change, old_name, new_name, change_type = am.update_user_info(
        group_id, user.id, username, first_name, last_name, admin_id=admin_id
    )
    if has_change:
        if change_type:
            await update.message.reply_text(
                f"🚨🚨 **用户信息变更提醒**\n\n"
                f"用户 {old_name}\n"
                f"已更新{change_type}为：\n"
                f"{new_name}",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                f"🚨🚨 **用户信息变更提醒**\n\n"
                f"用户 {old_name} → {new_name}",
                parse_mode='Markdown'
            )

async def welcome_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member = update.chat_member
    if not chat_member:
        print("❌ 没有 chat_member 数据")
        return
    old_status = chat_member.old_chat_member.status if chat_member.old_chat_member else None
    new_status = chat_member.new_chat_member.status if chat_member.new_chat_member else None
    print(f"[欢迎检测] 群组: {chat_member.chat.title}")
    print(f"[欢迎检测] 旧状态: {old_status}")
    print(f"[欢迎检测] 新状态: {new_status}")
    if new_status == 'member' and old_status in ['left', 'kicked', 'restricted']:
        print(f"✅ 检测到新成员加入！")
        user = chat_member.new_chat_member.user
        chat = chat_member.chat
        first_name = user.first_name or ""
        username = user.username
        if username:
            welcome_text = f"{first_name} @{username}\n欢迎加入本群"
        else:
            welcome_text = f"{first_name}\n欢迎加入本群"
        try:
            await context.bot.send_message(chat_id=chat.id, text=welcome_text)
            print(f"✅ 欢迎消息已发送: {welcome_text}")
        except Exception as e:
            print(f"❌ 发送欢迎消息失败: {e}")
    elif old_status == 'member' and new_status in ['left', 'kicked']:
        print(f"👋 检测到成员离开")
        user = chat_member.new_chat_member.user
        print(f"离开的用户: {user.first_name}")

async def handle_group_service_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return
    chat = message.chat
    if chat.type not in ['group', 'supergroup']:
        return
    group_id = str(chat.id)
    if message.new_chat_members:
        for new_member in message.new_chat_members:
            if new_member.id == context.bot.id:
                logger.info(f"机器人加入群组 {chat.title}，跳过欢迎消息")
                continue
            user_name = new_member.full_name
            username = f"@{new_member.username}" if new_member.username else ""
            welcome_message = f"{user_name} {username}\n欢迎进入本群组"
            try:
                await message.reply_text(welcome_message)
                logger.info(f"✅ 已发送欢迎消息给 {user_name} (群组: {chat.title})")
            except Exception as e:
                logger.error(f"发送欢迎消息失败: {e}")
            user_id = new_member.id
            admin_id = get_user_admin_id(user_id)
            am = get_accounting_manager(admin_id)
            try:
                am.update_user_info(
                    group_id,
                    new_member.id,
                    new_member.username or "",
                    new_member.first_name or "",
                    new_member.last_name or "",
                    admin_id=admin_id
                )
            except Exception as e:
                logger.error(f"更新用户信息失败: {e}")
        return
    if message.left_chat_member:
        left_member = message.left_chat_member
        if left_member.id == context.bot.id:
            logger.info(f"机器人退出群组 {chat.title}，跳过告别消息")
            return
        user_name = left_member.full_name
        username = f"@{left_member.username}" if left_member.username else ""
        leave_message = f"{user_name} {username}\n退出了本群组"
        try:
            await message.reply_text(leave_message)
            logger.info(f"✅ 已发送退出消息 (用户: {user_name}, 群组: {chat.title})")
        except Exception as e:
            logger.error(f"发送退出消息失败: {e}")
        return

def get_service_message_handler():
    from telegram.ext import MessageHandler, filters
    return MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS | filters.StatusUpdate.LEFT_CHAT_MEMBER,
        handle_group_service_message
    )

# ---------- 美化 HTML 账单生成 ----------
def generate_beautiful_bill_html(stats: Dict, records: List[Dict], title: str = "当前账单") -> str:
    """
    生成美化版 HTML 账单
    """
    from datetime import datetime

    # 分离入款和出款
    income_records = [r for r in records if r['type'] == 'income']
    expense_records = [r for r in records if r['type'] == 'expense']

    # 按备注分组
    categories = {}
    no_category_records = []
    for r in income_records:
        category = r.get('category', '') or ''
        if category:
            if category not in categories:
                categories[category] = []
            categories[category].append(r)
        else:
            no_category_records.append(r)

    # 统计数据
    fee_rate = stats.get('fee_rate', 0)
    exchange_rate = stats.get('exchange_rate', 1)
    per_transaction_fee = stats.get('per_transaction_fee', 0)
    total_income_cny = stats.get('income_total', 0)
    total_income_usdt = stats.get('income_usdt', 0)
    total_expense_usdt = stats.get('expense_usdt', 0)
    pending_usdt = total_income_usdt - total_expense_usdt

    now = datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')

    def get_flag(cat):
        if not cat:
            return ''
        cat_lower = cat.lower().strip()
        if cat_lower in COUNTRY_FLAGS:
            return COUNTRY_FLAGS[cat_lower]
        for country, flag in COUNTRY_FLAGS.items():
            if country in cat_lower or cat_lower in country:
                return flag
        return ''

    def fmt(num, decimals=2):
        if num == int(num):
            return str(int(num))
        return f"{num:.{decimals}f}"

    # ========== 生成总体入款记录表格 ==========
    income_rows = ""
    for r in income_records:
        dt = beijing_time(r['created_at'])
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M')
        cat = r.get('category', '') or ''
        flag = get_flag(cat)
        cat_display = f"{flag} {cat}" if flag else (cat or '无')
        fr = r.get('fee_rate', 0)
        rate = r.get('rate', 0)
        per_fee = r.get('per_transaction_fee', 0)
        fee_display = f"{fmt(fr)}%" if fr == int(fr) else f"{fr}%"
        rate_display = fmt(rate) if rate == int(rate) else f"{rate:.2f}"
        per_fee_display = f"{fmt(per_fee)}元" if per_fee > 0 else "-"
        operator = r.get('display_name', r.get('username', '未知'))
        income_rows += f"""
        <tr>
            <td>{date_str} {time_str}</td>
            <td class="income-amount">+{fmt(r['amount'])}</td>
            <td>{fee_display}</td>
            <td>{rate_display}</td>
            <td>{per_fee_display}</td>
            <td class="usdt-amount">{fmt(r['amount_usdt'])} USDT</td>
            <td>{cat_display}</td>
            <td>{operator}</td>
        </tr>"""

    # ========== 生成总体出款记录表格 ==========
    expense_rows = ""
    for r in expense_records:
        dt = beijing_time(r['created_at'])
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M')
        operator = r.get('display_name', r.get('username', '未知'))
        expense_rows += f"""
        <tr>
            <td>{date_str} {time_str}</td>
            <td class="expense-amount">-{fmt(r['amount_usdt'])} USDT</td>
            <td>{operator}</td>
        </tr>"""

    # ========== 按操作人分组 ==========
    operator_income = {}
    for r in income_records:
        operator = r.get('display_name', r.get('username', '未知'))
        user_id = r.get('user_id', 0)
        key = f"op_{user_id}_{hash(operator) % 10000}"
        if key not in operator_income:
            operator_income[key] = {
                'name': operator,
                'user_id': user_id,
                'records': [],
                'total_cny': 0.0,
                'total_usdt': 0.0,
            }
        operator_income[key]['records'].append(r)
        operator_income[key]['total_cny'] += r['amount']
        operator_income[key]['total_usdt'] += r['amount_usdt']

    operator_expense = {}
    for r in expense_records:
        operator = r.get('display_name', r.get('username', '未知'))
        user_id = r.get('user_id', 0)
        key = f"op_{user_id}_{hash(operator) % 10000}"
        if key not in operator_expense:
            operator_expense[key] = {
                'name': operator,
                'user_id': user_id,
                'records': [],
                'total_usdt': 0.0,
            }
        operator_expense[key]['records'].append(r)
        operator_expense[key]['total_usdt'] += r['amount_usdt']

    # ========== 生成每个操作人的可折叠记录 ==========
    operator_sections = ""
    all_operator_keys = set(list(operator_income.keys()) + list(operator_expense.keys()))

    if all_operator_keys:
        for idx, key in enumerate(sorted(all_operator_keys, key=lambda k: operator_income.get(k, {}).get('total_cny', 0), reverse=True)):
            safe_key = key.replace('.', '_').replace('-', '_')
            op_income = operator_income.get(key, {})
            op_expense = operator_expense.get(key, {})

            name = op_income.get('name') or op_expense.get('name', '未知')
            income_count = len(op_income.get('records', []))
            income_cny = op_income.get('total_cny', 0)
            income_usdt = op_income.get('total_usdt', 0)
            expense_count = len(op_expense.get('records', []))
            expense_usdt = op_expense.get('total_usdt', 0)

            operator_sections += f"""
        <div class="card operator-card">
            <div class="operator-header" onclick="toggleSection('op_{safe_key}')">
                <span>👤 {name} · 入款{income_count}笔 {fmt(income_cny)}元 ≈ {fmt(income_usdt)}USDT · 出款{expense_count}笔 {fmt(expense_usdt)}USDT</span>
                <span class="toggle-icon" id="icon_op_{safe_key}">▼</span>
            </div>
            <div class="operator-content" id="op_{safe_key}">
"""
            if op_income.get('records'):
                operator_sections += f"""
                <div style="margin-bottom:12px;">
                    <div class="section-label">📈 入款明细（{income_count}笔）</div>
                    <div class="table-responsive">
                        <table>
                            <thead>
                                <tr><th>日期时间</th><th>金额(元)</th><th>手续费</th><th>汇率</th><th>单笔费用</th><th>USDT</th><th>分类</th></tr>
                            </thead>
                            <tbody>
"""
                for r in op_income['records']:
                    dt = beijing_time(r['created_at'])
                    date_str = dt.strftime('%Y-%m-%d')
                    time_str = dt.strftime('%H:%M')
                    cat = r.get('category', '') or ''
                    flag = get_flag(cat)
                    cat_display = f"{flag} {cat}" if flag else (cat or '无')
                    fr = r.get('fee_rate', 0)
                    rate = r.get('rate', 0)
                    per_fee = r.get('per_transaction_fee', 0)
                    fee_display = f"{fmt(fr)}%" if fr == int(fr) else f"{fr}%"
                    rate_display = fmt(rate) if rate == int(rate) else f"{rate:.2f}"
                    per_fee_display = f"{fmt(per_fee)}元" if per_fee > 0 else "-"
                    operator_sections += f"""
                                <tr>
                                    <td>{date_str} {time_str}</td>
                                    <td class="income-amount">+{fmt(r['amount'])}</td>
                                    <td>{fee_display}</td>
                                    <td>{rate_display}</td>
                                    <td>{per_fee_display}</td>
                                    <td class="usdt-amount">{fmt(r['amount_usdt'])} USDT</td>
                                    <td>{cat_display}</td>
                                </tr>"""
                operator_sections += """
                            </tbody>
                        </table>
                    </div>
                </div>
"""
            if op_expense.get('records'):
                operator_sections += f"""
                <div>
                    <div class="section-label">📉 出款明细（{expense_count}笔）</div>
                    <div class="table-responsive">
                        <table>
                            <thead>
                                <tr><th>日期时间</th><th>金额(USDT)</th></tr>
                            </thead>
                            <tbody>
"""
                for r in op_expense['records']:
                    dt = beijing_time(r['created_at'])
                    date_str = dt.strftime('%Y-%m-%d')
                    time_str = dt.strftime('%H:%M')
                    operator_sections += f"""
                                <tr>
                                    <td>{date_str} {time_str}</td>
                                    <td class="expense-amount">-{fmt(r['amount_usdt'])} USDT</td>
                                </tr>"""
                operator_sections += """
                            </tbody>
                        </table>
                    </div>
                </div>
"""
            operator_sections += """
            </div>
        </div>
"""

    # 分组统计 - 可折叠版（显示汇总+详细记录）
    category_section = ""
    if categories:
        category_section = """
        <h2 style="color:white; margin: 24px 0 16px 0; font-size: 20px;">📊 入款分组统计</h2>
"""
        for cat, group_records in categories.items():
            cat_cny = sum(r['amount'] for r in group_records)
            cat_usdt = sum(r['amount_usdt'] for r in group_records)
            flag = get_flag(cat)
            cat_display = f"{flag} {cat}" if flag else cat
            safe_cat = cat.replace('.', '_').replace('-', '_').replace(' ', '_')

            category_section += f"""
        <div class="card collapsible-card">
            <div class="collapsible-header" onclick="toggleSection('cat_{safe_cat}')">
                <span>📊 {cat_display} · {len(group_records)}笔 · {fmt(cat_cny)}元 ≈ {fmt(cat_usdt)}USDT</span>
                <span class="toggle-icon" id="icon_cat_{safe_cat}">▼</span>
            </div>
            <div class="collapsible-content" id="cat_{safe_cat}">
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr><th>日期时间</th><th>金额(元)</th><th>手续费</th><th>汇率</th><th>单笔费用</th><th>USDT</th><th>操作人</th></tr>
                        </thead>
                        <tbody>
    """
            for r in group_records:
                dt = beijing_time(r['created_at'])
                date_str = dt.strftime('%Y-%m-%d')
                time_str = dt.strftime('%H:%M')
                fr = r.get('fee_rate', 0)
                rate = r.get('rate', 0)
                per_fee = r.get('per_transaction_fee', 0)
                fee_display = f"{fmt(fr)}%" if fr == int(fr) else f"{fr}%"
                rate_display = fmt(rate) if rate == int(rate) else f"{rate:.2f}"
                per_fee_display = f"{fmt(per_fee)}元" if per_fee > 0 else "-"
                operator = r.get('display_name', r.get('username', '未知'))
                category_section += f"""
                            <tr>
                                <td>{date_str} {time_str}</td>
                                <td class="income-amount">+{fmt(r['amount'])}</td>
                                <td>{fee_display}</td>
                                <td>{rate_display}</td>
                                <td>{per_fee_display}</td>
                                <td class="usdt-amount">{fmt(r['amount_usdt'])} USDT</td>
                                <td>{operator}</td>
                            </tr>"""
            category_section += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - 记账账单</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        .header {{
            background: white;
            border-radius: 20px;
            padding: 28px 24px;
            margin-bottom: 20px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .header h1 {{
            font-size: 26px;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }}
        .header .time {{ color: #888; font-size: 13px; }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }}
        .summary-card {{
            background: white;
            border-radius: 16px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 4px 15px rgba(0,0,0,0.08);
            transition: transform 0.2s;
        }}
        .summary-card:hover {{ transform: translateY(-2px); }}
        .summary-card .icon {{ font-size: 28px; margin-bottom: 8px; }}
        .summary-card .label {{ font-size: 12px; color: #888; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; }}
        .summary-card .value {{ font-size: 24px; font-weight: 700; }}
        .summary-card.income {{ border-bottom: 4px solid #10b981; }}
        .summary-card.income .value {{ color: #10b981; }}
        .summary-card.expense {{ border-bottom: 4px solid #ef4444; }}
        .summary-card.expense .value {{ color: #ef4444; }}
        .summary-card.pending {{ border-bottom: 4px solid #f59e0b; }}
        .summary-card.pending .value {{ color: #f59e0b; }}
        .config-bar {{
            background: white;
            border-radius: 16px;
            padding: 16px 24px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
            gap: 12px;
            text-align: center;
        }}
        .config-item .config-label {{ font-size: 11px; color: #888; margin-bottom: 4px; }}
        .config-item .config-value {{ font-size: 18px; font-weight: 600; color: #333; }}
        .card {{
            background: white;
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }}
        .card h2 {{
            font-size: 16px;
            color: #1e293b;
            margin-bottom: 16px;
            padding-bottom: 12px;
            border-bottom: 2px solid #f1f5f9;
        }}
        /* ===== 可折叠区域 ===== */
        .collapsible-card {{
            padding: 0;
            overflow: hidden;
        }}
        .collapsible-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 14px 20px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-weight: 600;
            font-size: 15px;
            user-select: none;
            -webkit-tap-highlight-color: transparent;
        }}
        .collapsible-header:hover {{ opacity: 0.95; }}
        .collapsible-header .toggle-icon {{
            transition: transform 0.3s;
            font-size: 14px;
        }}
        .collapsible-header.collapsed .toggle-icon {{
            transform: rotate(-90deg);
        }}
        .collapsible-content {{
            padding: 20px;
        }}
        .collapsible-content.collapsed {{
            display: none;
        }}
        /* ===== 操作人卡片 ===== */
        .operator-card {{
            padding: 0;
            overflow: hidden;
        }}
        .operator-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 14px 20px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-weight: 600;
            font-size: 15px;
            user-select: none;
            -webkit-tap-highlight-color: transparent;
        }}
        .operator-header:hover {{ opacity: 0.95; }}
        .operator-header .toggle-icon {{
            transition: transform 0.3s;
            font-size: 14px;
        }}
        .operator-header.collapsed .toggle-icon {{
            transform: rotate(-90deg);
        }}
        .operator-content {{
            padding: 20px;
        }}
        .operator-content.collapsed {{
            display: none;
        }}
        .section-label {{
            font-size: 14px;
            font-weight: 600;
            color: #475569;
            margin-bottom: 8px;
            padding-left: 8px;
            border-left: 4px solid #667eea;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        th {{
            background: #f8fafc;
            padding: 10px 12px;
            text-align: left;
            font-weight: 600;
            color: #475569;
            border-bottom: 2px solid #e2e8f0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #f1f5f9;
            color: #334155;
        }}
        tr:hover {{ background: #f8fafc; }}
        .income-amount {{ color: #10b981; font-weight: 600; }}
        .expense-amount {{ color: #ef4444; font-weight: 600; }}
        .usdt-amount {{ color: #6366f1; font-weight: 500; }}
        .table-responsive {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
        .footer {{
            text-align: center;
            padding: 24px;
            color: rgba(255,255,255,0.8);
            font-size: 12px;
        }}
        @media (max-width: 640px) {{
            .header h1 {{ font-size: 20px; }}
            .summary-card .value {{ font-size: 18px; }}
            th, td {{ padding: 8px 6px; font-size: 11px; }}
            .collapsible-header, .operator-header {{ font-size: 13px; padding: 12px 14px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📋 {title}</h1>
            <div class="time">生成时间：{now}</div>
        </div>

        <div class="config-bar">
            <div class="config-item">
                <div class="config-label">💰 手续费率</div>
                <div class="config-value">{fee_rate}%</div>
            </div>
            <div class="config-item">
                <div class="config-label">💱 汇率</div>
                <div class="config-value">1 USDT = {exchange_rate} 元</div>
            </div>
            <div class="config-item">
                <div class="config-label">📝 单笔费用</div>
                <div class="config-value">{per_transaction_fee} 元</div>
            </div>
        </div>

        <div class="summary-grid">
            <div class="summary-card income">
                <div class="icon">💰</div>
                <div class="label">总入款</div>
                <div class="value">{fmt(total_income_cny)} 元</div>
                <div style="font-size:14px;color:#666;">≈ {fmt(total_income_usdt)} USDT</div>
            </div>
            <div class="summary-card expense">
                <div class="icon">📤</div>
                <div class="label">总出款</div>
                <div class="value">{fmt(total_expense_usdt)} USDT</div>
            </div>
            <div class="summary-card pending">
                <div class="icon">⏳</div>
                <div class="label">待下发</div>
                <div class="value">{fmt(pending_usdt)} USDT</div>
            </div>
        </div>

        {category_section}

        <!-- ===== 所有入款记录（可折叠） ===== -->
        <div class="card collapsible-card">
            <div class="collapsible-header" onclick="toggleSection('all_income')">
                <span>📈 所有入款记录（{len(income_records)}笔）</span>
                <span class="toggle-icon" id="icon_all_income">▼</span>
            </div>
            <div class="collapsible-content" id="all_income">
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr><th>日期时间</th><th>金额(元)</th><th>手续费</th><th>汇率</th><th>单笔费用</th><th>USDT</th><th>分类</th><th>操作人</th></tr>
                        </thead>
                        <tbody>{income_rows if income_rows else '<tr><td colspan="8" style="text-align:center;color:#999;">暂无入款记录</td></tr>'}</tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ===== 所有出款记录（可折叠） ===== -->
        <div class="card collapsible-card">
            <div class="collapsible-header" onclick="toggleSection('all_expense')">
                <span>📉 所有出款记录（{len(expense_records)}笔）</span>
                <span class="toggle-icon" id="icon_all_expense">▼</span>
            </div>
            <div class="collapsible-content" id="all_expense">
                <div class="table-responsive">
                    <table>
                        <thead>
                            <tr><th>日期时间</th><th>金额(USDT)</th><th>操作人</th></tr>
                        </thead>
                        <tbody>{expense_rows if expense_rows else '<tr><td colspan="3" style="text-align:center;color:#999;">暂无出款记录</td></tr>'}</tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ===== 各操作人详细记录 ===== -->
        <h2 style="color:white; margin: 24px 0 16px 0; font-size: 20px;">👤 各操作人详细记录</h2>
        {operator_sections}

        <div class="footer">
            由 Telegram 记账机器人自动生成
        </div>
    </div>

    <script>
        function toggleSection(id) {{
            var content = document.getElementById(id);
            if (!content) return;
            var header = content.previousElementSibling;
            var icon = document.getElementById('icon_' + id);

            if (content.classList.contains('collapsed')) {{
                content.classList.remove('collapsed');
                header.classList.remove('collapsed');
            }} else {{
                content.classList.add('collapsed');
                header.classList.add('collapsed');
            }}
        }}
    </script>
</body>
</html>"""

    return html


async def handle_view_current_bill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理「查看当前账单」按钮回调，生成 HTML 文件并发送"""
    query = update.callback_query
    await query.answer("正在生成账单...")

    chat = query.message.chat
    group_id = str(chat.id)
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    # 获取当前账单数据
    stats = am.get_current_stats(group_id, admin_id=admin_id)
    records = am.get_current_records(group_id, admin_id=admin_id)

    if not records:
        await query.answer("当前没有账单记录", show_alert=True)
        return

    # 发送临时提示消息
    loading_msg = await query.message.reply_text("📊 正在生成美化账单，请稍候...")

    try:
        group_name = chat.title or chat.first_name or "当前群组"

        # 生成 HTML
        html_content = generate_beautiful_bill_html(stats, records, f"{group_name} - 当前账单")

        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', encoding='utf-8', delete=False) as f:
            f.write(html_content)
            temp_path = f.name

        try:
            with open(temp_path, 'rb') as f:
                await query.message.reply_document(
                    document=f,
                    filename=f"账单_{group_name}_当前.html",
                    caption=f"📊 **{group_name}** 当前账单\n\n"
                            f"💰 总入款：{stats['income_total']:.2f} 元 ≈ {stats['income_usdt']:.2f} USDT\n"
                            f"📤 总出款：{stats['expense_usdt']:.2f} USDT\n"
                            f"⏳ 待下发：{stats['pending_usdt']:.2f} USDT\n"
                            f"📝 共 {stats['income_count'] + stats['expense_count']} 条记录",
                    parse_mode='Markdown'
                )
        finally:
            try:
                os.unlink(temp_path)
            except:
                pass

        await loading_msg.delete()

    except Exception as e:
        logger.error(f"生成账单HTML失败: {e}")
        await loading_msg.edit_text(f"❌ 生成失败：{str(e)[:100]}")

def get_conversation_handler():
    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(handle_date_selection, pattern="^acct_date_"),
            CallbackQueryHandler(handle_clear_current_confirm, pattern="^clear_current_confirm$"),
            CallbackQueryHandler(handle_clear_current_cancel, pattern="^clear_current_cancel$"),
            CallbackQueryHandler(handle_clear_all_confirm, pattern="^clear_all_confirm$"),
            CallbackQueryHandler(handle_clear_all_cancel, pattern="^clear_all_cancel$"),
        ],
        states={
            ACCOUNTING_DATE_SELECT: [
                CallbackQueryHandler(handle_date_selection, pattern="^acct_date_"),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^acct_cancel$"),
            ],
            ACCOUNTING_CONFIRM_CLEAR: [
                CallbackQueryHandler(handle_clear_current_confirm, pattern="^clear_current_confirm$"),
                CallbackQueryHandler(handle_clear_current_cancel, pattern="^clear_current_cancel$"),
            ],
            ACCOUNTING_CONFIRM_CLEAR_ALL: [
                CallbackQueryHandler(handle_clear_all_confirm, pattern="^clear_all_confirm$"),
                CallbackQueryHandler(handle_clear_all_cancel, pattern="^clear_all_cancel$"),
            ],
            ACCOUNTING_VIEW_PAGE: [
                CallbackQueryHandler(handle_bill_pagination, pattern="^bill_page_"),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^bill_close$"),
            ],
            ACCOUNTING_YEAR_SELECT: [
                CallbackQueryHandler(handle_year_selection, pattern="^bill_year_"),
                CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^acct_cancel$"),
            ],
            ACCOUNTING_MONTH_SELECT: [
                CallbackQueryHandler(handle_month_selection, pattern="^bill_month_"),
                CallbackQueryHandler(handle_bill_navigation, pattern="^bill_back_to_years$"),
            ],
            ACCOUNTING_DATE_SELECT_PAGE: [
                CallbackQueryHandler(handle_day_selection, pattern="^bill_day_"),
                CallbackQueryHandler(handle_bill_navigation, pattern="^bill_days_prev$"),
                CallbackQueryHandler(handle_bill_navigation, pattern="^bill_days_next$"),
                CallbackQueryHandler(handle_bill_navigation, pattern="^bill_back_to_months$"),
            ],
        },
        fallbacks=[],
        per_message=False,
    )
    return conv_handler

async def handle_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import ReplyKeyboardMarkup, KeyboardButton
    keyboard = [
        [KeyboardButton("➕ 添加我进群"), KeyboardButton("◀️ 返回主菜单")]
    ]
    message = (
        "📒 **记账功能说明**\n\n"
        "💰 **入款操作**\n"
        "`+1000` 普通入款\n"
        "`+1000 德国` 带分类\n"
        "`+1000/7.2` 临时汇率\n"
        "`+1000*5` 临时费率\n"
        "`+1000*5/7.2 德国` 临时费率+汇率+分类\n"
        "`-500` 修正入款\n\n"
        "💸 **出款操作**\n"
        "`下发100u` 出款\n"
        "`下发-50u` 修正出款\n\n"
        "⚙️ **群组配置**\n"
        "`设置手续费 5` 手续费率\n"
        "`设置汇率 7.2` 汇率\n"
        "`设置单笔费用 2` 单笔费用\n\n"
        "📊 **查询账单**\n"
        "`今日总` / `总` / `当前账单`\n"
        "`查询账单` 按日期查询\n"
        "`导出账单` 导出HTML文件\n\n"
        "🗑️ **管理**\n"
        "`结束账单` 保存并结束\n"
        "`清理账单` 清空当前\n"
        "`清理总账单` 清空全部\n"
        "`移除上一笔` 撤销\n"
        "`撤销账单` 回复指定入款撤销\n\n"
        "🧮 **计算器**\n"
        "`100+200` `(10+5)*3` `sqrt(100)` `2^3`\n\n"
        "💡 **智能识别**\n"
        "• 发送USDT地址自动查余额\n"
        "• 分类备注自动显示国旗\n"
        "• `@机器人 问题` AI问答\n\n"
        "⚠️ **权限**\n"
        "• 记账/配置/AI需操作员权限\n"
        "• 地址查询/计算器所有人可用"
    )
    await update.message.reply_text(
        message,
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode='Markdown'
    )

# ==================== 群组消息主处理函数 ====================

async def handle_operator_mention(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理群组内通过 @ 添加/删除临时操作人"""
    message = update.message
    chat = update.effective_chat
    user = message.from_user  # ✅ 这个函数是独立的，可以正确定义 user
    user_id = user.id
    text = message.text.strip()

    # 权限检查
    from auth import is_authorized, get_user_admin_id
    if not is_authorized(user_id, require_full_access=False):  # ✅ 使用 user_id
        await message.reply_text("❌ 您没有权限管理操作人")
        return

    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await message.reply_text("❌ 您不是任何管理员，无法管理操作人")
        return

    is_add = text.startswith('添加操作人')
    is_remove = text.startswith('删除操作人')

    # 获取消息中 @ 的用户
    mentioned_users = []
    if message.entities:
        for entity in message.entities:
            if entity.type == "text_mention" and entity.user:
                mentioned_users.append(entity.user)
            elif entity.type == "mention":
                username = text[entity.offset:entity.offset + entity.length].lstrip('@')
                mentioned_users.append(username)

    if not mentioned_users:
        await message.reply_text(
            f"❌ 请使用 @ 指定用户\n\n"
            f"用法示例：\n"
            f"`添加操作人 @username1 @username2`\n"
            f"`删除操作人 @username1 @username2`",
            parse_mode="Markdown"
        )
        return

    # 解析用户
    from auth import batch_add_temp_operators, batch_remove_temp_operators

    user_ids_to_process = []
    user_info_list = []

    for mentioned in mentioned_users:
        if isinstance(mentioned, str):
            found = False
            from auth import admins
            for aid in admins.keys():
                from db_manager import get_conn
                conn = get_conn(aid)
                row = conn.execute(
                    "SELECT user_id, first_name, username FROM group_users WHERE username = ? LIMIT 1",
                    (mentioned,)
                ).fetchone()
                if row:
                    user_ids_to_process.append(row[0])
                    user_info_list.append({
                        "id": row[0],
                        "name": row[1] or row[2] or mentioned
                    })
                    found = True
                    break
            if not found:
                user_info_list.append({
                    "id": None,
                    "name": f"@{mentioned}",
                    "unknown": True
                })
        else:
            user_ids_to_process.append(mentioned.id)
            user_info_list.append({
                "id": mentioned.id,
                "name": mentioned.first_name or mentioned.username or str(mentioned.id)
            })

    valid_ids = [u["id"] for u in user_info_list if u["id"] is not None and not u.get("unknown")]
    unknown_users = [u for u in user_info_list if u.get("unknown")]

    if is_add:
        if not valid_ids:
            if unknown_users:
                await message.reply_text(
                    f"❌ 无法找到以下用户，请确认用户名正确：\n"
                    + "\n".join([f"• {u['name']}" for u in unknown_users]) +
                    "\n\n💡 提示：请使用 @用户名 或直接回复用户消息"
                )
            return

        results = await batch_add_temp_operators(valid_ids, admin_id, context)

        reply_parts = []
        if results["success"]:
            success_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["success"]]
            reply_parts.append(f"✅ 已添加临时操作人：{', '.join(success_names)}")
        if results["already_exists"]:
            exists_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["already_exists"]]
            reply_parts.append(f"⚠️ 以下用户已是操作人：{', '.join(exists_names)}")
        if results["failed"]:
            failed_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["failed"]]
            reply_parts.append(f"❌ 添加失败：{', '.join(failed_names)}")
        if unknown_users:
            reply_parts.append(f"❓ 未找到：{', '.join([u['name'] for u in unknown_users])}")

        reply = "\n".join(reply_parts)
        reply += "\n\n💡 临时操作人只能使用记账功能"

    elif is_remove:
        if not valid_ids:
            if unknown_users:
                await message.reply_text(
                    f"❌ 无法找到以下用户：\n"
                    + "\n".join([f"• {u['name']}" for u in unknown_users])
                )
            return

        results = batch_remove_temp_operators(valid_ids)

        reply_parts = []
        if results["success"]:
            success_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["success"]]
            reply_parts.append(f"✅ 已删除临时操作人：{', '.join(success_names)}")
        if results["not_found"]:
            notfound_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["not_found"]]
            reply_parts.append(f"⚠️ 以下用户不是临时操作人：{', '.join(notfound_names)}")
        if results["failed"]:
            failed_names = [next((u["name"] for u in user_info_list if u["id"] == uid), str(uid)) for uid in results["failed"]]
            reply_parts.append(f"❌ 删除失败：{', '.join(failed_names)}")
        if unknown_users:
            reply_parts.append(f"❓ 未找到：{', '.join([u['name'] for u in unknown_users])}")

        reply = "\n".join(reply_parts)

    await message.reply_text(reply)

async def handle_batch_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """处理组合设置：同时设置手续费、汇率、单笔费用"""
    message = update.message
    chat = update.effective_chat

    if chat.type not in ['group', 'supergroup']:
        await message.reply_text("❌ 此功能仅在群组中可用")
        return

    # ✅ 使用通用解析函数
    fee_rate, exchange_rate, per_transaction_fee = parse_batch_settings(text)

    if fee_rate is None and exchange_rate is None and per_transaction_fee is None:
        await message.reply_text(
            "❌ 未识别到有效的设置参数\n\n"
            "💡 用法示例：\n"
            "`设置手续费2 设置汇率20 设置单笔费用15`\n"
            "`设置汇率20 设置手续费2 设置单笔费用15`",
            parse_mode="Markdown"
        )
        return

    group_id = str(chat.id)
    user_id = message.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    results = []

    if fee_rate is not None:
        success = am.set_fee_rate(group_id, fee_rate, admin_id)
        if success:
            fee_display = int(fee_rate) if fee_rate == int(fee_rate) else fee_rate
            results.append(f"✅ 手续费率已设置为：{fee_display}%")
        else:
            results.append("❌ 手续费率设置失败")

    if exchange_rate is not None:
        success = am.set_exchange_rate(group_id, exchange_rate, admin_id)
        if success:
            rate_display = int(exchange_rate) if exchange_rate == int(exchange_rate) else exchange_rate
            results.append(f"✅ 汇率已设置为：{rate_display}")
        else:
            results.append("❌ 汇率设置失败")

    if per_transaction_fee is not None:
        success = am.set_per_transaction_fee(group_id, per_transaction_fee, admin_id)
        if success:
            fee_display = int(per_transaction_fee) if per_transaction_fee == int(per_transaction_fee) else per_transaction_fee
            results.append(f"✅ 单笔费用已设置为：{fee_display} 元")
        else:
            results.append("❌ 单笔费用设置失败")

    reply = "⚙️ **组合设置结果**\n\n" + "\n".join(results)

    if any("✅" in r for r in results):
        session = am.get_or_create_session(group_id, admin_id)
        reply += "\n\n📋 **当前默认配置**\n"
        reply += f"💰 手续费：{session['fee_rate']}%\n"
        reply += f"💱 汇率：{session['exchange_rate']}\n"
        reply += f"📝 单笔费用：{session.get('per_transaction_fee', 0)} 元"

    await message.reply_text(reply, parse_mode="Markdown")


def parse_batch_settings(text: str):
    """
    解析组合设置文本，支持多种格式和任意顺序：
    - 设置手续费2 设置汇率20 设置单笔费用15
    - 设置汇率20 手续费3 单笔费用2
    - 设置 @xxx 手续费5 汇率10 单笔费用12
    """
    import re

    fee_rate = None
    exchange_rate = None
    per_transaction_fee = None

    # 先移除 @用户名 部分，避免干扰数字匹配
    clean_text = re.sub(r'@\S+', '', text)

    # ✅ 提取手续费（同时支持"设置手续费"和单独的"手续费"）
    fee_match = re.search(r'(?:设置)?手续费\s*(\d+(?:\.\d+)?)', clean_text)
    if fee_match:
        fee_rate = float(fee_match.group(1))

    # ✅ 提取汇率（同时支持"设置汇率"和单独的"汇率"）
    rate_match = re.search(r'(?:设置)?汇率\s*(\d+(?:\.\d+)?)', clean_text)
    if rate_match:
        exchange_rate = float(rate_match.group(1))

    # ✅ 提取单笔费用（同时支持"设置单笔费用"和单独的"单笔费用"）
    fee_per_match = re.search(r'(?:设置)?单笔费用\s*(\d+(?:\.\d+)?)', clean_text)
    if fee_per_match:
        per_transaction_fee = float(fee_per_match.group(1))

    return fee_rate, exchange_rate, per_transaction_fee


async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.message
    if not message or chat.type not in ['group', 'supergroup']:
        return
    text = message.text.strip() if message.text else ""
    user_id = message.from_user.id
    admin_id = get_user_admin_id(user_id)
    am = get_accounting_manager(admin_id)

    # ========== USDT 地址查询（不需要权限）==========
    is_addr, address, chain_type = is_valid_address(text)
    if is_addr:
        user = message.from_user
        group_id = str(chat.id)
        status_msg = await message.reply_text(f"🔍 正在查询 {chain_type} 地址余额，请稍候...")
        if chain_type == 'TRC20':
            result = await query_trc20_balance(address)
        else:
            result = await query_erc20_balance(address)
        if result.get('success'):
            balance = result['balance']
            stats = None
            if am:
                am.record_address_query(group_id, address, chain_type, user.id, user.username or user.first_name, balance, admin_id)
                stats = am.get_address_stats(address, admin_id)
            if stats:
                first_time = datetime.fromtimestamp(stats['first_query']).strftime('%Y-%m-%d %H:%M') if stats.get('first_query') else '首次'
                last_time = datetime.fromtimestamp(stats['last_query']).strftime('%Y-%m-%d %H:%M') if stats.get('last_query') else '刚刚'
            else:
                first_time = '首次'
                last_time = '刚刚'
            reply = (
                f"💰 **USDT 地址查询结果**\n\n"
                f"📌 地址：`{address}`\n"
                f"⛓️ 网络：{chain_type}\n"
                f"💵 余额：**{balance:.2f} USDT**\n"
            )
            await status_msg.edit_text(reply, parse_mode='Markdown')
        else:
            error_msg = result.get('error', '查询失败，请稍后重试')
            await status_msg.edit_text(f"❌ 查询失败：{error_msg}")
        return

    # ========== 判断是否跳过用户信息追踪 ==========
    is_user_config_cmd = (
        ('设置' in text and ('手续费' in text or '汇率' in text or '单笔费用' in text) and 
         ('@' in text or (message.reply_to_message and message.reply_to_message.from_user))) or
        (text.startswith('删除') and '配置' in text) or
        (text == '查看配置' or text == '查看设置') or
        (text.count('设置手续费') + text.count('设置汇率') + text.count('设置单笔费用') >= 2) or
        ('设置' in text and sum([1 for k in ['手续费', '汇率', '单笔费用'] if k in text]) >= 2) or
        (text.startswith('添加操作人') or text.startswith('删除操作人'))
    )

    if not is_user_config_cmd:
        await handle_user_info_tracking(update, context)

    if not text:
        return

    # ========== 操作人管理（@方式）==========
    if message.reply_to_message and text in ['添加操作人', '删除操作人']:
        if not is_authorized(user_id, require_full_access=False):
            await message.reply_text("❌ 您没有权限管理操作人")
            return
        replied_user = message.reply_to_message.from_user
        if admin_id == 0:
            await message.reply_text("❌ 您不是任何管理员，无法管理操作人")
            return
        from auth import add_temp_operator, remove_temp_operator
        if text == '添加操作人':
            result = await add_temp_operator(replied_user.id, admin_id, context)
            if result:
                display_name = replied_user.first_name or replied_user.username or str(replied_user.id)
                await message.reply_text(f"✅ 已添加临时操作人：{display_name}\n💡 临时操作人只能使用记账功能")
            else:
                await message.reply_text("❌ 添加失败，该用户可能已是操作人")
        elif text == '删除操作人':
            result = remove_temp_operator(replied_user.id)
            if result:
                display_name = replied_user.first_name or replied_user.username or str(replied_user.id)
                await message.reply_text(f"✅ 已删除临时操作人：{display_name}")
            else:
                await message.reply_text("❌ 该用户不是临时操作人")
        return

    if text.startswith('添加操作人') or text.startswith('删除操作人'):
        await handle_operator_mention(update, context)
        return

    # ========== 计算器 ==========
    await handle_calculator(update, context)

    # ========== 权限检查 ==========
    if not is_authorized(user_id, require_full_access=False):
        return

    # ========== 用户个性化配置（带 @ 或回复消息）==========
    if '设置' in text and ('手续费' in text or '汇率' in text or '单笔费用' in text) and ('@' in text or message.reply_to_message):
        await handle_user_config_settings(update, context, text)
        return

    # ========== 删除指定人配置 ==========
    if text.startswith('删除') and '配置' in text and ('@' in text or message.reply_to_message):
        await handle_delete_user_config(update, context, text)
        return

    # ========== 查看配置 ==========
    if text == '查看配置' or text == '查看设置':
        await handle_view_config(update, context)
        return

    # ========== 组合/批量设置（多个参数）==========
    # 统计带"设置"前缀的次数
    settings_with_prefix = sum([
        text.count('设置手续费'),
        text.count('设置汇率'),
        text.count('设置单笔费用')
    ])
    # 统计参数关键字出现次数
    params_count = sum([1 for k in ['手续费', '汇率', '单笔费用'] if k in text])

    # 判断是否为组合设置
    if settings_with_prefix >= 2 or (text.startswith('设置') and params_count >= 2):
        await handle_batch_settings(update, context, text)
        return

    # ========== 单独设置命令 ==========
    if text.startswith('设置手续费'):
        try:
            rate_str = text.replace('设置手续费', '').strip()
            if rate_str:
                rate = float(rate_str)
                await handle_set_fee(update, context, rate)
        except:
            await message.reply_text("❌ 格式错误：设置手续费 数字（如：设置手续费5）")
        return
    elif text.startswith('设置汇率'):
        try:
            rate_str = text.replace('设置汇率', '').strip()
            if rate_str:
                rate = float(rate_str)
                await handle_set_exchange(update, context, rate)
        except:
            await message.reply_text("❌ 格式错误：设置汇率 数字（如：设置汇率7.2）")
        return
    elif text.startswith('设置单笔费用'):
        try:
            fee_str = text.replace('设置单笔费用', '').strip()
            if not fee_str:
                await message.reply_text("❌ 格式错误：设置单笔费用 数字（如：设置单笔费用10）")
                return
            fee = float(fee_str)
            await handle_set_per_transaction_fee(update, context, fee)
        except ValueError:
            await message.reply_text("❌ 金额格式错误，请输入数字")
        except Exception as e:
            await message.reply_text(f"❌ 设置失败：{str(e)[:50]}")
        return

    # ========== 账单命令 ==========
    elif text == '结束账单':
        await handle_end_bill(update, context)
        return
    elif text == '今日总':
        await handle_today_stats(update, context, admin_id=admin_id)
        return
    elif text == '总':
        await handle_total_stats(update, context, admin_id=admin_id)
        return
    elif text == '当前账单':
        await handle_current_bill(update, context, admin_id=admin_id)
        return
    elif text == '查询账单':
        await handle_query_bill(update, context, admin_id=admin_id)
        return
    elif text == '导出账单':
        await handle_export_bill(update, context)
        return
    elif text in ['清理账单', '清空账单']:
        await handle_clear_bill(update, context)
        return
    elif text in ['清理总账单', '清空总账单', '清空所有账单']:
        await handle_clear_all_bill(update, context)
        return
    elif text == '移除上一笔' or text == '删除上一笔':
        await handle_remove_last_record(update, context)
        return
    elif text == '撤销账单':
        await handle_revoke_record(update, context)
        return

    # ========== 入款 +金额 ==========
    elif text.startswith('+'):
        try:
            content = text[1:].strip()
            temp_rate = None
            temp_fee = None
            category = ""
            amount_str = content
            if '*' in content:
                star_parts = content.split('*', 1)
                amount_str = star_parts[0].strip()
                rest = star_parts[1].strip()
                if '/' in rest:
                    fee_part, rate_part = rest.split('/', 1)
                    temp_fee_str = fee_part.replace('%', '').strip()
                    try:
                        temp_fee = float(temp_fee_str)
                    except ValueError:
                        temp_fee = None
                    rate_part_clean = rate_part.split(' ', 1)[0].strip()
                    try:
                        temp_rate = float(rate_part_clean)
                    except ValueError:
                        temp_rate = None
                    if ' ' in rate_part:
                        category = rate_part.split(' ', 1)[1].strip()
                else:
                    fee_part = rest.split(' ', 1)[0].strip()
                    temp_fee_str = fee_part.replace('%', '').strip()
                    try:
                        temp_fee = float(temp_fee_str)
                    except ValueError:
                        temp_fee = None
                    if ' ' in rest:
                        category = rest.split(' ', 1)[1].strip()
            elif '/' in content:
                parts = content.split('/', 1)
                amount_str = parts[0].strip()
                rest = parts[1].strip()
                if ' ' in rest:
                    rate_part, category = rest.split(' ', 1)
                    try:
                        temp_rate = float(rate_part)
                    except ValueError:
                        category = rest
                        temp_rate = None
                else:
                    try:
                        temp_rate = float(rest)
                    except ValueError:
                        category = rest
                        temp_rate = None
            else:
                if ' ' in amount_str:
                    parts = amount_str.split(' ', 1)
                    amount_str = parts[0]
                    category = parts[1]
            if amount_str:
                amount = float(amount_str)
                await handle_add_income(update, context, amount, is_correction=False,
                       category=category, temp_rate=temp_rate, temp_fee=temp_fee,
                       admin_id=admin_id)
            else:
                await message.reply_text("❌ 格式错误")
        except ValueError:
            await message.reply_text("❌ 金额格式错误，请输入数字")
        except Exception as e:
            logger.error(f"解析入款失败: {e}")
            await message.reply_text("❌ 格式错误")
        return

    # ========== 修正入款 -金额 ==========
    elif text.startswith('-') and len(text) > 1:
        try:
            content = text[1:].strip()
            temp_rate = None
            temp_fee = None
            category = ""
            amount_str = content
            if '*' in content:
                star_parts = content.split('*', 1)
                amount_str = star_parts[0].strip()
                rest = star_parts[1].strip()
                if '/' in rest:
                    fee_part, rate_part = rest.split('/', 1)
                    temp_fee_str = fee_part.replace('%', '').strip()
                    try:
                        temp_fee = float(temp_fee_str)
                    except ValueError:
                        temp_fee = None
                    rate_part_clean = rate_part.split(' ', 1)[0].strip()
                    try:
                        temp_rate = float(rate_part_clean)
                    except ValueError:
                        temp_rate = None
                    if ' ' in rate_part:
                        category = rate_part.split(' ', 1)[1].strip()
                else:
                    fee_part = rest.split(' ', 1)[0].strip()
                    temp_fee_str = fee_part.replace('%', '').strip()
                    try:
                        temp_fee = float(temp_fee_str)
                    except ValueError:
                        temp_fee = None
                    if ' ' in rest:
                        category = rest.split(' ', 1)[1].strip()
            elif '/' in content:
                parts = content.split('/', 1)
                amount_str = parts[0].strip()
                rest = parts[1].strip()
                if ' ' in rest:
                    rate_part, category = rest.split(' ', 1)
                    try:
                        temp_rate = float(rate_part)
                    except ValueError:
                        category = rest
                        temp_rate = None
                else:
                    try:
                        temp_rate = float(rest)
                    except ValueError:
                        category = rest
                        temp_rate = None
            else:
                if ' ' in amount_str:
                    parts = amount_str.split(' ', 1)
                    amount_str = parts[0]
                    category = parts[1]
            if amount_str:
                amount = float(amount_str)
                await handle_add_income(update, context, amount, is_correction=True,
                       category=category, temp_rate=temp_rate, temp_fee=temp_fee,
                       admin_id=admin_id)
            else:
                await message.reply_text("❌ 格式错误")
        except ValueError:
            await message.reply_text("❌ 金额格式错误，请输入数字")
        except Exception as e:
            logger.error(f"解析修正入款失败: {e}")
            await message.reply_text("❌ 格式错误")
        return

    # ========== 出款 下发金额u ==========
    elif text.startswith('下发') and 'u' in text and not text.startswith('下发-'):
        try:
            amount_str = text.replace('下发', '').replace('u', '').strip()
            if amount_str:
                amount = float(amount_str)
                await handle_add_expense(update, context, amount, is_correction=False, admin_id=admin_id)
            else:
                await message.reply_text("❌ 格式错误：下发金额u（如：下发100u）")
        except:
            await message.reply_text("❌ 格式错误：下发金额u（如：下发100u）")
        return

    # ========== 修正出款 下发-金额u ==========
    elif text.startswith('下发-') and 'u' in text:
        try:
            amount_str = text.replace('下发-', '').replace('u', '').strip()
            if amount_str:
                amount = float(amount_str)
                await handle_add_expense(update, context, amount, is_correction=True, admin_id=admin_id)
            else:
                await message.reply_text("❌ 格式错误：下发-金额u（如：下发-50u）")
        except:
            await message.reply_text("❌ 格式错误：下发-金额u（如：下发-50u）")
        return

    # ========== AI 对话 ==========
    bot_username = context.bot.username
    is_at_bot = text.startswith(f"@{bot_username}") or f"@{bot_username}" in text
    if is_at_bot:
        question = text.replace(f"@{bot_username}", "").strip()
        if not question:
            return
        if not is_authorized(message.from_user.id, require_full_access=True):
            try:
                await context.bot.send_message(
                    chat_id=message.chat_id,
                    text="❌ AI 对话功能仅限管理员和操作员使用\n\n如需使用，请联系 @ChinaEdward 申请权限",
                    reply_to_message_id=message.message_id
                )
            except Exception as e:
                print(f"[ERROR] 发送权限提示失败: {e}")
                await message.reply_text("❌ 无权限使用 AI 对话")
            return
        thinking_msg = await message.reply_text("🤔 思考中...")
        from handlers.ai_client import get_ai_client
        ai_client = get_ai_client()
        reply = await ai_client.chat_with_data(
            prompt=question,
            group_id=str(chat.id),
            user_id=message.from_user.id
        )
        if len(reply) > 4000:
            reply = reply[:4000] + "...\n\n(回复过长已截断)"
        await thinking_msg.edit_text(reply)
        return
