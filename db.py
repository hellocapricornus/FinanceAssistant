# db.py - 物理隔离版本，所有业务数据存储在管理员独立数据库
import sqlite3
import os
import time
import re
from config import OWNER_ID
from db_manager import get_conn, init_admin_db, init_master_db, MASTER_DB_PATH, get_db

# ---------- 数据库初始化（兼容旧调用）----------
def init_db():
    from db_manager import init_master_db
    init_master_db()
    print("✅ 数据库系统初始化完成（物理隔离模式）")

DB_PATH = MASTER_DB_PATH

# ---------- 兼容旧代码的函数（不关闭连接）----------
def get_db_connection(admin_id: int = 0):
    """返回指定管理员的数据库连接，不自动关闭"""
    return get_conn(admin_id)

# ---------- 群组操作 ----------
def save_group(admin_id: int, group_id: str, title: str, category: str = None, admin_id_extra: int = 0):
    if admin_id == 0:
        return
    try:
        with get_db(admin_id) as conn:
            c = conn.cursor()
            c.execute("SELECT category, joined_at, admin_id FROM groups WHERE group_id = ?", (group_id,))
            row = c.fetchone()
            if row:
                existing_category = row[0]
                joined_at = row[1] if row[1] else 0
                existing_admin_id = row[2] if row[2] else admin_id
            else:
                existing_category = None
                joined_at = 0
                existing_admin_id = admin_id

            final_category = category if category is not None else (existing_category if existing_category else '未分类')
            if joined_at == 0:
                joined_at = int(time.time())

            if final_category == '未分类':
                from db import detect_country_from_group_name, ensure_country_category
                country = detect_country_from_group_name(title)
                if country and ensure_country_category(admin_id, country):
                    final_category = country

            c.execute("""
                INSERT OR REPLACE INTO groups 
                (group_id, title, last_seen, category, joined_at, admin_id) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (group_id, title, int(time.time()), final_category, joined_at, existing_admin_id))
    except Exception as e:
        print(f"❌ [DB Error] 保存群组失败: {e}")

def delete_group_from_db(admin_id: int, group_id: str):
    if admin_id == 0:
        return
    conn = get_conn(admin_id)
    conn.execute("DELETE FROM groups WHERE group_id = ?", (group_id,))
    conn.commit()

def get_all_groups_from_db(admin_id: int = 0, category: str = None):
    if admin_id == 0:
        return []
    conn = get_conn(admin_id)

    # ✅ 添加表存在检查
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='groups'")
    if not c.fetchone():
        return []
        
    if category:
        rows = conn.execute("SELECT group_id, title, last_seen, category, joined_at, admin_id FROM groups WHERE category = ?", (category,)).fetchall()
    else:
        rows = conn.execute("SELECT group_id, title, last_seen, category, joined_at, admin_id FROM groups").fetchall()
    result = []
    for row in rows:
        joined_at = row["joined_at"] or row["last_seen"] or 0
        result.append({
            "id": row["group_id"],
            "title": row["title"],
            "last_seen": row["last_seen"],
            "category": row["category"],
            "joined_at": joined_at,
            "admin_id": row["admin_id"] or 0
        })
    return result

def update_group_category(admin_id: int, group_id: str, category: str):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    try:
        conn.execute("UPDATE groups SET category = ? WHERE group_id = ?", (category, group_id))
        conn.commit()
        return True
    except:
        conn.rollback()
        return False

def get_groups_by_category(admin_id: int = 0):
    if admin_id == 0:
        return {}
    conn = get_conn(admin_id)
    rows = conn.execute("SELECT category, COUNT(*) as count FROM groups GROUP BY category ORDER BY count DESC").fetchall()
    return {r[0]: r[1] for r in rows}

def update_group_category_if_needed(admin_id: int, group_id: str, group_name: str):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    row = conn.execute("SELECT category FROM groups WHERE group_id = ?", (group_id,)).fetchone()
    if not row or row[0] != '未分类':
        return False
    country = detect_country_from_group_name(group_name)
    if country:
        if ensure_country_category(admin_id, country):
            conn.execute("UPDATE groups SET category = ? WHERE group_id = ?", (country, group_id))
            conn.commit()
            return True
    return False

# ---------- 分类操作 ----------
def get_all_categories(admin_id: int = 0):
    if admin_id == 0:
        return []
    conn = get_conn(admin_id)
    rows = conn.execute("SELECT category_name, description FROM group_categories ORDER BY category_id").fetchall()
    return [{"name": r[0], "description": r[1] or ""} for r in rows]

def add_category(admin_id: int, category_name: str, description: str = ""):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    try:
        conn.execute("INSERT INTO group_categories (category_name, description, created_at) VALUES (?, ?, ?)",
                     (category_name, description, int(time.time())))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def delete_category(admin_id: int, category_name: str):
    if admin_id == 0 or category_name == '未分类':
        return False
    conn = get_conn(admin_id)
    try:
        conn.execute("UPDATE groups SET category='未分类' WHERE category=?", (category_name,))
        conn.execute("DELETE FROM group_categories WHERE category_name=?", (category_name,))
        conn.commit()
        return True
    except:
        conn.rollback()
        return False

# ---------- 监控地址操作 ----------
def get_monitored_addresses(admin_id: int, user_id: int = None):
    if admin_id == 0:
        return []
    conn = get_conn(admin_id)
    if user_id is not None:
        rows = conn.execute("SELECT id, address, chain_type, added_by, added_at, last_check, note FROM monitored_addresses WHERE added_by = ? ORDER BY added_at DESC", (user_id,)).fetchall()
    else:
        rows = conn.execute("SELECT id, address, chain_type, added_by, added_at, last_check, note FROM monitored_addresses ORDER BY added_at DESC").fetchall()
    return [{"id": r[0], "address": r[1], "chain_type": r[2], "added_by": r[3], "added_at": r[4], "last_check": r[5], "note": r[6] or ""} for r in rows]

def add_monitored_address(admin_id: int, address: str, chain_type: str, added_by: int, note: str = ""):
    if admin_id == 0:
        return []
    conn = get_conn(admin_id)
    try:
        c = conn.cursor()
        c.execute("SELECT id FROM monitored_addresses WHERE address = ? AND added_by = ?", (address, added_by))
        if c.fetchone():
            return False
        current_time = int(time.time())
        c.execute("INSERT INTO monitored_addresses (address, chain_type, added_by, added_at, last_check, note) VALUES (?, ?, ?, ?, 0, ?)",
                  (address, chain_type, added_by, current_time, note))
        conn.commit()
        return True
    except Exception as e:
        print(f"添加监控地址失败: {e}")
        return False

def remove_monitored_address(admin_id: int, address_id: int):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    conn.execute("DELETE FROM monitored_addresses WHERE id = ?", (address_id,))
    conn.commit()
    return True

def update_address_last_check(admin_id: int, address: str, last_check: int):
    if admin_id == 0:
        return
    conn = get_conn(admin_id)
    conn.execute("UPDATE monitored_addresses SET last_check = ? WHERE address = ?", (last_check, address))
    conn.commit()

def add_transaction_record(admin_id: int, address: str, tx_id: str, from_addr: str, to_addr: str, amount: float, timestamp: int):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    try:
        conn.execute("INSERT INTO address_transactions (address, tx_id, from_addr, to_addr, amount, timestamp, notified) VALUES (?, ?, ?, ?, ?, ?, 0)",
                     (address, tx_id, from_addr, to_addr, amount, timestamp))
        conn.commit()
        return True
    except Exception as e:
        print(f"添加交易记录失败: {e}")
        return False

def is_tx_notified(admin_id: int, tx_id: str):
    if admin_id == 0:
        return False
    conn = get_conn(admin_id)
    row = conn.execute("SELECT notified FROM address_transactions WHERE tx_id = ?", (tx_id,)).fetchone()
    return row is not None and row[0] == 1

def mark_tx_notified(admin_id: int, tx_id: str):
    if admin_id == 0:
        return
    conn = get_conn(admin_id)
    conn.execute("UPDATE address_transactions SET notified = 1 WHERE tx_id = ?", (tx_id,))
    conn.commit()

# ---------- 用户偏好 ----------
def get_user_preferences(user_id: int, admin_id: int = 0):
    if admin_id == 0:
        return {"monitor_notify": True, "broadcast_signature": "", "daily_report_enabled": False, "daily_report_hour": 9, "role": "user"}
    conn = get_conn(admin_id)
    row = conn.execute(
        "SELECT monitor_notify, broadcast_signature, daily_report_enabled, daily_report_hour, role FROM user_preferences WHERE user_id = ?",
        (user_id,)).fetchone()
    if row:
        return {
            "monitor_notify": bool(row[0]),
            "broadcast_signature": row[1] or "",
            "daily_report_enabled": bool(row[2]),
            "daily_report_hour": row[3] or 9,
            "role": row[4] or "user"
        }
    return {"monitor_notify": True, "broadcast_signature": "", "daily_report_enabled": False, "daily_report_hour": 9, "role": "user"}

def set_user_preference(user_id: int, key: str, value, admin_id: int = 0):
    if admin_id == 0:
        return
    with get_db(admin_id) as conn:
        now = int(time.time())
        conn.execute("INSERT OR IGNORE INTO user_preferences (user_id, updated_at) VALUES (?, ?)", (user_id, now))
        if key == "monitor_notify":
            conn.execute("UPDATE user_preferences SET monitor_notify = ?, updated_at = ? WHERE user_id = ?", (1 if value else 0, now, user_id))
        elif key == "broadcast_signature":
            conn.execute("UPDATE user_preferences SET broadcast_signature = ?, updated_at = ? WHERE user_id = ?", (value, now, user_id))
        elif key == "daily_report_enabled":
            conn.execute("UPDATE user_preferences SET daily_report_enabled = ?, updated_at = ? WHERE user_id = ?", (1 if value else 0, now, user_id))
        elif key == "daily_report_hour":
            conn.execute("UPDATE user_preferences SET daily_report_hour = ?, updated_at = ? WHERE user_id = ?", (int(value), now, user_id))
        elif key == "role":
            conn.execute("UPDATE user_preferences SET role = ?, updated_at = ? WHERE user_id = ?", (value, now, user_id))

# ---------- 国家检测和分类辅助 ----------
COUNTRY_KEYWORDS = {
    # ========== 亚洲 ==========
    '中国': ['中国', 'china', 'cn', '🇨🇳', 'zhongguo'],
    '台湾': ['台湾', 'taiwan', 'tw', '🇹🇼', 'taiwan china'],
    '香港': ['香港', 'hong kong', 'hk', '🇭🇰', 'hongkong'],
    '澳门': ['澳门', 'macau', 'mo', '🇲🇴', 'macao'],
    '日本': ['日本', 'japan', 'jp', '🇯🇵', 'japon'],
    '韩国': ['韩国', 'korea', 'kr', '🇰🇷', 'south korea', 'republic of korea'],
    '朝鲜': ['朝鲜', 'north korea', 'kp', '🇰🇵', 'democratic people republic of korea'],
    '蒙古': ['蒙古', 'mongolia', 'mn', '🇲🇳'],
    '印度': ['印度', 'india', 'in', '🇮🇳', 'bharat'],
    '巴基斯坦': ['巴基斯坦', 'pakistan', 'pk', '🇵🇰'],
    '孟加拉国': ['孟加拉国', 'bangladesh', 'bd', '🇧🇩'],
    '尼泊尔': ['尼泊尔', 'nepal', 'np', '🇳🇵'],
    '不丹': ['不丹', 'bhutan', 'bt', '🇧🇹'],
    '斯里兰卡': ['斯里兰卡', 'sri lanka', 'lk', '🇱🇰'],
    '马尔代夫': ['马尔代夫', 'maldives', 'mv', '🇲🇻'],
    '泰国': ['泰国', 'thailand', 'th', '🇹🇭', 'siamese'],
    '越南': ['越南', 'vietnam', 'vn', '🇻🇳'],
    '老挝': ['老挝', 'laos', 'la', '🇱🇦'],
    '柬埔寨': ['柬埔寨', 'cambodia', 'kh', '🇰🇭'],
    '缅甸': ['缅甸', 'myanmar', 'mm', '🇲🇲', 'burma'],
    '马来西亚': ['马来西亚', 'malaysia', 'my', '🇲🇾'],
    '新加坡': ['新加坡', 'singapore', 'sg', '🇸🇬'],
    '印度尼西亚': ['印度尼西亚', 'indonesia', 'id', '🇮🇩'],
    '菲律宾': ['菲律宾', 'philippines', 'ph', '🇵🇭'],
    '东帝汶': ['东帝汶', 'timor leste', 'tl', '🇹🇱'],
    '文莱': ['文莱', 'brunei', 'bn', '🇧🇳'],
    '阿富汗': ['阿富汗', 'afghanistan', 'af', '🇦🇫'],
    '伊朗': ['伊朗', 'iran', 'ir', '🇮🇷', 'persia'],
    '伊拉克': ['伊拉克', 'iraq', 'iq', '🇮🇶'],
    '科威特': ['科威特', 'kuwait', 'kw', '🇰🇼'],
    '沙特阿拉伯': ['沙特', 'saudi', 'saudi arabia', 'sa', '🇸🇦'],
    '也门': ['也门', 'yemen', 'ye', '🇾🇪'],
    '阿曼': ['阿曼', 'oman', 'om', '🇴🇲'],
    '阿联酋': ['阿联酋', 'uae', 'united arab emirates', 'ae', '🇦🇪'],
    '卡塔尔': ['卡塔尔', 'qatar', 'qa', '🇶🇦'],
    '巴林': ['巴林', 'bahrain', 'bh', '🇧🇭'],
    '约旦': ['约旦', 'jordan', 'jo', '🇯🇴'],
    '黎巴嫩': ['黎巴嫩', 'lebanon', 'lb', '🇱🇧'],
    '叙利亚': ['叙利亚', 'syria', 'sy', '🇸🇾'],
    '塞浦路斯': ['塞浦路斯', 'cyprus', 'cy', '🇨🇾'],
    '以色列': ['以色列', 'israel', 'il', '🇮🇱'],
    '巴勒斯坦': ['巴勒斯坦', 'palestine', 'ps', '🇵🇸'],
    '土耳其': ['土耳其', 'turkey', 'tr', '🇹🇷'],
    '阿塞拜疆': ['阿塞拜疆', 'azerbaijan', 'az', '🇦🇿'],
    '格鲁吉亚': ['格鲁吉亚', 'georgia', 'ge', '🇬🇪'],
    '亚美尼亚': ['亚美尼亚', 'armenia', 'am', '🇦🇲'],
    '哈萨克斯坦': ['哈萨克斯坦', 'kazakhstan', 'kz', '🇰🇿'],
    '吉尔吉斯斯坦': ['吉尔吉斯斯坦', 'kyrgyzstan', 'kg', '🇰🇬'],
    '塔吉克斯坦': ['塔吉克斯坦', 'tajikistan', 'tj', '🇹🇯'],
    '乌兹别克斯坦': ['乌兹别克斯坦', 'uzbekistan', 'uz', '🇺🇿'],
    '土库曼斯坦': ['土库曼斯坦', 'turkmenistan', 'tm', '🇹🇲'],

    # ========== 欧洲 ==========
    '英国': ['英国', 'uk', 'united kingdom', 'england', 'britain', 'gb', '🇬🇧'],
    '法国': ['法国', 'france', 'fr', '🇫🇷'],
    '德国': ['德国', 'germany', 'de', '🇩🇪'],
    '意大利': ['意大利', 'italy', 'it', '🇮🇹'],
    '西班牙': ['西班牙', 'spain', 'es', '🇪🇸'],
    '葡萄牙': ['葡萄牙', 'portugal', 'pt', '🇵🇹'],
    '荷兰': ['荷兰', 'netherlands', 'nl', '🇳🇱', 'holland'],
    '比利时': ['比利时', 'belgium', 'be', '🇧🇪'],
    '卢森堡': ['卢森堡', 'luxembourg', 'lu', '🇱🇺'],
    '瑞士': ['瑞士', 'switzerland', 'ch', '🇨🇭'],
    '奥地利': ['奥地利', 'austria', 'at', '🇦🇹'],
    '列支敦士登': ['列支敦士登', 'liechtenstein', 'li', '🇱🇮'],
    '波兰': ['波兰', 'poland', 'pl', '🇵🇱'],
    '捷克': ['捷克', 'czech', 'cz', '🇨🇿', 'czech republic'],
    '斯洛伐克': ['斯洛伐克', 'slovakia', 'sk', '🇸🇰'],
    '匈牙利': ['匈牙利', 'hungary', 'hu', '🇭🇺'],
    '罗马尼亚': ['罗马尼亚', 'romania', 'ro', '🇷🇴'],
    '保加利亚': ['保加利亚', 'bulgaria', 'bg', '🇧🇬'],
    '塞尔维亚': ['塞尔维亚', 'serbia', 'rs', '🇷🇸'],
    '克罗地亚': ['克罗地亚', 'croatia', 'hr', '🇭🇷'],
    '斯洛文尼亚': ['斯洛文尼亚', 'slovenia', 'si', '🇸🇮'],
    '波黑': ['波黑', 'bosnia', 'ba', '🇧🇦', 'bosnia and herzegovina'],
    '黑山': ['黑山', 'montenegro', 'me', '🇲🇪'],
    '北马其顿': ['北马其顿', 'north macedonia', 'mk', '🇲🇰'],
    '阿尔巴尼亚': ['阿尔巴尼亚', 'albania', 'al', '🇦🇱'],
    '希腊': ['希腊', 'greece', 'gr', '🇬🇷'],
    '爱尔兰': ['爱尔兰', 'ireland', 'ie', '🇮🇪'],
    '丹麦': ['丹麦', 'denmark', 'dk', '🇩🇰'],
    '瑞典': ['瑞典', 'sweden', 'se', '🇸🇪'],
    '挪威': ['挪威', 'norway', 'no', '🇳🇴'],
    '芬兰': ['芬兰', 'finland', 'fi', '🇫🇮'],
    '冰岛': ['冰岛', 'iceland', 'is', '🇮🇸'],
    '俄罗斯': ['俄罗斯', 'russia', 'ru', '🇷🇺'],
    '爱沙尼亚': ['爱沙尼亚', 'estonia', 'ee', '🇪🇪'],
    '拉脱维亚': ['拉脱维亚', 'latvia', 'lv', '🇱🇻'],
    '立陶宛': ['立陶宛', 'lithuania', 'lt', '🇱🇹'],
    '白俄罗斯': ['白俄罗斯', 'belarus', 'by', '🇧🇾'],
    '摩尔多瓦': ['摩尔多瓦', 'moldova', 'md', '🇲🇩'],
    '乌克兰': ['乌克兰', 'ukraine', 'ua', '🇺🇦'],
    '摩纳哥': ['摩纳哥', 'monaco', 'mc', '🇲🇨'],
    '安道尔': ['安道尔', 'andorra', 'ad', '🇦🇩'],
    '圣马力诺': ['圣马力诺', 'san marino', 'sm', '🇸🇲'],
    '梵蒂冈': ['梵蒂冈', 'vatican', 'va', '🇻🇦'],
    '马耳他': ['马耳他', 'malta', 'mt', '🇲🇹'],
    '直布罗陀': ['直布罗陀', 'gibraltar', 'gi', '🇬🇮'],
    '马恩岛': ['马恩岛', 'isle of man', 'im', '🇮🇲'],
    '泽西岛': ['泽西岛', 'jersey', 'je', '🇯🇪'],
    '根西岛': ['根西岛', 'guernsey', 'gg', '🇬🇬'],
    '法罗群岛': ['法罗群岛', 'faroe islands', 'fo', '🇫🇴'],
    '奥兰群岛': ['奥兰群岛', 'aland islands', 'ax', '🇦🇽'],
    '斯瓦尔巴群岛': ['斯瓦尔巴', 'svalbard', 'sj', '🇸🇯'],

    # ========== 北美洲 ==========
    '美国': ['美国', 'usa', 'us', 'america', 'united states', '🇺🇸'],
    '加拿大': ['加拿大', 'canada', 'ca', '🇨🇦'],
    '墨西哥': ['墨西哥', 'mexico', 'mx', '🇲🇽'],
    '古巴': ['古巴', 'cuba', 'cu', '🇨🇺'],
    '牙买加': ['牙买加', 'jamaica', 'jm', '🇯🇲'],
    '海地': ['海地', 'haiti', 'ht', '🇭🇹'],
    '多米尼加': ['多米尼加', 'dominican', 'do', '🇩🇴'],
    '波多黎各': ['波多黎各', 'puerto rico', 'pr', '🇵🇷'],
    '巴哈马': ['巴哈马', 'bahamas', 'bs', '🇧🇸'],
    '特立尼达和多巴哥': ['特立尼达', 'trinidad', 'tt', '🇹🇹'],
    '巴巴多斯': ['巴巴多斯', 'barbados', 'bb', '🇧🇧'],
    '圣卢西亚': ['圣卢西亚', 'saint lucia', 'lc', '🇱🇨'],
    '格林纳达': ['格林纳达', 'grenada', 'gd', '🇬🇩'],
    '安提瓜和巴布达': ['安提瓜', 'antigua', 'ag', '🇦🇬'],
    '圣基茨和尼维斯': ['圣基茨', 'saint kitts', 'kn', '🇰🇳'],
    '伯利兹': ['伯利兹', 'belize', 'bz', '🇧🇿'],
    '哥斯达黎加': ['哥斯达黎加', 'costa rica', 'cr', '🇨🇷'],
    '萨尔瓦多': ['萨尔瓦多', 'el salvador', 'sv', '🇸🇻'],
    '危地马拉': ['危地马拉', 'guatemala', 'gt', '🇬🇹'],
    '洪都拉斯': ['洪都拉斯', 'honduras', 'hn', '🇭🇳'],
    '尼加拉瓜': ['尼加拉瓜', 'nicaragua', 'ni', '🇳🇮'],
    '巴拿马': ['巴拿马', 'panama', 'pa', '🇵🇦'],

    # ========== 南美洲 ==========
    '巴西': ['巴西', 'brazil', 'br', '🇧🇷'],
    '阿根廷': ['阿根廷', 'argentina', 'ar', '🇦🇷'],
    '乌拉圭': ['乌拉圭', 'uruguay', 'uy', '🇺🇾'],
    '巴拉圭': ['巴拉圭', 'paraguay', 'py', '🇵🇾'],
    '玻利维亚': ['玻利维亚', 'bolivia', 'bo', '🇧🇴'],
    '智利': ['智利', 'chile', 'cl', '🇨🇱'],
    '秘鲁': ['秘鲁', 'peru', 'pe', '🇵🇪'],
    '哥伦比亚': ['哥伦比亚', 'colombia', 'co', '🇨🇴'],
    '委内瑞拉': ['委内瑞拉', 'venezuela', 've', '🇻🇪'],
    '厄瓜多尔': ['厄瓜多尔', 'ecuador', 'ec', '🇪🇨'],
    '圭亚那': ['圭亚那', 'guyana', 'gy', '🇬🇾'],
    '苏里南': ['苏里南', 'suriname', 'sr', '🇸🇷'],
    '法属圭亚那': ['法属圭亚那', 'french guiana', 'gf', '🇬🇫'],

    # ========== 非洲 ==========
    '南非': ['南非', 'south africa', 'za', '🇿🇦'],
    '埃及': ['埃及', 'egypt', 'eg', '🇪🇬'],
    '摩洛哥': ['摩洛哥', 'morocco', 'ma', '🇲🇦'],
    '阿尔及利亚': ['阿尔及利亚', 'algeria', 'dz', '🇩🇿'],
    '突尼斯': ['突尼斯', 'tunisia', 'tn', '🇹🇳'],
    '利比亚': ['利比亚', 'libya', 'ly', '🇱🇾'],
    '苏丹': ['苏丹', 'sudan', 'sd', '🇸🇩'],
    '埃塞俄比亚': ['埃塞俄比亚', 'ethiopia', 'et', '🇪🇹'],
    '肯尼亚': ['肯尼亚', 'kenya', 'ke', '🇰🇪'],
    '坦桑尼亚': ['坦桑尼亚', 'tanzania', 'tz', '🇹🇿'],
    '乌干达': ['乌干达', 'uganda', 'ug', '🇺🇬'],
    '卢旺达': ['卢旺达', 'rwanda', 'rw', '🇷🇼'],
    '布隆迪': ['布隆迪', 'burundi', 'bi', '🇧🇮'],
    '索马里': ['索马里', 'somalia', 'so', '🇸🇴'],
    '吉布提': ['吉布提', 'djibouti', 'dj', '🇩🇯'],
    '厄立特里亚': ['厄立特里亚', 'eritrea', 'er', '🇪🇷'],
    '南苏丹': ['南苏丹', 'south sudan', 'ss', '🇸🇸'],
    '刚果金': ['刚果金', 'congo', 'cd', '🇨🇩', 'drc'],
    '刚果布': ['刚果布', 'congo brazzaville', 'cg', '🇨🇬'],
    '加蓬': ['加蓬', 'gabon', 'ga', '🇬🇦'],
    '赤道几内亚': ['赤道几内亚', 'equatorial guinea', 'gq', '🇬🇶'],
    '喀麦隆': ['喀麦隆', 'cameroon', 'cm', '🇨🇲'],
    '尼日利亚': ['尼日利亚', 'nigeria', 'ng', '🇳🇬'],
    '加纳': ['加纳', 'ghana', 'gh', '🇬🇭'],
    '科特迪瓦': ['科特迪瓦', 'ivory coast', 'ci', '🇨🇮'],
    '塞内加尔': ['塞内加尔', 'senegal', 'sn', '🇸🇳'],
    '几内亚': ['几内亚', 'guinea', 'gn', '🇬🇳'],
    '几内亚比绍': ['几内亚比绍', 'guinea bissau', 'gw', '🇬🇼'],
    '马里': ['马里', 'mali', 'ml', '🇲🇱'],
    '布基纳法索': ['布基纳法索', 'burkina faso', 'bf', '🇧🇫'],
    '尼日尔': ['尼日尔', 'niger', 'ne', '🇳🇪'],
    '乍得': ['乍得', 'chad', 'td', '🇹🇩'],
    '中非': ['中非', 'central african', 'cf', '🇨🇫'],
    '安哥拉': ['安哥拉', 'angola', 'ao', '🇦🇴'],
    '纳米比亚': ['纳米比亚', 'namibia', 'na', '🇳🇦'],
    '博茨瓦纳': ['博茨瓦纳', 'botswana', 'bw', '🇧🇼'],
    '赞比亚': ['赞比亚', 'zambia', 'zm', '🇿🇲'],
    '津巴布韦': ['津巴布韦', 'zimbabwe', 'zw', '🇿🇼'],
    '莫桑比克': ['莫桑比克', 'mozambique', 'mz', '🇲🇿'],
    '马拉维': ['马拉维', 'malawi', 'mw', '🇲🇼'],
    '马达加斯加': ['马达加斯加', 'madagascar', 'mg', '🇲🇬'],
    '毛里求斯': ['毛里求斯', 'mauritius', 'mu', '🇲🇺'],
    '塞舌尔': ['塞舌尔', 'seychelles', 'sc', '🇸🇨'],
    '科摩罗': ['科摩罗', 'comoros', 'km', '🇰🇲'],
    '毛里塔尼亚': ['毛里塔尼亚', 'mauritania', 'mr', '🇲🇷'],
    '西撒哈拉': ['西撒哈拉', 'western sahara', 'eh', '🇪🇭'],
    '冈比亚': ['冈比亚', 'gambia', 'gm', '🇬🇲'],
    '塞拉利昂': ['塞拉利昂', 'sierra leone', 'sl', '🇸🇱'],
    '利比里亚': ['利比里亚', 'liberia', 'lr', '🇱🇷'],
    '贝宁': ['贝宁', 'benin', 'bj', '🇧🇯'],
    '多哥': ['多哥', 'togo', 'tg', '🇹🇬'],

    # ========== 大洋洲 ==========
    '澳大利亚': ['澳大利亚', 'australia', 'au', '🇦🇺'],
    '新西兰': ['新西兰', 'new zealand', 'nz', '🇳🇿'],
    '斐济': ['斐济', 'fiji', 'fj', '🇫🇯'],
    '巴布亚新几内亚': ['巴布亚新几内亚', 'papua new guinea', 'pg', '🇵🇬'],
    '所罗门群岛': ['所罗门群岛', 'solomon islands', 'sb', '🇸🇧'],
    '瓦努阿图': ['瓦努阿图', 'vanuatu', 'vu', '🇻🇺'],
    '新喀里多尼亚': ['新喀里多尼亚', 'new caledonia', 'nc', '🇳🇨'],
    '萨摩亚': ['萨摩亚', 'samoa', 'ws', '🇼🇸'],
    '汤加': ['汤加', 'tonga', 'to', '🇹🇴'],
    '密克罗尼西亚': ['密克罗尼西亚', 'micronesia', 'fm', '🇫🇲'],
    '马绍尔群岛': ['马绍尔群岛', 'marshall islands', 'mh', '🇲🇭'],
    '帕劳': ['帕劳', 'palau', 'pw', '🇵🇼'],
    '瑙鲁': ['瑙鲁', 'nauru', 'nr', '🇳🇷'],
    '基里巴斯': ['基里巴斯', 'kiribati', 'ki', '🇰🇮'],
    '图瓦卢': ['图瓦卢', 'tuvalu', 'tv', '🇹🇻'],
}

def detect_country_from_group_name(group_name: str) -> str:
    if not group_name:
        return None
    group_name_lower = group_name.lower()
    for country, keywords in COUNTRY_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in group_name_lower:
                return country
    return None

def ensure_country_category(admin_id: int = 0, country_name: str = ""):
    if admin_id == 0:
        return False
    categories = get_all_categories(admin_id)
    category_names = [cat['name'] for cat in categories]
    if country_name not in category_names:
        return add_category(admin_id, country_name, f"自动创建的{country_name}分类")
    return True

def fix_joined_at():
    pass

def record_address_query(group_id: str, address: str, chain_type: str, user_id: int, username: str, balance: float, admin_id: int = 0):
    if admin_id == 0:
        return
    now = int(time.time())
    conn = get_conn(admin_id)
    conn.execute("""
        INSERT OR REPLACE INTO address_queries 
        (group_id, address, chain_type, query_time, user_id, username, balance)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (group_id, address, chain_type, now, user_id, username, balance))
    conn.execute("INSERT INTO address_query_log (address, query_time, balance) VALUES (?, ?, ?)", (address, now, balance))
    conn.commit()

def get_address_stats(admin_id: int = 0, address: str = ""):
    if admin_id == 0:
        return {'total_queries': 0, 'first_query': None, 'last_query': None}
    conn = get_conn(admin_id)
    total = conn.execute("SELECT COUNT(*) FROM address_query_log WHERE address = ?", (address,)).fetchone()[0]
    first = conn.execute("SELECT MIN(query_time) FROM address_query_log WHERE address = ?", (address,)).fetchone()[0]
    last = conn.execute("SELECT MAX(query_time) FROM address_query_log WHERE address = ?", (address,)).fetchone()[0]
    return {'total_queries': total, 'first_query': first, 'last_query': last}