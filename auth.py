# auth.py
import sqlite3
import os
import time
from typing import Dict, Optional
from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from db_manager import get_conn, get_db
from db import get_user_preferences as db_get_prefs, set_user_preference as db_set_prefs

# 内存缓存（仅用于快速权限判断）
operators: Dict[int, dict] = {}
temp_operators: Dict[int, dict] = {}
admins: Dict[int, dict] = {}


def load_admins_from_db():
    global admins
    try:
        with get_db(0) as conn:
            # ✅ 确保列存在
            try:
                conn.execute("ALTER TABLE admins ADD COLUMN expire_date INTEGER DEFAULT 0")
            except:
                pass  # 列已存在
            try:
                conn.execute("ALTER TABLE admins ADD COLUMN source TEXT DEFAULT 'manual'")
            except:
                pass  # 列已存在
            c = conn.cursor()
            c.execute("SELECT admin_id, added_by, created_at, deleted_at, expire_date, source FROM admins WHERE deleted_at=0")
            rows = c.fetchall()
        admins = {}
        for row in rows:
            admins[row["admin_id"]] = {
                "id": row["admin_id"],
                "added_by": row["added_by"],
                "created_at": row["created_at"],
                "deleted_at": row["deleted_at"] or 0,
                "expire_date": row["expire_date"] or 0,
                "source": row["source"] or "manual",
            }
        print(f"✅ 已加载 {len(admins)} 名管理员")
    except Exception as e:
        print(f"❌ 加载管理员失败: {e}")
        admins = {}


def init_operators_from_db() -> Dict[int, dict]:
    global operators
    try:
        with get_db(0) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS operators (
                    user_id TEXT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    added_by INTEGER DEFAULT 0
                )
            """)
            c = conn.cursor()
            c.execute("SELECT user_id, username, first_name, last_name, added_by FROM operators")
            rows = c.fetchall()
        operators = {}
        for row in rows:
            user_id = int(row['user_id'])
            operators[user_id] = {
                "id": user_id,
                "username": row['username'],
                "first_name": row['first_name'],
                "last_name": row['last_name'],
                "added_by": row['added_by'] or 0
            }
        print(f"✅ 已从数据库加载 {len(operators)} 名操作员")
        return operators
    except Exception as e:
        print(f"❌ 加载操作员失败: {e}")
        operators = {}
        return operators


def init_temp_operators_from_db():
    global temp_operators
    try:
        with get_db(0) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS temp_operators (user_id TEXT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, added_at INTEGER, added_by INTEGER)")
            c = conn.cursor()
            c.execute("SELECT user_id, username, first_name, last_name, added_by FROM temp_operators")
            rows = c.fetchall()
        temp_operators = {}
        for row in rows:
            user_id = int(row['user_id'])
            temp_operators[user_id] = {
                "id": user_id,
                "username": row['username'],
                "first_name": row['first_name'],
                "last_name": row['last_name'],
                "added_by": row['added_by'] or 0
            }
        print(f"✅ 已从数据库加载 {len(temp_operators)} 名临时操作人")
        return temp_operators
    except Exception as e:
        print(f"❌ 加载临时操作人失败: {e}")
        temp_operators = {}
        return temp_operators


def init_auth():
    with get_db(0) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                admin_id INTEGER PRIMARY KEY,
                added_by INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                deleted_at INTEGER DEFAULT 0,
                db_path TEXT
            )
        """)
    load_admins_from_db()
    init_operators_from_db()
    init_temp_operators_from_db()
    # 确保超级管理员在管理员列表中
    if OWNER_ID not in admins:
        add_admin(OWNER_ID)


# 权限检查函数
def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    if user_id not in admins:
        return False
    admin_info = admins.get(user_id, {})
    if admin_info.get("deleted_at", 0) > 0:
        return False
    expire_date = admin_info.get("expire_date", 0)
    if expire_date > 0 and int(time.time()) > expire_date:
        return False
    return True


def is_authorized(user_id: int, require_full_access: bool = False) -> bool:
    if user_id == OWNER_ID:
        return True
    if is_admin(user_id):
        return True
    if require_full_access:
        return user_id in operators
    return user_id in operators or user_id in temp_operators

def get_user_admin_id(user_id: int) -> int:
    if user_id == OWNER_ID:
        return OWNER_ID
    if user_id in admins:
        return user_id
    if user_id in operators:
        return operators[user_id].get("added_by", 0)
    if user_id in temp_operators:
        return temp_operators[user_id].get("added_by", 0)
    return 0


def get_user_preferences(user_id: int) -> dict:
    """获取用户偏好（主库确定角色，独立库读取详细偏好）"""
    if user_id == OWNER_ID:
        admin_id = OWNER_ID
    else:
        if user_id in admins:
            admin_id = user_id
        elif user_id in operators:
            admin_id = operators[user_id].get("added_by", 0)
        elif user_id in temp_operators:
            admin_id = temp_operators[user_id].get("added_by", 0)
        else:
            admin_id = 0
    return db_get_prefs(user_id, admin_id)


def set_user_preference(user_id: int, key: str, value):
    if key == "role":
        if user_id in operators:
            admin_id = operators[user_id].get("added_by", 0)
        elif user_id in temp_operators:
            admin_id = temp_operators[user_id].get("added_by", 0)
        elif user_id in admins:
            admin_id = user_id
        else:
            admin_id = get_user_admin_id(user_id)
    else:
        admin_id = get_user_admin_id(user_id)
    db_set_prefs(user_id, key, value, admin_id)


def add_admin(admin_id: int) -> bool:
    if admin_id in admins:
        return False
    now = int(time.time())
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO admins (admin_id, added_by, created_at) VALUES (?, ?, ?)",
                      (admin_id, OWNER_ID, now))
        admins[admin_id] = {"id": admin_id, "added_by": OWNER_ID, "created_at": now}
        from db_manager import init_admin_db
        init_admin_db(admin_id)
        set_user_preference(admin_id, "role", "admin")
        print(f"✅ 已添加管理员 {admin_id}")
        return True
    except Exception as e:
        print(f"❌ 添加管理员失败: {e}")
        return False


def remove_admin(admin_id: int) -> bool:
    if admin_id not in admins:
        return False
    now = int(time.time())
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute("UPDATE admins SET deleted_at=? WHERE admin_id=?", (now, admin_id))
            c.execute("DELETE FROM operators WHERE added_by = ?", (admin_id,))
            c.execute("DELETE FROM temp_operators WHERE added_by = ?", (admin_id,))

        admins.pop(admin_id, None)

        to_remove = [uid for uid, info in operators.items() if info.get("added_by") == admin_id]
        for uid in to_remove:
            operators.pop(uid, None)

        to_remove_temp = [uid for uid, info in temp_operators.items() if info.get("added_by") == admin_id]
        for uid in to_remove_temp:
            temp_operators.pop(uid, None)

        print(f"✅ 已标记管理员 {admin_id} 为删除，7天后彻底清理")
        print(f"✅ 已清除该管理员下的 {len(to_remove)} 名操作员和 {len(to_remove_temp)} 名临时操作员")
        return True
    except Exception as e:
        print(f"❌ 移除管理员失败: {e}")
        return False


# 操作员管理
async def add_operator(user_id: int, context: ContextTypes.DEFAULT_TYPE = None, added_by: int = 0) -> bool:
    if user_id in operators:
        if added_by != 0:
            try:
                with get_db(added_by) as conn:
                    now = int(time.time())
                    conn.execute(
                        "INSERT OR REPLACE INTO user_preferences (user_id, role, updated_at) VALUES (?, 'operator', ?)",
                        (user_id, now)
                    )
                print(f"✅ 已修复操作员 {user_id} 的 role 为 operator")
            except Exception as e:
                print(f"❌ 修复操作员 role 失败: {e}")
        return False
    username = None
    first_name = None
    last_name = None
    if context:
        try:
            user = await context.bot.get_chat(user_id)
            username = user.username
            first_name = user.first_name
            last_name = user.last_name
        except Exception as e:
            print(f"⚠️ 无法获取用户 {user_id} 的详细信息: {e}")
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO operators (user_id, username, first_name, last_name, added_by) VALUES (?, ?, ?, ?, ?)",
                (str(user_id), username, first_name, last_name, added_by)
            )
    except Exception as e:
        print(f"❌ 主库添加操作员失败: {e}")
        return False
    operators[user_id] = {
        "id": user_id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "added_by": added_by
    }
    if added_by != 0:
        try:
            with get_db(added_by) as conn:
                now = int(time.time())
                conn.execute("INSERT OR REPLACE INTO user_preferences (user_id, role, updated_at) VALUES (?, 'operator', ?)", (user_id, now))
        except Exception as e:
            print(f"设置操作员role失败: {e}")
    return True


def remove_operator(user_id: int) -> bool:
    if user_id not in operators:
        return False
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM operators WHERE user_id = ?", (str(user_id),))
        operators.pop(user_id, None)
        return True
    except Exception as e:
        print(f"❌ 主库删除操作员失败: {e}")
        return False


def list_operators(added_by: int = None) -> Dict[int, dict]:
    """返回操作员字典，可指定 added_by 过滤"""
    if added_by is None:
        return operators
    return {uid: info for uid, info in operators.items() if info.get("added_by") == added_by}


def get_operator_info(user_id: int) -> Optional[dict]:
    return operators.get(user_id)


def get_operators_list_text(user_id: int = None) -> str:
    """生成格式化的操作员列表文本"""
    text = "📋 **操作人列表**\n" + "━" * 20 + "\n\n"
    admin_id = get_user_admin_id(user_id) if user_id else 0
    if user_id == OWNER_ID:
        if admins:
            text += "👑 **管理员**\n"
            for aid, info in admins.items():
                text += f"  🆔 ID: `{aid}`\n"
            text += "\n"
    text += "👤 **正式操作人**\n"
    if user_id == OWNER_ID or admin_id == 0:
        filtered_ops = operators
    else:
        filtered_ops = {uid: info for uid, info in operators.items() if info.get("added_by") == admin_id}
    if filtered_ops:
        for uid, info in filtered_ops.items():
            display_name = info.get('first_name', '')
            username = info.get('username', '')
            if username:
                display_name = f"{display_name} (@{username})" if display_name else f"@{username}"
            else:
                display_name = display_name or f"用户{uid}"
            text += f"  👤 {display_name}\n     🆔 ID: `{uid}`\n"
    else:
        text += "  📭 暂无正式操作人\n"
    text += "\n" + "━" * 20 + "\n\n"
    text += "👥 **临时操作人**（仅记账权限）\n"
    if user_id == OWNER_ID or admin_id == 0:
        filtered_temps = temp_operators
    else:
        filtered_temps = {uid: info for uid, info in temp_operators.items() if info.get("added_by") == admin_id}
    if filtered_temps:
        for uid, info in filtered_temps.items():
            display_name = info.get('first_name', '')
            username = info.get('username', '')
            if username:
                display_name = f"{display_name} (@{username})" if display_name else f"@{username}"
            else:
                display_name = display_name or f"用户{uid}"
            text += f"  👤 {display_name}\n     🆔 ID: `{uid}`\n"
    else:
        text += "  📭 暂无临时操作人\n"
    return text


async def add_temp_operator(user_id: int, added_by: int, context: ContextTypes.DEFAULT_TYPE = None) -> bool:
    if user_id in temp_operators or user_id in operators:
        return False
    username = None
    first_name = None
    last_name = None
    if context:
        try:
            user = await context.bot.get_chat(user_id)
            username = user.username
            first_name = user.first_name
            last_name = user.last_name
        except Exception as e:
            print(f"⚠️ 无法获取用户 {user_id} 的详细信息: {e}")
    temp_operators[user_id] = {
        "id": user_id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "added_by": added_by
    }
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO temp_operators (user_id, username, first_name, last_name, added_at, added_by) VALUES (?, ?, ?, ?, ?, ?)",
                (str(user_id), username, first_name, last_name, int(time.time()), added_by)
            )
        if added_by != 0:
            try:
                with get_db(added_by) as conn:
                    now = int(time.time())
                    conn.execute(
                        "INSERT OR REPLACE INTO user_preferences (user_id, role, updated_at) VALUES (?, 'temp', ?)",
                        (user_id, now)
                    )
            except Exception as e:
                print(f"设置临时操作员role失败: {e}")
        return True
    except Exception as e:
        print(f"❌ [DB Error] 保存临时操作员失败: {e}")
        temp_operators.pop(user_id, None)
        return False


def remove_temp_operator(user_id: int) -> bool:
    if user_id not in temp_operators:
        return False
    temp_operators.pop(user_id, None)
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM temp_operators WHERE user_id = ?", (str(user_id),))
        return True
    except Exception as e:
        print(f"❌ [DB Error] 删除临时操作员失败: {e}")
        return False


def get_temp_operators_list_text() -> str:
    if not temp_operators:
        return "📭 当前没有临时操作人"
    text = "👥 临时操作人列表：\n" + "━" * 20 + "\n"
    for user_id, info in temp_operators.items():
        display_name = info.get('first_name', '')
        username = info.get('username', '')
        if username:
            display_name = f"{display_name} (@{username})" if display_name else f"@{username}"
        else:
            display_name = display_name or f"用户{user_id}"
        text += f"👤 {display_name}\n🆔 ID: `{user_id}`\n" + "━" * 20 + "\n"
    return text


async def update_all_operators_info(context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    my_operators = {uid: info for uid, info in operators.items() if info.get("added_by") == admin_id}
    if not my_operators:
        return 0
    updated_count = 0
    failed_count = 0
    print(f"🔄 开始更新 {len(my_operators)} 个操作员的信息...")
    for user_id in list(my_operators.keys()):
        try:
            user = await context.bot.get_chat(user_id)
            operators[user_id] = {
                "id": user_id,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "added_by": admin_id
            }
            with get_db(0) as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT OR REPLACE INTO operators (user_id, username, first_name, last_name, added_by) VALUES (?, ?, ?, ?, ?)",
                    (str(user_id), user.username, user.first_name, user.last_name, admin_id)
                )
            updated_count += 1
            print(f"✅ 已更新: {user.first_name} (@{user.username}) - ID: {user_id}")
        except Exception as e:
            failed_count += 1
            print(f"❌ 更新失败 ID {user_id}: {e}")
    print(f"📊 更新完成！成功: {updated_count}, 失败: {failed_count}")
    return updated_count


async def cmd_update_operator_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此命令")
        return
    await update.message.reply_text("🔄 正在更新操作人信息，请稍候...")
    count = await update_all_operators_info(context, admin_id)
    if count > 0:
        await update.message.reply_text(f"✅ 已成功更新 {count} 个操作人的信息")
        text = get_operators_list_text(user_id)
        await update.message.reply_text(text, parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ 没有操作人被更新，或更新失败")


# 初始化
init_auth()