# handlers/performance.py - 完整业绩汇总模块（支持可配置提成）

import asyncio
import tempfile
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from auth import is_authorized, get_user_admin_id, OWNER_ID, is_admin
from handlers.accounting import get_accounting_manager

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import bot_logger as logger

# 状态定义
PERFORMANCE_MENU = 170
PERFORMANCE_RECORD = 171
PERFORMANCE_VIEW = 172
PERFORMANCE_MONTH_SELECT = 173
PERFORMANCE_EDIT = 174
PERFORMANCE_DELETE = 175
PERFORMANCE_COMMISSION_SET = 176  # 新增：设置提成比例
PERFORMANCE_COMMISSION_INPUT = 177  # 新增：输入提成比例

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

# ==================== 提成配置管理 ====================

def get_commission_config(admin_id: int) -> dict:
    """获取管理员的提成配置"""
    if admin_id == 0:
        return {"channel_rate": 10.0, "customer_rate": 10.0}

    from db_manager import get_conn
    conn = get_conn(admin_id)

    # 确保表存在
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commission_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_rate REAL DEFAULT 10.0,
            customer_rate REAL DEFAULT 10.0,
            updated_at INTEGER DEFAULT 0,
            updated_by INTEGER DEFAULT 0
        )
    """)
    conn.commit()

    row = conn.execute(
        "SELECT channel_rate, customer_rate FROM commission_config LIMIT 1"
    ).fetchone()

    if row:
        return {
            "channel_rate": float(row[0]),
            "customer_rate": float(row[1])
        }

    # 插入默认配置
    now = int(time.time())
    conn.execute(
        "INSERT INTO commission_config (channel_rate, customer_rate, updated_at) VALUES (10.0, 10.0, ?)",
        (now,)
    )
    conn.commit()

    return {"channel_rate": 10.0, "customer_rate": 10.0}


def set_commission_config(admin_id: int, channel_rate: float, customer_rate: float, updated_by: int) -> bool:
    """设置管理员的提成配置"""
    if admin_id == 0:
        return False

    from db_manager import get_conn
    conn = get_conn(admin_id)

    # 确保表存在
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commission_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_rate REAL DEFAULT 10.0,
            customer_rate REAL DEFAULT 10.0,
            updated_at INTEGER DEFAULT 0,
            updated_by INTEGER DEFAULT 0
        )
    """)

    now = int(time.time())

    # 检查是否存在记录
    existing = conn.execute("SELECT id FROM commission_config LIMIT 1").fetchone()

    if existing:
        conn.execute(
            "UPDATE commission_config SET channel_rate = ?, customer_rate = ?, updated_at = ?, updated_by = ? WHERE id = ?",
            (channel_rate, customer_rate, now, updated_by, existing[0])
        )
    else:
        conn.execute(
            "INSERT INTO commission_config (channel_rate, customer_rate, updated_at, updated_by) VALUES (?, ?, ?, ?)",
            (channel_rate, customer_rate, now, updated_by)
        )

    conn.commit()
    return True


# ==================== 业绩记录操作（使用配置的提成比例）====================

def add_performance_record(admin_id: int, country: str, channel_income: float, customer_expense: float,
                           channel_group: str, customer_group: str,
                           channel_employee_id: int, channel_employee_name: str,
                           customer_employee_id: int, customer_employee_name: str,
                           created_by: int) -> bool:
    """添加业绩记录（使用管理员的提成配置）"""
    from db_manager import get_conn

    if admin_id == 0:
        return False

    conn = get_conn(admin_id)
    profit = channel_income + customer_expense
    now = int(time.time())
    date_str = datetime.fromtimestamp(now, tz=BEIJING_TZ).strftime('%Y-%m-%d')

    # 获取提成配置（用于记录当时的提成比例）
    config = get_commission_config(admin_id)
    channel_rate = config["channel_rate"]
    customer_rate = config["customer_rate"]

    # 生成编号
    date_prefix = datetime.fromtimestamp(now, tz=BEIJING_TZ).strftime('%y%m%d')
    cursor = conn.execute("SELECT COUNT(*) FROM performance_records WHERE date = ?", (date_str,))
    count = cursor.fetchone()[0] + 1
    record_id = int(f"{date_prefix}{count:02d}")

    while True:
        cursor = conn.execute("SELECT id FROM performance_records WHERE id = ?", (record_id,))
        if not cursor.fetchone():
            break
        count += 1
        record_id = int(f"{date_prefix}{count:02d}")

    conn.execute("""
        INSERT INTO performance_records 
        (id, country, channel_income, customer_expense, profit, channel_group, customer_group,
         channel_employee_id, channel_employee_name, customer_employee_id, customer_employee_name,
         channel_rate, customer_rate, created_by, created_at, date, admin_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (record_id, country, channel_income, customer_expense, profit, channel_group, customer_group,
          channel_employee_id, channel_employee_name, customer_employee_id, customer_employee_name,
          channel_rate, customer_rate, created_by, now, date_str, admin_id))
    conn.commit()
    return True


def get_performance_records(admin_id: int, year: int = None, month: int = None) -> list:
    """获取业绩记录（只获取当前管理员的）"""
    if admin_id == 0:
        return []

    from db_manager import get_conn
    conn = get_conn(admin_id)

    if year and month:
        date_prefix = f"{year}-{month:02d}"
        rows = conn.execute(
            "SELECT * FROM performance_records WHERE date LIKE ? ORDER BY created_at DESC",
            (f"{date_prefix}%",)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM performance_records ORDER BY created_at DESC"
        ).fetchall()

    return [dict(row) for row in rows]


def get_performance_summary(admin_id: int, year: int, month: int) -> dict:
    """获取指定月份的业绩汇总（使用记录的提成比例）"""
    records = get_performance_records(admin_id, year, month)

    if not records:
        return {"records": [], "total_profit": 0, "employee_commission": {}, "employee_performance": {}}

    total_profit = sum(r['profit'] for r in records)

    employee_commission = {}
    employee_performance = {}

    for r in records:
        profit = r['profit']
        channel_rate = r.get('channel_rate', 10.0)
        customer_rate = r.get('customer_rate', 10.0)

        # 通道员工提成
        ch_id = r['channel_employee_id']
        ch_name = r['channel_employee_name'] or f"员工{ch_id}"
        if ch_id not in employee_commission:
            employee_commission[ch_id] = {"name": ch_name, "commission": 0}
            employee_performance[ch_id] = {"name": ch_name, "performance": 0}
        employee_commission[ch_id]["commission"] += profit * channel_rate / 100
        employee_performance[ch_id]["performance"] += profit / 2

        # 客户员工提成
        cu_id = r['customer_employee_id']
        cu_name = r['customer_employee_name'] or f"员工{cu_id}"
        if cu_id not in employee_commission:
            employee_commission[cu_id] = {"name": cu_name, "commission": 0}
            employee_performance[cu_id] = {"name": cu_name, "performance": 0}
        employee_commission[cu_id]["commission"] += profit * customer_rate / 100
        employee_performance[cu_id]["performance"] += profit / 2

    return {
        "records": records,
        "total_profit": total_profit,
        "employee_commission": employee_commission,
        "employee_performance": employee_performance
    }


def get_performance_available_months(admin_id: int) -> list:
    """获取有业绩记录的月份列表"""
    if admin_id == 0:
        return []

    from db_manager import get_conn
    conn = get_conn(admin_id)
    rows = conn.execute(
        "SELECT DISTINCT substr(date, 1, 7) as month FROM performance_records ORDER BY month DESC"
    ).fetchall()
    return [row[0] for row in rows]


def get_performance_record_by_id(admin_id: int, record_id: int):
    """根据ID获取单条业绩记录"""
    if admin_id == 0:
        return None

    from db_manager import get_conn
    conn = get_conn(admin_id)
    row = conn.execute("SELECT * FROM performance_records WHERE id = ?", (record_id,)).fetchone()
    return dict(row) if row else None


def update_performance_record(admin_id: int, record_id: int, country: str, channel_income: float,
                              customer_expense: float, channel_group: str, customer_group: str,
                              channel_employee_id: int, channel_employee_name: str,
                              customer_employee_id: int, customer_employee_name: str) -> bool:
    """修改业绩记录（使用当前配置的提成比例）"""
    if admin_id == 0:
        return False

    from db_manager import get_conn
    conn = get_conn(admin_id)

    profit = channel_income + customer_expense
    config = get_commission_config(admin_id)
    channel_rate = config["channel_rate"]
    customer_rate = config["customer_rate"]

    conn.execute("""
        UPDATE performance_records 
        SET country=?, channel_income=?, customer_expense=?, profit=?,
            channel_group=?, customer_group=?,
            channel_employee_id=?, channel_employee_name=?,
            customer_employee_id=?, customer_employee_name=?,
            channel_rate=?, customer_rate=?
        WHERE id=?
    """, (country, channel_income, customer_expense, profit,
          channel_group, customer_group,
          channel_employee_id, channel_employee_name,
          customer_employee_id, customer_employee_name,
          channel_rate, customer_rate, record_id))
    conn.commit()
    return True


def delete_performance_record(admin_id: int, record_id: int) -> bool:
    """删除业绩记录"""
    if admin_id == 0:
        return False

    from db_manager import get_conn
    conn = get_conn(admin_id)
    conn.execute("DELETE FROM performance_records WHERE id=?", (record_id,))
    conn.commit()
    return True


# ==================== 键盘按钮检查 ====================

def _is_keyboard_button(text: str) -> bool:
    """检查是否是键盘按钮"""
    keyboard_buttons = {
        "◀️ 返回主菜单", "📒 记账", "🔔 USDT监控", "📢 群发", "💰 USDT查询",
        "👤 操作人管理", "🔄 互转查询", "📁 群组管理", "📖 使用说明", "👤 个人中心",
        "➕ 添加操作人", "➖ 删除操作人", "📋 操作人列表", "🔄 更新操作人信息", "👥 临时操作人",
        "➕ 添加临时操作人", "➖ 删除临时操作人", "📋 临时操作人列表", "◀️ 返回操作人管理",
        "➕ 添加监控地址", "📋 监控列表", "📊 月度统计", "❌ 删除监控地址",
        "📊 群组统计", "📁 查看分类", "➕ 创建分类", "🏷️ 设置群组分类", "🗑️ 删除分类",
        "🔍 转账查询", "🕸️ 转账分析",
    }
    return text in keyboard_buttons


# ==================== 业绩汇总主菜单 ====================

async def performance_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """业绩汇总主菜单"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    # 权限检查：仅正式操作员和管理员
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 临时操作员不能使用业绩汇总功能", show_alert=True)
        return
    await query.answer()

    months = get_performance_available_months(admin_id)
    config = get_commission_config(admin_id)

    # 当前月份
    now = datetime.now()
    current_month = f"{now.year}-{now.month:02d}"

    if not months:
        text = f"📊 **业绩汇总**\n\n"
        text += f"⚙️ **当前提成配置**\n"
        text += f"• 通道提成：{config['channel_rate']}%\n"
        text += f"• 客户提成：{config['customer_rate']}%\n\n"
        text += "📭 暂无业绩记录\n\n请选择操作："
        keyboard = [
            [InlineKeyboardButton("⚙️ 设置提成比例", callback_data="perf_commission_set")],
            [InlineKeyboardButton("➕ 记录业绩", callback_data="perf_record")],
            [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
        ]
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return PERFORMANCE_MENU

    # 默认显示当前月，如果没有则显示最近一个月
    if current_month in months:
        display_month = current_month
    else:
        display_month = months[0]

    year, month = display_month.split('-')
    year = int(year)
    month = int(month)
    summary = get_performance_summary(admin_id, year, month)

    # 标题
    text = f"📊 **{year}年{month}月 业绩汇总**\n\n"
    text += f"⚙️ **当前提成配置**\n"
    text += f"• 通道提成：{config['channel_rate']}%\n"
    text += f"• 客户提成：{config['customer_rate']}%\n\n"
    text += f"<blockquote><b>公司总利润：{summary['total_profit']:.2f} USDT</b></blockquote>\n\n"

    if summary['records']:
        display_records = summary['records'][:20]

        # 表头
        text += f"`{'编号':<9}{'日期':<7}{'国家':<6}{'通道':>6}{'客户':>6}{'利润':>6}{'通道员工':<7}{'客户员工':<7}`\n"

        for r in display_records:
            date_str = r['date'][-5:] if r['date'] else ''
            country = r['country'][:4]
            ch_name = (r['channel_employee_name'] or f"ID{r['channel_employee_id']}")[:5]
            cu_name = (r['customer_employee_name'] or f"ID{r['customer_employee_id']}")[:5]
            text += f"`{r['id']:<9}{date_str:<7}{country:<6}{r['channel_income']:>6.0f}{r['customer_expense']:>6.0f}{r['profit']:>6.0f}{ch_name:<7}{cu_name:<7}`\n"

        if len(summary['records']) > 20:
            text += f"\n`... 仅显示20条，共 {len(summary['records'])} 条（导出HTML查看全部）`\n"

    # 员工提成（使用记录的提成比例）
    text += "\n<blockquote><b>💰 员工提成汇总</b></blockquote>\n"
    for emp_id, data in summary['employee_commission'].items():
        perf = summary['employee_performance'].get(emp_id, {}).get('performance', 0)
        text += f"• {data['name']}：{data['commission']:.2f} USDT（业绩 {perf:.2f} USDT）\n"

    text += "\n💡 提成 = 利润×提成比例 | 业绩 = 利润÷2"

    keyboard = [
        [InlineKeyboardButton("⚙️ 设置提成比例", callback_data="perf_commission_set")],
        [InlineKeyboardButton("➕ 记录业绩", callback_data="perf_record")],
        [InlineKeyboardButton("📅 查看其他月份", callback_data="perf_view")],
        [InlineKeyboardButton("📥 导出HTML", callback_data="perf_export_select")],
    ]

    if user_id == OWNER_ID:
        # 超级管理员也能修改/删除
        keyboard.append([
            InlineKeyboardButton("✏️ 修改业绩", callback_data="perf_edit"),
            InlineKeyboardButton("🗑️ 删除业绩", callback_data="perf_delete"),
        ])
        keyboard.append([InlineKeyboardButton("📝 记录追溯", callback_data="perf_trace")])

    keyboard.append([InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")])

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
    return PERFORMANCE_MENU


# ==================== 设置提成比例 ====================

async def performance_commission_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """设置提成比例"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    # 权限检查：只有管理员可以设置
    if not is_admin(user_id) and user_id != OWNER_ID:
        await query.answer("❌ 只有管理员可以设置提成比例", show_alert=True)
        return

    await query.answer()

    config = get_commission_config(admin_id)
    context.user_data["perf_action"] = "commission_set"
    context.user_data["profile_input_state"] = True

    keyboard = [[InlineKeyboardButton("◀️ 返回业绩菜单", callback_data="perf_menu")]]

    await query.message.edit_text(
        f"⚙️ **设置提成比例**\n\n"
        f"📊 **当前配置**\n"
        f"• 通道提成：**{config['channel_rate']}%**\n"
        f"• 客户提成：**{config['customer_rate']}%**\n\n"
        f"请输入新的提成比例，格式：`通道比例 客户比例`\n"
        f"例如：`10 15` 表示通道10%，客户15%\n\n"
        f"💡 提示：\n"
        f"• 提成比例是百分比（如10表示10%）\n"
        f"• 此配置对该公司的**所有员工**生效\n"
        f"• 修改后，**新记录的业绩**使用新比例\n"
        f"• **已有业绩**保持原比例不变\n\n"
        f"❌ 发送 /cancel 取消",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_COMMISSION_INPUT


async def performance_commission_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """接收提成比例输入"""
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    text = update.message.text.strip()

    # 检查键盘按钮
    if _is_keyboard_button(text):
        context.user_data.pop("perf_action", None)
        context.user_data.pop("profile_input_state", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    if text == '/cancel':
        context.user_data.pop("perf_action", None)
        context.user_data.pop("profile_input_state", None)
        await update.message.reply_text("❌ 已取消设置")
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    # 解析输入
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text(
            "❌ 格式错误，请输入两个数字（通道比例 客户比例）\n"
            "例如：`10 15`\n\n"
            "请重新输入："
        )
        return PERFORMANCE_COMMISSION_INPUT

    try:
        channel_rate = float(parts[0])
        customer_rate = float(parts[1])
    except ValueError:
        await update.message.reply_text("❌ 比例必须是数字，请重新输入：")
        return PERFORMANCE_COMMISSION_INPUT

    # 验证范围
    if channel_rate < 0 or channel_rate > 100:
        await update.message.reply_text("❌ 通道提成比例必须在 0-100 之间，请重新输入：")
        return PERFORMANCE_COMMISSION_INPUT

    if customer_rate < 0 or customer_rate > 100:
        await update.message.reply_text("❌ 客户提成比例必须在 0-100 之间，请重新输入：")
        return PERFORMANCE_COMMISSION_INPUT

    # 保存配置
    success = set_commission_config(admin_id, channel_rate, customer_rate, user_id)

    if success:
        # 显示更新后的配置
        config = get_commission_config(admin_id)
        await update.message.reply_text(
            f"✅ **提成比例已更新**\n\n"
            f"📊 **新配置**\n"
            f"• 通道提成：{config['channel_rate']}%\n"
            f"• 客户提成：{config['customer_rate']}%\n\n"
            f"💡 新记录的业绩将使用此比例"
        )
    else:
        await update.message.reply_text("❌ 设置失败，请稍后重试")

    context.user_data.pop("perf_action", None)
    context.user_data.pop("profile_input_state", None)

    # 返回业绩菜单
    from handlers.menu import get_main_menu
    await update.message.reply_text(
        "请选择功能：",
        reply_markup=get_main_menu(user_id)
    )
    return ConversationHandler.END


# ==================== 记录业绩 ====================

async def performance_record_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """记录业绩 - 输入信息"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()

    context.user_data["profile_input_state"] = True
    context.user_data["perf_action"] = "record"

    # 获取当前提成配置
    config = get_commission_config(admin_id)

    # 获取现有正式操作员列表
    from auth import operators as auth_operators
    employee_list = ""
    if auth_operators:
        employee_list = "\n\n📋 **现有员工列表：**\n"
        for uid, info in auth_operators.items():
            name = info.get('first_name') or ''
            uname = f" @{info['username']}" if info.get('username') else ''
            employee_list += f"• {name}{uname}（ID: `{uid}`）\n"
    else:
        employee_list = "\n\n⚠️ 暂无正式操作员，请先添加操作员"

    keyboard = [[InlineKeyboardButton("◀️ 返回业绩菜单", callback_data="perf_menu")]]

    await query.message.edit_text(
        f"➕ **记录业绩**\n\n"
        f"⚙️ **当前提成配置**\n"
        f"• 通道提成：{config['channel_rate']}%\n"
        f"• 客户提成：{config['customer_rate']}%\n\n"
        "请输入信息，用空格分隔：\n"
        "`国家 通道收入 客户支出 通道群名 客户群名 @通道员工 @客户员工`\n\n"
        "例如：\n"
        "`德国 5000 -3000 德国通道群 德国客户群 @张三 @李四`\n\n"
        "💡 **说明**：\n"
        "• 通道收入填正数（如5000）\n"
        "• 客户支出填负数（如-3000）\n"
        "• 利润 = 通道收入 + 客户支出\n"
        "• 员工用 @用户名 或 用户ID\n"
        "• 只能选择正式操作员\n"
        f"{employee_list}"
        "\n❌ 发送 /cancel 取消",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_RECORD


async def performance_record_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """接收业绩记录"""
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    text = update.message.text.strip()

    # 检查键盘按钮
    if _is_keyboard_button(text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    if text == '/cancel':
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        await update.message.reply_text("❌ 已取消记录")
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    # 如果输入看起来不像业绩记录，可能是想退出
    if len(text.split()) < 3 and not any(c.isdigit() for c in text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "已取消业绩记录\n\n请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    context.user_data["_message_handled"] = True

    # 解析输入
    import shlex
    try:
        parts = shlex.split(text)
    except:
        parts = text.split()

    if len(parts) < 7:
        await update.message.reply_text(
            "❌ 格式错误，至少需要7个参数\n"
            "格式：国家 通道收入 客户支出 通道群名 客户群名 通道员工 客户员工\n\n"
            "请重新输入："
        )
        return PERFORMANCE_RECORD

    country = parts[0]
    try:
        channel_income = float(parts[1])
        customer_expense = float(parts[2])
    except ValueError:
        await update.message.reply_text("❌ 金额格式错误，请输入数字\n请重新输入：")
        return PERFORMANCE_RECORD

    channel_group = parts[3]
    customer_group = parts[4]

    # 解析员工
    channel_employee_id = 0
    channel_employee_name = ""
    customer_employee_id = 0
    customer_employee_name = ""

    # 通道员工
    ch_emp = parts[5]
    from auth import operators as auth_operators

    if ch_emp.startswith('@'):
        ch_username = ch_emp[1:]
        found = False
        for oid, info in auth_operators.items():
            if info.get('username') == ch_username:
                channel_employee_id = oid
                channel_employee_name = info.get('first_name') or ch_username
                found = True
                break
        if not found:
            await update.message.reply_text(f"❌ 未找到正式操作员：{ch_emp}\n请重新输入：")
            return PERFORMANCE_RECORD
    elif ch_emp.isdigit():
        channel_employee_id = int(ch_emp)
        if channel_employee_id in auth_operators:
            channel_employee_name = auth_operators[channel_employee_id].get('first_name') or str(channel_employee_id)
        else:
            await update.message.reply_text(f"❌ 未找到正式操作员ID：{ch_emp}\n请重新输入：")
            return PERFORMANCE_RECORD
    else:
        await update.message.reply_text(f"❌ 员工格式错误：{ch_emp}\n请使用 @用户名 或 用户ID\n请重新输入：")
        return PERFORMANCE_RECORD

    # 客户员工
    cu_emp = parts[6] if len(parts) > 6 else ""
    if cu_emp.startswith('@'):
        cu_username = cu_emp[1:]
        found = False
        for oid, info in auth_operators.items():
            if info.get('username') == cu_username:
                customer_employee_id = oid
                customer_employee_name = info.get('first_name') or cu_username
                found = True
                break
        if not found:
            await update.message.reply_text(f"❌ 未找到正式操作员：{cu_emp}\n请重新输入：")
            return PERFORMANCE_RECORD
    elif cu_emp.isdigit():
        customer_employee_id = int(cu_emp)
        if customer_employee_id in auth_operators:
            customer_employee_name = auth_operators[customer_employee_id].get('first_name') or str(customer_employee_id)
        else:
            await update.message.reply_text(f"❌ 未找到正式操作员ID：{cu_emp}\n请重新输入：")
            return PERFORMANCE_RECORD
    else:
        await update.message.reply_text(f"❌ 员工格式错误：{cu_emp}\n请使用 @用户名 或 用户ID\n请重新输入：")
        return PERFORMANCE_RECORD

    # 保存记录
    success = add_performance_record(
        admin_id, country, channel_income, customer_expense,
        channel_group, customer_group,
        channel_employee_id, channel_employee_name,
        customer_employee_id, customer_employee_name,
        user_id
    )

    if success:
        profit = channel_income + customer_expense
        config = get_commission_config(admin_id)
        ch_commission = profit * config['channel_rate'] / 100
        cu_commission = profit * config['customer_rate'] / 100
        ch_performance = profit / 2
        cu_performance = profit / 2

        reply = (
            f"✅ **已记录业绩！**\n\n"
            f"📋 **记录详情：**\n"
            f"• 国家：{country}\n"
            f"• 通道收入：{channel_income} USDT（群：{channel_group}）\n"
            f"• 客户支出：{customer_expense} USDT（群：{customer_group}）\n"
            f"• 利润：{profit} USDT\n\n"
            f"⚙️ **提成配置**\n"
            f"• 通道提成：{config['channel_rate']}%\n"
            f"• 客户提成：{config['customer_rate']}%\n\n"
            f"👤 **通道员工：{channel_employee_name}**\n"
            f"   提成：{ch_commission:.2f} USDT | 业绩：{ch_performance:.2f} USDT\n\n"
            f"👤 **客户员工：{customer_employee_name}**\n"
            f"   提成：{cu_commission:.2f} USDT | 业绩：{cu_performance:.2f} USDT"
        )
        if channel_employee_id == customer_employee_id:
            reply += f"\n\n💡 通道和客户为同一人，提成 {ch_commission + cu_commission:.2f} USDT，业绩 {ch_performance + cu_performance:.2f} USDT"

        await update.message.reply_text(reply, parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ 记录失败，请稍后重试")

    context.user_data.pop("profile_input_state", None)
    context.user_data.pop("perf_action", None)

    from handlers.menu import get_main_menu
    await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END


# ==================== 查看其他月份 ====================

async def performance_view_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看汇总 - 选择月份"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()

    months = get_performance_available_months(admin_id)

    if not months:
        await query.message.edit_text("📭 暂无业绩记录")
        return PERFORMANCE_MENU

    keyboard = []
    for m in months[:12]:
        year, month = m.split('-')
        keyboard.append([InlineKeyboardButton(
            f"📅 {year}年{int(month)}月",
            callback_data=f"perf_month_{m}"
        )])

    keyboard.append([InlineKeyboardButton("◀️ 返回业绩菜单", callback_data="perf_menu")])

    await query.message.edit_text(
        "📊 **查看业绩汇总**\n\n请选择月份：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_MONTH_SELECT


async def performance_view_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示指定月份的业绩汇总"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()

    month_str = query.data.replace("perf_month_", "")
    year, month = month_str.split('-')
    year = int(year)
    month = int(month)

    config = get_commission_config(admin_id)
    summary = get_performance_summary(admin_id, year, month)

    if not summary['records']:
        await query.message.edit_text(f"📭 {year}年{month}月暂无业绩记录")
        return PERFORMANCE_MONTH_SELECT

    text = f"📊 **{year}年{month}月 业绩汇总**\n\n"
    text += f"⚙️ **当前提成配置**\n"
    text += f"• 通道提成：{config['channel_rate']}%\n"
    text += f"• 客户提成：{config['customer_rate']}%\n\n"
    text += f"<blockquote><b>公司总利润：{summary['total_profit']:.2f} USDT</b></blockquote>\n\n"

    if summary['records']:
        display_records = summary['records'][:20]

        text += f"`{'编号':<9}{'日期':<7}{'国家':<6}{'通道':>6}{'客户':>6}{'利润':>6}{'通道员工':<7}{'客户员工':<7}`\n"

        for r in display_records:
            date_str = r['date'][-5:] if r['date'] else ''
            country = r['country'][:4]
            ch_name = (r['channel_employee_name'] or f"ID{r['channel_employee_id']}")[:5]
            cu_name = (r['customer_employee_name'] or f"ID{r['customer_employee_id']}")[:5]
            text += f"`{r['id']:<9}{date_str:<7}{country:<6}{r['channel_income']:>6.0f}{r['customer_expense']:>6.0f}{r['profit']:>6.0f}{ch_name:<7}{cu_name:<7}`\n"

        if len(summary['records']) > 20:
            text += f"\n`... 仅显示20条，共 {len(summary['records'])} 条（导出HTML查看全部）`\n"

    text += "\n<blockquote><b>💰 员工提成汇总</b></blockquote>\n"
    for emp_id, data in summary['employee_commission'].items():
        perf = summary['employee_performance'].get(emp_id, {}).get('performance', 0)
        text += f"• {data['name']}：{data['commission']:.2f} USDT（业绩 {perf:.2f} USDT）\n"

    text += "\n💡 提成 = 利润×提成比例 | 业绩 = 利润÷2"

    keyboard = [
        [InlineKeyboardButton("◀️ 返回月份选择", callback_data="perf_view")],
        [InlineKeyboardButton("◀️ 返回业绩汇总", callback_data="perf_menu")]
    ]

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )
    return PERFORMANCE_VIEW


# ==================== 修改业绩 ====================

async def performance_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """修改业绩 - 输入信息"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    # 只有管理员可以修改
    if not is_admin(user_id) and user_id != OWNER_ID:
        await query.answer("❌ 只有管理员才能修改业绩", show_alert=True)
        return

    await query.answer()
    context.user_data["profile_input_state"] = True
    context.user_data["perf_action"] = "edit"

    config = get_commission_config(admin_id)

    from auth import operators as auth_operators
    employee_list = ""
    if auth_operators:
        employee_list = "\n\n📋 **现有员工列表：**\n"
        for uid, info in auth_operators.items():
            name = info.get('first_name') or ''
            uname = f" @{info['username']}" if info.get('username') else ''
            employee_list += f"• {name}{uname}（ID: `{uid}`）\n"

    keyboard = [[InlineKeyboardButton("◀️ 返回业绩菜单", callback_data="perf_menu")]]

    await query.message.edit_text(
        f"✏️ **修改业绩**\n\n"
        f"⚙️ **当前提成配置**\n"
        f"• 通道提成：{config['channel_rate']}%\n"
        f"• 客户提成：{config['customer_rate']}%\n\n"
        "请输入信息，用空格分隔：\n"
        "`编号 国家 通道收入 客户支出 通道群名 客户群名 @通道员工 @客户员工`\n\n"
        "例如：\n"
        "`1 德国 5000 -3000 德国通道群 德国客户群 @张三 @李四`\n\n"
        "💡 编号是汇总列表中第一列的序号\n"
        "⚠️ 修改后会使用当前提成配置重新计算\n"
        f"{employee_list}"
        "\n❌ 发送 /cancel 取消",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_EDIT


async def performance_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """接收修改的业绩记录"""
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_admin(user_id) and user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有管理员才能修改业绩")
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        return ConversationHandler.END

    text = update.message.text.strip()

    # 检查键盘按钮
    if _is_keyboard_button(text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    if text == '/cancel':
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        await update.message.reply_text("❌ 已取消修改")
        from handlers.menu import get_main_menu
        await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return ConversationHandler.END

    if len(text.split()) < 3 and not any(c.isdigit() for c in text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text("已返回主菜单", reply_markup=get_main_menu(user_id))
        return ConversationHandler.END

    context.user_data["_message_handled"] = True

    import shlex
    try:
        parts = shlex.split(text)
    except:
        parts = text.split()

    if len(parts) < 8:
        await update.message.reply_text("❌ 格式错误，至少需要8个参数\n请重新输入：")
        return PERFORMANCE_EDIT

    try:
        record_id = int(parts[0])
    except ValueError:
        await update.message.reply_text("❌ 编号必须是数字\n请重新输入：")
        return PERFORMANCE_EDIT

    record = get_performance_record_by_id(admin_id, record_id)
    if not record:
        await update.message.reply_text(f"❌ 未找到编号 {record_id} 的业绩记录\n请重新输入：")
        return PERFORMANCE_EDIT

    country = parts[1]
    try:
        channel_income = float(parts[2])
        customer_expense = float(parts[3])
    except ValueError:
        await update.message.reply_text("❌ 金额格式错误\n请重新输入：")
        return PERFORMANCE_EDIT

    channel_group = parts[4]
    customer_group = parts[5]

    # 解析员工
    channel_employee_id = 0
    channel_employee_name = ""
    customer_employee_id = 0
    customer_employee_name = ""

    from auth import operators as auth_operators

    ch_emp = parts[6]
    if ch_emp.startswith('@'):
        ch_username = ch_emp[1:]
        for oid, info in auth_operators.items():
            if info.get('username') == ch_username:
                channel_employee_id = oid
                channel_employee_name = info.get('first_name') or ch_username
                break
        if channel_employee_id == 0:
            await update.message.reply_text(f"❌ 未找到正式操作员：{ch_emp}\n请重新输入：")
            return PERFORMANCE_EDIT
    elif ch_emp.isdigit():
        channel_employee_id = int(ch_emp)
        if channel_employee_id in auth_operators:
            channel_employee_name = auth_operators[channel_employee_id].get('first_name') or str(channel_employee_id)
        else:
            await update.message.reply_text(f"❌ 未找到正式操作员ID：{ch_emp}\n请重新输入：")
            return PERFORMANCE_EDIT
    else:
        await update.message.reply_text(f"❌ 员工格式错误：{ch_emp}\n请重新输入：")
        return PERFORMANCE_EDIT

    cu_emp = parts[7]
    if cu_emp.startswith('@'):
        cu_username = cu_emp[1:]
        for oid, info in auth_operators.items():
            if info.get('username') == cu_username:
                customer_employee_id = oid
                customer_employee_name = info.get('first_name') or cu_username
                break
        if customer_employee_id == 0:
            await update.message.reply_text(f"❌ 未找到正式操作员：{cu_emp}\n请重新输入：")
            return PERFORMANCE_EDIT
    elif cu_emp.isdigit():
        customer_employee_id = int(cu_emp)
        if customer_employee_id in auth_operators:
            customer_employee_name = auth_operators[customer_employee_id].get('first_name') or str(customer_employee_id)
        else:
            await update.message.reply_text(f"❌ 未找到正式操作员ID：{cu_emp}\n请重新输入：")
            return PERFORMANCE_EDIT
    else:
        await update.message.reply_text(f"❌ 员工格式错误：{cu_emp}\n请重新输入：")
        return PERFORMANCE_EDIT

    success = update_performance_record(
        admin_id, record_id, country, channel_income, customer_expense,
        channel_group, customer_group,
        channel_employee_id, channel_employee_name,
        customer_employee_id, customer_employee_name
    )

    if success:
        await update.message.reply_text(f"✅ 已修改编号 {record_id} 的业绩记录")
    else:
        await update.message.reply_text("❌ 修改失败")

    context.user_data.pop("profile_input_state", None)
    context.user_data.pop("perf_action", None)

    from handlers.menu import get_main_menu
    await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END


# ==================== 删除业绩 ====================

async def performance_delete_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """删除业绩 - 输入编号"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_admin(user_id) and user_id != OWNER_ID:
        await query.answer("❌ 只有管理员才能删除业绩", show_alert=True)
        return

    await query.answer()
    context.user_data["profile_input_state"] = True
    context.user_data["perf_action"] = "delete"

    keyboard = [[InlineKeyboardButton("◀️ 返回业绩菜单", callback_data="perf_menu")]]

    await query.message.edit_text(
        "🗑️ **删除业绩**\n\n"
        "请输入要删除的业绩编号：\n"
        "例如：`1`\n\n"
        "💡 编号是汇总列表中第一列的序号\n"
        "❌ 发送 /cancel 取消",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_DELETE


async def performance_delete_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """接收删除编号"""
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_admin(user_id) and user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有管理员才能删除业绩")
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        return ConversationHandler.END

    text = update.message.text.strip()

    # 检查键盘按钮
    if _is_keyboard_button(text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text(
            "请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
        return ConversationHandler.END

    if text == '/cancel':
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        await update.message.reply_text("❌ 已取消删除")
        from handlers.menu import get_main_menu
        await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return ConversationHandler.END

    if len(text.split()) < 3 and not any(c.isdigit() for c in text):
        context.user_data.pop("profile_input_state", None)
        context.user_data.pop("perf_action", None)
        from handlers.menu import get_main_menu
        await update.message.reply_text("已返回主菜单", reply_markup=get_main_menu(user_id))
        return ConversationHandler.END

    context.user_data["_message_handled"] = True

    if not text.isdigit():
        await update.message.reply_text("❌ 请输入数字编号\n请重新输入：")
        return PERFORMANCE_DELETE

    record_id = int(text)

    record = get_performance_record_by_id(admin_id, record_id)
    if not record:
        await update.message.reply_text(f"❌ 未找到编号 {record_id} 的业绩记录\n请重新输入：")
        return PERFORMANCE_DELETE

    if delete_performance_record(admin_id, record_id):
        await update.message.reply_text(f"✅ 已删除编号 {record_id} 的业绩记录")
    else:
        await update.message.reply_text("❌ 删除失败")

    context.user_data.pop("profile_input_state", None)
    context.user_data.pop("perf_action", None)

    from handlers.menu import get_main_menu
    await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END


# ==================== 导出业绩 ====================

async def performance_export_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """导出业绩 - 选择月份"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()

    months = get_performance_available_months(admin_id)
    if not months:
        await query.message.edit_text("📭 暂无业绩记录可导出")
        return PERFORMANCE_MENU

    keyboard = []
    for m in months[:12]:
        year, month = m.split('-')
        keyboard.append([InlineKeyboardButton(
            f"📅 {year}年{int(month)}月",
            callback_data=f"perf_export_{m}"
        )])

    keyboard.append([InlineKeyboardButton("📥 导出全部", callback_data="perf_export_all")])
    keyboard.append([InlineKeyboardButton("◀️ 返回业绩汇总", callback_data="perf_menu")])

    await query.message.edit_text(
        "📥 **导出业绩汇总**\n\n请选择要导出的月份，或导出全部：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return PERFORMANCE_MONTH_SELECT


async def performance_export_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行导出"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    data = query.data

    config = get_commission_config(admin_id)

    if data == "perf_export_all":
        records = get_performance_records(admin_id)
        if not records:
            await query.answer("暂无业绩记录", show_alert=True)
            return
        year_str = "全部"
        total_profit = sum(r['profit'] for r in records)

        employee_commission = {}
        employee_performance = {}
        for r in records:
            profit = r['profit']
            channel_rate = r.get('channel_rate', config['channel_rate'])
            customer_rate = r.get('customer_rate', config['customer_rate'])

            for emp_id, emp_name, rate in [
                (r['channel_employee_id'], r['channel_employee_name'], channel_rate),
                (r['customer_employee_id'], r['customer_employee_name'], customer_rate)
            ]:
                if emp_id not in employee_commission:
                    employee_commission[emp_id] = {"name": emp_name or f"ID{emp_id}", "commission": 0}
                    employee_performance[emp_id] = {"name": emp_name or f"ID{emp_id}", "performance": 0}
                employee_commission[emp_id]["commission"] += profit * rate / 100
                employee_performance[emp_id]["performance"] += profit / 2
    else:
        month_str = data.replace("perf_export_", "")
        year, month = month_str.split('-')
        year = int(year)
        month = int(month)
        year_str = f"{year}年{month}月"

        summary = get_performance_summary(admin_id, year, month)
        records = summary['records']
        total_profit = summary['total_profit']
        employee_commission = summary['employee_commission']
        employee_performance = summary['employee_performance']

    await query.answer("正在生成HTML...")

    html = generate_performance_html(records, total_profit, employee_commission, employee_performance, year_str, config)

    import tempfile, os
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', encoding='utf-8', delete=False) as f:
        f.write(html)
        temp_path = f.name

    try:
        with open(temp_path, 'rb') as f:
            await query.message.reply_document(
                document=f,
                filename=f"业绩汇总_{year_str}.html",
                caption=f"📊 {year_str}业绩汇总已导出"
            )
    finally:
        os.unlink(temp_path)


def generate_performance_html(records, total_profit, employee_commission, employee_performance, title, config):
    """生成业绩HTML"""
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>{title} 业绩汇总</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #0f172a, #1e3a5f);
            min-height: 100vh; padding: 40px 20px; color: #e2e8f0;
        }}
        .container {{ max-width: 1000px; margin: 0 auto; }}
        .header {{
            text-align: center; padding: 30px; background: rgba(255,255,255,0.05);
            border-radius: 20px; margin-bottom: 30px; backdrop-filter: blur(10px);
        }}
        .header h1 {{ font-size: 28px; background: linear-gradient(135deg, #f59e0b, #ef4444); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
        .header .profit {{ font-size: 36px; font-weight: bold; color: #10b981; margin-top: 10px; }}
        .config {{
            background: rgba(255,255,255,0.05); border-radius: 16px; padding: 16px 20px;
            margin-bottom: 30px; backdrop-filter: blur(10px); display: flex; justify-content: center; gap: 40px;
        }}
        .config-item {{ text-align: center; }}
        .config-item .label {{ font-size: 12px; color: #94a3b8; }}
        .config-item .value {{ font-size: 20px; font-weight: bold; color: #f59e0b; }}
        .table-container {{
            background: rgba(255,255,255,0.05); border-radius: 16px; padding: 20px;
            margin-bottom: 30px; backdrop-filter: blur(10px); overflow-x: auto;
        }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{
            background: rgba(245,158,11,0.2); padding: 14px 12px; text-align: left;
            font-weight: 600; color: #f59e0b; border-bottom: 2px solid rgba(245,158,11,0.3);
            white-space: nowrap;
        }}
        td {{ padding: 12px; border-bottom: 1px solid rgba(255,255,255,0.06); }}
        tr:hover {{ background: rgba(255,255,255,0.03); }}
        .income {{ color: #10b981; }}
        .expense {{ color: #ef4444; }}
        .profit-cell {{ color: #f59e0b; font-weight: 600; }}
        .employee-section {{
            background: rgba(255,255,255,0.05); border-radius: 16px; padding: 24px;
            margin-bottom: 30px; backdrop-filter: blur(10px);
        }}
        .employee-section h2 {{ color: #f59e0b; margin-bottom: 16px; }}
        .employee-card {{
            display: flex; justify-content: space-between; align-items: center;
            padding: 16px; margin-bottom: 10px; background: rgba(255,255,255,0.03);
            border-radius: 12px; border: 1px solid rgba(255,255,255,0.06);
        }}
        .employee-name {{ font-size: 16px; font-weight: 600; }}
        .employee-commission {{ color: #10b981; font-size: 18px; font-weight: bold; }}
        .employee-perf {{ color: #94a3b8; font-size: 14px; margin-left: 10px; }}
        .footer {{ text-align: center; color: #64748b; font-size: 12px; padding: 20px; }}
        @media (max-width: 640px) {{
            .employee-card {{ flex-direction: column; text-align: center; gap: 8px; }}
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 {title} 业绩汇总</h1>
        <div class="profit">公司总利润：{total_profit:.2f} USDT</div>
    </div>
    <div class="config">
        <div class="config-item"><div class="label">通道提成</div><div class="value">{config['channel_rate']}%</div></div>
        <div class="config-item"><div class="label">客户提成</div><div class="value">{config['customer_rate']}%</div></div>
    </div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>编号</th><th>日期</th><th>国家</th><th>通道群名</th><th>客户群名</th><th>通道收入</th><th>客户支出</th><th>利润</th><th>通道员工</th><th>客户员工</th><th>通道提成</th><th>客户提成</th>
                </tr>
            </thead>
            <tbody>
"""
    for r in records:
        ch_name = r.get('channel_employee_name') or f"ID{r.get('channel_employee_id')}"
        cu_name = r.get('customer_employee_name') or f"ID{r.get('customer_employee_id')}"
        ch_rate = r.get('channel_rate', config['channel_rate'])
        cu_rate = r.get('customer_rate', config['customer_rate'])
        html += f"""                <tr>
                    <td>{r.get('id', '')}</td><td>{r.get('date', '')}</td><td>{r.get('country', '')}</td>
                    <td>{r.get('channel_group', '')}</td><td>{r.get('customer_group', '')}</td>
                    <td class="income">{r.get('channel_income', 0):.0f}</td>
                    <td class="expense">{r.get('customer_expense', 0):.0f}</td>
                    <td class="profit-cell">{r.get('profit', 0):.0f}</td>
                    <td>{ch_name}</td><td>{cu_name}</td>
                    <td>{ch_rate}%</td><td>{cu_rate}%</td>
                </tr>
"""
    html += """            </tbody>
        </table>
    </div>
    <div class="employee-section">
        <h2>💰 员工提成汇总</h2>
"""
    for emp_id, data in employee_commission.items():
        perf = employee_performance.get(emp_id, {}).get('performance', 0)
        html += f"""        <div class="employee-card">
            <span class="employee-name">{data['name']}</span>
            <span>
                <span class="employee-commission">{data['commission']:.2f} USDT</span>
                <span class="employee-perf">业绩 {perf:.2f} USDT</span>
            </span>
        </div>
"""
    html += """    </div>
    <div class="footer">由记账机器人自动生成 · 提成=利润×提成比例 · 业绩=利润÷2</div>
</div>
</body>
</html>"""
    return html


# ==================== 记录追溯 ====================

async def performance_trace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """记录追溯 - 显示本月操作日志"""
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)

    if not is_admin(user_id) and user_id != OWNER_ID:
        await query.answer("❌ 只有管理员才能查看追溯", show_alert=True)
        return

    await query.answer()

    from datetime import datetime, timezone, timedelta
    from auth import operators as auth_operators

    beijing_tz = timezone(timedelta(hours=8))
    now = datetime.now(beijing_tz)
    current_month = now.strftime('%Y-%m')

    all_records = get_performance_records(admin_id)
    month_records = [r for r in all_records if r.get('date', '').startswith(current_month)]

    if not month_records:
        await query.message.edit_text(
            f"📝 **记录追溯 - {now.strftime('%Y年%m月')}**\n\n📭 本月暂无记录",
            parse_mode="Markdown"
        )
        return

    month_records.sort(key=lambda x: x.get('created_at', 0), reverse=True)

    def get_user_name(uid):
        if uid in auth_operators:
            name = auth_operators[uid].get('first_name', '')
            uname = auth_operators[uid].get('username', '')
            return f"{name}(@{uname})" if uname else name or str(uid)
        return f"ID{uid}"

    text = f"📝 **记录追溯 - {now.strftime('%Y年%m月')}**\n"
    text += f"共 **{len(month_records)}** 条记录\n\n"

    for i, r in enumerate(month_records, 1):
        created_time = datetime.fromtimestamp(r['created_at'], tz=beijing_tz).strftime('%m-%d %H:%M')
        operator = get_user_name(r['created_by'])
        ch_name = r.get('channel_employee_name') or f"ID{r['channel_employee_id']}"
        cu_name = r.get('customer_employee_name') or f"ID{r['customer_employee_id']}"
        profit = r['channel_income'] + r['customer_expense']
        ch_rate = r.get('channel_rate', 10)
        cu_rate = r.get('customer_rate', 10)

        text += f"`{i:<3} {created_time}`\n"
        text += f"   编号：`{r['id']}` | {r['country']} | 通道:{r['channel_income']:.0f} | 客户:{r['customer_expense']:.0f} | 利润:{profit:.0f}\n"
        text += f"   提成比例：通道{ch_rate}% / 客户{cu_rate}%\n"
        text += f"   通道员工：{ch_name} | 客户员工：{cu_name}\n"
        text += f"   操作人：{operator}\n\n"

    keyboard = [[InlineKeyboardButton("◀️ 返回业绩汇总", callback_data="perf_menu")]]

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


# ==================== 取消处理 ====================

async def performance_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """取消业绩记录操作"""
    user_id = update.effective_user.id

    context.user_data.pop("profile_input_state", None)
    context.user_data.pop("perf_action", None)

    await update.message.reply_text("❌ 已取消")

    from handlers.menu import get_main_menu
    await update.message.reply_text(
        "请选择功能：",
        reply_markup=get_main_menu(user_id)
    )
    return ConversationHandler.END


# ==================== 导出 ====================

__all__ = [
    'performance_menu',
    'performance_record_start',
    'performance_record_input',
    'performance_view_start',
    'performance_view_show',
    'performance_edit_start',
    'performance_edit_input',
    'performance_delete_start',
    'performance_delete_input',
    'performance_export_select',
    'performance_export_do',
    'performance_trace',
    'performance_commission_set',
    'performance_commission_input',
    'performance_cancel',
    'get_commission_config',
    'PERFORMANCE_MENU',
    'PERFORMANCE_RECORD',
    'PERFORMANCE_VIEW',
    'PERFORMANCE_MONTH_SELECT',
    'PERFORMANCE_EDIT',
    'PERFORMANCE_DELETE',
    'PERFORMANCE_COMMISSION_SET',
    'PERFORMANCE_COMMISSION_INPUT',
]
