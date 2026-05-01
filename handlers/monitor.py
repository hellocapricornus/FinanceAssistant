# handlers/monitor.py - 完整版（适配物理隔离）

import asyncio
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters, ConversationHandler, CommandHandler
from auth import is_authorized, get_user_admin_id, OWNER_ID
from db import (
    get_monitored_addresses as db_get_monitored_addresses,
    add_monitored_address as db_add_monitored_address,
    remove_monitored_address as db_remove_monitored_address,
    update_address_last_check as db_update_address_last_check,
    add_transaction_record as db_add_transaction_record,
    is_tx_notified as db_is_tx_notified,
    mark_tx_notified as db_mark_tx_notified,
    get_user_preferences as db_get_user_preferences,
    get_db_connection
)
from handlers.menu import get_main_menu
from db_manager import get_conn

# 状态定义
MONITOR_MENU = 0
MONITOR_ADD = 1
MONITOR_ADD_NOTE = 2
MONITOR_REMOVE = 3

# 设置北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

# API 配置
TRONGRID_API = "https://api.trongrid.io"
TRONGRID_API_KEY = "b7f1c9fa-a622-49ad-972e-9ce838faccbe"
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

# ==================== 并发控制配置（新增） ====================
MONITOR_SEMAPHORE = asyncio.Semaphore(8)   # 最多同时检查 8 个地址
ADDRESS_CHECK_TIMEOUT = 25                 # 单个地址检查超时 25 秒
logger = logging.getLogger(__name__)       # 日志记录器


# ==================== 链上查询函数（无需数据库） ====================
async def get_address_balance(address: str) -> float:
    import aiohttp
    try:
        url = f"{TRONGRID_API}/v1/accounts/{address}"
        headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                if data.get('data') and len(data['data']) > 0:
                    trc20 = data['data'][0].get('trc20', [])
                    for token in trc20:
                        if USDT_CONTRACT in token:
                            return int(token[USDT_CONTRACT]) / 1_000_000
    except Exception as e:
        print(f"查询余额失败: {e}")
    return 0.0


async def get_monthly_stats(address: str) -> dict:
    import aiohttp
    from datetime import datetime
    now = datetime.now()
    first_day = datetime(now.year, now.month, 1, 0, 0, 0)
    start_timestamp = int(first_day.timestamp())
    total_received = 0.0
    total_sent = 0.0
    try:
        url = f"{TRONGRID_API}/v1/accounts/{address}/transactions/trc20"
        headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY}
        params = {
            "contract_address": USDT_CONTRACT,
            "limit": 200,
            "min_timestamp": start_timestamp * 1000
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    txs = data.get("data", [])
                    for tx in txs:
                        from_addr = tx.get("from", "")
                        to_addr = tx.get("to", "")
                        raw_amount = tx.get("value", 0)
                        amount = int(raw_amount) / 1_000_000 if raw_amount else 0
                        if to_addr == address:
                            total_received += amount
                        elif from_addr == address:
                            total_sent += amount
    except Exception as e:
        print(f"查询月度统计失败: {e}")
    return {
        "received": total_received,
        "sent": total_sent,
        "net": total_received - total_sent
    }


async def get_trc20_transactions(address: str, min_timestamp: int = 0, limit: int = 200, offset: int = 0):
    import aiohttp
    import asyncio
    max_retries = 3
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            url = f"{TRONGRID_API}/v1/accounts/{address}/transactions/trc20"
            headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY}
            params = {
                "contract_address": USDT_CONTRACT,
                "limit": limit,
                "min_timestamp": min_timestamp if min_timestamp > 0 else 0,
                "offset": offset
            }
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("data", [])
                    else:
                        print(f"API返回错误状态码: {resp.status}")
                        return []
        except asyncio.CancelledError:
            print(f"请求被取消: {address}")
            return []
        except Exception as e:
            print(f"查询交易失败 (尝试 {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                return []
    return []


# ==================== 定时检查（遍历每个管理员独立库） ====================
async def check_address_transactions(context: ContextTypes.DEFAULT_TYPE):
    """
    并发检查所有管理员的监控地址交易（改造后 - 支持并发）
    """
    from auth import admins

    tasks = []
    current_time = int(time.time())
    total_addresses = 0

    # 1. 收集所有需要检查的地址
    for admin_id in list(admins.keys()):
        # 跳过已删除的管理员
        admin_info = admins.get(admin_id, {})
        if admin_info.get("deleted_at", 0) > 0:
            continue

        addresses = db_get_monitored_addresses(admin_id)
        if not addresses:
            continue

        total_addresses += len(addresses)

        for addr_info in addresses:
            # 为每个地址创建一个并发任务
            task = asyncio.create_task(
                _check_single_address_safe(addr_info, admin_id, context, current_time)
            )
            tasks.append(task)

    if not tasks:
        return

    logger.info(f"开始并发检查 {total_addresses} 个监控地址（最多 8 个同时进行）...")

    # 2. 等待所有任务完成（总超时 60 秒）
    done, pending = await asyncio.wait(tasks, timeout=60)

    # 3. 取消超时的任务
    if pending:
        logger.warning(f"{len(pending)} 个监控任务超时，已取消")
        for task in pending:
            task.cancel()

    # 4. 统计结果
    success = sum(1 for t in done if not t.exception())
    failed = len(done) - success

    logger.info(f"监控检查完成：成功 {success}，失败 {failed}，超时 {len(pending)}")

# ==================== 新增的辅助函数 ====================

async def _check_single_address_safe(addr_info, admin_id, context, current_time):
    """带信号量和超时保护的单地址检查"""
    async with MONITOR_SEMAPHORE:  # 控制并发数，最多 8 个同时执行
        try:
            await asyncio.wait_for(
                _check_single_address(addr_info, admin_id, context, current_time),
                timeout=ADDRESS_CHECK_TIMEOUT
            )
        except asyncio.TimeoutError:
            address = addr_info.get("address", "未知")
            logger.warning(f"地址检查超时: {address[:12]}...")
        except Exception as e:
            logger.error(f"地址检查失败: {e}")


async def _check_single_address(addr_info, admin_id, context, current_time):
    """
    检查单个地址的交易（从原 check_address_transactions 提取出来的逻辑）
    这段代码就是原来 check_address_transactions 里面处理单个地址的部分
    """
    bot = context.bot
    address = addr_info["address"]
    last_check = addr_info.get("last_check", 0)
    added_at = addr_info.get("added_at", 0)
    note = addr_info.get("note", "")
    added_by = addr_info.get("added_by")

    # 检查该用户是否关闭了监控通知
    if added_by:
        prefs = db_get_user_preferences(added_by, admin_id)
        if not prefs.get("monitor_notify", True):
            return  # 直接返回，不检查

    # 确定查询起始时间
    if last_check == 0:
        min_timestamp = added_at * 1000
    else:
        min_timestamp = last_check * 1000

    txs = await get_trc20_transactions(address, min_timestamp)
    if not txs:
        db_update_address_last_check(admin_id, address, current_time)
        return

    # 获取当前余额和月度统计
    current_balance = await get_address_balance(address)
    monthly_stats = await get_monthly_stats(address)

    for tx in txs:
        tx_id = tx.get("transaction_id", "")
        from_addr = tx.get("from", "")
        to_addr = tx.get("to", "")
        raw_amount = tx.get("value", 0)
        amount = int(raw_amount) / 1_000_000 if raw_amount else 0
        timestamp = tx.get("block_timestamp", 0) / 1000

        if db_is_tx_notified(admin_id, tx_id):
            continue

        if last_check == 0 and timestamp < added_at:
            db_mark_tx_notified(admin_id, tx_id)
            continue

        # 检查是否已存在
        conn = get_conn(admin_id)
        c = conn.cursor()
        c.execute("SELECT id, notified FROM address_transactions WHERE tx_id = ?", (tx_id,))
        existing = c.fetchone()
        if existing:
            if existing[1] == 0:
                db_mark_tx_notified(admin_id, tx_id)
            continue

        db_add_transaction_record(admin_id, address, tx_id, from_addr, to_addr, amount, int(timestamp))

        # 发送通知
        direction = "收到" if to_addr == address else "转出"
        short_from = f"{from_addr[:6]}...{from_addr[-6:]}" if len(from_addr) > 12 else from_addr
        short_to = f"{to_addr[:6]}...{to_addr[-6:]}" if len(to_addr) > 12 else to_addr
        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        beijing_time = utc_time.astimezone(BEIJING_TZ)
        time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')

        message = (
            f"🔔 **USDT 交易监控提醒**\n\n"
            f"📌 监控地址：`{address[:8]}...{address[-6:]}`\n"
        )
        if note:
            message += f"📝 备注：{note}\n"
        else:
            message += "\n"
        message += f"💎 当前余额：**{current_balance:.2f} USDT**\n\n"
        message += (
            f"💰 金额：**{amount:.2f} USDT**\n"
            f"🔄 方向：{direction}\n"
            f"📤 发送方：`{short_from}`\n"
            f"📥 接收方：`{short_to}`\n"
            f"⏰ 时间：{time_str}\n\n"
        )
        message += (
            f"📅 **本月统计**\n"
            f"• 累计收到：**{monthly_stats['received']:.2f} USDT**\n"
            f"• 累计转出：**{monthly_stats['sent']:.2f} USDT**\n"
            f"• 净收入：**{monthly_stats['net']:.2f} USDT**\n\n"
        )

        try:
            await bot.send_message(chat_id=added_by, text=message, parse_mode="Markdown")
            logger.info(f"已发送监控通知给用户 {added_by} (备注: {note or '无'})")
        except Exception as e:
            logger.error(f"发送给用户 {added_by} 失败: {e}")

        db_mark_tx_notified(admin_id, tx_id)

    db_update_address_last_check(admin_id, address, current_time)

# ==================== 键盘菜单 ====================
def get_monitor_keyboard_markup(user_id: int = None):
    if user_id:
        admin_id = get_user_admin_id(user_id)
        addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    else:
        # 无 user_id 时返回空键盘（仅添加按钮）
        addresses = []
    keyboard = [[KeyboardButton("➕ 添加监控地址")]]
    if addresses:
        keyboard.append([KeyboardButton("📋 监控列表"), KeyboardButton("📊 月度统计")])
        keyboard.append([KeyboardButton("❌ 删除监控地址")])
    keyboard.append([KeyboardButton("◀️ 返回主菜单")])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ==================== 回调命令处理 ====================
async def monitor_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        await query.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    keyboard = [
        [InlineKeyboardButton("➕ 添加监控地址", callback_data="monitor_add")],
    ]
    if addresses:
        keyboard += [
            [InlineKeyboardButton("📋 查看监控列表", callback_data="monitor_list")],
            [InlineKeyboardButton("📊 月度统计", callback_data="monitor_stats")],
            [InlineKeyboardButton("❌ 删除监控地址", callback_data="monitor_remove")],
        ]
    keyboard.append([InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")])
    if len(addresses) == 0:
        text = (
            "🔔 USDT 地址监控\n\n"
            f"📊 您的监控地址数：0 个\n\n"
            "⚠️ 暂无监控地址，请先添加监控地址。\n\n"
            "当您监控的地址有 USDT 交易时，会发送通知给您。\n\n"
            "💡 提示：监控间隔约 30 秒\n\n"
            "📝 支持为地址添加备注，方便识别"
        )
    else:
        text = (
            f"🔔 USDT 地址监控\n\n"
            f"📊 您的监控地址数：{len(addresses)} 个\n\n"
            "当您监控的地址有 USDT 交易时，会发送通知给您。\n\n"
            "💡 提示：监控间隔约 30 秒\n\n"
            "📝 支持为地址添加备注，方便识别"
        )
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def monitor_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    if not addresses:
        await query.message.edit_text("📭 您还没有添加任何监控地址")
        await asyncio.sleep(1)
        await monitor_menu(update, context)
        return
    await query.message.edit_text("📊 正在查询月度统计，请稍候...")
    text = "📊 **监控地址月度统计**\n\n"
    for addr_info in addresses:
        address = addr_info["address"]
        note = addr_info.get("note", "")
        short_addr = f"{address[:8]}...{address[-6:]}"
        stats = await get_monthly_stats(address)
        text += f"📌 {short_addr}"
        if note:
            text += f" ({note})"
        text += f"\n   💰 本月收到：**{stats['received']:.2f} USDT**"
        text += f"\n   📤 本月转出：**{stats['sent']:.2f} USDT**"
        text += f"\n   📈 净收入：**{stats['net']:.2f} USDT**\n\n"
    keyboard = [[InlineKeyboardButton("◀️ 返回", callback_data="monitor_menu")]]
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def monitor_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    if not addresses:
        await query.message.edit_text("📭 您还没有添加任何监控地址，请先添加。")
        await asyncio.sleep(1)
        await monitor_menu(update, context)
        return
    text = "📋 **您的监控地址列表**\n\n"
    for i, addr in enumerate(addresses, 1):
        full_addr = addr['address']
        note = addr.get('note', '')
        text += f"{i}. `{full_addr}` ({addr['chain_type']})\n"
        if note:
            text += f"   📝 备注：{note}\n"
        added_time = datetime.fromtimestamp(addr['added_at'], tz=timezone.utc).astimezone(BEIJING_TZ)
        text += f"   📅 添加时间：{added_time.strftime('%Y-%m-%d %H:%M')}\n\n"
    keyboard = [[InlineKeyboardButton("◀️ 返回", callback_data="monitor_menu")]]
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


async def monitor_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["monitor_action"] = "add"
    text = (
        "➕ 添加监控地址\n\n"
        "请输入要监控的 USDT 地址：\n\n"
        "支持格式：\n"
        "• TRC20: T 开头，34位\n"
        "• ERC20: 0x 开头，42位\n\n"
        "❌ 输入 /cancel_monitor 取消"
    )
    try:
        await query.message.edit_text(text, parse_mode=None)
    except Exception as e:
        await query.message.reply_text(text, parse_mode=None)
    return MONITOR_ADD


async def monitor_add_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if text == "/cancel_monitor":
        context.user_data.pop("monitor_action", None)
        await update.message.reply_text("❌ 已取消添加", reply_markup=get_monitor_keyboard_markup(user_id))
        return ConversationHandler.END
    # 验证地址格式
    trc20_pattern = r'^T[0-9A-Za-z]{33}$'
    erc20_pattern = r'^0x[0-9a-fA-F]{40}$'
    if re.match(trc20_pattern, text):
        chain_type = "TRC20"
        address = text
    elif re.match(erc20_pattern, text):
        chain_type = "ERC20"
        address = text
    else:
        await update.message.reply_text(
            "❌ 地址格式不正确，请重新输入：\n\n输入 /cancel_monitor 取消",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◀️ 返回主菜单")]], resize_keyboard=True)
        )
        return MONITOR_ADD
    context.user_data["monitor_temp"] = {
        "address": address,
        "chain_type": chain_type
    }
    context.user_data["monitor_action"] = "add_note"
    await update.message.reply_text(
        f"✅ 地址已识别：`{address}` ({chain_type})\n\n"
        "📝 请输入备注（可选，用于标识这个地址）：\n"
        "例如：币安钱包、个人钱包、测试地址等\n\n"
        "直接发送「跳过」跳过备注\n\n"
        "❌ 输入 /cancel_monitor 取消",
        parse_mode=None,
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◀️ 返回主菜单")]], resize_keyboard=True)
    )
    return MONITOR_ADD_NOTE


async def monitor_add_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if text == "/cancel_monitor":
        context.user_data.pop("monitor_action", None)
        context.user_data.pop("monitor_temp", None)
        await update.message.reply_text("❌ 已取消添加", reply_markup=get_monitor_keyboard_markup(user_id))
        return ConversationHandler.END
    if text == "/skip" or text == "跳过":  # ✅ 支持 "跳过" 文本
        note = ""
    else:
        note = text
    temp = context.user_data.get("monitor_temp", {})
    address = temp.get("address", "")
    chain_type = temp.get("chain_type", "")
    if not address:
        await update.message.reply_text("❌ 会话已过期，请重新添加", reply_markup=get_monitor_keyboard_markup(user_id))
        return ConversationHandler.END
    admin_id = get_user_admin_id(user_id)
    if db_add_monitored_address(admin_id, address, chain_type, user_id, note):
        msg = f"✅ 已添加监控地址\n\n📌 地址：`{address}`\n⛓️ 网络：{chain_type}"
        if note:
            msg += f"\n📝 备注：{note}"
        await update.message.reply_text(msg, parse_mode=None, reply_markup=get_monitor_keyboard_markup(user_id))
    else:
        await update.message.reply_text("❌ 添加失败，地址可能已存在", reply_markup=get_monitor_keyboard_markup(user_id))
    context.user_data.pop("monitor_action", None)
    context.user_data.pop("monitor_temp", None)
    return ConversationHandler.END


async def monitor_remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    if not addresses:
        await query.message.edit_text("📭 您还没有添加任何监控地址")
        await asyncio.sleep(1)
        await monitor_menu(update, context)
        return
    keyboard = []
    for addr in addresses:
        full_addr = addr['address']
        note = addr.get('note', '')
        short_addr = f"{full_addr[:12]}...{full_addr[-8:]}"
        if note:
            button_text = f"🗑️ {short_addr} ({note})"
        else:
            button_text = f"🗑️ {short_addr}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"monitor_del_{addr['id']}")])
    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="monitor_menu")])
    await query.message.edit_text(
        "🗑️ **删除监控地址**\n\n选择要删除的地址：",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return MONITOR_REMOVE


async def monitor_remove_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    address_id = int(query.data.split("_")[2])
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    is_owner = any(addr['id'] == address_id for addr in addresses)
    if not is_owner:
        await query.answer("❌ 只能删除自己添加的地址", show_alert=True)
        return
    if db_remove_monitored_address(admin_id, address_id):
        await query.answer("✅ 已删除")
        await query.message.edit_text("✅ 已删除监控地址")
    else:
        await query.answer("❌ 删除失败")
    return ConversationHandler.END


async def monitor_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    context.user_data.pop("monitor_action", None)
    context.user_data.pop("monitor_temp", None)
    await update.message.reply_text("❌ 已取消监控操作", reply_markup=get_monitor_keyboard_markup(user_id))
    return ConversationHandler.END


# ==================== 键盘版入口 ====================
async def monitor_menu_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    keyboard = [[KeyboardButton("➕ 添加监控地址")]]
    if addresses:
        keyboard.append([KeyboardButton("📋 查看监控列表"), KeyboardButton("📊 月度统计")])
        keyboard.append([KeyboardButton("❌ 删除监控地址")])
    keyboard.append([KeyboardButton("◀️ 返回主菜单")])
    if not addresses:
        text = "🔔 USDT 地址监控\n\n📊 您的监控地址数：0 个\n\n⚠️ 暂无监控地址，请先添加。"
    else:
        text = f"🔔 USDT 地址监控\n\n📊 您的监控地址数：{len(addresses)} 个\n\n当您监控的地址有交易时，会发送通知。"
    await update.message.reply_text(text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))


# ==================== ConversationHandler ====================
def get_monitor_conversation_handler():
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(monitor_add_start, pattern="^monitor_add$"),
            CallbackQueryHandler(monitor_remove_start, pattern="^monitor_remove$"),
        ],
        states={
            MONITOR_ADD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, monitor_add_input),
            ],
            MONITOR_ADD_NOTE: [
                CommandHandler("skip", monitor_add_note),
                MessageHandler(filters.TEXT & ~filters.COMMAND, monitor_add_note),
            ],
            MONITOR_REMOVE: [
                CallbackQueryHandler(monitor_remove_confirm, pattern="^monitor_del_"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel_monitor", monitor_cancel),
        ],
        per_message=False,
        allow_reentry=True,
    )


__all__ = [
    'get_address_balance',
    'get_trc20_transactions',
    'get_monthly_stats',
    'check_address_transactions',
    'monitor_menu',
    'monitor_list',
    'monitor_menu_keyboard',
    'get_monitor_keyboard_markup',
    'get_monitor_conversation_handler',
    'monitor_cancel',
]