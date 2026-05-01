# handlers/transfer.py - 适配物理隔离

import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from auth import is_authorized, get_user_admin_id

# --- 配置 ---
TRON_GRID_API = "https://api.trongrid.io"
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"  # Tron USDT 合约地址

# --- 状态常量 ---
TRANSFER_QUERY_WAIT_ADDR = "transfer_query_wait_addr"
TRANSFER_ANALYSIS_WAIT_ADDR = "transfer_analysis_wait_addr"

# --- 辅助函数：调用 TronGrid API ---
def get_trc20_transfers(address, limit=200):
    """获取指定地址的 TRC20 转账记录"""
    url = f"{TRON_GRID_API}/v1/accounts/{address}/transactions/trc20"
    params = {
        "only_to": False,
        "limit": limit,
        "contract_address": USDT_CONTRACT
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])
        else:
            return []
    except Exception as e:
        print(f"API Error: {e}")
        return []

def extract_counterparties(transfers, my_address):
    """从交易列表中提取所有交易对手地址"""
    counterparties = set()
    for tx in transfers:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        if from_addr and from_addr != my_address:
            counterparties.add(from_addr)
        if to_addr and to_addr != my_address:
            counterparties.add(to_addr)
    return counterparties

# transfer.py

def get_trc20_transfers_paginated(address, limit=200, max_total=1000):
    """分页获取 TRC20 转账记录，最多 max_total 条"""
    all_txs = []
    page = 0

    while len(all_txs) < max_total:
        remaining = min(limit, max_total - len(all_txs))
        url = f"{TRON_GRID_API}/v1/accounts/{address}/transactions/trc20"
        params = {
            "only_to": False,
            "limit": remaining,
            "contract_address": USDT_CONTRACT
        }
        if page > 0:
            # 需要添加 fingerprint 或 offset 来翻页
            params["fingerprint"] = fingerprint

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                txs = data.get("data", [])
                if not txs:
                    break
                all_txs.extend(txs)
                fingerprint = data.get("fingerprint", "")
                if not fingerprint:
                    break
                page += 1
            else:
                break
        except Exception as e:
            print(f"分页查询失败: {e}")
            break

    return all_txs

# --- 功能 1: 转账查询 (直接交易记录) ---
async def start_transfer_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    if not is_authorized(user_id, require_full_access=True):
        await query.message.reply_text("❌ 无权限使用此功能")
        return ConversationHandler.END
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await query.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return ConversationHandler.END
    context.user_data["active_module"] = "transfer"
    context.user_data["admin_id"] = admin_id
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    await query.message.reply_text(
        "🔍 **转账查询**\n\n请输入两个 USDT 地址，中间用空格隔开：\n"
        "例如：`Txxxx... Tyyyy...`",
        parse_mode="Markdown"
    )
    return TRANSFER_QUERY_WAIT_ADDR

async def process_transfer_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ 格式错误，请输入两个地址，用空格隔开。")
        return TRANSFER_QUERY_WAIT_ADDR
    addr_a, addr_b = parts[0], parts[1]
    if not (addr_a.startswith('T') and addr_b.startswith('T')) or len(addr_a) != 34 or len(addr_b) != 34:
        await update.message.reply_text("❌ 地址格式看起来不正确 (Tron 地址以 T 开头，长度 34)。")
        return TRANSFER_QUERY_WAIT_ADDR
    await update.message.reply_text("⏳ 正在查询链上数据，请稍候...")
    history_a = get_trc20_transfers_paginated(addr_a, limit=200, max_total=1000)
    history_b = get_trc20_transfers_paginated(addr_b, limit=200, max_total=1000)
    matches = []
    for tx in history_a:
        if tx.get("to") == addr_b or tx.get("from") == addr_b:
            matches.append(tx)
    seen_tx_ids = set()
    unique_matches = []
    for tx in matches:
        tx_id = tx.get("txID")
        if tx_id not in seen_tx_ids:
            seen_tx_ids.add(tx_id)
            unique_matches.append(tx)
    if not unique_matches:
        for tx in history_b:
            if tx.get("to") == addr_a or tx.get("from") == addr_a:
                if tx.get("txID") not in seen_tx_ids:
                    unique_matches.append(tx)
    if not unique_matches:
        await update.message.reply_text(f"📭 未找到直接转账记录。", parse_mode="HTML")
        context.user_data.pop("active_module", None)
        return ConversationHandler.END
    context.user_data["transfer_results"] = unique_matches
    context.user_data["current_page"] = 0
    context.user_data["query_type"] = "direct"
    await send_transfer_page(update, context, 0)
    return ConversationHandler.END

# --- 功能 2: 转账分析 (共同交易对手) ---
async def start_transfer_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    if not is_authorized(user_id, require_full_access=True):
        await query.message.reply_text("❌ 无权限使用此功能")
        return ConversationHandler.END
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await query.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return ConversationHandler.END
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data["active_module"] = "transfer"
    context.user_data["admin_id"] = admin_id
    await query.message.reply_text(
        "🕵️ **转账分析**\n\n将分析是否有第三方地址与这两个地址都产生过交易。\n"
        "请输入两个 USDT 地址，中间用空格隔开：\n"
        "例如：`Txxxx... Tyyyy...`\n"
        "💡 提示：输入 /cancel 可取消操作",
        parse_mode="Markdown"
    )
    return TRANSFER_ANALYSIS_WAIT_ADDR

async def process_transfer_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["active_module"] = "transfer"
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ 格式错误，请输入两个地址，用空格隔开。")
        return TRANSFER_ANALYSIS_WAIT_ADDR
    addr_a, addr_b = parts[0], parts[1]
    if not (addr_a.startswith('T') and addr_b.startswith('T')) or len(addr_a) != 34 or len(addr_b) != 34:
        await update.message.reply_text("❌ 地址格式看起来不正确。")
        return TRANSFER_ANALYSIS_WAIT_ADDR
    await update.message.reply_text("⏳ 正在深度分析链上关系，这可能需要一点时间...")
    history_a = get_trc20_transfers_paginated(addr_a, limit=200, max_total=1000)
    history_b = get_trc20_transfers_paginated(addr_b, limit=200, max_total=1000)
    set_a = extract_counterparties(history_a, addr_a)
    set_b = extract_counterparties(history_b, addr_b)
    common_parties = list(set_a.intersection(set_b))
    common_parties = [p for p in common_parties if p != addr_a and p != addr_b]
    if not common_parties:
        await update.message.reply_text(f"📭 未发现共同交易对手。", parse_mode="HTML")
        context.user_data.pop("active_module", None)
        return ConversationHandler.END
    context.user_data["transfer_results"] = common_parties
    context.user_data["current_page"] = 0
    context.user_data["query_type"] = "analysis"
    await send_transfer_page(update, context, 0)
    return ConversationHandler.END

# --- 通用：发送分页结果 ---
async def send_transfer_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page_num):
    results = context.user_data.get("transfer_results", [])
    query_type = context.user_data.get("query_type")
    if not results:
        return
    page_size = 5
    total_pages = (len(results) + page_size - 1) // page_size
    start_idx = page_num * page_size
    end_idx = min(start_idx + page_size, len(results))
    current_items = results[start_idx:end_idx]
    text = ""
    keyboard = []
    if query_type == "direct":
        text = f"🔗 **直接转账记录** (第 {page_num+1}/{total_pages} 页)\n\n"
        for i, tx in enumerate(current_items, start=start_idx):
            raw_amount = tx.get("value") or tx.get("amount")
            try:
                if raw_amount is None:
                    amount_float = 0.0
                else:
                    amount_float = float(str(raw_amount)) / 1_000_000.0
            except (ValueError, TypeError) as e:
                print(f"ERROR converting amount: {e}, raw value: {raw_amount}")
                amount_float = 0.0
            amount_str = f"{amount_float:.2f}" if amount_float >= 0.01 else f"{amount_float:.6f}"
            from_addr = tx.get("from", "Unknown")
            to_addr = tx.get("to", "Unknown")
            from_short = f"{from_addr[:6]}...{from_addr[-6:]}" if len(from_addr) >= 12 else from_addr
            to_short = f"{to_addr[:6]}...{to_addr[-6:]}" if len(to_addr) >= 12 else to_addr
            text += f"{i+1}. 💰 **{amount_str} USDT**\n"
            text += f"   🟢 {from_short} ➡️ 🔴 {to_short}\n"
            timestamp = tx.get("block_timestamp", 0)
            if timestamp:
                from datetime import datetime
                dt = datetime.fromtimestamp(timestamp / 1000)
                text += f"   ⏰ {dt.strftime('%Y-%m-%d %H:%M:%S')}\n"
            tx_id = tx.get("txID", "")
            if tx_id:
                text += f"   🔗 [查看详情](https://tronscan.org/#/transaction/{tx_id})\n"
            text += "\n"
    elif query_type == "analysis":
        text = f"🕸️ **共同交易对手地址** (第 {page_num+1}/{total_pages} 页)\n\n"
        text += "以下地址同时与您查询的两个地址有过交易：\n\n"
        for i, addr in enumerate(current_items, start=start_idx):
            short_addr = addr[:8] + "..." + addr[-6:]
            text += f"{i+1}. `{addr}`\n   ({short_addr})\n\n"
            keyboard.append([InlineKeyboardButton(f"📋 复制地址 {i+1}", callback_data=f"copy_addr_{addr}")])
    nav_buttons = []
    if page_num > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f"trans_page_{page_num-1}"))
    if page_num < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f"trans_page_{page_num+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("◀️ 返回主菜单", callback_data="transfer_back_to_main")])
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=reply_markup, disable_web_page_preview=True)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=reply_markup, disable_web_page_preview=True)

# --- 处理分页点击 ---
async def handle_transfer_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("trans_page_"):
        page_num = int(data.split("_")[2])
        await send_transfer_page(update, context, page_num)
    elif data.startswith("copy_addr_"):
        addr = data.replace("copy_addr_", "")
        await query.message.reply_text(f"📋 已获取地址，长按复制：\n<code>{addr}</code>", parse_mode="HTML")
        context.user_data.pop("transfer_results", None)
        context.user_data.pop("current_page", None)
        context.user_data.pop("query_type", None)
        context.user_data.pop("active_module", None)
        await query.message.edit_text("✅ 查询完成，返回主菜单")
        from handlers.menu import get_main_menu
        await query.message.reply_text("请选择功能：", reply_markup=get_main_menu(query.from_user.id))

# --- 取消 ---
async def cancel_transfer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("active_module", None)
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    user_id = update.effective_user.id
    await update.message.reply_text("❌ 已取消互转查询")
    from handlers.menu import get_main_menu
    await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END

# --- 主菜单入口 ---
async def show_transfer_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        await query.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await query.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data["active_module"] = "transfer"
    context.user_data["admin_id"] = admin_id
    keyboard = [
        [InlineKeyboardButton("🔍 转账查询 (直接记录)", callback_data="trans_direct")],
        [InlineKeyboardButton("🕸️ 转账分析 (共同对手)", callback_data="trans_analysis")],
        [InlineKeyboardButton("◀️ 返回主菜单", callback_data="transfer_back_to_main")]
    ]
    await query.message.reply_text(
        "💱 **互转查询功能**\n请选择操作：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def transfer_back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """返回主菜单并清除互转查询状态"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    # 清除所有互转查询相关的状态
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data.pop("active_module", None)

    from handlers.menu import get_main_menu
    try:
        await query.message.edit_text(
            "✅ 已退出互转查询\n\n请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
    except Exception:
        # 编辑失败则发送新消息
        await query.message.reply_text(
            "✅ 已退出互转查询\n\n请选择功能：",
            reply_markup=get_main_menu(user_id)
        )
    return ConversationHandler.END

async def cancel_transfer_from_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data.pop("active_module", None)
    user_id = update.effective_user.id
    await update.message.reply_text("❌ 已取消互转查询")
    from handlers.menu import get_main_menu
    await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END

# --- 键盘版入口 ---
async def show_transfer_menu_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import ReplyKeyboardMarkup, KeyboardButton
    user_id = update.effective_user.id
    if not is_authorized(user_id, require_full_access=True):
        await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data["active_module"] = "transfer"
    context.user_data["admin_id"] = admin_id
    keyboard = [
        [KeyboardButton("🔍 转账查询"), KeyboardButton("🕸️ 转账分析")],
        [KeyboardButton("◀️ 返回主菜单")],
    ]
    await update.message.reply_text(
        "💱 **互转查询功能**\n请选择操作：",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

__all__ = [
    'get_trc20_transfers',
    'extract_counterparties',
    'start_transfer_query',
    'process_transfer_query',
    'start_transfer_analysis',
    'process_transfer_analysis',
    'send_transfer_page',
    'handle_transfer_pagination',
    'cancel_transfer',
    'show_transfer_menu',
    'transfer_back_to_main',
    'cancel_transfer_from_message',
    'show_transfer_menu_keyboard',
]