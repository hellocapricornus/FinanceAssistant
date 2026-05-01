# handlers/usdt.py - 适配物理隔离（修复会话过期问题）

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
from auth import is_authorized, get_user_admin_id

USDT_CONTRACT_ADDR = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
PAGE_SIZE = 5

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        await query.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return

    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await query.answer("❌ 您不是任何管理员，无法使用此功能", show_alert=True)
        await query.message.reply_text("❌ 您不是任何管理员的成员，请联系超级管理员。")
        return

    await query.answer()

    # 设置模块标识和会话
    context.user_data["active_module"] = "usdt"
    context.user_data["usdt_session"] = {
        "waiting_for_address": True,
        "admin_id": admin_id
    }

    await query.message.reply_text("💰 请输入 TRON TRC20 地址（T 开头）：")

async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = context.user_data.get("usdt_session")
    if not session or not session.get("waiting_for_address"):
        return

    address = update.message.text.strip()
    admin_id = session.get("admin_id", 0)

    # 如果 session 中没有 admin_id，尝试重新获取
    if admin_id == 0:
        user_id = update.effective_user.id
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            context.user_data.pop("active_module", None)
            context.user_data.pop("usdt_session", None)
            return
        # 更新 session
        session["admin_id"] = admin_id
        context.user_data["usdt_session"] = session

    await update.message.reply_text("🔍 查询中，请稍等…")

    try:
        trx, usdt, txs = await query_tron(address)
        context.user_data["usdt_session"]["data"] = {
            "address": address,
            "trx": trx,
            "usdt": usdt,
            "transactions": txs,
            "page": 0,
            "admin_id": admin_id
        }
        context.user_data["usdt_session"]["waiting_for_address"] = False

        if trx == 0 and usdt == 0 and len(txs) == 0:
            await update.message.reply_text(
                f"📊 地址：<code>{address}</code>\n❌ 该地址无余额或未激活",
                parse_mode="HTML"
            )
            context.user_data.pop("active_module", None)
            context.user_data.pop("usdt_session", None)
        else:
            await send_trx_usdt_page(update, context)

    except Exception as e:
        print("❗ USDT 查询异常:", e)
        await update.message.reply_text("❌ 查询失败，请稍后再试")
        context.user_data.pop("active_module", None)
        context.user_data.pop("usdt_session", None)

async def query_tron(address: str):
    async with aiohttp.ClientSession() as session:
        url_balance = f"https://api.trongrid.io/v1/accounts/{address}"
        async with session.get(url_balance) as resp:
            data = await resp.json()

        trx = 0
        usdt = 0
        if data.get("data"):
            trx = int(data["data"][0].get("balance", 0)) / 1_000_000
            trc20 = data["data"][0].get("trc20", [])
            for t in trc20:
                if USDT_CONTRACT_ADDR in t:
                    usdt = int(t[USDT_CONTRACT_ADDR]) / 1_000_000
                    break

        url_tx = f"https://api.trongrid.io/v1/accounts/{address}/transactions/trc20?limit=50&contract_address={USDT_CONTRACT_ADDR}"
        async with session.get(url_tx) as resp:
            tx_data = await resp.json()

        txs = []
        for tx in tx_data.get("data", []):
            from_addr = tx.get("from", "")
            to_addr = tx.get("to", "")
            amount = int(tx.get("value", 0)) / 1_000_000
            ts = tx.get("block_timestamp", 0)
            dt = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S") if ts else "未知时间"
            direction = "收" if to_addr == address else "支"
            txs.append(f"{dt}\n{direction} | {amount} USDT | <code>{from_addr}</code> → <code>{to_addr}</code>")

        return trx, usdt, txs

async def send_trx_usdt_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = context.user_data.get("usdt_session")
    if not session or "data" not in session:
        context.user_data.pop("active_module", None)
        context.user_data.pop("usdt_session", None)
        await update.message.reply_text("⚠️ 查询数据已过期，请重新查询。")
        return

    data = session["data"]
    page = data["page"]
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_tx = data["transactions"][start:end]

    text = f"📊 地址：<code>{data['address']}</code>\n💠 TRX余额：{data['trx']}\n💰 USDT余额：{data['usdt']}\n\n📜 最近交易（第 {page+1} 页）：\n"
    text += "\n\n".join(page_tx) if page_tx else "暂无交易记录"

    buttons = []
    if start > 0:
        buttons.append(InlineKeyboardButton("⬅ 上一页", callback_data="usdt_prev"))
    if end < len(data["transactions"]):
        buttons.append(InlineKeyboardButton("➡ 下一页", callback_data="usdt_next"))
    buttons.append(InlineKeyboardButton("✅ 完成", callback_data="usdt_done"))

    markup = InlineKeyboardMarkup([buttons]) if buttons else None

    if update.callback_query:
        await update.callback_query.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    else:
        await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    if not is_authorized(user_id, require_full_access=True):
        await query.message.reply_text("❌ 只有操作人才能使用此功能")
        return

    session = context.user_data.get("usdt_session")

    if query.data == "usdt_done":
        context.user_data.pop("active_module", None)
        context.user_data.pop("usdt_session", None)
        from handlers.menu import get_main_menu
        try:
            await query.message.edit_text(
                "✅ 查询完成\n\n请选择功能：",
                reply_markup=get_main_menu(user_id)
            )
        except Exception:
            await query.message.reply_text(
                "✅ 查询完成\n\n请选择功能：",
                reply_markup=get_main_menu(user_id)
            )
        return

    if not session or "data" not in session:
        context.user_data.pop("active_module", None)
        context.user_data.pop("usdt_session", None)
        from handlers.menu import get_main_menu
        try:
            await query.message.edit_text(
                "⚠️ 查询数据已过期，请重新查询。",
                reply_markup=get_main_menu(user_id)
            )
        except Exception:
            await query.message.reply_text(
                "⚠️ 查询数据已过期，请重新查询。",
                reply_markup=get_main_menu(user_id)
            )
        return

    data = session["data"]
    if query.data == "usdt_prev":
        data["page"] = max(data["page"] - 1, 0)
    elif query.data == "usdt_next":
        data["page"] = min(data["page"] + 1, (len(data["transactions"]) - 1) // PAGE_SIZE)

    await send_trx_usdt_page(update, context)

async def handle_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import ReplyKeyboardMarkup, KeyboardButton

    user_id = update.effective_user.id
    if not is_authorized(user_id, require_full_access=True):
        await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return

    admin_id = get_user_admin_id(user_id)
    if admin_id == 0:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return

    context.user_data["active_module"] = "usdt"
    context.user_data["usdt_session"] = {
        "waiting_for_address": True,
        "admin_id": admin_id
    }

    keyboard = [[KeyboardButton("◀️ 返回主菜单")]]
    await update.message.reply_text(
        "💰 请输入 TRON TRC20 地址（T 开头）：",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )