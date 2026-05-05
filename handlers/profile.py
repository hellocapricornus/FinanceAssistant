# handlers/profile.py - 适配物理隔离

import asyncio
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from handlers.menu import get_main_menu
from auth import is_authorized, OWNER_ID, is_admin, get_user_admin_id, list_operators, add_admin
from db import (
    get_monitored_addresses as db_get_monitored_addresses,
    get_user_preferences as db_get_user_preferences,
    set_user_preference as db_set_user_preference,
    get_all_groups_from_db as db_get_all_groups
)
from handlers.accounting import get_accounting_manager, get_today_beijing, beijing_time
from handlers.monitor import get_monthly_stats, get_trc20_transactions, get_address_balance
from handlers.subscription import get_user_subscription
import time

# 状态定义
PROFILE_MAIN = 1
SET_SIGNATURE = 2
FEEDBACK = 3
EXPORT_DATA = 4
TRIAL_DURATION_DAYS = 0.003  # 试用天数

# 北京时区
BEIJING_TZ = timezone(timedelta(hours=8))

# ---------- 辅助：构建个人中心菜单 ----------
async def _build_profile_menu(user_id: int, prefs: dict = None, display_name: str = "", admin_id: int = 0) -> tuple:
    """返回 (消息文本, InlineKeyboardMarkup)"""
    if prefs is None:
        prefs = db_get_user_preferences(user_id, admin_id)
    full_access = is_authorized(user_id, require_full_access=True)
    limited_access = is_authorized(user_id, require_full_access=False) and not full_access
    admin = is_admin(user_id)
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    # 身份文本
    if user_id == OWNER_ID:
        role = "👑 超级管理员"
    elif admin:
        # 检查是否是试用用户
        trial = get_trial_info(user_id)
        sub = get_user_subscription(user_id)  # ✅ 新增：也查会员
        if trial and trial["status"] == "active":
            remaining_seconds = max(0, trial["expire_date"] - int(time.time()))
            remaining_days = remaining_seconds // 86400
            remaining_hours = (remaining_seconds % 86400) // 3600
            remaining_minutes = (remaining_seconds % 3600) // 60
            if remaining_days > 0:
                role = f"🎁 试用用户 ({remaining_days}天{remaining_hours}小时)"
            elif remaining_hours > 0:
                role = f"🎁 试用用户 ({remaining_hours}小时{remaining_minutes}分钟)"
            elif remaining_minutes > 0:
                role = f"🎁 试用用户 ({remaining_minutes}分钟)"
            else:
                role = "🎁 试用用户 (即将到期)"
        elif sub and sub["status"] == "active":  # ✅ 新增：活跃会员
            remaining_seconds = max(0, sub["expire_date"] - int(time.time()))
            remaining_days = remaining_seconds // 86400
            remaining_hours = (remaining_seconds % 86400) // 3600
            remaining_minutes = (remaining_seconds % 3600) // 60
            if remaining_days > 0:
                role = f"👤 管理员 ({remaining_days}天{remaining_hours}小时)"
            elif remaining_hours > 0:
                role = f"👤 管理员 ({remaining_hours}小时{remaining_minutes}分钟)"
            elif remaining_minutes > 0:
                role = f"👤 管理员 ({remaining_minutes}分钟)"
            else:
                role = "👤 管理员 (今日到期)"
        else:
            role = "👤 管理员"
    elif full_access:
        role = "👤 正式操作员"
    elif limited_access:
        role = "👥 临时操作员"
    else:
        # ✅ 检查是否有过期记录
        trial = get_trial_info(user_id)
        sub = get_user_subscription(user_id)
        if trial:
            role = "🎁 试用用户 (已过期)"
        elif sub and sub.get("status") in ("expired", "cancelled"):
            role = "💳 会员 (已过期)"
        else:
            role = "🙍 普通用户"
    keyboard = []
    # 个人记账统计
    if full_access or limited_access:
        keyboard.append([InlineKeyboardButton("📊 个人记账统计", callback_data="profile_stats")])
    # 我的监控地址
    if full_access:
        keyboard.append([InlineKeyboardButton("📁 我的监控地址", callback_data="profile_addresses")])
    # 监控交易提醒
    if full_access and addresses:
        notify = "🟢 已开启" if prefs["monitor_notify"] else "🔴 已关闭"
        keyboard.append([InlineKeyboardButton(f"🔔 监控交易提醒：{notify}", callback_data="profile_toggle_notify")])
    # 联系管理员、发送反馈
    if user_id != OWNER_ID:
        keyboard.append([InlineKeyboardButton("📞 联系管理员", callback_data="profile_contact")])
        keyboard.append([InlineKeyboardButton("💬 发送反馈", callback_data="profile_feedback")])

    # 会员相关按钮
    if user_id == OWNER_ID:
        # 超级管理员 → 会员管理
        keyboard.append([InlineKeyboardButton("💰 会员管理", callback_data="subscription_manage")])
        keyboard.append([InlineKeyboardButton("📢 用户广播", callback_data="user_broadcast")])
    else:
        sub = get_user_subscription(user_id)
        if sub:
            expire_date = datetime.fromtimestamp(sub["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
            remaining_seconds = max(0, sub["expire_date"] - int(time.time()))
            remaining_days = remaining_seconds // 86400
            remaining_hours = (remaining_seconds % 86400) // 3600
            remaining_minutes = (remaining_seconds % 3600) // 60
            if remaining_days > 0:
                label = f"💳 续费会员 ({remaining_days}天{remaining_hours}小时)"
            elif remaining_hours > 0:
                label = f"💳 续费会员 ({remaining_hours}小时{remaining_minutes}分钟)"
            elif remaining_minutes > 0:
                label = f"💳 续费会员 ({remaining_minutes}分钟)"
            else:
                label = "💳 续费会员 (今日到期)"
            keyboard.append([InlineKeyboardButton(label, callback_data="subscription_menu")])
        else:
            # 检查是否是试用用户
            trial = get_trial_info(user_id)
            if full_access or limited_access:
                # ✅ 只有非管理员的正式操作员和临时操作员不显示试用/会员按钮
                if not admin:
                    pass
                elif trial:
                    # 试用管理员的按钮
                    remaining_seconds = max(0, trial["expire_date"] - int(time.time()))
                    remaining_days = remaining_seconds // 86400
                    remaining_hours = (remaining_seconds % 86400) // 3600
                    remaining_minutes = (remaining_seconds % 3600) // 60
                    expire_date = datetime.fromtimestamp(trial["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
                    if trial["status"] == "active" and remaining_seconds > 0:
                        if remaining_days > 0:
                            label = f"🎁 试用中 ({remaining_days}天{remaining_hours}小时) - 升级"
                        elif remaining_hours > 0:
                            label = f"🎁 试用中 ({remaining_hours}小时{remaining_minutes}分钟) - 升级"
                        elif remaining_minutes > 0:
                            label = f"🎁 试用中 ({remaining_minutes}分钟) - 升级"
                        keyboard.append([InlineKeyboardButton(label, callback_data="subscription_menu")])
                    else:
                        keyboard.append([InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")])
                else:
                    keyboard.append([InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")])
            elif trial:
                remaining_seconds = max(0, trial["expire_date"] - int(time.time()))
                remaining_days = remaining_seconds // 86400
                remaining_hours = (remaining_seconds % 86400) // 3600
                remaining_minutes = (remaining_seconds % 3600) // 60
                expire_date = datetime.fromtimestamp(trial["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
                if trial["status"] == "active" and remaining_seconds > 0:
                    if remaining_days > 0:
                        label = f"🎁 试用中 ({remaining_days}天{remaining_hours}小时) - 升级"
                    elif remaining_hours > 0:
                        label = f"🎁 试用中 ({remaining_hours}小时{remaining_minutes}分钟) - 升级"
                    elif remaining_minutes > 0:
                        label = f"🎁 试用中 ({remaining_minutes}分钟) - 升级"
                    keyboard.append([InlineKeyboardButton(label, callback_data="subscription_menu")])
                else:
                    keyboard.append([InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")])
            else:
                keyboard.append([InlineKeyboardButton("🎁 申请试用", callback_data="profile_trial_start")])
                keyboard.append([InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")])
        
    # 默认群发附言、数据分析导出、早报
    if full_access:
        keyboard.append([InlineKeyboardButton("📝 默认群发附言", callback_data="profile_signature")])
        report_enabled = prefs.get('daily_report_enabled', False)
        status = "🟢 已开启" if report_enabled else "🔴 已关闭"
        keyboard.append([InlineKeyboardButton(f"📋 每日早报 {status}", callback_data="profile_report_toggle")])
        keyboard.append([InlineKeyboardButton("📈 数据分析导出", callback_data="profile_export")])
    # ✅ 监控群组（除超级管理员外所有用户可见）
    if user_id != OWNER_ID:
        keyboard.append([InlineKeyboardButton("📡 监控群组", callback_data="profile_monitor_group")])
    keyboard.append([InlineKeyboardButton("◀️ 返回主菜单", callback_data="profile_back")])
    notify_status = ""
    if full_access and addresses:
        notify_status = f"📢 监控交易提醒：{'🟢 已开启' if prefs['monitor_notify'] else '🔴 已关闭'}\n"
    user_info_line = f"👤 用户名：{display_name}\n" if display_name else ""
    text = (f"👤 个人中心\n\n"
            f"{user_info_line}"
            f"🆔 用户 ID：`{user_id}`\n"
            f"🏷️ 当前身份：{role}\n"
            f"{notify_status}")
    return text, InlineKeyboardMarkup(keyboard)

# ---------- 入口 ----------
async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("profile_input_state", None)
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        user = query.from_user
        msg = query.message
    else:
        user = update.effective_user
        msg = update.message
    user_id = user.id
    admin_id = get_user_admin_id(user_id)
    display_name = user.username or user.first_name or ""
    prefs = db_get_user_preferences(user_id, admin_id)
    text, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
    if update.callback_query:
        await msg.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        await msg.reply_text(text, reply_markup=markup, parse_mode="Markdown")
    return PROFILE_MAIN

# ---------- 个人记账统计 ----------
async def profile_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()
    am = get_accounting_manager(admin_id)
    if not am:
        await query.message.edit_text("❌ 记账模块未初始化")
        return
    # 获取该管理员的所有群组（独立库中的 groups 表已过滤）
    groups = db_get_all_groups(admin_id=admin_id)
    total_income_cny = 0.0
    total_income_usdt = 0.0
    total_income_count = 0
    total_expense_usdt = 0.0
    total_expense_count = 0
    today = get_today_beijing()
    today_income = 0.0
    month_income = 0.0
    now = datetime.now()
    for g in groups:
        records = am.get_total_records(g['id'], admin_id=admin_id)
        for r in records:
            # ✅ 只看自己的记录
            if r.get('user_id') != user_id:
                continue
            if r['type'] == 'income':
                total_income_cny += r['amount']
                total_income_usdt += r['amount_usdt']
                total_income_count += 1
                ts = r.get('created_at')
                if ts:
                    dt = datetime.fromtimestamp(ts)
                    if dt.strftime('%Y-%m-%d') == today:
                        today_income += r['amount']
                    if dt.year == now.year and dt.month == now.month:
                        month_income += r['amount']
            else:
                total_expense_usdt += r['amount_usdt']
                total_expense_count += 1
    text = "📊 个人记账统计（本团队所有群组）\n\n"
    text += f"• 今日入款：{today_income:.2f} 元\n"
    text += f"• 本月入款：{month_income:.2f} 元\n"
    text += f"• 总入款：{total_income_cny:.2f} 元（{total_income_count}笔）\n"
    text += f"• 总入款(USDT)：{total_income_usdt:.2f} USDT\n"
    text += f"• 总出款：{total_expense_usdt:.2f} USDT（{total_expense_count}笔）\n"
    text += "\n💡 仅统计您个人的记账记录。"
    await query.message.edit_text(text, parse_mode="Markdown")

# ---------- 我的监控地址 ----------
async def profile_addresses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()
    addresses = db_get_monitored_addresses(admin_id=admin_id, user_id=user_id)
    if not addresses:
        await query.message.edit_text("📭 您还没有添加监控地址")
        return
    text = "📁 我的监控地址\n\n"
    for addr in addresses:
        stats = await get_monthly_stats(addr['address'])
        note = f" ({addr['note']})" if addr['note'] else ""
        short = f"{addr['address'][:8]}...{addr['address'][-6:]}"
        text += f"📌 {short}{note}\n"
        text += f"   ⛓️ {addr['chain_type']}  |  本月净收入：{stats['net']:.2f} USDT\n\n"
    await query.message.edit_text(text, parse_mode="Markdown")

# ---------- 通知开关 ----------
async def profile_toggle_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    display_name = query.from_user.username or query.from_user.first_name or ""
    await query.answer()
    prefs = db_get_user_preferences(user_id, admin_id)
    new_state = not prefs["monitor_notify"]
    db_set_user_preference(user_id, "monitor_notify", new_state, admin_id)
    prefs = db_get_user_preferences(user_id, admin_id)
    text, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
    try:
        await query.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception:
        pass  # ✅ 忽略 Message is not modified 错误

# ---------- 默认群发附言 ----------
async def profile_signature_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()
    prefs = db_get_user_preferences(user_id, admin_id)
    current_sig = prefs.get("broadcast_signature", "")
    if current_sig:
        hint = f"当前附言：\n「{current_sig}」\n\n"
    else:
        hint = "当前没有默认附言。\n\n"
    context.user_data["profile_input_state"] = True
    context.user_data["admin_id"] = admin_id
    await query.message.edit_text(
        hint +
        "📝 请发送新的附言内容（直接发送文字即可）。\n"
        "若要去除附言，请发送 /remove\n"
        "取消请发送 /cancel",
        parse_mode="Markdown"
    )
    return SET_SIGNATURE

async def profile_signature_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_id = context.user_data.get("admin_id", get_user_admin_id(user_id))
    display_name = update.effective_user.username or update.effective_user.first_name or ""
    text = update.message.text.strip()
    if text == '/cancel':
        await update.message.reply_text("❌ 已取消")
        prefs = db_get_user_preferences(user_id, admin_id)
        _, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
        await update.message.reply_text("已返回个人中心", reply_markup=markup, parse_mode="Markdown")
        context.user_data.pop("admin_id", None)
        return ConversationHandler.END
    if text == '/remove':
        db_set_user_preference(user_id, "broadcast_signature", "", admin_id)
        await update.message.reply_text("✅ 默认附言已删除")
    else:
        db_set_user_preference(user_id, "broadcast_signature", text, admin_id)
        await update.message.reply_text(f"✅ 默认附言已设置为：\n附言：{text}")
    prefs = db_get_user_preferences(user_id, admin_id)
    _, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
    await update.message.reply_text("已返回个人中心", reply_markup=markup, parse_mode="Markdown")
    context.user_data.pop("admin_id", None)
    return ConversationHandler.END

# ---------- 联系管理员 ----------
async def profile_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.edit_text(
        f"📞 管理员：@ChinaEdward\n"
        f"或直接私聊 [点击联系](tg://user?id={OWNER_ID})",
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

# ---------- 发送反馈 ----------
async def profile_feedback_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["profile_input_state"] = True
    await query.message.edit_text(
        "💬 请输入您的反馈内容（支持文字）。\n发送 /cancel 取消。",
        parse_mode="Markdown"
    )
    return FEEDBACK

async def profile_feedback_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    display_name = user.username or user.first_name or ""
    text = update.message.text.strip()
    if text == '/cancel':
        await update.message.reply_text("❌ 已取消")
        admin_id = get_user_admin_id(user.id)
        prefs = db_get_user_preferences(user.id, admin_id)
        _, markup = await _build_profile_menu(user.id, prefs, display_name, admin_id)
        await update.message.reply_text("已返回个人中心", reply_markup=markup, parse_mode="Markdown")
        return ConversationHandler.END
    feedback_msg = (
        f"📨 用户反馈\n"
        f"来自：{user.mention_html()}\n"
        f"内容：{text}"
    )
    try:
        await context.bot.send_message(chat_id=OWNER_ID, text=feedback_msg, parse_mode="HTML")
        await update.message.reply_text("✅ 反馈已发送，感谢您的意见！")
    except Exception as e:
        await update.message.reply_text(f"❌ 发送失败：{e}")
    admin_id = get_user_admin_id(user.id)
    prefs = db_get_user_preferences(user.id, admin_id)
    _, markup = await _build_profile_menu(user.id, prefs, display_name, admin_id)
    await update.message.reply_text("已返回个人中心", reply_markup=markup, parse_mode="Markdown")
    return ConversationHandler.END

# ---------- 数据分析导出相关函数 ----------
def _get_period_timestamps(period: str, now: datetime):
    beijing_tz = now.tzinfo if now.tzinfo else timezone(timedelta(hours=8))
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "today":
        start = today_start
    elif period == "week":
        start = today_start - timedelta(days=now.weekday())
    elif period == "month":
        start = today_start.replace(day=1)
    elif period == "year":
        start = today_start.replace(month=1, day=1)
    else:
        start = today_start
    return int(start.timestamp() * 1000), int(now.timestamp() * 1000)

async def profile_export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()
    await query.message.edit_text("⏳ 正在生成数据分析报告，请稍候...")

    am = get_accounting_manager(admin_id)
    if not am:
        await query.message.edit_text("❌ 记账模块未初始化")
        return

    from db import get_all_groups_from_db, get_monitored_addresses
    from auth import list_operators, OWNER_ID
    from auth import is_authorized as auth_is_authorized
    from auth import temp_operators, is_admin

    beijing_tz = timezone(timedelta(hours=8))
    now = datetime.now().astimezone(beijing_tz)
    is_owner = (user_id == OWNER_ID)

    # 身份
    if is_owner:
        role = "超级管理员"
        identity_color = "#e74c3c"
    elif is_admin(user_id):
        role = "管理员"
        identity_color = "#e74c3c"
    elif auth_is_authorized(user_id, require_full_access=True):
        role = "正式操作员"
        identity_color = "#3498db"
    elif auth_is_authorized(user_id, require_full_access=False):
        role = "临时操作员"
        identity_color = "#f39c12"
    else:
        role = "普通用户"
        identity_color = "#95a5a6"

    # 当前用户的管理员ID（超级管理员为0）
    cur_admin_id = OWNER_ID if is_owner else admin_id

    # ---------- 记账数据（只取本管理员的数据） ----------
    all_groups = db_get_all_groups(admin_id=cur_admin_id if cur_admin_id != 0 else None)
    # 可见群组（用于群组加入统计）
    visible_groups = all_groups if is_owner else [g for g in all_groups if g.get('admin_id', 0) == cur_admin_id]

    records = []
    period_income_cny = {"today": 0.0, "week": 0.0, "month": 0.0, "total": 0.0}
    period_income_usdt = {"today": 0.0, "week": 0.0, "month": 0.0, "total": 0.0}
    period_expense = {"today": 0.0, "week": 0.0, "month": 0.0, "total": 0.0}
    group_detail = {"today": {}, "week": {}, "month": {}, "total": {}}
    today_str = now.strftime('%Y-%m-%d')

    for g in all_groups:
        gname = g['title']
        recs = am.get_total_records(g['id'], admin_id=cur_admin_id if cur_admin_id != 0 else None)
        for r in recs:
            date = r.get('date', '')
            if r['type'] == 'income':
                cny = r['amount']
                usdt = r['amount_usdt']
                if date == today_str:
                    period_income_cny["today"] += cny
                    period_income_usdt["today"] += usdt
                    d = group_detail["today"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})
                    d["income_cny"] += cny
                    d["income_usdt"] += usdt
                dt = datetime.strptime(date, '%Y-%m-%d') if date and date != '' else None
                if dt:
                    if dt.isocalendar()[1] == now.isocalendar()[1] and dt.year == now.year:
                        period_income_cny["week"] += cny
                        period_income_usdt["week"] += usdt
                        dw = group_detail["week"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})
                        dw["income_cny"] += cny
                        dw["income_usdt"] += usdt
                    if dt.month == now.month and dt.year == now.year:
                        period_income_cny["month"] += cny
                        period_income_usdt["month"] += usdt
                        dm = group_detail["month"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})
                        dm["income_cny"] += cny
                        dm["income_usdt"] += usdt
                period_income_cny["total"] += cny
                period_income_usdt["total"] += usdt
                dtot = group_detail["total"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})
                dtot["income_cny"] += cny
                dtot["income_usdt"] += usdt
            else:
                usdt = r['amount_usdt']
                if date == today_str:
                    period_expense["today"] += usdt
                    group_detail["today"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})["expense"] += usdt
                dt = datetime.strptime(date, '%Y-%m-%d') if date and date != '' else None
                if dt:
                    if dt.isocalendar()[1] == now.isocalendar()[1] and dt.year == now.year:
                        period_expense["week"] += usdt
                        group_detail["week"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})["expense"] += usdt
                    if dt.month == now.month and dt.year == now.year:
                        period_expense["month"] += usdt
                        group_detail["month"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})["expense"] += usdt
                period_expense["total"] += usdt
                group_detail["total"].setdefault(gname, {"income_cny": 0.0, "income_usdt": 0.0, "expense": 0.0})["expense"] += usdt

    def format_group_detail(detail):
        res = []
        for gname, v in sorted(detail.items(), key=lambda x: x[1].get("income_cny", 0), reverse=True):
            res.append({
                "name": gname,
                "income_cny": round(v.get("income_cny", 0.0), 2),
                "income_usdt": round(v.get("income_usdt", 0.0), 2),
                "expense": round(v.get("expense", 0.0), 2),
                "pending": round(v.get("income_usdt", 0.0) - v.get("expense", 0.0), 2)
            })
        return res

    accounting_tabs = {
        "today": {
            "income_cny": round(period_income_cny["today"], 2),
            "income_usdt": round(period_income_usdt["today"], 2),
            "expense": round(period_expense["today"], 2),
            "pending": round(period_income_usdt["today"] - period_expense["today"], 2),
            "groups": format_group_detail(group_detail["today"])
        },
        "week": {
            "income_cny": round(period_income_cny["week"], 2),
            "income_usdt": round(period_income_usdt["week"], 2),
            "expense": round(period_expense["week"], 2),
            "pending": round(period_income_usdt["week"] - period_expense["week"], 2),
            "groups": format_group_detail(group_detail["week"])
        },
        "month": {
            "income_cny": round(period_income_cny["month"], 2),
            "income_usdt": round(period_income_usdt["month"], 2),
            "expense": round(period_expense["month"], 2),
            "pending": round(period_income_usdt["month"] - period_expense["month"], 2),
            "groups": format_group_detail(group_detail["month"])
        },
        "total": {
            "income_cny": round(period_income_cny["total"], 2),
            "income_usdt": round(period_income_usdt["total"], 2),
            "expense": round(period_expense["total"], 2),
            "pending": round(period_income_usdt["total"] - period_expense["total"], 2),
            "groups": format_group_detail(group_detail["total"])
        }
    }

    # 30天趋势图数据
    daily_income = {}
    daily_expense = {}
    for g in all_groups:
        recs = am.get_total_records(g['id'], admin_id=cur_admin_id if cur_admin_id != 0 else None)
        for r in recs:
            date = r.get('date', '')
            if r['type'] == 'income':
                daily_income[date] = daily_income.get(date, 0.0) + r['amount']
            else:
                daily_expense[date] = daily_expense.get(date, 0.0) + r['amount_usdt']
    date_list = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(29, -1, -1)]
    income_series = [round(daily_income.get(d, 0.0), 2) for d in date_list]
    expense_series = [round(daily_expense.get(d, 0.0), 2) for d in date_list]

    # ---------- USDT 监控地址 ----------
    if is_owner:
        my_addresses = db_get_monitored_addresses(admin_id=0)  # 超管不区分，但可获取所有？这里简化，超管不生成地址统计
        my_addresses = []
    else:
        my_addresses = db_get_monitored_addresses(admin_id=cur_admin_id, user_id=user_id)
    addr_stats = []
    for addr in my_addresses:
        address = addr['address']
        note = addr.get('note', '')
        async def fetch_period(period):
            start_ms, end_ms = _get_period_timestamps(period, now)
            txs = await get_trc20_transactions(address, start_ms)
            received = 0.0
            sent = 0.0
            for tx in txs:
                raw_amount = tx.get("value", 0)
                amount = int(raw_amount) / 1_000_000 if raw_amount else 0
                to_addr = tx.get("to", "")
                if to_addr == address:
                    received += amount
                else:
                    sent += amount
            return received, sent, received - sent
        p = {}
        for period in ["today", "week", "month", "year"]:
            p[period] = await fetch_period(period)
        addr_stats.append({
            "address": address,
            "note": note,
            "periods": p,
        })
    address_tabs = {}
    for period in ["today", "week", "month", "year"]:
        address_tabs[period] = [{
            "address": a["address"],
            "note": a["note"],
            "received": round(a["periods"][period][0], 2),
            "sent": round(a["periods"][period][1], 2),
            "net": round(a["periods"][period][2], 2)
        } for a in addr_stats]

    # ---------- 群组加入统计 ----------
    group_join_detail = {"today": [], "week": [], "month": [], "year": []}
    for g in visible_groups:
        jt = g.get('joined_at', 0)
        if jt:
            dt = datetime.fromtimestamp(jt, tz=beijing_tz)
            info = {"name": g['title'], "joined_at": dt.strftime('%Y-%m-%d %H:%M'), "category": g.get('category', '未分类')}
            if dt.date() == now.date():
                group_join_detail["today"].append(info)
            if dt.isocalendar()[1] == now.isocalendar()[1] and dt.year == now.year:
                group_join_detail["week"].append(info)
            if dt.month == now.month and dt.year == now.year:
                group_join_detail["month"].append(info)
            if dt.year == now.year:
                group_join_detail["year"].append(info)
    groups_data = {
        "today": {"count": len(group_join_detail["today"]), "list": group_join_detail["today"]},
        "week": {"count": len(group_join_detail["week"]), "list": group_join_detail["week"]},
        "month": {"count": len(group_join_detail["month"]), "list": group_join_detail["month"]},
        "year": {"count": len(group_join_detail["year"]), "list": group_join_detail["year"]}
    }

    # ---------- 操作人列表 ----------
    operator_list = []
    if is_owner or auth_is_authorized(user_id, require_full_access=True):
        ops = list_operators(added_by=cur_admin_id) if not is_owner else list_operators()
        for op_id, info in ops.items():
            # 尝试从独立库获取显示名，简单处理使用主库信息
            display = f"{info.get('first_name','')} (@{info.get('username','')})" if info.get('username') else info.get('first_name', str(op_id))
            operator_list.append(f"{display} - 正式操作员")
        temps = temp_operators
        if not is_owner:
            temps = {uid: info for uid, info in temps.items() if info.get('added_by') == cur_admin_id}
        for temp_id, info in temps.items():
            display = f"{info.get('first_name','')} (@{info.get('username','')})" if info.get('username') else info.get('first_name', str(temp_id))
            operator_list.append(f"{display} - 临时操作员")
    else:
        operator_list.append("您没有权限查看操作人列表")

    # ---------- 生成 HTML ----------
    html = _build_beautiful_html(
        user_id=user_id,
        role=role,
        identity_color=identity_color,
        accounting_tabs=accounting_tabs,
        chart_data={"dates": date_list, "income": income_series, "expense": expense_series},
        address_tabs=address_tabs,
        groups_data=groups_data,
        operators=operator_list,
    )
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', encoding='utf-8', delete=False) as f:
        f.write(html)
        temp_path = f.name
    await query.message.reply_document(
        document=open(temp_path, 'rb'),
        filename="数据分析报告.html",
        caption="📈 您的个人数据分析报告"
    )
    os.unlink(temp_path)

def _build_beautiful_html(user_id, role, identity_color, accounting_tabs, chart_data, address_tabs, groups_data, operators):
    import json as _json
    def safe_serialize(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        result = str(obj)
        # ✅ 转义危险的 HTML 序列
        result = result.replace('</', '<\\/')
        return result
    acc_json = _json.dumps(accounting_tabs, default=safe_serialize)
    addr_json = _json.dumps(address_tabs, default=safe_serialize)
    grp_json = _json.dumps(groups_data, default=safe_serialize)
    trend_json = _json.dumps(chart_data, default=safe_serialize)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>数据分析报告</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            background: #f0f2f5; color: #333; line-height: 1.6; -webkit-text-size-adjust: 100%;
        }}
        .container {{ max-width: 1100px; margin: 0 auto; padding: 16px 12px 60px; }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; padding: 28px 20px; border-radius: 20px;
            margin-bottom: 20px; box-shadow: 0 8px 24px rgba(102,126,234,0.3);
        }}
        .header h1 {{ font-size: 24px; margin-bottom: 6px; }}
        .header .sub {{ opacity: 0.9; font-size: 14px; }}
        .identity-badge {{
            display: inline-block; padding: 4px 14px; border-radius: 20px;
            color: white; background: {identity_color}; font-weight: 600; margin-top: 10px; font-size: 13px;
        }}
        .card {{
            background: white; border-radius: 16px; padding: 18px 14px; margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.04);
        }}
        .card h2 {{ margin: 0 0 12px 0; font-size: 18px; color: #1e293b; display: flex; align-items: center; }}
        .card h2 .icon {{ margin-right: 8px; font-size: 20px; }}
        .tabs {{ display: flex; gap: 6px; margin-bottom: 16px; flex-wrap: wrap; }}
        .tab-btn {{
            padding: 7px 14px; border: none; border-radius: 20px; background: #e2e8f0;
            color: #475569; font-weight: 600; cursor: pointer; transition: 0.2s; font-size: 14px;
            touch-action: manipulation; user-select: none; -webkit-tap-highlight-color: transparent;
        }}
        .tab-btn.active {{ background: #667eea; color: white; }}
        .summary-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            gap: 12px; margin-bottom: 20px;
        }}
        .summary-item {{
            background: #f8fafc; border-radius: 12px; padding: 14px 10px; text-align: center;
            border: 1px solid #e2e8f0;
        }}
        .summary-item .label {{ font-size: 12px; color: #64748b; margin-bottom: 6px; }}
        .summary-item .value {{ font-size: 18px; font-weight: 700; }}
        .positive {{ color: #16a34a; }}
        .negative {{ color: #dc2626; }}
        .chart-container {{ position: relative; width: 100%; max-height: 300px; margin: 16px 0; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 10px; }}
        th, td {{ padding: 8px 6px; border-bottom: 1px solid #e5e7eb; text-align: left; }}
        th {{ background: #f9fafb; font-weight: 600; color: #374151; white-space: nowrap; }}
        td {{ vertical-align: middle; }}
        .table-responsive {{ overflow-x: auto; -webkit-overflow-scrolling: touch; margin: 0 -4px; padding: 0 4px; }}
        ul {{ padding-left: 20px; }}
        .footer {{ text-align: center; color: #94a3b8; margin-top: 30px; font-size: 12px; }}
        @media (max-width: 640px) {{
            .header h1 {{ font-size: 20px; }}
            .summary-grid {{ grid-template-columns: repeat(2, 1fr); }}
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 数据分析报告</h1>
        <div class="sub">用户ID: {user_id} | 身份: <span class="identity-badge">{role}</span></div>
    </div>

    <!-- 记账总览 -->
    <div class="card">
        <h2><span class="icon">💰</span>记账总览</h2>
        <div class="tabs" id="accountTabs">
            <button class="tab-btn active" data-period="today">今日</button>
            <button class="tab-btn" data-period="week">本周</button>
            <button class="tab-btn" data-period="month">本月</button>
            <button class="tab-btn" data-period="total">总计</button>
        </div>
        <div id="accountTabContent">加载中...</div>
        <div class="chart-container">
            <canvas id="trendChart"></canvas>
        </div>
    </div>

    <!-- USDT 地址统计 -->
    <div class="card">
        <h2><span class="icon">🪙</span>USDT 地址统计</h2>
        <div class="tabs" id="addressTabs">
            <button class="tab-btn active" data-period="today">今日</button>
            <button class="tab-btn" data-period="week">本周</button>
            <button class="tab-btn" data-period="month">本月</button>
            <button class="tab-btn" data-period="year">本年</button>
        </div>
        <div id="addressTabContent">加载中...</div>
        <div class="chart-container">
            <canvas id="addressChart"></canvas>
        </div>
    </div>

    <!-- 群组加入统计 -->
    <div class="card">
        <h2><span class="icon">📁</span>群组加入统计</h2>
        <div class="tabs" id="groupTabs">
            <button class="tab-btn active" data-period="today">今日加入</button>
            <button class="tab-btn" data-period="week">本周加入</button>
            <button class="tab-btn" data-period="month">本月加入</button>
            <button class="tab-btn" data-period="year">本年加入</button>
        </div>
        <div id="groupTabContent">加载中...</div>
    </div>

    <!-- 操作人列表 -->
    <div class="card">
        <h2><span class="icon">👥</span>操作人列表</h2>
        <ul>{''.join(f'<li>{op}</li>' for op in operators)}</ul>
    </div>

    <div class="footer">由记账机器人自动生成 · 数据仅供参考</div>
</div>

<script>
    const accountingData = {acc_json};
    const addressData = {addr_json};
    const groupsData = {grp_json};
    const trendData = {trend_json};

    function setupTabs(tabContainerId, onSwitch) {{
        const container = document.getElementById(tabContainerId);
        if(!container) return;
        const buttons = container.querySelectorAll('.tab-btn');
        buttons.forEach(btn => {{
            const handler = function(e) {{
                e.preventDefault();
                const period = this.getAttribute('data-period');
                if(period) onSwitch(period, buttons);
            }};
            btn.addEventListener('click', handler);
            btn.addEventListener('touchend', handler);
        }});
    }}

    function renderAccountTab(period) {{
        const data = accountingData[period];
        if(!data) return;
        let html = `<div class="summary-grid">
            <div class="summary-item"><div class="label">入款 (元)</div><div class="value positive">${{data.income_cny.toFixed(2)}}</div></div>
            <div class="summary-item"><div class="label">入款 (USDT)</div><div class="value">${{data.income_usdt.toFixed(2)}}</div></div>
            <div class="summary-item"><div class="label">下发 (USDT)</div><div class="value negative">${{data.expense.toFixed(2)}}</div></div>
            <div class="summary-item"><div class="label">待下发 (USDT)</div><div class="value">${{data.pending.toFixed(2)}}</div></div>
        </div>`;
        if(data.groups && data.groups.length > 0) {{
            html += '<div class="table-responsive"><table><thead><tr><th>群组</th><th>入款 (元)</th><th>入款 (USDT)</th><th>下发 (USDT)</th><th>待下发</th></tr></thead><tbody>';
            data.groups.forEach(g => {{
                html += `<tr><td>${{g.name}}</td><td class="positive">${{g.income_cny.toFixed(2)}}</td><td>${{g.income_usdt.toFixed(2)}}</td><td class="negative">${{g.expense.toFixed(2)}}</td><td>${{g.pending.toFixed(2)}}</td></tr>`;
            }});
            html += '</tbody></table></div>';
        }} else {{
            html += '<p style="text-align:center;color:#94a3b8;">暂无群组数据</p>';
        }}
        document.getElementById('accountTabContent').innerHTML = html;
    }}

    function switchAccountTab(period, buttons) {{
        buttons.forEach(b => b.classList.remove('active'));
        const activeBtn = Array.from(buttons).find(b => b.getAttribute('data-period') === period);
        if(activeBtn) activeBtn.classList.add('active');
        renderAccountTab(period);
    }}

    setupTabs('accountTabs', switchAccountTab);

    const trendCtx = document.getElementById('trendChart').getContext('2d');
    new Chart(trendCtx, {{
        type: 'bar',
        data: {{
            labels: trendData.dates,
            datasets: [
                {{ label: '每日入款 (元)', data: trendData.income, backgroundColor: 'rgba(22,163,74,0.6)', borderColor: '#16a34a', borderWidth: 1 }},
                {{ label: '每日下发 (USDT)', data: trendData.expense, backgroundColor: 'rgba(220,38,38,0.6)', borderColor: '#dc2626', borderWidth: 1 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ position: 'top' }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
    }});

    let addressChart;
    function renderAddressTab(period) {{
        const list = addressData[period] || [];
        let html = '<div class="table-responsive"><table><thead><tr><th>地址</th><th>备注</th><th>收款</th><th>转出</th><th>净收入</th></tr></thead><tbody>';
        list.forEach(a => {{
            html += `<tr><td>${{a.address}}</td><td>${{a.note}}</td><td class="positive">${{a.received.toFixed(2)}}</td><td class="negative">${{a.sent.toFixed(2)}}</td><td>${{a.net.toFixed(2)}}</td></tr>`;
        }});
        html += '</tbody></table></div>';
        document.getElementById('addressTabContent').innerHTML = html;

        if(addressChart) addressChart.destroy();
        const ctx = document.getElementById('addressChart').getContext('2d');
        const labels = list.map(a => a.address);
        const data = list.map(a => a.net);
        addressChart = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [{{ label: '净收入 (USDT)', data: data, backgroundColor: data.map(v => v>=0 ? 'rgba(22,163,74,0.6)' : 'rgba(220,38,38,0.6)') }}]
            }},
            options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
        }});
    }}

    function switchAddressTab(period, buttons) {{
        buttons.forEach(b => b.classList.remove('active'));
        const activeBtn = Array.from(buttons).find(b => b.getAttribute('data-period') === period);
        if(activeBtn) activeBtn.classList.add('active');
        renderAddressTab(period);
    }}

    setupTabs('addressTabs', switchAddressTab);

    function renderGroupTab(period) {{
        const data = groupsData[period];
        if(!data) return;
        let html = `<div class="summary-item" style="margin-bottom:12px;"><div class="label">${{period==='today'?'今日':period==='week'?'本周':period==='month'?'本月':'本年'}}加入数量</div><div class="value">${{data.count}}</div></div>`;
        if(data.list && data.list.length > 0) {{
            html += '<div class="table-responsive"><table><thead><tr><th>群组名称</th><th>加入时间</th><th>分类</th></tr></thead><tbody>';
            data.list.forEach(g => {{
                html += `<tr><td>${{g.name}}</td><td>${{g.joined_at}}</td><td>${{g.category}}</td></tr>`;
            }});
            html += '</tbody></table></div>';
        }} else {{
            html += '<p style="text-align:center;color:#94a3b8;">暂无数据</p>';
        }}
        document.getElementById('groupTabContent').innerHTML = html;
    }}

    function switchGroupTab(period, buttons) {{
        buttons.forEach(b => b.classList.remove('active'));
        const activeBtn = Array.from(buttons).find(b => b.getAttribute('data-period') === period);
        if(activeBtn) activeBtn.classList.add('active');
        renderGroupTab(period);
    }}

    setupTabs('groupTabs', switchGroupTab);

    renderAccountTab('today');
    renderAddressTab('today');
    renderGroupTab('today');
</script>
</body>
</html>"""

# ---------- 返回主菜单 ----------
async def profile_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    try:
        await query.message.edit_text("请选择功能：", reply_markup=get_main_menu(user_id))
    except Exception:
        await context.bot.send_message(chat_id=user_id, text="请选择功能：", reply_markup=get_main_menu(user_id))
    return ConversationHandler.END

# ---------- 每日早报开关 ----------
async def profile_report_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    display_name = query.from_user.username or query.from_user.first_name or ""
    await query.answer()
    prefs = db_get_user_preferences(user_id, admin_id)
    new_state = not prefs.get("daily_report_enabled", False)
    db_set_user_preference(user_id, "daily_report_enabled", new_state, admin_id)
    prefs = db_get_user_preferences(user_id, admin_id)
    text, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
    try:
        await query.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception:
        pass  # ✅ 忽略 Message is not modified 错误

async def profile_monitor_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """监控群组介绍"""
    query = update.callback_query
    await query.answer()

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 加入监控群组", url="https://t.me/+BjHkQhpqknczYjk5")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ])

    await query.edit_message_text(
        "📡 **监控群组**\n\n"
        "监控群组是一个专属的 Telegram 群组，机器人会自动监控并转发以下信息：\n\n"
        "🔍 **新币上线监控** - 自动爬取新币上线的公告消息\n"
        "👥 **客户信息捕获** - 及时查看群组动态，不漏掉任何潜在客户\n"
        "⚡ **实时推送** - 第一时间获取最新币圈动态和客户线索\n\n"
        "🎁 **新用户福利**：加入即可获得 **24 小时免费试用**！\n\n"
        "👉 点击下方按钮加入群组，开始监控吧！",
        parse_mode="Markdown",
        reply_markup=keyboard
    )

def get_trial_info(user_id: int) -> dict:
    """获取用户试用信息"""
    from db_manager import get_conn
    conn = get_conn(0)
    row = conn.execute(
        "SELECT * FROM trial_users WHERE user_id = ?", (user_id,)
    ).fetchone()
    if row:
        return dict(row)
    return None

def create_trial(user_id: int) -> bool:
    from db_manager import get_conn
    now = int(time.time())
    expire = now + TRIAL_DURATION_DAYS * 86400
    try:
        # ✅ 先添加管理员（会重置 source 和 expire_date）
        add_admin(user_id)

        conn = get_conn(0)
        conn.execute(
            "INSERT INTO trial_users (user_id, start_date, expire_date) VALUES (?, ?, ?)",
            (user_id, now, expire)
        )
        # ✅ 再 UPDATE 设置正确的 source 和 expire_date
        conn.execute(
            "UPDATE admins SET source = 'trial', expire_date = ? WHERE admin_id = ?",
            (expire, user_id)
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"创建试用失败: {e}")
        return False

async def profile_trial_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """申请试用 - 确认弹窗"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    # 检查是否已经试用过
    trial = get_trial_info(user_id)
    if trial:
        expire_date = datetime.fromtimestamp(trial["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d %H:%M')
        remaining_seconds = max(0, trial["expire_date"] - int(time.time()))
        remaining_days = remaining_seconds // 86400
        remaining_hours = (remaining_seconds % 86400) // 3600

        remaining_minutes = (remaining_seconds % 3600) // 60
        if trial["status"] == "active" and remaining_seconds > 0:
            if remaining_days > 0:
                remain_text = f"{remaining_days}天{remaining_hours}小时"
            elif remaining_hours > 0:
                remain_text = f"{remaining_hours}小时{remaining_minutes}分钟"
            else:
                remain_text = f"{remaining_minutes}分钟"

            await query.message.edit_text(
                f"🎁 **您已经是试用用户**\n\n"
                f"📅 试用到期时间：{expire_date}\n"
                f"⏳ 剩余时间：**{remain_text}**\n\n"
                f"💡 试用期间您可以使用机器人的全部功能\n"
                f"⚠️ 试用到期后数据保留 7 天\n\n"
                f"👉 点击下方「升级会员」成为正式用户：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")],
                    [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.message.edit_text(
                f"😔 **试用已过期**\n\n"
                f"📅 试用到期时间：{expire_date}\n\n"
                f"⚠️ 您的数据将在到期后 7 天内清除\n"
                f"👉 点击下方「升级会员」保留全部数据：",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")],
                    [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
                ]),
                parse_mode="Markdown"
            )
        return

    # 确认试用弹窗
    keyboard = [
        [InlineKeyboardButton("✅ 确认试用", callback_data="trial_confirm")],
        [InlineKeyboardButton("❌ 取消", callback_data="profile_return")],
    ]

    await query.message.edit_text(
        f"🎁 **申请试用**\n\n"
        f"📅 试用时长：**{TRIAL_DURATION_DAYS} 天**\n"
        f"🎯 试用权限：\n"
        f"• 所有功能均可使用\n"
        f"• 独立数据空间\n"
        f"• 群组管理权限\n\n"
        f"⚠️ **注意事项**：\n"
        f"• 每个用户仅可试用一次\n"
        f"• 试用到期后数据保留 7 天\n"
        f"• 请及时升级会员保留数据\n\n"
        f"确认开始试用吗？",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def trial_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """确认试用"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    # 再次检查是否已有试用
    trial = get_trial_info(user_id)
    if trial:
        await query.answer("您已经申请过试用", show_alert=True)
        return await profile_trial_start(update, context)

    # 创建试用
    success = create_trial(user_id)

    if success:
        from auth import load_admins_from_db
        load_admins_from_db()  # 重新加载管理员列表

        # ✅ 写入 admin_users 表存储用户信息
        user = query.from_user
        from db_manager import get_conn
        conn = get_conn(0)
        conn.execute(
            "INSERT OR REPLACE INTO admin_users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
            (user_id, user.username or '', user.first_name or '', user.last_name or '')
        )
        conn.commit()

        expire_date = datetime.fromtimestamp(
            int(time.time()) + TRIAL_DURATION_DAYS * 86400, 
            tz=BEIJING_TZ
        ).strftime('%Y-%m-%d %H:%M')

        await query.message.edit_text(
            f"🎉 **试用开通成功！**\n\n"
            f"📅 试用到期时间：{expire_date}\n"
            f"⏳ 试用时长：{TRIAL_DURATION_DAYS} 天\n\n"
            f"✅ 您现在可以使用机器人的全部功能了！\n"
            f"💡 试用到期后数据保留 7 天\n"
            f"⚠️ 请及时升级会员以保留数据\n\n"
            f"👉 点击下方查看个人中心：",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⭐ 升级会员", callback_data="subscription_menu")],
                [InlineKeyboardButton("👤 个人中心", callback_data="profile_return")],
            ]),
            parse_mode="Markdown"
        )
    else:
        await query.message.edit_text(
            "❌ 试用开通失败，请稍后重试",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
            ])
        )
