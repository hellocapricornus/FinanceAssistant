# handlers/operator.py - 适配物理隔离

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes, ConversationHandler
from auth import (
    add_operator, remove_operator, get_operators_list_text, 
    cmd_update_operator_info, update_all_operators_info,
    add_temp_operator, remove_temp_operator, get_temp_operators_list_text,
    is_authorized, get_user_admin_id, add_admin, remove_admin, admins, is_admin
)
from config import OWNER_ID
from handlers.accounting import BEIJING_TZ
from datetime import datetime, timezone, timedelta
from db import get_db_connection as get_user_db_conn, set_user_preference as set_user_pref
from db_manager import get_conn as get_master_conn
from typing import Dict, Optional

# 状态标识
ADD_OPERATOR = 1
REMOVE_OPERATOR = 2
ADD_TEMP_OPERATOR = 3
REMOVE_TEMP_OPERATOR = 4
ADD_ADMIN = 5
REMOVE_ADMIN = 6

BEIJING_TZ = timezone(timedelta(hours=8))

def get_operator_info(user_id: int) -> Optional[dict]:
    """获取单个操作员信息"""
    return operators.get(user_id)

def list_operators(added_by: int = None) -> Dict[int, dict]:
    """返回操作员字典，可指定 added_by 过滤"""
    if added_by is None:
        return operators
    return {uid: info for uid, info in operators.items() if info.get("added_by") == added_by}

def get_admin_list_text() -> str:
    if not admins:
        return "📭 当前没有管理员"
    text = "👑 **管理员列表**\n"
    # 过滤掉超级管理员
    filtered_admins = {aid: info for aid, info in admins.items() if aid != OWNER_ID}
    if not filtered_admins:
        return "📭 当前没有其他管理员"
    for admin_id, info in filtered_admins.items():
        created_at = info.get("created_at", 0)
        if created_at:
            dt = datetime.fromtimestamp(created_at, tz=BEIJING_TZ).strftime('%Y-%m-%d %H:%M')
        else:
            dt = "未知"
        # 获取昵称和用户名
        display_name = f"用户{admin_id}"
        username = ""
        # 从该管理员的独立库 group_users 表查询用户自己的信息
        try:
            from db_manager import get_conn
            conn = get_conn(admin_id)
            row = conn.execute("SELECT first_name, username FROM group_users WHERE user_id = ? LIMIT 1", (admin_id,)).fetchone()
            if row:
                first_name = row[0] or ""
                username = row[1] or ""
                if username:
                    display_name = f"{first_name} (@{username})" if first_name else f"@{username}"
                else:
                    display_name = first_name if first_name else f"用户{admin_id}"
        except Exception:
            pass
        text += f"• {display_name} (ID: `{admin_id}`)  添加时间: {dt}\n"
    return text

def get_operator_keyboard(user_id: int):
    """返回操作人管理的固定键盘"""
    if user_id == OWNER_ID:
        keyboard = [
            [KeyboardButton("👑 管理员管理")],
            [KeyboardButton("◀️ 返回主菜单")],
        ]
    elif is_admin(user_id):
        keyboard = [
            [KeyboardButton("➕ 添加操作人"), KeyboardButton("➖ 删除操作人"), KeyboardButton("📋 操作人列表")],
            [KeyboardButton("🔄 更新操作人信息"), KeyboardButton("👥 临时操作人")],
            [KeyboardButton("◀️ 返回主菜单")],
        ]
    elif is_authorized(user_id, require_full_access=True):
        keyboard = [
            [KeyboardButton("👥 临时操作人")],
            [KeyboardButton("◀️ 返回主菜单")],
        ]
    else:
        keyboard = [[KeyboardButton("◀️ 返回主菜单")]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if user_id != OWNER_ID:
        await query.answer("❌ 只有超级管理员可以管理管理员", show_alert=True)
        return
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("➕ 添加管理员", callback_data="op_admin_add")],
        [InlineKeyboardButton("➖ 移除管理员", callback_data="op_admin_remove")],
        [InlineKeyboardButton("📋 管理员列表", callback_data="op_admin_list")],
        [InlineKeyboardButton("◀️ 返回", callback_data="operator")],
    ]
    await query.message.edit_text(
        "👑 **管理员管理**\n\n请选择操作：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 只有控制人或操作人可以使用此功能", show_alert=True)
        await query.message.reply_text("❌ 只有控制人或操作人可以使用此功能")
        return
    context.user_data.pop("current_action", None)
    context.user_data.pop("active_module", None)
    if user_id == OWNER_ID:
        keyboard = [
            [InlineKeyboardButton("➕ 添加操作人", callback_data="op_add")],
            [InlineKeyboardButton("➖ 删除操作人", callback_data="op_remove")],
            [InlineKeyboardButton("📋 查询操作人", callback_data="op_list")],
            [InlineKeyboardButton("🔄 更新信息", callback_data="op_update")],
            [InlineKeyboardButton("👥 临时操作人", callback_data="op_temp_menu")],
            [InlineKeyboardButton("👑 管理员管理", callback_data="op_admin_menu")],
            [InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")],
        ]
        await query.message.edit_text(
            "👤 操作人管理\n\n作为超级管理员，您只需管理【管理员】。\n管理员可自行管理操作员和临时操作员。\n\n请选择功能：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif is_admin(user_id):
        keyboard = [
            [InlineKeyboardButton("➕ 添加操作人", callback_data="op_add")],
            [InlineKeyboardButton("➖ 删除操作人", callback_data="op_remove")],
            [InlineKeyboardButton("📋 查询操作人", callback_data="op_list")],
            [InlineKeyboardButton("🔄 更新信息", callback_data="op_update")],
            [InlineKeyboardButton("👥 临时操作人", callback_data="op_temp_menu")],
            [InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")],
        ]
        await query.message.edit_text("👤 操作人管理：请选择功能", reply_markup=InlineKeyboardMarkup(keyboard))
    elif is_authorized(user_id, require_full_access=True):
        keyboard = [
            [InlineKeyboardButton("👥 临时操作人", callback_data="op_temp_menu")],
            [InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")],
        ]
        await query.message.edit_text(
            "👤 操作人管理\n\n⚠️ 操作人只能管理【临时操作人】\n正式操作人管理需要管理员权限\n\n请选择功能：",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await query.message.reply_text("❌ 无权限")

async def add_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return
    if not context.args:
        await update.message.reply_text("用法：/addadmin <用户ID>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
        return
    if add_admin(target_id):
        await update.message.reply_text(f"✅ 已添加管理员 {target_id}")
    else:
        await update.message.reply_text("❌ 添加失败，用户可能已经是管理员或不存在")

async def remove_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return
    if not context.args:
        await update.message.reply_text("用法：/removeadmin <用户ID>")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
        return
    if remove_admin(target_id):
        await update.message.reply_text(f"✅ 已移除管理员 {target_id}，数据将保留7天")
    else:
        await update.message.reply_text("❌ 移除失败，可能不是管理员")

async def list_admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return
    if not admins:
        await update.message.reply_text("📭 当前没有管理员")
        return
    text = "👑 **管理员列表**\n"
    for admin_id in admins:
        text += f"• ID: `{admin_id}`\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def temp_operator_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        return
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("➕ 添加临时操作人", callback_data="op_temp_add")],
        [InlineKeyboardButton("➖ 删除临时操作人", callback_data="op_temp_remove")],
        [InlineKeyboardButton("📋 查询临时操作人", callback_data="op_temp_list")],
        [InlineKeyboardButton("◀️ 返回", callback_data="operator")],
    ]
    await query.message.edit_text(
        "👥 **临时操作人管理**\n\n"
        "临时操作人**只能使用记账功能**，不能使用：\n"
        "❌ USDT查询\n❌ 群发消息\n❌ 互转查询\n❌ 群组管理\n❌ 操作人管理\n\n"
        "✅ 只能使用：记账、地址查询、计算器\n\n请选择操作：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    if not is_authorized(user_id, require_full_access=True):
        await query.message.reply_text("❌ 只有控制人或操作人可以使用此功能")
        return
    data = query.data
    # 正式操作人管理按钮：仅管理员可用
    if data in ["op_add", "op_remove", "op_list", "op_update"]:
        if user_id != OWNER_ID and not is_admin(user_id):
            await query.answer("❌ 只有管理员或超级管理员可以管理正式操作人", show_alert=True)
            return
    if data == "op_add":
        context.user_data["current_action"] = ADD_OPERATOR
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要添加的用户ID（纯数字）：")
    elif data == "op_remove":
        context.user_data["current_action"] = REMOVE_OPERATOR
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要删除的用户ID（纯数字）：")
    elif data == "op_list":
        # 获取当前用户的 admin_id，用于过滤显示
        admin_id = get_user_admin_id(user_id)
        text = get_operators_list_text(user_id)  # get_operators_list_text 内部使用了 get_user_admin_id
        await query.message.reply_text(text, parse_mode="Markdown")
        context.user_data.pop("active_module", None)
    elif data == "op_update":
        await query.message.reply_text("🔄 正在更新操作人信息，请稍候...")
        count = await update_all_operators_info(context)
        if count > 0:
            await query.message.reply_text(f"✅ 已成功更新 {count} 个操作人的信息")
        else:
            await query.message.reply_text("⚠️ 没有操作人被更新，或更新失败")
    elif data == "op_temp_menu":
        await temp_operator_menu(update, context)
    elif data == "op_temp_add":
        context.user_data["current_action"] = ADD_TEMP_OPERATOR
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要添加的**临时操作人**ID（纯数字）：\n\n💡 临时操作人只能使用记账功能")
    elif data == "op_temp_remove":
        context.user_data["current_action"] = REMOVE_TEMP_OPERATOR
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要删除的**临时操作人**ID（纯数字）：")
    elif data == "op_temp_list":
        text = get_temp_operators_list_text()
        await query.message.reply_text(text, parse_mode="Markdown")
        context.user_data.pop("active_module", None)
    elif data == "op_admin_menu":
        await admin_menu(update, context)
    elif data == "op_admin_add":
        if user_id != OWNER_ID:
            await query.answer("❌ 只有超级管理员可以添加管理员", show_alert=True)
            return
        context.user_data["current_action"] = ADD_ADMIN
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要添加的管理员用户ID（纯数字）：")
    elif data == "op_admin_remove":
        if user_id != OWNER_ID:
            await query.answer("❌ 只有超级管理员可以移除管理员", show_alert=True)
            return
        context.user_data["current_action"] = REMOVE_ADMIN
        context.user_data["active_module"] = "operator"
        await query.message.reply_text("请输入要移除的管理员用户ID（纯数字）：")
    elif data == "op_admin_list":
        if not admins:
            await query.message.reply_text("📭 当前没有管理员")
        else:
            text = "👑 **管理员列表**\n"
            for admin_id in admins:
                text += f"• ID: `{admin_id}`\n"
            await query.message.reply_text(text, parse_mode='Markdown')
        context.user_data.pop("active_module", None)

async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    action = context.user_data.get("current_action")
    if action not in [ADD_OPERATOR, REMOVE_OPERATOR, ADD_TEMP_OPERATOR, REMOVE_TEMP_OPERATOR, ADD_ADMIN, REMOVE_ADMIN]:
        return
    if not is_authorized(user_id, require_full_access=True):
        await update.message.reply_text("❌ 只有控制人或操作人可以使用此功能")
        return
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ 请输入正确的用户ID（纯数字）")
        return
    target_id = int(text)
    if action == ADD_OPERATOR:
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以添加操作员")
            return
        admin_id = get_user_admin_id(user_id)
        success = await add_operator(target_id, context, added_by=admin_id)
        if success:
            from auth import get_operator_info
            info = get_operator_info(target_id)
            if info and info.get('first_name'):
                display = f"{info['first_name']}"
                if info.get('username'):
                    display += f" (@{info['username']})"
                await update.message.reply_text(f"✅ 已添加操作人：{display}")
            else:
                await update.message.reply_text(f"✅ 已添加操作人ID：{target_id}")
        else:
            await update.message.reply_text(f"❌ 添加失败，操作人可能已存在")
    elif action == REMOVE_OPERATOR:
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以删除操作员")
            return
        if remove_operator(target_id):
            await update.message.reply_text(f"✅ 已删除操作人：{target_id}")
        else:
            await update.message.reply_text(f"❌ 未找到操作人：{target_id}")
    elif action == ADD_TEMP_OPERATOR:
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有管理员或操作员可以添加临时操作员")
            return
        success = await add_temp_operator(target_id, added_by=admin_id, context=context)
        if success:
            # 在独立库中设置该用户的偏好 role='temp'
            set_user_pref(target_id, "role", "temp", admin_id=admin_id)
            await update.message.reply_text(
                f"✅ 已添加临时操作人：{target_id}\n\n"
                "⚠️ 该用户只能使用记账功能\n（记账、地址查询、计算器）"
            )
        else:
            await update.message.reply_text(f"❌ 添加失败，临时操作人可能已存在")
    elif action == REMOVE_TEMP_OPERATOR:
        # 注意：删除临时操作人时，也需要清除其独立库中的 role 和 added_by？
        # 由于该操作人可能属于当前管理员，role 重置为 user
        admin_id = get_user_admin_id(user_id)
        if remove_temp_operator(target_id):
            set_user_pref(target_id, "role", "user", admin_id=admin_id)
            await update.message.reply_text(f"✅ 已删除临时操作人：{target_id}")
        else:
            await update.message.reply_text(f"❌ 未找到临时操作人：{target_id}")
    elif action == ADD_ADMIN:
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员")
            return
        if add_admin(target_id):
            await update.message.reply_text(f"✅ 已添加管理员 {target_id}")
        else:
            await update.message.reply_text(f"❌ 添加失败，用户可能已经是管理员或不存在")
    elif action == REMOVE_ADMIN:
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以移除管理员")
            return
        if remove_admin(target_id):
            await update.message.reply_text(f"✅ 已移除管理员 {target_id}，数据将保留7天")
        else:
            await update.message.reply_text(f"❌ 移除失败，可能不是管理员")
    context.user_data.pop("current_action", None)
    context.user_data.pop("active_module", None)
    await update.message.reply_text(
        "👤 操作人管理：请选择功能",
        reply_markup=get_operator_keyboard(user_id)
    )

async def cancel_operator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("current_action", None)
    context.user_data.pop("active_module", None)
    await update.message.reply_text("❌ 已取消操作")
    user_id = update.effective_user.id
    await update.message.reply_text(
        "👤 操作人管理：请选择功能",
        reply_markup=get_operator_keyboard(user_id)
    )
    return ConversationHandler.END

async def handle_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pass