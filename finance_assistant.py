# main.py - 适配物理隔离
try:
    import pysqlite3
    import sys
    sys.modules['sqlite3'] = pysqlite3
    print("✅ Replit 环境：使用 pysqlite3")
except ImportError:
    pass  # 服务器环境，使用默认 sqlite3

import asyncio
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler,
    ChatMemberHandler
)
from config import BOT_TOKEN, OWNER_ID
from handlers.user_broadcast import get_user_broadcast_handler
from auth import is_authorized, init_operators_from_db, get_user_admin_id, is_admin
from db import init_master_db, save_group, delete_group_from_db, DB_PATH, get_monitored_addresses, get_user_preferences
from handlers.start import start
from handlers import monitor, operator, usdt, accounting, broadcast, transfer
from auth import cmd_update_operator_info
from handlers.git_update import get_git_handlers
from handlers.group_manager import (
    group_manager_menu, show_stats, list_categories, 
    add_category_start, delete_category_start, 
    delete_category_confirm, set_group_category_start,
    select_group_for_category, set_group_category,
    handle_text_input
)
from handlers.menu import get_main_menu
from handlers.accounting import get_service_message_handler, get_accounting_manager
from handlers.ai_client import get_ai_client
from handlers.help import handle_help
from handlers.profile import (
    handle_profile, profile_stats, profile_addresses,
    profile_toggle_notify, profile_signature_start, profile_signature_input,
    profile_contact, profile_feedback_start, profile_feedback_input,
    profile_export_data, profile_back, profile_report_toggle,
    SET_SIGNATURE, FEEDBACK, profile_monitor_group
)
from handlers.operator import add_admin_cmd, remove_admin_cmd, list_admins_cmd, get_admin_list_text
from db_manager import close_all_connections, init_admin_db, get_conn, get_db

# ==================== 辅助键盘函数 ====================
def get_admin_management_keyboard():
    keyboard = [
        [KeyboardButton("➕ 添加管理员"), KeyboardButton("➖ 删除管理员"), KeyboardButton("📋 管理员列表")],
        [KeyboardButton("◀️ 返回操作人管理"), KeyboardButton("◀️ 返回主菜单")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_monitor_keyboard(user_id: int):
    from db import get_monitored_addresses as get_addrs
    admin_id = get_user_admin_id(user_id)
    addresses = get_addrs(admin_id=admin_id, user_id=user_id) if admin_id != 0 else []
    keyboard = [[KeyboardButton("➕ 添加监控地址")]]
    if addresses:
        keyboard.append([KeyboardButton("📋 监控列表"), KeyboardButton("📊 月度统计")])
        keyboard.append([KeyboardButton("❌ 删除监控地址")])
    keyboard.append([KeyboardButton("◀️ 返回主菜单")])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_operator_keyboard(user_id: int):
    from auth import is_admin
    if user_id == OWNER_ID:
        # 超级管理员看到和普通管理员一样的菜单
        keyboard = [
            [KeyboardButton("➕ 添加操作人"), KeyboardButton("➖ 删除操作人"), KeyboardButton("📋 操作人列表")],
            [KeyboardButton("🔄 更新操作人信息"), KeyboardButton("👥 临时操作人")],
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

def get_temp_operator_keyboard():
    keyboard = [
        [KeyboardButton("➕ 添加临时操作人"), KeyboardButton("➖ 删除临时操作人"), KeyboardButton("📋 临时操作人列表")],
        [KeyboardButton("◀️ 返回操作人管理"), KeyboardButton("◀️ 返回主菜单")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_group_manager_keyboard():
    keyboard = [
        [KeyboardButton("📊 群组统计"), KeyboardButton("📁 查看分类"), KeyboardButton("➕ 创建分类")],
        [KeyboardButton("🏷️ 设置群组分类"), KeyboardButton("🗑️ 删除分类")],
        [KeyboardButton("◀️ 返回主菜单")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_transfer_keyboard():
    keyboard = [
        [KeyboardButton("🔍 转账查询"), KeyboardButton("🕸️ 转账分析")],
        [KeyboardButton("◀️ 返回主菜单")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_input_cancel_keyboard():
    keyboard = [[KeyboardButton("◀️ 返回主菜单")]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ==================== 已知按钮文本集合 ====================
ALL_KNOWN_BUTTONS = {
    # 主菜单
    "📒 记账", "🔔 USDT监控", "📢 群发", "💰 USDT查询","➕ 添加我进群",
    "👤 操作人管理", "🔄 互转查询", "📁 群组管理",
    # 监控
    "➕ 添加监控地址", "📋 监控列表", "📊 月度统计", "❌ 删除监控地址",
    # 操作人
    "➕ 添加操作人", "➖ 删除操作人", "📋 操作人列表", "🔄 更新操作人信息", "👥 临时操作人",
    # 临时操作人
    "➕ 添加临时操作人", "➖ 删除临时操作人", "📋 临时操作人列表",
    "◀️ 返回操作人管理",
    # 群组管理
    "📊 群组统计", "📁 查看分类", "➕ 创建分类", "🏷️ 设置群组分类", "🗑️ 删除分类",
    # 转账
    "🔍 转账查询", "🕸️ 转账分析",
    # 返回
    "◀️ 返回主菜单",
    "📖 使用说明", "👤 个人中心",
    "👑 管理员管理",
    "➕ 添加管理员", "➖ 删除管理员", "📋 管理员列表",
}

# ==================== 键盘处理器 (group=0) ====================
async def keyboard_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user_id = update.effective_user.id
    if chat.type != 'private':
        return
    text = update.message.text.strip()
    if text not in ALL_KNOWN_BUTTONS:
        return
    print(f"[KEYBOARD] 收到按钮: {text}")
    # ==================== 返回主菜单 ====================
    if text == "◀️ 返回主菜单":
        keys_to_clear = [
            "active_module", "usdt_session", "monitor_action", "monitor_temp",
            "current_action", "transfer_results", "current_page", "query_type",
            "in_broadcast", "selecting_group", "group_list", "filter_type",
            "selected_group_id",
        ]
        for key in keys_to_clear:
            context.user_data.pop(key, None)
        from handlers.group_manager import user_states
        if user_id in user_states:
            del user_states[user_id]
        await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return
    # ==================== 主菜单按钮 ====================
    if text == "📒 记账":
        context.user_data.clear()
        if not is_authorized(user_id, require_full_access=False):
            await update.message.reply_text("❌ 记账功能仅限管理员/操作员/临时操作员才能使用\n\n如需使用，请联系 @ChinaEdward 申请权限", reply_markup=get_main_menu(user_id))
            return
        await accounting.handle_keyboard(update, context)
        return
    elif text == "➕ 添加我进群":
        await update.message.reply_text(
            "👆 [点击这里添加机器人到你的群组](https://t.me/CardKingBot?startgroup=start)",
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        return
    elif text == "🔔 USDT监控":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        await show_monitor_menu(update, context)
        return
    elif text == "📢 群发":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        keyboard = [
            [InlineKeyboardButton("🚀 开始设置群发", callback_data="broadcast")],
            [InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")],
        ]
        await update.message.reply_text(
            "📢 **群发消息**\n\n点击下方按钮开始设置群发内容。",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return
    elif text == "💰 USDT查询":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        await start_usdt_query(update, context)
        return
    elif text == "👤 操作人管理":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        await show_operator_menu(update, context)
        return
    elif text == "🔄 互转查询":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        await show_transfer_menu(update, context)
        return
    elif text == "📁 群组管理":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward", reply_markup=get_main_menu(user_id))
            return
        await show_group_manager_menu(update, context)
        return
    elif text == "📖 使用说明":
        await handle_help(update, context)
        return
    elif text == "👤 个人中心":
        await handle_profile(update, context)
        return
    # ==================== USDT 监控子菜单 ====================
    elif text == "➕ 添加监控地址":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data["monitor_action"] = "add"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text(
            "➕ 添加监控地址\n\n请输入要监控的 USDT 地址：\n\n支持格式：\n• TRC20: T 开头，34位\n• ERC20: 0x 开头，42位\n\n❌ 点击「返回主菜单」取消",
            reply_markup=get_input_cancel_keyboard()
        )
        return
    elif text == "📋 监控列表":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        from db import get_monitored_addresses as get_addrs
        addresses = get_addrs(admin_id=admin_id, user_id=user_id)
        if not addresses:
            await update.message.reply_text("📭 您还没有添加任何监控地址", reply_markup=get_monitor_keyboard(user_id))
            return
        text_msg = "📋 **您的监控地址列表**\n\n"
        for i, addr_info in enumerate(addresses, 1):
            full_addr = addr_info['address']
            note = addr_info.get('note', '')
            text_msg += f"{i}. `{full_addr}` ({addr_info['chain_type']})\n"
            if note:
                text_msg += f"   📝 备注：{note}\n"
            text_msg += "\n"
        await update.message.reply_text(text_msg, reply_markup=get_monitor_keyboard(user_id), parse_mode=None)
        return
    elif text == "📊 月度统计":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        from db import get_monitored_addresses as get_addrs
        addresses = get_addrs(admin_id=admin_id, user_id=user_id)
        if not addresses:
            await update.message.reply_text("📭 您还没有添加任何监控地址", reply_markup=get_monitor_keyboard(user_id))
            return
        temp_msg = await update.message.reply_text("📊 正在查询月度统计，请稍候...")
        text_msg = "📊 **监控地址月度统计**\n\n"
        for addr_info in addresses:
            address = addr_info["address"]
            note = addr_info.get("note", "")
            short_addr = f"{address[:8]}...{address[-6:]}"
            stats = await monitor.get_monthly_stats(address)
            text_msg += f"📌 {short_addr}"
            if note:
                text_msg += f" ({note})"
            text_msg += f"\n   💰 本月收到：**{stats['received']:.2f} USDT**"
            text_msg += f"\n   📤 本月转出：**{stats['sent']:.2f} USDT**"
            text_msg += f"\n   📈 净收入：**{stats['net']:.2f} USDT**\n\n"
        await temp_msg.edit_text(text_msg, parse_mode=None)
        await update.message.reply_text("选择操作：", reply_markup=get_monitor_keyboard(user_id))
        return
    elif text == "❌ 删除监控地址":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        from db import get_monitored_addresses as get_addrs
        addresses = get_addrs(admin_id=admin_id, user_id=user_id)
        if not addresses:
            await update.message.reply_text("📭 您还没有添加任何监控地址", reply_markup=get_monitor_keyboard(user_id))
            return
        keyboard = []
        for addr in addresses:
            full_addr = addr['address']
            note = addr.get('note', '')
            short_addr = f"{full_addr[:12]}...{full_addr[-8:]}"
            button_text = f"🗑️ {short_addr} ({note})" if note else f"🗑️ {short_addr}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"monitor_del_{addr['id']}")])
        await update.message.reply_text(
            "🗑️ **删除监控地址**\n\n选择要删除的地址：\n\n💡 点击「返回主菜单」取消",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    # ==================== 操作人管理子菜单 ====================
    elif text == "👑 管理员管理":
        if user_id != OWNER_ID:
            return
        await update.message.reply_text(
            "👑 **管理员管理**\n\n请选择操作：",
            reply_markup=get_admin_management_keyboard(),
            parse_mode="Markdown"
        )
        return
    elif text == "➕ 添加管理员":
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以添加管理员", reply_markup=get_main_menu(user_id))
            return
        context.user_data["current_action"] = operator.ADD_ADMIN
        context.user_data["active_module"] = "operator"
        await update.message.reply_text("请输入要添加的管理员用户ID（纯数字）：", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "➖ 删除管理员":
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以删除管理员", reply_markup=get_main_menu(user_id))
            return
        context.user_data["current_action"] = operator.REMOVE_ADMIN
        context.user_data["active_module"] = "operator"
        await update.message.reply_text("请输入要删除的管理员用户ID（纯数字）：", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "📋 管理员列表":
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以查看管理员列表", reply_markup=get_main_menu(user_id))
            return
        text_msg = get_admin_list_text()
        await update.message.reply_text(text_msg, parse_mode="Markdown", reply_markup=get_admin_management_keyboard())
        return
    elif text == "➕ 添加操作人":
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以管理正式操作人", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data["current_action"] = operator.ADD_OPERATOR
        context.user_data["active_module"] = "operator"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text("请输入要添加的用户ID（纯数字）：", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "➖ 删除操作人":
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以管理正式操作人", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data["current_action"] = operator.REMOVE_OPERATOR
        context.user_data["active_module"] = "operator"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text("请输入要删除的用户ID（纯数字）：", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "📋 操作人列表":
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以查看正式操作人", reply_markup=get_main_menu(user_id))
            return
        from auth import get_operators_list_text
        text_msg = get_operators_list_text(user_id)
        await update.message.reply_text(text_msg, parse_mode="Markdown", reply_markup=get_operator_keyboard(user_id))
        return
    elif text == "🔄 更新操作人信息":
        if not is_admin(user_id):
            await update.message.reply_text("❌ 只有管理员可以更新操作人信息", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        await update.message.reply_text("🔄 正在更新操作人信息，请稍候...")
        from auth import update_all_operators_info
        count = await update_all_operators_info(context, admin_id)   # 传入 admin_id
        if count > 0:
            await update.message.reply_text(f"✅ 已成功更新 {count} 个操作人的信息", reply_markup=get_operator_keyboard(user_id))
        else:
            await update.message.reply_text("⚠️ 没有操作人被更新，或更新失败", reply_markup=get_operator_keyboard(user_id))
        return
    elif text == "👥 临时操作人":
        await update.message.reply_text(
            "👥 **临时操作人管理**\n\n临时操作人**只能使用记账功能**\n\n请选择操作：",
            reply_markup=get_temp_operator_keyboard(),
            parse_mode="Markdown"
        )
        return
    elif text == "➕ 添加临时操作人":
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data["current_action"] = operator.ADD_TEMP_OPERATOR
        context.user_data["active_module"] = "operator"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text("请输入要添加的**临时操作人**ID（纯数字）：\n\n💡 临时操作人只能使用记账功能", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "➖ 删除临时操作人":
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data["current_action"] = operator.REMOVE_TEMP_OPERATOR
        context.user_data["active_module"] = "operator"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text("请输入要删除的**临时操作人**ID（纯数字）：", reply_markup=get_input_cancel_keyboard())
        return
    elif text == "📋 临时操作人列表":
        from auth import get_temp_operators_list_text
        text_msg = get_temp_operators_list_text()
        await update.message.reply_text(text_msg, parse_mode="Markdown", reply_markup=get_temp_operator_keyboard())
        return
    elif text == "◀️ 返回操作人管理":
        await show_operator_menu(update, context)
        return
    # ==================== 群组管理子菜单 ====================
    elif text == "📊 群组统计":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        await show_group_manager_menu(update, context)
        return
    elif text == "📁 查看分类":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        await show_group_manager_menu(update, context)
        return
    elif text == "➕ 创建分类":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        from handlers.group_manager import user_states
        user_states[user_id] = {"action": "add_category_name", "timestamp": asyncio.get_event_loop().time()}
        await update.message.reply_text(
            "➕ **创建新分类**\n\n请输入分类名称（如：VIP群组）：\n\n❌ 点击「返回主菜单」取消",
            parse_mode="Markdown",
            reply_markup=get_input_cancel_keyboard()
        )
        return
    elif text == "🏷️ 设置群组分类":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        from db import get_all_groups_from_db
        groups = get_all_groups_from_db(admin_id=admin_id)
        if not groups:
            await update.message.reply_text("📭 暂无群组", reply_markup=get_group_manager_keyboard())
            return
        context.user_data['group_list'] = groups
        context.user_data['current_page'] = 0
        context.user_data['admin_id'] = admin_id
        await show_group_list_inline(update, context)
        return
    elif text == "🗑️ 删除分类":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        from db import get_all_categories as db_get_all_categories
        categories = db_get_all_categories(admin_id=admin_id)
        deletable = [cat for cat in categories if cat['name'] != '未分类']
        if not deletable:
            await update.message.reply_text("⚠️ 没有可删除的分类（「未分类」不能删除）")
            return
        page = context.user_data.get('del_cat_page', 0)
        items_per_page = 10
        total_pages = (len(deletable) + items_per_page - 1) // items_per_page
        start = page * items_per_page
        end = min(start + items_per_page, len(deletable))
        page_cats = deletable[start:end]
        keyboard = []
        for cat in page_cats:
            keyboard.append([InlineKeyboardButton(f"🗑️ {cat['name']}", callback_data=f"del_cat_{cat['name']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data="del_cat_page_prev"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data="del_cat_page_next"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="group_manager")])
        text = f"🗑️ **删除分类**\n\n选择要删除的分类：\n共 {len(deletable)} 个分类"
        if total_pages > 1:
            text += f"（第 {page+1}/{total_pages} 页）"
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return
    # ==================== 互转查询子菜单 ====================
    elif text == "🔍 转账查询":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data.pop("transfer_results", None)
        context.user_data.pop("current_page", None)
        context.user_data.pop("query_type", None)
        context.user_data["active_module"] = "transfer_query"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text(
            "🔍 **转账查询**\n\n请输入两个 USDT 地址，中间用空格隔开：\n例如：`Txxxx... Tyyyy...`",
            parse_mode="Markdown",
            reply_markup=get_input_cancel_keyboard()
        )
        return
    elif text == "🕸️ 转账分析":
        if not is_authorized(user_id, require_full_access=True):
            await update.message.reply_text("❌ 无权限", reply_markup=get_main_menu(user_id))
            return
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0 and user_id != OWNER_ID:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
            return
        context.user_data.pop("transfer_results", None)
        context.user_data.pop("current_page", None)
        context.user_data.pop("query_type", None)
        context.user_data["active_module"] = "transfer_analysis"
        context.user_data["admin_id"] = admin_id
        await update.message.reply_text(
            "🕵️ **转账分析**\n\n将分析是否有第三方地址与这两个地址都产生过交易。\n请输入两个 USDT 地址，中间用空格隔开：\n例如：`Txxxx... Tyyyy...`",
            parse_mode="Markdown",
            reply_markup=get_input_cancel_keyboard()
        )
        return

# ==================== 模块输入处理器 (group=1) ====================
async def module_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user_id = update.effective_user.id
    if chat.type != 'private':
        return
    if context.user_data.pop("profile_input_state", False):
        return ConversationHandler.END
    text = update.message.text.strip() if update.message.text else ""
    if text in ALL_KNOWN_BUTTONS:
        return

    # ✅ 修改：同时检查用户广播和群发广播状态
    if context.user_data.get("ub_in_broadcast") or context.user_data.get("in_broadcast"):
        return None  # 让广播相关的 ConversationHandler 处理
        
    # 1. 检查群组管理状态
    from handlers.group_manager import user_states
    if user_id in user_states:
        from handlers.group_manager import handle_text_input
        await handle_text_input(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    # 2. 检查监控模块状态
    monitor_action = context.user_data.get("monitor_action")
    if monitor_action == "add":
        await monitor.monitor_add_input(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    elif monitor_action == "add_note":
        # ✅ /skip 放行，让 ConversationHandler 中的 CommandHandler 处理
        if text == "/skip":
            return None
        await monitor.monitor_add_note(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    # 3. 检查操作员管理状态
    current_action = context.user_data.get("current_action")
    if current_action in [operator.ADD_OPERATOR, operator.REMOVE_OPERATOR, 
                           operator.ADD_TEMP_OPERATOR, operator.REMOVE_TEMP_OPERATOR,
                           operator.ADD_ADMIN, operator.REMOVE_ADMIN]:
        await operator.handle_input(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    # 4. 检查 USDT 地址查询状态
    usdt_session = context.user_data.get("usdt_session")
    if usdt_session and usdt_session.get("waiting_for_address"):
        try:
            await usdt.handle_input(update, context)
        except Exception as e:
            context.user_data.pop("active_module", None)
            context.user_data.pop("usdt_session", None)
            await update.message.reply_text("❌ USDT 查询出错，请重试")
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    # 5. 检查互转查询状态
    active_module = context.user_data.get("active_module")
    if active_module == "transfer_query":
        await transfer.process_transfer_query(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    elif active_module == "transfer_analysis":
        await transfer.process_transfer_analysis(update, context)
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    # 6. 检查广播模块状态
    if context.user_data.get("in_broadcast", False):
        context.user_data["_message_handled"] = True
        return ConversationHandler.END
    return None

# ==================== AI 对话处理器 (group=2) ====================
async def ai_chat_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user_id = update.effective_user.id
    if chat.type != 'private':
        return
    if context.user_data.pop("profile_input_state", False):
        return
    if context.user_data.get("_message_handled"):
        context.user_data.pop("_message_handled", None)
        return

    # ✅ 添加：广播状态中不触发 AI
    if context.user_data.get("ub_in_broadcast"):
        return
    
    text = update.message.text.strip() if update.message.text else ""
    if text in ALL_KNOWN_BUTTONS or text.startswith('/'):
        return
    if any([
        context.user_data.get("active_module"),
        context.user_data.get("monitor_action"),
        context.user_data.get("current_action"),
        context.user_data.get("in_broadcast"),
        context.user_data.get("usdt_session"),
        context.user_data.get("transfer_results"),
        context.user_data.get("selecting_group"),
    ]):
        return
    from handlers.group_manager import user_states
    if user_id in user_states:
        return
    import re
    if re.match(r'^T[0-9A-Za-z]{33}\s+T[0-9A-Za-z]{33}$', text):
        return
    if not text:
        return
    print(f"[AI_CHAT] 进入 AI 对话: {text[:50]}")
    if not is_authorized(user_id, require_full_access=True):
        await update.message.reply_text("❌ AI 对话功能仅限管理员和操作员使用\n\n如需使用，请联系 @ChinaEdward 申请权限")
        return
    thinking_msg = await update.message.reply_text("🤔 思考中...")
    try:
        ai_client = get_ai_client()
        reply = await ai_client.chat_with_data(text, user_id=user_id)
        if len(reply) > 4000:
            reply = reply[:4000] + "...\n\n(回复过长已截断)"
        await thinking_msg.edit_text(reply)
    except Exception as e:
        print(f"[DEBUG] AI 调用失败: {e}")
        await thinking_msg.edit_text(f"❌ AI 服务出错: {str(e)[:100]}")

# ==================== 菜单显示函数 ====================
async def show_monitor_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0 and user_id != OWNER_ID:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return
    from db import get_monitored_addresses as get_addrs
    addresses = get_addrs(admin_id=admin_id, user_id=user_id)
    if len(addresses) == 0:
        text = "🔔 USDT 地址监控\n\n📊 您的监控地址数：0 个\n\n⚠️ 暂无监控地址，请先添加。\n\n💡 支持为地址添加备注"
    else:
        text = f"🔔 USDT 地址监控\n\n📊 您的监控地址数：{len(addresses)} 个\n\n当监控地址有交易时，会发送通知。\n\n💡 监控间隔约 30 秒"
    await update.message.reply_text(text, reply_markup=monitor.get_monitor_keyboard_markup(user_id), parse_mode=None)

async def show_operator_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id == OWNER_ID:
        text = "👤 操作人管理\n\n作为超级管理员，您只需管理管理员。"
    elif is_admin(user_id):
        text = "👤 操作人管理：请选择功能"
    else:
        text = "👤 操作人管理\n\n⚠️ 操作人只能管理临时操作人"
    await update.message.reply_text(text, reply_markup=get_operator_keyboard(user_id))

async def show_transfer_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("transfer_results", None)
    context.user_data.pop("current_page", None)
    context.user_data.pop("query_type", None)
    context.user_data.pop("active_module", None)
    await update.message.reply_text("💱 **互转查询功能**\n请选择操作：", reply_markup=get_transfer_keyboard(), parse_mode="Markdown")

async def show_group_manager_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id  # ✅ 先定义
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0 and user_id != OWNER_ID:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return
    from db import get_all_categories, get_groups_by_category
    categories = get_all_categories(admin_id=admin_id)
    groups_by_cat = get_groups_by_category(admin_id=admin_id)
    total_groups = sum(groups_by_cat.values())
    text = f"📁 **群组分类管理**\n\n📊 总群组数：**{total_groups}** 个\n🏷️ 分类数量：**{len(categories)}** 个\n\n💡 点击下方按钮进行操作"
    await update.message.reply_text(text, reply_markup=get_group_manager_keyboard(), parse_mode="Markdown")

async def start_usdt_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id  # ✅ 先定义
    admin_id = get_user_admin_id(user_id)
    if admin_id == 0 and user_id != OWNER_ID:
        await update.message.reply_text("❌ 您不是任何管理员，无法使用此功能")
        return
    context.user_data["active_module"] = "usdt"
    context.user_data["usdt_session"] = {"waiting_for_address": True}
    context.user_data["admin_id"] = admin_id
    await update.message.reply_text("💰 请输入 TRON TRC20 地址（T 开头）：", reply_markup=get_input_cancel_keyboard())

# ==================== 互转查询输入处理 ====================
async def handle_transfer_query_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ 格式错误，请输入两个地址，用空格隔开。", reply_markup=get_input_cancel_keyboard())
        return
    addr_a, addr_b = parts[0], parts[1]
    if not (addr_a.startswith('T') and addr_b.startswith('T')) or len(addr_a) != 34 or len(addr_b) != 34:
        await update.message.reply_text("❌ 地址格式不正确 (Tron 地址以 T 开头，长度 34)。", reply_markup=get_input_cancel_keyboard())
        return
    await update.message.reply_text("⏳ 正在查询链上数据，请稍候...")
    from handlers.transfer import get_trc20_transfers
    history_a = get_trc20_transfers(addr_a, limit=200)
    history_b = get_trc20_transfers(addr_b, limit=200)
    matches = []
    for tx in history_a:
        if tx.get("to") == addr_b or tx.get("from") == addr_b:
            matches.append(tx)
    seen_tx_ids = set()
    unique_matches = []
    for tx in matches:
        tx_id = tx.get("txID") or tx.get("transaction_id")
        if tx_id not in seen_tx_ids:
            seen_tx_ids.add(tx_id)
            unique_matches.append(tx)
    if not unique_matches:
        for tx in history_b:
            if tx.get("to") == addr_a or tx.get("from") == addr_a:
                tx_id = tx.get("txID") or tx.get("transaction_id")
                if tx_id not in seen_tx_ids:
                    unique_matches.append(tx)
    if not unique_matches:
        await update.message.reply_text("📭 未找到直接转账记录。", reply_markup=get_transfer_keyboard())
        context.user_data.pop("active_module", None)
        return
    context.user_data["transfer_results"] = unique_matches
    context.user_data["current_page"] = 0
    context.user_data["query_type"] = "direct"
    context.user_data["active_module"] = "transfer_result"
    await transfer.send_transfer_page(update, context, 0)

async def handle_transfer_analysis_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ 格式错误，请输入两个地址，用空格隔开。", reply_markup=get_input_cancel_keyboard())
        return
    addr_a, addr_b = parts[0], parts[1]
    if not (addr_a.startswith('T') and addr_b.startswith('T')) or len(addr_a) != 34 or len(addr_b) != 34:
        await update.message.reply_text("❌ 地址格式不正确。", reply_markup=get_input_cancel_keyboard())
        return
    await update.message.reply_text("⏳ 正在深度分析链上关系，这可能需要一点时间...")
    from handlers.transfer import get_trc20_transfers, extract_counterparties
    history_a = get_trc20_transfers(addr_a, limit=200)
    history_b = get_trc20_transfers(addr_b, limit=200)
    set_a = extract_counterparties(history_a, addr_a)
    set_b = extract_counterparties(history_b, addr_b)
    common_parties = list(set_a.intersection(set_b))
    common_parties = [p for p in common_parties if p != addr_a and p != addr_b]
    if not common_parties:
        await update.message.reply_text("📭 未发现共同交易对手。", reply_markup=get_transfer_keyboard())
        context.user_data.pop("active_module", None)
        return
    context.user_data["transfer_results"] = common_parties
    context.user_data["current_page"] = 0
    context.user_data["query_type"] = "analysis"
    context.user_data["active_module"] = "transfer_result"
    await transfer.send_transfer_page(update, context, 0)

# ==================== 群组列表内联显示 ====================
async def show_group_list_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ✅ 兼容回调查询和键盘按钮
    if update.callback_query:
        msg = update.callback_query.message
        is_callback = True
    else:
        msg = update.message
        is_callback = False
    groups = context.user_data.get('group_list', [])
    current_page = context.user_data.get('current_page', 0)
    admin_id = context.user_data.get('admin_id', 0)
    if not groups:
        await msg.reply_text("📭 暂无群组", reply_markup=get_group_manager_keyboard())
        return
    ITEMS_PER_PAGE = 8
    total_pages = (len(groups) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = current_page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(groups))
    current_groups = groups[start_idx:end_idx]
    keyboard = []
    keyboard.append([
        InlineKeyboardButton("📋 未分类", callback_data="filter_uncategorized"),
        InlineKeyboardButton("✅ 已分类", callback_data="filter_categorized")
    ])
    for group in current_groups:
        title = group['title'][:25]
        current_cat = group.get('category', '未分类')
        keyboard.append([InlineKeyboardButton(f"{title} (当前: {current_cat})", callback_data=f"sel_group_{group['id']}")])
    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data="group_page_prev"))
    if current_page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data="group_page_next"))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("🔄 刷新列表", callback_data="refresh_group_list")])
    text = f"🏷️ **设置群组分类**\n\n请选择要设置分类的群组：\n共 **{len(groups)}** 个群组，第 **{current_page + 1}/{total_pages}** 页\n\n💡 点击「返回主菜单」取消"
    # ✅ 兼容两种方式
    if is_callback:
        try:
            await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        except Exception:
            pass  # 忽略 "Message is not modified" 错误
    else:
        await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ==================== 内联按钮路由处理器 ====================
async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    data = query.data
    print(f"[BUTTON_ROUTER] 收到: {data}")
    if data == "view_current_bill":
        from handlers.accounting import handle_view_current_bill
        await handle_view_current_bill(update, context)
        return
    if data.startswith("op_"):
        await operator.handle_buttons(update, context)
        return
    if data.startswith("ub_"):
        return

    if data == "profile_back":
        from handlers.profile import profile_back
        await profile_back(update, context)
        return ConversationHandler.END
    if data == "profile_return":
        context.user_data.pop("ub_targets", None)
        context.user_data.pop("ub_selected", None)
        context.user_data.pop("ub_message_data", None)
        context.user_data.pop("ub_in_broadcast", None)

        user_id = query.from_user.id
        admin_id = get_user_admin_id(user_id)
        display_name = query.from_user.username or query.from_user.first_name or ""
        prefs = get_user_preferences(user_id, admin_id)
        from handlers.profile import _build_profile_menu
        text, markup = await _build_profile_menu(user_id, prefs, display_name, admin_id)
        try:
            await query.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
        except Exception:
            pass
        return ConversationHandler.END
    if data.startswith("profile_") and data != "profile_back" and data != "profile_return":
        return

    # ========== 🔴临时迁移按钮🔴 ==========
    if data.startswith("migrate_confirm_"):
        target_admin_id = int(data.replace("migrate_confirm_", ""))
        source_db = context.user_data.get("migrate_source", "")

        if not source_db:
            await query.message.edit_text("❌ 迁移数据已过期，请重新执行 /migrate")
            return

        await do_migrate_data(query, source_db, target_admin_id)
        return

    if data == "migrate_cancel":
        await query.message.edit_text("✅ 已取消迁移")
        context.user_data.pop("migrate_source", None)
        context.user_data.pop("migrate_target", None)
        return
    
    # ========== 会员系统 ==========
    if data == "subscription_menu":
        from handlers.subscription import subscription_menu
        await subscription_menu(update, context)
        return
    if data == "subscription_manage":
        from handlers.subscription import subscription_manage_menu
        await subscription_manage_menu(update, context)
        return
    if data.startswith("sub_plan_"):
        from handlers.subscription import select_plan
        await select_plan(update, context)
        return
    if data.startswith("sub_check_"):
        from handlers.subscription import check_payment
        await check_payment(update, context)
        return
    if data == "sub_cancel":
        from handlers.subscription import cancel_order
        await cancel_order(update, context)
        return
    if data == "sub_renew":
        from handlers.subscription import show_plans
        await show_plans(update, context)
        return
    # 管理端
    if data == "sub_manage_users":
        from handlers.subscription import manage_users
        await manage_users(update, context)
        return
    if data == "sub_manage_orders":
        from handlers.subscription import manage_orders
        await manage_orders(update, context)
        return
    if data == "sub_manage_plans":
        from handlers.subscription import manage_plans
        await manage_plans(update, context)
        return
    if data == "sub_manage_addresses":
        from handlers.subscription import manage_addresses
        await manage_addresses(update, context)
        return

    if data.startswith("sub_confirm_"):
        from handlers.subscription import manual_confirm_payment
        await manual_confirm_payment(update, context)
        return
    if data.startswith("sub_cancel_admin_"):  # ✅ 新增
        from handlers.subscription import admin_cancel_order
        await admin_cancel_order(update, context)
        return

    if data.startswith("sub_filter_"):
        from handlers.subscription import sub_filter
        await sub_filter(update, context)
        return

    if data.startswith("order_filter_"):
        from handlers.subscription import order_filter
        await order_filter(update, context)
        return
    if data == "sub_delete_expired":
        from handlers.subscription import delete_expired_orders
        await delete_expired_orders(update, context)
        return
    # 订单分页
    if data == "order_page_prev":
        page = context.user_data.get('order_page', 0)
        context.user_data['order_page'] = max(0, page - 1)
        from handlers.subscription import manage_orders
        await manage_orders(update, context)
        return
    if data == "order_page_next":
        page = context.user_data.get('order_page', 0)
        context.user_data['order_page'] = page + 1
        from handlers.subscription import manage_orders
        await manage_orders(update, context)
        return
    # ========== 转账分页 ==========
    if data.startswith("trans_page_"):
        page_num = int(data.split("_")[2])
        await transfer.send_transfer_page(update, context, page_num)
        return
    if data.startswith("copy_addr_"):
        addr = data.replace("copy_addr_", "")
        await query.message.reply_text(f"📋 已获取地址：\n<code>{addr}</code>", parse_mode="HTML")
        return

    # 添加以下代码处理返回主菜单
    if data == "transfer_back_to_main":
        await transfer.transfer_back_to_main(update, context)
        return
        
    if data == "main_menu":
        keys_to_clean = [
            "in_broadcast", "bc_all_groups", "bc_selected_ids", "bc_message_content",
            "bc_temp_target_ids", "bc_selected_category", "bc_batches", "bc_current_batch",
            "bc_batch_results", "bc_waiting_for_next", "bc_current_state", "bc_current_page",
            "active_module", "transfer_results", "current_page", "query_type"
        ]
        for k in keys_to_clean:
            context.user_data.pop(k, None)
        from handlers.menu import get_main_menu
        try:
            await query.message.edit_text("请选择功能：", reply_markup=get_main_menu(user_id))
        except Exception:
            await query.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return
    if data == "group_manager":
        from handlers.group_manager import group_manager_menu
        await group_manager_menu(update, context)
        return
    if data == "gm_back_to_manager":
        await group_manager_menu(update, context)
        return
    if data == "gm_set_cat":
        await set_group_category_start(update, context)
        return
    if data == "gm_del_cat":
        from handlers.group_manager import delete_category_start
        await delete_category_start(update, context)
        return
    # ========== 监控模块 ==========
    if data.startswith("monitor_del_"):
        await monitor.monitor_remove_confirm(update, context)
        return
    # ========== 群组分类分页 ==========
    if data == "cat_page_prev":
        page = context.user_data.get('cat_page', 0)
        context.user_data['cat_page'] = max(0, page - 1)
        await select_group_for_category(update, context)
        return
    if data == "cat_page_next":
        page = context.user_data.get('cat_page', 0)
        context.user_data['cat_page'] = page + 1
        await select_group_for_category(update, context)
        return
    # ========== 群组分类选择 ==========
    if data == "del_cat_page_prev":
        page = context.user_data.get('del_cat_page', 0)
        context.user_data['del_cat_page'] = max(0, page - 1)
        from handlers.group_manager import delete_category_start
        await delete_category_start(update, context)
        return
    if data == "del_cat_page_next":
        page = context.user_data.get('del_cat_page', 0)
        context.user_data['del_cat_page'] = page + 1
        from handlers.group_manager import delete_category_start
        await delete_category_start(update, context)
        return
    if data.startswith("del_cat_"):
        await delete_category_confirm(update, context)
        return
    if data.startswith("sel_group_"):
        await select_group_for_category(update, context)
        return
    if data.startswith("set_cat_"):
        await set_group_category(update, context)
        return
    # ========== 群组列表分页和筛选 ==========
    if data == "group_page_prev":
        current_page = context.user_data.get('current_page', 0)
        context.user_data['current_page'] = max(0, current_page - 1)
        await show_group_list_inline(update, context)
        return
    if data == "group_page_next":
        current_page = context.user_data.get('current_page', 0)
        groups = context.user_data.get('group_list', [])
        ITEMS_PER_PAGE = 8
        total_pages = (len(groups) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        context.user_data['current_page'] = min(total_pages - 1, current_page + 1)
        await show_group_list_inline(update, context)
        return
    if data == "refresh_group_list":
        admin_id = context.user_data.get('admin_id', 0)
        if admin_id == 0 and user_id != OWNER_ID:
            admin_id = get_user_admin_id(user_id)
        from db import get_all_groups_from_db
        groups = get_all_groups_from_db(admin_id=admin_id)
        context.user_data['group_list'] = groups
        context.user_data['current_page'] = 0
        context.user_data.pop('filter_type', None)
        await show_group_list_inline(update, context)
        return
    if data == "filter_uncategorized":
        admin_id = context.user_data.get('admin_id', 0)
        if admin_id == 0 and user_id != OWNER_ID:
            admin_id = get_user_admin_id(user_id)
        from db import get_all_groups_from_db
        all_groups = get_all_groups_from_db(admin_id=admin_id)
        filtered = [g for g in all_groups if g.get('category', '未分类') == '未分类']
        if not filtered:
            await query.message.edit_text("📭 暂无未分类的群组")
            await asyncio.sleep(1)
            context.user_data['group_list'] = all_groups
            context.user_data['current_page'] = 0
            await show_group_list_inline(update, context)
            return
        context.user_data['group_list'] = filtered
        context.user_data['current_page'] = 0
        context.user_data['filter_type'] = 'uncategorized'
        await show_group_list_inline(update, context)
        return
    if data == "filter_categorized":
        admin_id = context.user_data.get('admin_id', 0)
        if admin_id == 0 and user_id != OWNER_ID:
            admin_id = get_user_admin_id(user_id)
        from db import get_all_groups_from_db
        all_groups = get_all_groups_from_db(admin_id=admin_id)
        filtered = [g for g in all_groups if g.get('category', '未分类') != '未分类']
        if not filtered:
            await query.message.edit_text("📭 暂无已分类的群组")
            await asyncio.sleep(1)
            context.user_data['group_list'] = all_groups
            context.user_data['current_page'] = 0
            await show_group_list_inline(update, context)
            return
        context.user_data['group_list'] = filtered
        context.user_data['current_page'] = 0
        context.user_data['filter_type'] = 'categorized'
        await show_group_list_inline(update, context)
        return
    # ========== USDT 分页 ==========
    if data.startswith("usdt_"):
        await usdt.handle_buttons(update, context)
        return
    # ========== 账单分页 ==========
    if data.startswith("bill_page_"):
        from handlers.accounting import handle_bill_pagination
        await handle_bill_pagination(update, context)
        return
    if data == "bill_close":
        from handlers.accounting import handle_bill_pagination
        await handle_bill_pagination(update, context)
        return
    # ========== 记账日期选择 ==========
    if data.startswith("bill_year_"):
        from handlers.accounting import handle_year_selection
        await handle_year_selection(update, context)
        return
    if data.startswith("bill_month_"):
        from handlers.accounting import handle_month_selection
        await handle_month_selection(update, context)
        return
    if data.startswith("bill_day_"):
        from handlers.accounting import handle_day_selection
        await handle_day_selection(update, context)
        return
    if data in ["bill_back_to_years", "bill_back_to_months", "bill_days_prev", "bill_days_next"]:
        from handlers.accounting import handle_bill_navigation
        await handle_bill_navigation(update, context)
        return
    # ========== 导出账单 ==========
    if data.startswith("export_year_"):
        from handlers.accounting import handle_export_year_selection
        await handle_export_year_selection(update, context)
        return
    if data.startswith("export_full_year_"):
        from handlers.accounting import handle_export_month_selection
        await handle_export_month_selection(update, context)
        return

    if data.startswith("export_year_"):
        from handlers.accounting import handle_export_year_selection
        await handle_export_year_selection(update, context)
        return

    if data.startswith("export_month_"):
        from handlers.accounting import handle_export_month_selection
        await handle_export_month_selection(update, context)
        return

    if data.startswith("export_day_") or data.startswith("export_full_month_") or data in ["export_days_prev", "export_days_next", "export_back_to_months"]:
        from handlers.accounting import handle_export_day_selection
        await handle_export_day_selection(update, context)
        return
    # ✅ 单独处理 export_back_to_years
    if data == "export_back_to_years":
        from handlers.accounting import handle_export_month_selection
        await handle_export_month_selection(update, context)
        return
    if data == "export_cancel":
        await query.message.edit_text("✅ 已取消导出")
        return

    # ========== 清理确认 ==========
    if data in ["clear_current_confirm", "clear_current_cancel", "clear_all_confirm", "clear_all_cancel"]:
        from handlers.accounting import (
            handle_clear_current_confirm, handle_clear_current_cancel,
            handle_clear_all_confirm, handle_clear_all_cancel
        )
        if data == "clear_current_confirm":
            await handle_clear_current_confirm(update, context)
        elif data == "clear_current_cancel":
            await handle_clear_current_cancel(update, context)
        elif data == "clear_all_confirm":
            await handle_clear_all_confirm(update, context)
        elif data == "clear_all_cancel":
            await handle_clear_all_cancel(update, context)
        return
    if data.startswith("acct_date_"):
        from handlers.accounting import handle_date_selection
        await handle_date_selection(update, context)
        return
    if data == "acct_cancel":
        await query.message.edit_text("✅ 已取消")
        return
    print(f"[BUTTON_ROUTER] 未处理: {data}")

# ==================== 原有函数保留 ====================
async def auto_save_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.type not in ['group', 'supergroup']:
        return
    chat_id = str(update.effective_chat.id)
    title = update.effective_chat.title

    # 检查机器人是否还在群组
    try:
        bot_member = await context.bot.get_chat_member(chat_id, context.bot.id)
        if bot_member.status not in ['member', 'administrator']:
            return
    except:
        return

    from db import get_all_groups_from_db, save_group
    from auth import get_user_admin_id, admins, OWNER_ID

    current_user_id = update.effective_user.id
    # ✅ 获取当前用户的 admin_id
    current_admin_id = get_user_admin_id(current_user_id)

    if current_admin_id == 0:
        # 普通用户发言，不处理
        return

    # 检查当前管理员的独立库中是否已有该群组
    my_groups = get_all_groups_from_db(admin_id=current_admin_id)
    existing = next((g for g in my_groups if g['id'] == chat_id), None)

    if existing:
        # 更新群组信息
        save_group(current_admin_id, chat_id, title, existing.get('category', '未分类'), current_admin_id)
        print(f"[群组归属] 管理员 {current_admin_id} 的群组 {title} 已更新")
    else:
        # 当前管理员首次遇到该群组，为他创建独立记录
        # 尝试从其他管理员的库中获取已有的分类（可选）
        category = '未分类'
        for admin_id, info in admins.items():
            if admin_id == current_admin_id:
                continue
            groups = get_all_groups_from_db(admin_id=admin_id)
            for g in groups:
                if g['id'] == chat_id:
                    category = g.get('category', '未分类')
                    break
            if category != '未分类':
                break
        save_group(current_admin_id, chat_id, title, category, current_admin_id)
        print(f"[群组归属] 为新管理员 {current_admin_id} 创建群组记录: {title}")

async def auto_classify_all_groups_on_startup(app: Application):
    from db import get_all_groups_from_db, update_group_category_if_needed
    await asyncio.sleep(3)
    from auth import admins
    for admin_id in admins.keys():
        try:
            groups = get_all_groups_from_db(admin_id=admin_id)
            for group in groups:
                if group.get('category', '未分类') == '未分类':
                    if update_group_category_if_needed(admin_id, group['id'], group['title']):
                        print(f"[自动分类] 管理员 {admin_id} 群组 {group['title']} 自动分类")
        except Exception as e:
            print(f"[自动分类] 管理员 {admin_id} 出错: {e}")

async def on_bot_join_or_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    my_chat_member = update.my_chat_member
    chat = my_chat_member.chat
    new_status = my_chat_member.new_chat_member.status
    chat_id = str(chat.id)
    title = chat.title
    if new_status in ['member', 'administrator']:
        # 确定该群组归属哪个管理员（由邀请人决定）
        admin_id = 0
        if my_chat_member.from_user:
            admin_id = get_user_admin_id(my_chat_member.from_user.id)
        save_group(admin_id, chat_id, title, '未分类', admin_id)
    elif new_status in ['left', 'kicked', 'banned']:
        delete_group_from_db(0, chat_id)  # 需要从所有管理员库中删除？这里简化，只从主库标记，实际应遍历所有管理员库
        # 由于物理隔离，无法一次删除所有管理员库中的群组，建议在清理任务中处理
        await asyncio.sleep(1)

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from handlers.group_manager import user_states, handle_cancel_in_group_manager
    if user_id in user_states:
        await handle_cancel_in_group_manager(update, context)
        return
    if context.user_data.get("in_broadcast", False):
        return
    context.user_data.clear()
    await update.message.reply_text("❌ 已取消所有操作")

async def send_daily_reports(app: Application):
    from auth import is_authorized, OWNER_ID, get_user_admin_id, admins
    from db import get_all_groups_from_db, get_user_preferences
    from handlers.accounting import get_accounting_manager
    from datetime import timezone, timedelta, datetime

    beijing_tz = timezone(timedelta(hours=8))
    yesterday = (datetime.now(beijing_tz) - timedelta(days=1)).strftime('%Y-%m-%d')

    # ✅ 收集所有开启了早报的用户
    enabled_users = []  # [(user_id, admin_id), ...]

    for admin_id in admins.keys():
        try:
            conn = get_conn(admin_id)  # ✅ 从管理员的独立库查询
            c = conn.cursor()
            c.execute("SELECT user_id FROM user_preferences WHERE daily_report_enabled = 1")
            rows = c.fetchall()
            for row in rows:
                enabled_users.append((row[0], admin_id))
        except:
            pass

    for uid, admin_id in enabled_users:
        if not is_authorized(uid, require_full_access=True):
            continue
        if admin_id == 0 and uid != OWNER_ID:
            continue

        am = get_accounting_manager(admin_id)
        visible_groups = get_all_groups_from_db(admin_id=admin_id)
        if not visible_groups:
            continue

        report = f"📋 **每日早报** ({yesterday})\n\n"
        total_income_cny = 0.0
        total_income_usdt = 0.0
        total_expense_usdt = 0.0
        group_details = []

        for g in visible_groups:
            try:
                stats = am.get_stats_by_date(g['id'], yesterday, admin_id=admin_id)
                income_cny = stats['income_total']
                income_usdt = stats['income_usdt']
                expense = stats['expense_usdt']
                pending = stats['pending_usdt']
                if income_cny == 0 and expense == 0:
                    continue
                total_income_cny += income_cny
                total_income_usdt += income_usdt
                total_expense_usdt += expense
                group_details.append(f"• {g['title']}：入 {income_cny:.2f}元 / {income_usdt:.2f}U，出 {expense:.2f}U，待 {pending:.2f}U")
                if len(group_details) == 10:
                    group_details.append("... 仅显示前10个")
                    break
            except:
                pass

        report += f"💰 **总入款**：{total_income_cny:.2f} 元 ≈ {total_income_usdt:.2f} USDT\n"
        report += f"📤 **总下发**：{total_expense_usdt:.2f} USDT\n"
        report += f"⏳ **总待下发**：{total_income_usdt - total_expense_usdt:.2f} USDT\n\n"
        if group_details:
            report += "📊 **群组明细**\n" + "\n".join(group_details) + "\n\n"

        joined_yesterday = 0
        for g in visible_groups:
            jt = g.get('joined_at', 0)
            if jt:
                dt = datetime.fromtimestamp(jt, tz=beijing_tz)
                if dt.strftime('%Y-%m-%d') == yesterday:
                    joined_yesterday += 1
        report += f"📁 **昨日新加入群组**：{joined_yesterday} 个\n"

        from db import get_monitored_addresses
        my_addrs = get_monitored_addresses(admin_id, user_id=uid)
        if my_addrs:
            addr_lines = []
            for addr in my_addrs:
                stats_month = await monitor.get_monthly_stats(addr['address'])
                addr_lines.append(f"  {addr['note'] or addr['address'][:8]}: 月净 {stats_month['net']:.2f}U")
            if addr_lines:
                report += "🪙 **监控地址月度净收入**\n" + "\n".join(addr_lines) + "\n"
        report += f"\n📌 由记账机器人自动生成"

        try:
            await app.bot.send_message(chat_id=uid, text=report, parse_mode="Markdown")
            print(f"✅ 早报已发送至 {uid}")
        except Exception as e:
            print(f"❌ 发送早报失败 {uid}: {e}")

async def cleanup_deleted_admins(app: Application):
    """清理已标记删除且超过7天的管理员数据"""
    now = int(time.time())
    retention = 7 * 24 * 3600
    try:
        with get_db(0) as conn:
            c = conn.cursor()
            c.execute("SELECT admin_id FROM admins WHERE deleted_at > 0 AND ? - deleted_at > ?", (now, retention))
            expired_admins = [row[0] for row in c.fetchall()]
            for admin_id in expired_admins:
                print(f"🗑️ 清理已过期管理员 {admin_id} 的数据")
                # 删除主库记录
                c.execute("DELETE FROM admins WHERE admin_id = ?", (admin_id,))
                c.execute("DELETE FROM operators WHERE added_by = ?", (admin_id,))
                c.execute("DELETE FROM temp_operators WHERE added_by = ?", (admin_id,))
                c.execute("UPDATE user_preferences SET role = 'user' WHERE user_id = ?", (admin_id,))
                # 物理删除独立数据库文件
                import os
                db_path = f"data/admin_{admin_id}.db"
                if os.path.exists(db_path):
                    os.remove(db_path)
                    print(f"🗑️ 已删除独立数据库文件: {db_path}")
        if expired_admins:
            print(f"✅ 已清理 {len(expired_admins)} 个过期管理员的数据")
    except Exception as e:
        print(f"❌ 清理过期管理员失败: {e}")

async def cleanup_admin_loop(app: Application):
    await asyncio.sleep(60)
    while True:
        try:
            await cleanup_deleted_admins(app)
        except Exception as e:
            print(f"⚠️ 管理员清理循环出错: {e}")
        await asyncio.sleep(3600)

async def daily_report_loop(app: Application):
    await asyncio.sleep(30)
    sent_today = False
    while True:
        now = datetime.now()
        beijing_now = now.astimezone(timezone(timedelta(hours=8)))
        if beijing_now.hour == 9 and not sent_today:
            sent_today = True
            try:
                await send_daily_reports(app)
            except Exception as e:
                print(f"❌ 每日早报发送失败: {e}")
        elif beijing_now.hour != 9:
            sent_today = False
        await asyncio.sleep(60)

# ==================== 🔴临时迁移 函数🔴 ====================
# finance_assistant.py - do_migrate_data 函数

async def do_migrate_data(query, source_db_path: str, target_admin_id: int):
    """执行数据迁移"""
    import sqlite3
    import os

    await query.message.edit_text("🔄 正在迁移数据，请稍候...")

    from db_manager import init_admin_db
    init_admin_db(target_admin_id)

    target_db_path = f"data/admin_{target_admin_id}.db"

    try:
        source_conn = sqlite3.connect(source_db_path)
        source_conn.row_factory = sqlite3.Row

        target_conn = sqlite3.connect(target_db_path)

        total_migrated = 0
        errors = []

        # 获取旧数据库中所有表
        tables = source_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        for table in table_names:
            try:
                # ✅ 先尝试用 UPDATE 直接修改已有的数据（如果之前迁移过）
                target_conn.execute(f"UPDATE {table} SET admin_id = {target_admin_id} WHERE admin_id = 0 OR admin_id IS NULL")
                target_conn.execute(f"UPDATE {table} SET added_by = {target_admin_id} WHERE added_by = 0 OR added_by IS NULL")
            except:
                pass  # 表可能没有这些字段，忽略
                
            try:
                # 获取列名
                cols = source_conn.execute(f"PRAGMA table_info({table})").fetchall()
                col_names = [c[1] for c in cols]

                # 获取数据
                rows = source_conn.execute(f"SELECT * FROM {table}").fetchall()
                if not rows:
                    continue

                placeholders = ','.join(['?' for _ in col_names])
                columns_str = ','.join(col_names)

                # finance_assistant.py - do_migrate_data 函数中的循环部分

                migrated = 0
                for row in rows:
                    values = []
                    for c in col_names:
                        val = row[c]
                        if val is None:
                            values.append(None)
                        elif isinstance(val, (int, float)):
                            values.append(val)
                        else:
                            values.append(str(val))

                    # ✅ 修改 admin_id 字段
                    if 'admin_id' in col_names:
                        idx = col_names.index('admin_id')
                        values[idx] = target_admin_id

                    # ✅ 修改 added_by 字段
                    if 'added_by' in col_names:
                        idx = col_names.index('added_by')
                        if values[idx] is None or values[idx] == 0:
                            values[idx] = target_admin_id

                    # ✅ 使用 INSERT OR REPLACE
                    target_conn.execute(
                        f"INSERT OR REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})",
                        values
                    )
                    migrated += 1

                target_conn.commit()
                total_migrated += migrated
                print(f"✅ 迁移 {table}: {migrated} 条")

            except Exception as e:
                error_msg = f"❌ 迁移 {table} 失败: {e}"
                errors.append(error_msg)
                print(error_msg)

        # 在所有表迁移完成后，统一修复 admin_id
        for table in table_names:
            try:
                target_conn.execute(f"UPDATE {table} SET admin_id = {target_admin_id} WHERE admin_id = 0")
                target_conn.execute(f"UPDATE {table} SET added_by = {target_admin_id} WHERE added_by = 0")
            except:
                pass

        target_conn.commit()

        source_conn.close()
        target_conn.close()

        if errors:
            error_text = "\n".join(errors[:5])
            if len(errors) > 5:
                error_text += f"\n... 还有 {len(errors) - 5} 个错误"
            await query.message.edit_text(
                f"⚠️ **迁移部分完成**\n\n"
                f"📦 源数据库：`{source_db_path}`\n"
                f"📌 目标管理员：`{target_admin_id}`\n"
                f"📊 成功迁移：**{total_migrated}** 条\n\n"
                f"**错误信息：**\n{error_text}",
                parse_mode="Markdown"
            )
        else:
            await query.message.edit_text(
                f"✅ **数据迁移完成！**\n\n"
                f"📦 源数据库：`{source_db_path}`\n"
                f"📌 目标管理员：`{target_admin_id}`\n"
                f"📊 共迁移：**{total_migrated}** 条记录\n\n"
                f"💡 旧数据仍保留在 `{source_db_path}` 中，确认无误后可以手动删除。",
                parse_mode="Markdown"
            )

    except Exception as e:
        await query.message.edit_text(f"❌ 迁移失败: {e}")
        
async def cmd_migrate_old_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """超级管理员专用：将旧数据库数据迁移到指定管理员名下"""
    user_id = update.effective_user.id

    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "用法：/migrate 目标管理员ID\n"
            "例如：/migrate 8107909168\n\n"
            "会将 bot.db 中的所有数据迁移到指定管理员的独立数据库中"
        )
        return

    try:
        target_admin_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ 管理员ID必须是数字")
        return

    # 确认操作
    keyboard = [
        [InlineKeyboardButton("✅ 确认迁移", callback_data=f"migrate_confirm_{target_admin_id}")],
        [InlineKeyboardButton("❌ 取消", callback_data="migrate_cancel")],
    ]

    # 检查旧数据库是否存在
    import os
    old_db_paths = ["bot.db", "master.db", "data/bot.db", "data/master.db"]
    found_db = None
    for path in old_db_paths:
        if os.path.exists(path) and path != "master.db":  # master.db 是新的主库，不能迁移
            found_db = path
            break

    if not found_db:
        await update.message.reply_text("📭 未找到可迁移的旧数据库文件")
        return

    # 检查旧数据库内容
    import sqlite3
    try:
        old_conn = sqlite3.connect(found_db)
        old_conn.row_factory = sqlite3.Row

        # 统计各表数据
        tables_info = {}
        for table in ["accounting_records", "accounting_sessions", "monitored_addresses", 
                       "address_transactions", "groups", "group_users", "group_accounting_config",
                       "address_queries", "address_query_log"]:
            try:
                count = old_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count > 0:
                    tables_info[table] = count
            except:
                pass

        old_conn.close()

        if not tables_info:
            await update.message.reply_text(f"📭 旧数据库 {found_db} 中没有可迁移的数据")
            return

        # 显示将要迁移的数据
        info_text = f"📦 找到旧数据库：`{found_db}`\n\n"
        info_text += "将要迁移的数据：\n"
        for table, count in tables_info.items():
            info_text += f"  • {table}：{count} 条\n"
        info_text += f"\n📌 目标管理员：`{target_admin_id}`\n"
        info_text += f"📌 目标数据库：`data/admin_{target_admin_id}.db`\n\n"
        info_text += "⚠️ 迁移后旧数据仍保留在原数据库中\n\n"
        info_text += "确认迁移？"

        context.user_data["migrate_source"] = found_db
        context.user_data["migrate_target"] = target_admin_id

        await update.message.reply_text(
            info_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

    except Exception as e:
        await update.message.reply_text(f"❌ 检查旧数据库失败: {e}")

# ==================== main 函数 ====================
def main():
    # 初始化主数据库
    init_master_db()
    # 初始化操作员缓存
    init_operators_from_db()
    # 创建应用
    # ✅ 启用并发处理
    app = Application.builder() \
        .token(BOT_TOKEN) \
        .concurrent_updates(True) \
        .build()
    # 后台任务
    async def cleanup_expired_states():
        while True:
            try:
                await asyncio.sleep(300)
                from handlers.group_manager import cleanup_expired_states
                await cleanup_expired_states()
            except Exception as e:
                print(f"⚠️ 清理过期状态失败: {e}")
    async def migrate_data_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id != OWNER_ID:
            await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
            return
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("用法：/migrate_data <操作员ID> <新管理员ID>")
            return
        try:
            operator_id = int(args[0])
            new_admin_id = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ ID 必须是数字")
            return
        # 迁移数据需要从旧库读取，由于物理隔离后无全局库，此命令废弃或需重新实现
        await update.message.reply_text("⚠️ 物理隔离模式下，数据迁移请使用 Web 端工具")
        
    async def force_clean_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        admin_id = get_user_admin_id(user_id)
        if admin_id == 0:
            await update.message.reply_text("❌ 您不是任何管理员，无法使用此命令")
            return
        msg = await update.message.reply_text("🔍 开始强制检查您的群组...")
        from db import get_all_groups_from_db, delete_group_from_db
        groups = get_all_groups_from_db(admin_id=admin_id)
        if not groups:
            await msg.edit_text("📭 您没有任何群组记录")
            return
        deleted, kept, errors = 0, 0, 0
        for group in groups:
            group_id = group['id']
            group_name = group.get('title', '未知')
            try:
                await context.bot.send_chat_action(chat_id=group_id, action="typing")
                kept += 1
                await msg.edit_text(f"✅ 仍在群组: {group_name}\n已检查: {kept + deleted + errors}/{len(groups)}")
            except Exception as e:
                error_msg = str(e).lower()
                if any(kw in error_msg for kw in ["chat not found", "bot was kicked", "bot is not a member"]):
                    delete_group_from_db(admin_id, group_id)
                    deleted += 1
                else:
                    errors += 1
                await asyncio.sleep(0.5)
        await msg.edit_text(f"✅ 清理完成！\n\n您的群组中：\n• 删除无效群组: {deleted} 个\n• 保留有效群组: {kept} 个\n• 错误: {errors} 个")
        
    # 注册处理器
    app.add_handler(CommandHandler("cancel", cancel_command))
    from handlers.group_manager import skip_command
    app.add_handler(CommandHandler("skip", skip_command))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("update_ops", cmd_update_operator_info))
    app.add_handler(CommandHandler("clean", force_clean_groups))
    app.add_handler(CommandHandler("addadmin", add_admin_cmd))
    app.add_handler(CommandHandler("removeadmin", remove_admin_cmd))
    app.add_handler(CommandHandler("listadmins", list_admins_cmd))
    app.add_handler(CommandHandler("migrate_data", migrate_data_cmd))
    app.add_handler(get_user_broadcast_handler(), group=1)
    # ✅ 会员系统命令
    from handlers.subscription import (
        cmd_add_plan, cmd_del_plan, cmd_toggle_plan, cmd_edit_plan,
        cmd_add_address, cmd_del_address,
        cmd_add_subscription, cmd_remove_subscription, 
        cmd_extend_subscription, cmd_toggle_subscription
    )
    app.add_handler(CommandHandler("addplan", cmd_add_plan))
    app.add_handler(CommandHandler("delplan", cmd_del_plan))
    app.add_handler(CommandHandler("toggleplan", cmd_toggle_plan))
    app.add_handler(CommandHandler("editplan", cmd_edit_plan))
    app.add_handler(CommandHandler("addaddr", cmd_add_address))
    app.add_handler(CommandHandler("deladdr", cmd_del_address))
    app.add_handler(CommandHandler("addsub", cmd_add_subscription))
    app.add_handler(CommandHandler("delsub", cmd_remove_subscription))
    app.add_handler(CommandHandler("extendsub", cmd_extend_subscription))
    app.add_handler(CommandHandler("togglesub", cmd_toggle_subscription))
    # 🔴临时迁移函数🔴
    app.add_handler(CommandHandler("migrate", cmd_migrate_old_data))
        
    for handler in get_git_handlers():
        app.add_handler(handler)
    # Transfer ConversationHandler
    transfer_conv_handler = ConversationHandler(
        entry_points=[],
        states={},
        fallbacks=[CommandHandler("cancel", transfer.cancel_transfer)],
        per_message=False,
    )
    app.add_handler(transfer_conv_handler, group=1)
    # Broadcast
    for handler in broadcast.get_handlers():
        app.add_handler(handler, group=1)
    # Monitor
    monitor_conv_handler = monitor.get_monitor_conversation_handler()
    app.add_handler(monitor_conv_handler)
    app.add_handler(CommandHandler("cancel_monitor", monitor.monitor_cancel))
    # 内联按钮路由
    app.add_handler(CallbackQueryHandler(button_router), group=0)
    # ========== 个人中心 ConversationHandler ==========
    profile_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(profile_stats, pattern="^profile_stats$"),
            CallbackQueryHandler(profile_addresses, pattern="^profile_addresses$"),
            CallbackQueryHandler(profile_toggle_notify, pattern="^profile_toggle_notify$"),
            CallbackQueryHandler(profile_signature_start, pattern="^profile_signature$"),
            CallbackQueryHandler(profile_contact, pattern="^profile_contact$"),
            CallbackQueryHandler(profile_feedback_start, pattern="^profile_feedback$"),
            CallbackQueryHandler(profile_export_data, pattern="^profile_export$"),
            CallbackQueryHandler(profile_report_toggle, pattern="^profile_report_toggle$"),
            CallbackQueryHandler(profile_monitor_group, pattern="^profile_monitor_group$"),
        ],
        states={
            SET_SIGNATURE: [MessageHandler(filters.TEXT, profile_signature_input)],
            FEEDBACK: [MessageHandler(filters.TEXT, profile_feedback_input)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
        per_message=False,
        allow_reentry=True,
    )
    app.add_handler(profile_conv, group=1)
    # 三层私聊处理器
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, keyboard_handler), group=0)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, module_input_handler), group=1)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, ai_chat_handler), group=2)
    # 群组消息
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, accounting.handle_group_message), group=1)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, auto_save_group), group=2)
    app.add_handler(ChatMemberHandler(on_bot_join_or_leave, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(get_service_message_handler())
    
    async def post_init(app: Application):
        await auto_classify_all_groups_on_startup(app)
        asyncio.create_task(daily_report_loop(app))
        asyncio.create_task(cleanup_admin_loop(app))
        asyncio.create_task(cleanup_expired_states())

        # finance_assistant.py 的 post_init 中添加

        async def subscription_expiry_check():
            await asyncio.sleep(120)  # 启动后等2分钟
            while True:
                try:
                    conn = get_conn(0)
                    now = int(time.time())
                    # 到期前3天提醒
                    remind_start = now + 3 * 86400
                    remind_end = now + 4 * 86400  # 只提醒一次（在3-4天之间）

                    subs = conn.execute(
                        """SELECT u.user_id, u.expire_date, p.name 
                           FROM user_subscriptions u 
                           LEFT JOIN subscription_plans p ON u.plan_id = p.plan_id 
                           WHERE u.status = 'active' 
                           AND u.expire_date > ? AND u.expire_date < ?""",
                        (remind_start, remind_end)
                    ).fetchall()

                    for sub in subs:
                        days_left = max(0, (sub["expire_date"] - now) // 86400)
                        try:
                            await app.bot.send_message(
                                chat_id=sub["user_id"],
                                text=f"⏰ **会员到期提醒**\n\n"
                                     f"您的会员将于 **{days_left} 天后**到期。\n"
                                     f"到期后数据保留 7 天，7 天后自动清除。\n"
                                     f"请及时续费以继续使用全部功能。\n\n"
                                     f"💡 个人中心 → 续费会员",
                                parse_mode="Markdown"
                            )
                            print(f"✅ 到期提醒已发送至 {sub['user_id']}")
                        except Exception as e:
                            print(f"❌ 发送到期提醒失败 {sub['user_id']}: {e}")

                    # 到期当天提醒
                    expire_today = conn.execute(
                        """SELECT u.user_id, u.expire_date, p.name 
                           FROM user_subscriptions u 
                           LEFT JOIN subscription_plans p ON u.plan_id = p.plan_id 
                           WHERE u.status = 'active' 
                           AND u.expire_date > ? AND u.expire_date < ?""",
                        (now, now + 86400)
                    ).fetchall()

                    for sub in expire_today:
                        try:
                            await app.bot.send_message(
                                chat_id=sub["user_id"],
                                text=f"⚠️ **会员今日到期！**\n\n"
                                     f"您的会员今天到期，数据将保留 7 天。\n"
                                     f"如未续费，7 天后所有数据将被清除。\n\n"
                                     f"💡 个人中心 → 续费会员",
                                parse_mode="Markdown"
                            )
                        except Exception as e:
                            print(f"❌ 发送到期通知失败 {sub['user_id']}: {e}")

                except Exception as e:
                    print(f"⚠️ 会员到期检查失败: {e}")

                await asyncio.sleep(3600)  # 每小时检查一次

        asyncio.create_task(subscription_expiry_check())
        
        # ✅ 添加支付检测任务
        from handlers.subscription import check_subscription_payments
        async def subscription_check_loop():
            await asyncio.sleep(15)
            while True:
                try:
                    await check_subscription_payments(app)
                except Exception as e:
                    print(f"⚠️ 支付检测失败: {e}")
                await asyncio.sleep(30)  # 每30秒检查一次
        asyncio.create_task(subscription_check_loop())

        # ✅ 放在这里
        async def auto_clean_expired_orders():
            await asyncio.sleep(60)
            while True:
                try:
                    conn = get_conn(0)
                    now = int(time.time())

                    # 已取消/过期：保留 7 天
                    cutoff_7d = now - 7 * 86400
                    conn.execute(
                        "DELETE FROM payment_orders WHERE status IN ('expired', 'cancelled') AND created_at < ?",
                        (cutoff_7d,)
                    )

                    # 已支付：保留 365 天
                    cutoff_365d = now - 365 * 86400
                    conn.execute(
                        "DELETE FROM payment_orders WHERE status = 'paid' AND created_at < ?",
                        (cutoff_365d,)
                    )

                    conn.commit()
                except Exception as e:
                    print(f"⚠️ 清理订单失败: {e}")
                await asyncio.sleep(86400)
        asyncio.create_task(auto_clean_expired_orders())
        
        async def monitor_check_loop():
            await asyncio.sleep(10)
            class ContextWrapper:
                def __init__(self, bot):
                    self.bot = bot
            ctx = ContextWrapper(app.bot)
            while True:
                try:
                    await monitor.check_address_transactions(ctx)
                    await asyncio.sleep(30)
                except Exception as e:
                    print(f"⚠️ 监控检查失败: {e}")
                    await asyncio.sleep(30)
        asyncio.create_task(monitor_check_loop())
    app.post_init = post_init
    print("=" * 50)
    print("🤖 机器人启动成功...")
    print("=" * 50)
    try:
        app.run_polling()
    finally:
        close_all_connections()

if __name__ == "__main__":
    main()
