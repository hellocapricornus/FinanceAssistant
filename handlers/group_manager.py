# handlers/group_manager.py - 适配物理隔离

import time
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from auth import is_authorized, get_user_admin_id, OWNER_ID
from db import (
    get_all_groups_from_db as db_get_all_groups,
    get_all_categories as db_get_all_categories,
    update_group_category as db_update_group_category,
    add_category as db_add_category,
    delete_category as db_delete_category,
    get_groups_by_category as db_get_groups_by_category,
    get_db_connection
)
from typing import List, Dict

# 存储用户输入状态的字典
user_states = {}

# 分页常量
ITEMS_PER_PAGE = 10

# 用户状态超时清理（1分钟）
USER_STATE_TIMEOUT = 60

# ==================== 辅助函数 ====================
def _get_visible_groups(admin_id: int) -> List[Dict]:
    """根据管理员权限返回可见的群组列表（从独立库中读取）"""
    groups = db_get_all_groups(admin_id=admin_id)  # 传入 admin_id
    return groups

def get_group_manager_keyboard():
    keyboard = [
        [KeyboardButton("📊 群组统计"), KeyboardButton("📁 查看分类"), KeyboardButton("➕ 创建分类")],
        [KeyboardButton("🏷️ 设置群组分类"), KeyboardButton("🗑️ 删除分类")],
        [KeyboardButton("◀️ 返回主菜单")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ==================== 状态清理 ====================
async def cleanup_expired_states():
    current_time = time.time()
    expired_users = []
    for user_id, state in user_states.items():
        if 'timestamp' not in state:
            state['timestamp'] = current_time
        elif current_time - state['timestamp'] > USER_STATE_TIMEOUT:
            expired_users.append(user_id)
    for user_id in expired_users:
        del user_states[user_id]
        print(f"[清理] 已清除用户 {user_id} 的过期状态")

# ==================== 内联菜单 ====================
async def group_manager_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if not is_authorized(user_id, require_full_access=True):
        await query.answer("❌ 无权限", show_alert=True)
        await query.message.reply_text("❌ 管理人/操作员才能使用，如需使用请联系 @ChinaEdward")
        return
    await query.answer()
    if user_id in user_states:
        del user_states[user_id]
    admin_id = get_user_admin_id(user_id)
    groups = _get_visible_groups(admin_id)
    total_groups = len(groups)
    categories = db_get_all_categories(admin_id=admin_id)
    keyboard = [
        [InlineKeyboardButton("📊 查看统计", callback_data="gm_stats")],
        [InlineKeyboardButton("📁 查看所有分类", callback_data="gm_list_cats")],
        [InlineKeyboardButton("➕ 创建分类", callback_data="gm_add_cat")],
        [InlineKeyboardButton("🏷️ 设置群组分类", callback_data="gm_set_cat")],
        [InlineKeyboardButton("🗑️ 删除分类", callback_data="gm_del_cat")],
        [InlineKeyboardButton("◀️ 返回主菜单", callback_data="main_menu")]
    ]
    text = f"📁 **群组分类管理**\n\n"
    text += f"📊 总群组数：**{total_groups}** 个\n"
    text += f"🏷️ 分类数量：**{len(categories)}** 个\n\n"
    text += "💡 点击下方按钮进行操作"
    try:
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except Exception as e:
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    groups = _get_visible_groups(admin_id)
    cat_count = {}
    for g in groups:
        cat = g.get('category', '未分类')
        cat_count[cat] = cat_count.get(cat, 0) + 1
    categories = db_get_all_categories(admin_id=admin_id)
    text = "📊 **群组统计**\n\n"
    for cat in categories:
        cat_name = cat['name']
        count = cat_count.get(cat_name, 0)
        if count == 0:
            continue
        text += f"• **{cat_name}**：{count} 个群组\n"
    text += f"\n总计：**{len(groups)}** 个群组"
    await query.message.edit_text(text, parse_mode="Markdown")
    await query.message.reply_text("请继续操作：", reply_markup=get_group_manager_keyboard())

async def list_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    groups = _get_visible_groups(admin_id)
    categories = db_get_all_categories(admin_id=admin_id)
    cat_count = {}
    for g in groups:
        cat = g.get('category', '未分类')
        cat_count[cat] = cat_count.get(cat, 0) + 1
    text = "📁 **现有分类**\n\n"
    has_any = False
    for cat in categories:
        cat_name = cat['name']
        count = cat_count.get(cat_name, 0)
        if count == 0:
            continue
        has_any = True
        text += f"• **{cat_name}** ({count}个群组)\n"
    if not has_any:
        text += "📭 暂无分类"
    await query.message.edit_text(text, parse_mode="Markdown")
    await query.message.reply_text("请继续操作：", reply_markup=get_group_manager_keyboard())

# ==================== 创建分类 ====================
async def add_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    user_states[user_id] = {"action": "add_category_name", "timestamp": time.time()}
    await query.message.edit_text(
        "➕ **创建新分类**\n\n"
        "请输入分类名称（如：VIP群组）：\n\n"
        "❌ 输入 /cancel 取消",
        parse_mode="Markdown"
    )

async def add_category_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message
    text = message.text.strip()
    if text == "/cancel":
        if user_id in user_states:
            del user_states[user_id]
        await message.reply_text("❌ 已取消创建分类")
        from handlers.menu import get_main_menu
        await message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return
    if len(text) < 2:
        await message.reply_text("❌ 分类名称至少2个字符，请重新输入：\n\n输入 /cancel 取消")
        return
    admin_id = get_user_admin_id(user_id)
    categories = db_get_all_categories(admin_id=admin_id)
    if any(cat['name'] == text for cat in categories):
        await message.reply_text(f"❌ 分类「{text}」已存在，请使用其他名称：\n\n输入 /cancel 取消")
        return
    user_states[user_id] = {"action": "add_category_desc", "name": text, "admin_id": admin_id}
    await message.reply_text(f"📝 分类名称：{text}\n\n请输入分类描述（可选，直接发送 /skip 跳过）：\n\n❌ 输入 /cancel 取消")

async def add_category_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message
    text = message.text.strip()
    if text == "/cancel":
        if user_id in user_states:
            del user_states[user_id]
        await message.reply_text("❌ 已取消创建分类")
        from handlers.menu import get_main_menu
        await message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return
    if text == "/skip":
        description = ""
    else:
        description = text
    state = user_states.get(user_id, {})
    name = state.get("name", "")
    admin_id = state.get("admin_id", 0)
    if not name or admin_id == 0:
        await message.reply_text("❌ 会话已过期，请重新开始")
        if user_id in user_states:
            del user_states[user_id]
        return
    if db_add_category(admin_id, name, description):
        await message.reply_text(f"✅ 分类「{name}」创建成功！")
    else:
        await message.reply_text(f"❌ 创建失败")
    if user_id in user_states:
        del user_states[user_id]
    from handlers.menu import get_main_menu
    await message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))

# ==================== 删除分类 ====================
async def delete_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    categories = db_get_all_categories(admin_id=admin_id)
    deletable = [cat for cat in categories if cat['name'] != '未分类']
    if not deletable:
        await query.message.edit_text("⚠️ 没有可删除的分类（「未分类」不能删除）")
        return

    # ✅ 分页
    page = context.user_data.get('del_cat_page', 0)
    items_per_page = 10
    total_pages = (len(deletable) + items_per_page - 1) // items_per_page
    start = page * items_per_page
    end = min(start + items_per_page, len(deletable))
    page_cats = deletable[start:end]

    keyboard = []
    for cat in page_cats:
        keyboard.append([InlineKeyboardButton(f"🗑️ {cat['name']}", callback_data=f"del_cat_{cat['name']}")])

    # ✅ 分页导航
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data="del_cat_page_prev"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("下一页 ➡️", callback_data="del_cat_page_next"))
    if nav:
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="group_manager")])

    await query.message.edit_text(
        f"🗑️ **删除分类**\n\n选择要删除的分类：\n共 {len(deletable)} 个分类"
        + (f"（第 {page+1}/{total_pages} 页）" if total_pages > 1 else ""),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def delete_category_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    category_name = query.data.replace("del_cat_", "")
    user_id = query.from_user.id
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    if db_delete_category(admin_id, category_name):
        await query.message.edit_text(f"✅ 已删除分类「{category_name}」")
    else:
        await query.message.edit_text(f"❌ 删除失败")
    await asyncio.sleep(1)
    await query.message.reply_text("📁 群组分类管理：", reply_markup=get_group_manager_keyboard())

# ==================== 设置群组分类 ====================
async def set_group_category_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    admin_id = get_user_admin_id(user_id)
    groups = _get_visible_groups(admin_id)
    if not groups:
        await query.message.edit_text("📭 暂无群组")
        return
    context.user_data['group_list'] = groups
    context.user_data['current_page'] = 0
    context.user_data['selecting_group'] = True
    context.user_data['admin_id'] = admin_id
    await show_group_list_page(update, context)

async def show_group_list_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    groups = context.user_data.get('group_list', [])
    current_page = context.user_data.get('current_page', 0)
    if not groups:
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.message.edit_text("📭 暂无群组")
        else:
            await update.message.reply_text("📭 暂无群组")
        return
    total_pages = (len(groups) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = current_page * ITEMS_PER_PAGE
    end_idx = min(start_idx + ITEMS_PER_PAGE, len(groups))
    current_groups = groups[start_idx:end_idx]
    keyboard = []
    keyboard.append([InlineKeyboardButton("📋 未分类", callback_data="filter_uncategorized"), InlineKeyboardButton("✅ 已分类", callback_data="filter_categorized")])
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
    keyboard.append([InlineKeyboardButton("🔄 刷新", callback_data="refresh_group_list"), InlineKeyboardButton("◀️ 返回", callback_data="group_manager")])
    text = f"🏷️ **设置群组分类**\n\n请选择要设置分类的群组：\n共 **{len(groups)}** 个群组，第 **{current_page + 1}/{total_pages}** 页\n\n📌 **筛选选项**：\n• 未分类：显示未设置分类的群组\n• 已分类：显示已设置分类的群组"
    if isinstance(update, Update) and update.callback_query:
        await update.callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def handle_group_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    data = query.data
    if data == "group_page_prev":
        current_page = context.user_data.get('current_page', 0)
        context.user_data['current_page'] = max(0, current_page - 1)
        await show_group_list_page(update, context)
    elif data == "group_page_next":
        current_page = context.user_data.get('current_page', 0)
        groups = context.user_data.get('group_list', [])
        total_pages = (len(groups) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        context.user_data['current_page'] = min(total_pages - 1, current_page + 1)
        await show_group_list_page(update, context)
    elif data == "refresh_group_list":
        groups = _get_visible_groups(admin_id)
        context.user_data['group_list'] = groups
        context.user_data['current_page'] = 0
        context.user_data.pop('filter_type', None)
        await show_group_list_page(update, context)
    elif data == "filter_uncategorized":
        await filter_groups(update, context, "uncategorized")
    elif data == "filter_categorized":
        await filter_groups(update, context, "categorized")

async def filter_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, filter_type: str):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    await query.answer()
    all_groups = _get_visible_groups(admin_id)
    if filter_type == "uncategorized":
        filtered_groups = [g for g in all_groups if g.get('category', '未分类') == '未分类']
        filter_name = "未分类"
    else:
        filtered_groups = [g for g in all_groups if g.get('category', '未分类') != '未分类']
        filter_name = "已分类"
    if not filtered_groups:
        await query.message.edit_text(f"📭 暂无{filter_name}的群组")
        await asyncio.sleep(1)
        await show_group_list_page(update, context)
        return
    context.user_data['group_list'] = filtered_groups
    context.user_data['current_page'] = 0
    context.user_data['filter_type'] = filter_type
    await show_group_list_page(update, context)

async def select_group_for_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    group_id = query.data.replace("sel_group_", "")
    await query.answer()
    context.user_data['selected_group_id'] = group_id

    groups = _get_visible_groups(admin_id)
    used_categories = set()
    for g in groups:
        cat = g.get('category', '未分类')
        used_categories.add(cat)

    # ✅ 获取所有分类（包括未使用的）
    categories = db_get_all_categories(admin_id=admin_id)

    # ✅ 分类按钮
    keyboard = []
    keyboard.append([InlineKeyboardButton(f"📂 未分类", callback_data=f"set_cat_未分类_{group_id}")])

    # ✅ 把所有分类都显示出来（不只是 used_categories 中的）
    for cat in categories:
        if cat['name'] != '未分类':
            keyboard.append([InlineKeyboardButton(f"📁 {cat['name']}", callback_data=f"set_cat_{cat['name']}_{group_id}")])

    # ✅ 添加分页（如果分类超过 10 个）
    # 每页显示 10 个分类
    page = context.user_data.get('cat_page', 0)
    items_per_page = 10
    # 跳过"未分类"按钮，从第 1 行开始分页
    start = page * items_per_page + 1
    end = start + items_per_page
    all_cat_rows = keyboard[1:]  # 去掉"未分类"行
    keyboard = [keyboard[0]]  # 保留"未分类"
    keyboard.extend(all_cat_rows[start:end])

    total_pages = (len(all_cat_rows) + items_per_page - 1) // items_per_page
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data="cat_page_prev"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("下一页 ➡️", callback_data="cat_page_next"))
        if nav:
            keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="gm_set_cat")])

    await query.message.edit_text(
        "🏷️ **选择分类**\n\n"
        "请选择要分配给该群组的分类：\n"
        f"共 {len(categories)} 个分类" + (f" (第 {page+1}/{total_pages} 页)" if total_pages > 1 else ""),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def set_group_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    admin_id = get_user_admin_id(user_id)
    data_parts = query.data.replace("set_cat_", "").split("_")
    category_name = data_parts[0]
    group_id = data_parts[1]
    await query.answer()
    if db_update_group_category(admin_id, group_id, category_name):
        groups = _get_visible_groups(admin_id)
        group_info = next((g for g in groups if g['id'] == group_id), None)
        group_title = group_info['title'] if group_info else group_id
        await query.message.edit_text(f"✅ 已将群组「{group_title}」的分类设置为「{category_name}」")
        await asyncio.sleep(1)
        all_groups = _get_visible_groups(admin_id)
        context.user_data['group_list'] = all_groups
        context.user_data['current_page'] = 0
        context.user_data.pop('filter_type', None)
        await show_group_list_page(update, context)
    else:
        await query.message.edit_text(f"❌ 设置失败")
        await asyncio.sleep(1)
        await group_manager_menu(update, context)

# ==================== 文本输入处理 ====================
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message
    text = message.text.strip()
    if user_id not in user_states:
        return
    if text == "/cancel":
        if user_id in user_states:
            del user_states[user_id]
        await message.reply_text("❌ 已取消操作")
        from handlers.menu import get_main_menu
        await message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
        return
    state = user_states[user_id]
    action = state.get("action")
    if text == "/skip":
        if action == "add_category_desc":
            await add_category_desc(update, context)
            return
        else:
            await message.reply_text("❌ 当前状态不支持 /skip")
            return
    if action == "add_category_name":
        await add_category_name(update, context)
    elif action == "add_category_desc":
        await add_category_desc(update, context)

async def handle_cancel_in_group_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_states:
        del user_states[user_id]
    context.user_data.pop("active_module", None)
    if update.message:
        await update.message.reply_text("❌ 已取消创建分类")
    elif update.callback_query:
        await update.callback_query.message.reply_text("❌ 已取消创建分类")
    from handlers.menu import get_main_menu
    if update.message:
        await update.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))
    elif update.callback_query:
        await update.callback_query.message.reply_text("请选择功能：", reply_markup=get_main_menu(user_id))

async def skip_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_states:
        await update.message.reply_text("❌ 当前没有进行中的操作")
        return
    state = user_states[user_id]
    action = state.get("action")
    if action == "add_category_desc":
        await add_category_desc(update, context)
    else:
        await update.message.reply_text("❌ 当前状态不支持 /skip")

__all__ = [
    'group_manager_menu', 'show_stats', 'list_categories', 'add_category_start',
    'delete_category_start', 'delete_category_confirm', 'set_group_category_start',
    'select_group_for_category', 'set_group_category', 'handle_text_input',
    'handle_cancel_in_group_manager', 'skip_command', 'show_group_list_page',
    'handle_group_pagination', 'filter_groups', 'user_states', 'ITEMS_PER_PAGE'
]