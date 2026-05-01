# handlers/user_broadcast.py - 面向用户的广播功能（仅超级管理员）

import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler, MessageHandler, filters, CommandHandler, CallbackQueryHandler

logger = logging.getLogger(__name__)

# 状态
UB_SELECT_TARGET = 1
UB_INPUT_MESSAGE = 2
UB_CONFIRM_SEND = 3


def get_target_users():
    """获取所有可广播的目标用户（管理员、操作员、临时操作员，不含普通用户）"""
    from auth import admins, operators, temp_operators
    from db_manager import get_conn

    users = {}  # {user_id: display_name}

    # 管理员
    for aid in admins:
        if aid not in users:
            users[aid] = f"管理员 {aid}"

    # 操作员
    for oid in operators:
        if oid not in users:
            users[oid] = f"操作员 {oid}"

    # 临时操作员
    for tid in temp_operators:
        if tid not in users:
            users[tid] = f"临时操作员 {tid}"

    return users


async def user_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """开始广播 - 选择目标"""
    query = update.callback_query
    user_id = query.from_user.id

    from config import OWNER_ID
    if user_id != OWNER_ID:
        await query.answer("❌ 只有超级管理员可以使用此功能", show_alert=True)
        return

    await query.answer()

    targets = get_target_users()
    context.user_data["ub_targets"] = targets
    context.user_data["ub_selected"] = []

    keyboard = [
        [InlineKeyboardButton(f"📢 发送给所有管理员/操作员 ({len(targets)}人)", callback_data="ub_select_all")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ]

    try:
        await query.message.edit_text(
            f"📢 **用户广播**\n\n"
            f"目标用户：管理员、操作员、临时操作员\n"
            f"共 {len(targets)} 人\n\n"
            f"点击下方按钮开始设置广播内容：",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception:
        pass
    return UB_SELECT_TARGET


async def ub_select_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """选择全部目标，进入消息输入"""
    query = update.callback_query
    await query.answer()

    targets = context.user_data.get("ub_targets", {})
    context.user_data["ub_selected"] = list(targets.keys())
    context.user_data["ub_in_broadcast"] = True

    await query.message.edit_text(
        f"📝 **请输入要广播的内容**\n\n"
        f"🎯 目标：{len(targets)} 人\n\n"
        f"支持：文字、图片、视频、文件\n\n"
        f"💡 输入 /cancel 取消",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ 取消", callback_data="profile_return")
        ]])
    )
    return UB_INPUT_MESSAGE


async def ub_receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """接收广播消息"""
    message = update.message
    user_id = update.effective_user.id

    from config import OWNER_ID
    if user_id != OWNER_ID:
        return ConversationHandler.END

    if message.text and message.text.strip() == "/cancel":
        context.user_data.pop("ub_targets", None)
        context.user_data.pop("ub_selected", None)
        context.user_data.pop("ub_in_broadcast", None)
        await message.reply_text("❌ 已取消广播")
        return ConversationHandler.END

    message_data = {}
    if message.text:
        message_data["type"] = "text"
        message_data["content"] = message.text
    elif message.photo:
        message_data["type"] = "photo"
        message_data["file_id"] = message.photo[-1].file_id
        message_data["caption"] = message.caption or ""
    elif message.video:
        message_data["type"] = "video"
        message_data["file_id"] = message.video.file_id
        message_data["caption"] = message.caption or ""
    elif message.document:
        message_data["type"] = "document"
        message_data["file_id"] = message.document.file_id
        message_data["caption"] = message.caption or ""
        message_data["filename"] = message.document.file_name
    else:
        await message.reply_text("❌ 不支持此消息类型，请发送文字、图片、视频或文件")
        return UB_INPUT_MESSAGE

    context.user_data["ub_message_data"] = message_data
    count = len(context.user_data.get("ub_selected", []))

    keyboard = [
        [InlineKeyboardButton("🚀 确认发送", callback_data="ub_exec_send")],
        [InlineKeyboardButton("✏️ 重新输入", callback_data="ub_reinput")],
        [InlineKeyboardButton("❌ 取消", callback_data="profile_return")],
    ]

    preview = message_data.get("content", message_data.get("caption", ""))[:200]
    await message.reply_text(
        f"📋 **发送预览**\n\n"
        f"类型：{message_data['type']}\n"
        f"内容：{preview}...\n\n"
        f"🎯 目标：{count} 人\n\n"
        f"确认发送？",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return UB_CONFIRM_SEND


async def ub_exec_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行发送"""
    query = update.callback_query
    await query.answer("正在发送...")

    target_ids = context.user_data.get("ub_selected", [])
    message_data = context.user_data.get("ub_message_data", {})

    if not target_ids or not message_data:
        await query.message.edit_text("❌ 数据异常，请重新开始")
        return ConversationHandler.END

    success = 0
    failed = 0

    progress_msg = await query.message.reply_text(f"📤 正在发送... 0/{len(target_ids)}")

    for i, uid in enumerate(target_ids):
        try:
            msg_type = message_data["type"]
            if msg_type == "text":
                await context.bot.send_message(chat_id=uid, text=message_data["content"])
            elif msg_type == "photo":
                await context.bot.send_photo(chat_id=uid, photo=message_data["file_id"], caption=message_data.get("caption", ""))
            elif msg_type == "video":
                await context.bot.send_video(chat_id=uid, video=message_data["file_id"], caption=message_data.get("caption", ""))
            elif msg_type == "document":
                await context.bot.send_document(chat_id=uid, document=message_data["file_id"], caption=message_data.get("caption", ""))
            success += 1
        except Exception as e:
            logger.error(f"广播发送失败 uid={uid}: {e}")
            failed += 1

        if (i + 1) % 10 == 0:
            try:
                await progress_msg.edit_text(f"📤 正在发送... {i+1}/{len(target_ids)}")
            except:
                pass
        await asyncio.sleep(0.1)

    await progress_msg.edit_text(
        f"✅ **广播完成**\n\n"
        f"✅ 成功：{success}\n"
        f"❌ 失败：{failed}\n"
        f"📊 总计：{len(target_ids)}"
    )

    context.user_data.pop("ub_targets", None)
    context.user_data.pop("ub_selected", None)
    context.user_data.pop("ub_message_data", None)
    context.user_data.pop("ub_in_broadcast", None)

    return ConversationHandler.END


async def ub_reinput(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """重新输入"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("ub_message_data", None)

    await query.message.edit_text(
        "📝 **请重新输入广播内容**\n\n"
        "支持：文字、图片、视频、文件",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ 取消", callback_data="profile_return")
        ]])
    )
    return UB_INPUT_MESSAGE


def get_user_broadcast_handler():
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(user_broadcast_start, pattern="^user_broadcast$"),
        ],
        states={
            UB_SELECT_TARGET: [
                CallbackQueryHandler(ub_select_all, pattern="^ub_select_all$"),
            ],
            UB_INPUT_MESSAGE: [
                MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL & ~filters.COMMAND, ub_receive_message),
                CommandHandler("cancel", lambda u, c: ConversationHandler.END),
            ],
            UB_CONFIRM_SEND: [
                CallbackQueryHandler(ub_exec_send, pattern="^ub_exec_send$"),
                CallbackQueryHandler(ub_reinput, pattern="^ub_reinput$"),
            ],
        },
        fallbacks=[],
        per_message=False,
        allow_reentry=True,  # ✅ 加上这个
    )