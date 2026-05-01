# handlers/subscription.py - 会员订阅系统

import time
import uuid
import logging
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from db_manager import get_conn, get_db  # ✅ 添加 get_db 导入
from auth import is_authorized, get_user_admin_id, OWNER_ID, is_admin
from config import OWNER_ID

logger = logging.getLogger(__name__)

BEIJING_TZ = timezone(timedelta(hours=8))

# 状态定义
SUBSCRIPTION_SELECT_PLAN = 1
SUBSCRIPTION_CONFIRM_ORDER = 2
SUBSCRIPTION_MANAGE_MENU = 3
SUBSCRIPTION_ADD_ADDRESS = 4
SUBSCRIPTION_MANAGE_PLANS = 5

# 订单超时时间（秒）
ORDER_TIMEOUT = 10 * 60  # 10分钟


# ==================== 数据库操作 ====================

def get_plans():
    """获取所有启用的套餐"""
    conn = get_conn(0)
    rows = conn.execute(
        "SELECT plan_id, name, price_usdt, duration_days FROM subscription_plans WHERE is_active = 1 ORDER BY price_usdt"
    ).fetchall()
    return [{"plan_id": r[0], "name": r[1], "price_usdt": r[2], "duration_days": r[3]} for r in rows]

def get_all_plans():
    """获取所有套餐（包括禁用的）"""
    conn = get_conn(0)
    rows = conn.execute(
        "SELECT plan_id, name, price_usdt, duration_days, is_active FROM subscription_plans ORDER BY plan_id"
    ).fetchall()
    return [{"plan_id": r[0], "name": r[1], "price_usdt": r[2], "duration_days": r[3], "is_active": r[4]} for r in rows]


def create_order(user_id: int, plan_id: int, amount_usdt: float, address: str) -> str:
    """创建订单，返回订单ID"""
    order_id = f"ORD{int(time.time())}{uuid.uuid4().hex[:6].upper()}"
    now = int(time.time())
    with get_db(0) as conn:
        conn.execute(
            """INSERT INTO payment_orders (order_id, user_id, plan_id, amount_usdt, status, payment_address, created_at, expire_at)
               VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)""",
            (order_id, user_id, plan_id, amount_usdt, address, now, now + ORDER_TIMEOUT)
        )
        # 标记地址为已用
        conn.execute("UPDATE payment_addresses SET status = 'used' WHERE address = ?", (address,))
    return order_id


def get_order(order_id: str) -> dict:
    """获取订单信息"""
    conn = get_conn(0)
    row = conn.execute("SELECT * FROM payment_orders WHERE order_id = ?", (order_id,)).fetchone()
    if row:
        return dict(row)
    return None


def get_pending_order_by_user(user_id: int) -> dict:
    """获取用户待支付的订单（未过期）"""
    conn = get_conn(0)
    now = int(time.time())
    row = conn.execute(
        "SELECT * FROM payment_orders WHERE user_id = ? AND status = 'pending' AND expire_at > ? ORDER BY created_at DESC LIMIT 1",
        (user_id, now)
    ).fetchone()
    if row:
        return dict(row)
    return None


def get_available_address() -> str:
    """获取一个空闲的收款地址"""
    conn = get_conn(0)
    row = conn.execute(
        "SELECT address FROM payment_addresses WHERE status = 'idle' LIMIT 1"
    ).fetchone()
    if row:
        return row[0]
    return None


def activate_subscription(user_id: int, plan_id: int, duration_days: int):
    """开通/续费会员"""
    now = int(time.time())
    with get_db(0) as conn:
        # 检查是否已有会员记录
        existing = conn.execute("SELECT * FROM user_subscriptions WHERE user_id = ?", (user_id,)).fetchone()

        if existing and existing["status"] == "active" and existing["expire_date"] > now:
            # 续费：在现有到期时间基础上延长
            new_expire = existing["expire_date"] + duration_days * 86400
        else:
            # 新开通
            new_expire = now + duration_days * 86400

        conn.execute(
            """INSERT OR REPLACE INTO user_subscriptions (user_id, plan_id, start_date, expire_date, status)
               VALUES (?, ?, ?, ?, 'active')""",
            (user_id, plan_id, now, new_expire)
        )

        # 更新 admins 表
        conn.execute(
            "UPDATE admins SET source = 'subscription', expire_date = ? WHERE admin_id = ?",
            (new_expire, user_id)
        )


def get_user_subscription(user_id: int) -> dict:
    """获取用户会员信息（包括停用状态）"""
    conn = get_conn(0)
    row = conn.execute(
        "SELECT * FROM user_subscriptions WHERE user_id = ? AND status IN ('active', 'suspended')",
        (user_id,)
    ).fetchone()
    if row:
        return dict(row)
    return None


def add_payment_address(address: str, chain_type: str = "TRC20"):
    """添加收款地址到地址池"""
    try:
        with get_db(0) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO payment_addresses (address, chain_type, status, added_at) VALUES (?, ?, 'idle', ?)",
                (address, chain_type, int(time.time()))
            )
        return True
    except:
        return False


def get_all_addresses():
    """获取所有收款地址"""
    conn = get_conn(0)
    rows = conn.execute("SELECT * FROM payment_addresses ORDER BY added_at DESC").fetchall()
    return [dict(r) for r in rows]


def get_all_subscriptions():
    """获取所有会员信息"""
    conn = get_conn(0)
    rows = conn.execute(
        """SELECT u.user_id, u.plan_id, u.start_date, u.expire_date, u.status, p.name as plan_name
           FROM user_subscriptions u
           LEFT JOIN subscription_plans p ON u.plan_id = p.plan_id
           ORDER BY u.expire_date DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


def get_all_orders(limit: int = 50):
    """获取所有订单"""
    conn = get_conn(0)
    rows = conn.execute(
        "SELECT * FROM payment_orders ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def release_expired_orders():
    """释放过期订单的地址"""
    now = int(time.time())
    with get_db(0) as conn:
        expired = conn.execute(
            "SELECT order_id, payment_address FROM payment_orders WHERE status = 'pending' AND expire_at < ?",
            (now,)
        ).fetchall()
        for row in expired:
            conn.execute("UPDATE payment_orders SET status = 'expired' WHERE order_id = ?", (row[0],))
            conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (row[1],))
        return len(expired)


# ==================== 用户端：升级会员 ====================

async def subscription_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击升级/续费会员"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    if user_id == OWNER_ID:
        return await subscription_manage_menu(update, context)

    # ✅ 先检查待支付订单
    pending = get_pending_order_by_user(user_id)
    if pending:
        return await show_pending_order(update, context, pending)

    # 没有待支付订单，显示套餐列表
    return await show_plans(update, context)

async def show_pending_order(update: Update, context: ContextTypes.DEFAULT_TYPE, order: dict):
    """显示未支付的订单"""
    query = update.callback_query
    plans = get_all_plans()
    plan = next((p for p in plans if p["plan_id"] == order["plan_id"]), None)
    plan_name = plan["name"] if plan else "未知"

    expire_time = datetime.fromtimestamp(order["expire_at"], tz=BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
    remain = max(0, order["expire_at"] - int(time.time()))
    remain_min = remain // 60

    keyboard = [
        [InlineKeyboardButton("✅ 我已支付", callback_data=f"sub_check_{order['order_id']}")],
        [InlineKeyboardButton("🔄 重新选择套餐", callback_data="sub_renew")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ]

    try:
        await query.message.edit_text(
            f"⏳ **您有一个待支付订单**\n\n"
            f"📦 订单编号：`{order['order_id']}`\n"
            f"📦 套餐：{plan_name}\n"
            f"💰 金额：**{order['amount_usdt']} USDT**\n"
            f"📌 地址：`{order['payment_address']}`\n"
            f"⛓ 网络：TRC20\n\n"
            f"⏰ 剩余时间：**{remain_min} 分钟**（{expire_time} 截止）\n\n"
            f"💡 支付后点击「我已支付」按钮确认\n"
            f"🔄 如需更换套餐请点击「重新选择套餐」",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception as e:
        # ✅ 忽略 "Message is not modified" 错误
        if "not modified" not in str(e).lower():
            raise

    return SUBSCRIPTION_CONFIRM_ORDER

# handlers/subscription.py - show_plans

async def show_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示套餐列表"""
    query = update.callback_query
    user_id = query.from_user.id
    plans = get_plans()

    if not plans:
        try:
            await query.message.edit_text("📭 暂无可用的会员套餐，请联系管理员。")
        except:
            pass
        return ConversationHandler.END

    # 获取当前会员信息
    sub = get_user_subscription(user_id)

    if sub:
        expire_date = datetime.fromtimestamp(sub["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
        remaining_days = max(0, (sub["expire_date"] - int(time.time())) // 86400)
        header = f"💳 **续费会员**\n\n当前到期：{expire_date}（剩余 {remaining_days} 天）\n\n选择套餐进行续费：\n\n"
    else:
        header = ("⭐ **升级会员**\n\n"
                  "选择套餐后，您将获得：\n"
                  "• 独立数据库，数据隔离\n"
                  "• 管理群组和操作员\n"
                  "• 全部功能使用权限\n\n"
                  "请选择套餐：\n\n")

    keyboard = []
    for plan in plans:
        keyboard.append([
            InlineKeyboardButton(
                f"{plan['name']} - {plan['price_usdt']} USDT / {plan['duration_days']}天",
                callback_data=f"sub_plan_{plan['plan_id']}"
            )
        ])

    # ✅ 如果有待支付订单，添加查看按钮
    pending = get_pending_order_by_user(user_id)
    bottom_row = []
    if pending:
        bottom_row.append(InlineKeyboardButton("📋 查看待支付订单", callback_data="subscription_menu"))
    bottom_row.append(InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return"))
    keyboard.append(bottom_row)

    try:
        await query.message.edit_text(
            header,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception as e:
        if "not modified" not in str(e).lower():
            raise

    return SUBSCRIPTION_SELECT_PLAN

async def select_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """选择套餐，生成订单"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    plan_id = int(query.data.replace("sub_plan_", ""))
    plans = get_plans()
    plan = next((p for p in plans if p["plan_id"] == plan_id), None)

    if not plan:
        await query.message.edit_text("❌ 套餐不存在")
        return ConversationHandler.END

    # ✅ 检查是否有未过期订单，如果有就复用
    pending = get_pending_order_by_user(user_id)
    if pending:
        # 如果已经选择了同一个套餐，直接显示待支付页面
        if pending["plan_id"] == plan_id:
            return await show_pending_order(update, context, pending)
        # 不同套餐，取消旧订单，生成新订单
        with get_db(0) as conn:
            conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (pending["payment_address"],))
            conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (pending["order_id"],))

    address = get_available_address()

    if not address:
        await query.message.edit_text("😔 支付通道繁忙，请稍后重试。")
        return ConversationHandler.END

    order_id = create_order(user_id, plan_id, plan["price_usdt"], address)
    expire_time = datetime.fromtimestamp(int(time.time()) + ORDER_TIMEOUT, tz=BEIJING_TZ).strftime('%H:%M:%S')

    keyboard = [
        [InlineKeyboardButton("✅ 我已支付", callback_data=f"sub_check_{order_id}")],
        [InlineKeyboardButton("🔄 重新选择套餐", callback_data="sub_renew")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ]

    await query.message.edit_text(
        f"💳 **订单已生成**\n\n"
        f"📦 订单编号：`{order_id}`\n"
        f"📦 套餐：{plan['name']}\n"
        f"💰 金额：**{plan['price_usdt']} USDT**\n"
        f"📌 地址：`{address}`\n"
        f"⛓ 网络：TRC20\n\n"
        f"⏰ 请在 **{expire_time}** 前完成支付\n\n"
        f"💡 支付后点击「我已支付」\n"
        f"💡 退出后可再次进入「升级会员」查看订单",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return SUBSCRIPTION_CONFIRM_ORDER

# handlers/subscription.py - check_payment 函数

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """用户点击"我已支付"，手动检查"""
    query = update.callback_query
    user_id = query.from_user.id

    order_id = query.data.replace("sub_check_", "")
    logger.info(f"check_payment: 开始检测订单 {order_id}")

    order = get_order(order_id)

    if not order:
        logger.info(f"check_payment: 订单 {order_id} 不存在")
        await query.answer("❌ 订单不存在", show_alert=True)
        return

    if order["status"] == "paid":
        logger.info(f"check_payment: 订单 {order_id} 已支付")
        # ✅ 直接跳转到会员状态页面
        sub = get_user_subscription(user_id)
        if sub:
            return await show_renew_menu(update, context, sub)
        await query.answer("✅ 支付成功！会员已开通。", show_alert=True)
        return

    if int(time.time()) > order["expire_at"]:
        logger.info(f"check_payment: 订单 {order_id} 已超时")
        await query.answer("⏰ 订单已超时，请重新下单。", show_alert=True)
        with get_db(0) as conn:
            conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (order["payment_address"],))
            conn.execute("UPDATE payment_orders SET status = 'expired' WHERE order_id = ?", (order_id,))
        return

    # 显示检测中
    await query.answer("🔍 正在检测链上支付，请稍候...", show_alert=True)

    from handlers.monitor import get_trc20_transactions
    txs = await get_trc20_transactions(order["payment_address"], min_timestamp=(order["created_at"] - 300) * 1000, limit=10)
    logger.info(f"check_payment: 查到 {len(txs)} 笔交易")

    found = False
    for tx in txs:
        to_addr = tx.get("to", "")
        raw_amount = tx.get("value", 0)
        amount = int(raw_amount) / 1_000_000 if raw_amount else 0
        tx_id = tx.get("transaction_id", "")

        if to_addr == order["payment_address"] and amount >= order["amount_usdt"]:
            logger.info(f"check_payment: 找到匹配交易 {tx_id}, 金额 {amount}")

            with get_db(0) as conn:
                now = int(time.time())

                existing = conn.execute(
                    "SELECT order_id FROM payment_orders WHERE tx_id = ? AND status = 'paid'",
                    (tx_id,)
                ).fetchone()
                if existing:
                    continue

                conn.execute(
                    "UPDATE payment_orders SET status = 'paid', tx_id = ?, paid_at = ? WHERE order_id = ?",
                    (tx_id, now, order_id)
                )

                conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (order["payment_address"],))

                plan = conn.execute("SELECT * FROM subscription_plans WHERE plan_id = ?", (order["plan_id"],)).fetchone()
                if plan:
                    conn.execute(
                        "UPDATE user_subscriptions SET status = 'active' WHERE user_id = ? AND status = 'suspended'",
                        (order["user_id"],)
                    )

                    existing_sub = conn.execute("SELECT * FROM user_subscriptions WHERE user_id = ?", (order["user_id"],)).fetchone()
                    if existing_sub and existing_sub["status"] == "active" and existing_sub["expire_date"] > now:
                        new_expire = existing_sub["expire_date"] + plan["duration_days"] * 86400
                    else:
                        new_expire = now + plan["duration_days"] * 86400

                    conn.execute(
                        """INSERT OR REPLACE INTO user_subscriptions (user_id, plan_id, start_date, expire_date, status)
                           VALUES (?, ?, ?, ?, 'active')""",
                        (order["user_id"], order["plan_id"], now, new_expire)
                    )

                    conn.execute(
                        "UPDATE admins SET source = 'subscription', expire_date = ? WHERE admin_id = ?",
                        (new_expire, order["user_id"])
                    )

                    all_pending = conn.execute(
                        "SELECT order_id, payment_address FROM payment_orders WHERE user_id = ? AND status = 'pending' AND order_id != ?",
                        (order["user_id"], order_id)
                    ).fetchall()
                    for po in all_pending:
                        conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (po["payment_address"],))
                        conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (po["order_id"],))

            from auth import add_admin, load_admins_from_db
            add_admin(order["user_id"])
            load_admins_from_db()

            found = True
            break  # ✅ 找到匹配就退出循环

    if found:
        # ✅ 支付成功，跳转到会员状态页面
        sub = get_user_subscription(user_id)
        if sub:
            await query.answer("✅ 支付成功！", show_alert=True)
            return await show_renew_menu(update, context, sub)
        else:
            await query.message.edit_text(
                f"🎉 **支付成功！**\n\n"
                f"会员已开通，有效期 {plan['duration_days']} 天。\n"
                f"您现在可以使用机器人的全部功能了！",
                parse_mode="Markdown"
            )
    else:
        logger.info(f"check_payment: 未找到匹配交易")
        try:
            await query.message.edit_text(
                f"⌛ **暂未检测到支付**\n\n"
                f"📦 订单：`{order_id}`\n"
                f"📌 地址：`{order['payment_address']}`\n"
                f"💰 金额：{order['amount_usdt']} USDT\n\n"
                f"请确认已转账后，稍等片刻重新检测\n"
                f"或联系管理员手动确认。",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 再次检测", callback_data=f"sub_check_{order_id}"),
                    InlineKeyboardButton("◀️ 返回", callback_data="subscription_menu"),
                    InlineKeyboardButton("🔄 重新选择套餐", callback_data="sub_renew"),
                ]]),
                parse_mode="Markdown"
            )
        except Exception:
            pass

async def cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """取消订单"""
    query = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    with get_db(0) as conn:
        pending = conn.execute(
            "SELECT * FROM payment_orders WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
            (user_id,)
        ).fetchone()

        if pending:
            conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (pending["payment_address"],))
            conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (pending["order_id"],))

    await query.message.edit_text("✅ 订单已取消。")
    return ConversationHandler.END

async def show_renew_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, sub: dict = None):
    """显示续费菜单"""
    query = update.callback_query
    user_id = query.from_user.id

    if not sub:
        sub = get_user_subscription(user_id)

    expire_date = datetime.fromtimestamp(sub["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
    now = int(time.time())
    remaining_days = max(0, (sub["expire_date"] - now) // 86400)

    keyboard = [
        [InlineKeyboardButton("💳 续费会员", callback_data="sub_renew")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ]

    await query.message.edit_text(
        f"💎 **会员状态**\n\n"
        f"📅 到期时间：{expire_date}\n"
        f"⏳ 剩余天数：**{remaining_days} 天**\n\n"
        f"到期后数据保留 7 天，请及时续费。",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


# ==================== 管理端 ====================

async def subscription_manage_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """超级管理员 → 会员管理菜单"""
    query = update.callback_query
    await query.answer()

    keyboard = [
        [InlineKeyboardButton("👥 付费用户管理", callback_data="sub_manage_users")],
        [InlineKeyboardButton("📋 订单管理", callback_data="sub_manage_orders")],
        [InlineKeyboardButton("📦 套餐管理", callback_data="sub_manage_plans")],
        [InlineKeyboardButton("🏦 地址池管理", callback_data="sub_manage_addresses")],
        [InlineKeyboardButton("◀️ 返回个人中心", callback_data="profile_return")],
    ]

    await query.message.edit_text(
        "💰 **会员管理**\n\n请选择管理功能：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    return SUBSCRIPTION_MANAGE_MENU


async def manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """付费用户列表"""
    query = update.callback_query
    await query.answer()

    filter_type = context.user_data.get('sub_filter', 'active')

    subs = get_all_subscriptions_full()

    # 筛选
    if filter_type == 'active':
        subs = [s for s in subs if s['status'] in ('active', 'suspended')]
    elif filter_type == 'expired':
        now = int(time.time())
        subs = [
            s for s in subs 
            if s['status'] in ('expired', 'cancelled')
            and s.get('deleted_at', 0) > 0 
            and (now - s['deleted_at']) <= 7 * 86400
        ]

    filter_names = {'active': '正常会员', 'expired': '已过期会员（7天内）', 'all': '全部会员'}

    if subs:
        text = f"👥 **{filter_names.get(filter_type, '全部会员')}**（共 {len(subs)} 人）\n\n"

        now = int(time.time())
        for s in subs:
            expire_date = datetime.fromtimestamp(s["expire_date"], tz=BEIJING_TZ).strftime('%Y-%m-%d')
            remaining = max(0, (s["expire_date"] - now) // 86400)

            if s["status"] == "active" and remaining > 0:
                status = f"✅ 正常（剩余 {remaining} 天）"
            elif s["status"] == "suspended":
                status = f"⏸️ 已停用（剩余 {remaining} 天）"
            elif s["status"] == "cancelled":
                status = "🚫 已取消"
            elif s["status"] == "expired":
                deleted_at = s.get("deleted_at", 0)
                if deleted_at > 0:
                    retain_remaining = max(0, 7 - (now - deleted_at) // 86400)
                    status = f"❌ 已过期（{retain_remaining} 天后删除）"
                else:
                    status = "❌ 已过期"
            else:
                status = f"❓ {s['status']}"

            text += f"• 用户: `{s['user_id']}` | 套餐ID: `{s['plan_id']}` | {s.get('plan_name', '未知')}\n"
            text += f"   📅 到期: {expire_date} | {status}\n\n"
            if len(text) > 3500:
                text += "... 仅显示前部分"
                break
    else:
        text = f"📭 暂无{filter_names.get(filter_type, '')}用户\n\n"

    text += "━━━━━━━━━━━━━━━━\n"
    text += "💡 **用户管理命令**：\n"
    text += "`/addsub 用户ID 套餐ID` - 开通会员\n"
    text += "`/delsub 用户ID` - 取消会员\n"
    text += "`/extendsub 用户ID 天数` - 延长会员\n"
    text += "`/togglesub 用户ID` - 停用/启用会员"

    keyboard = []
    filter_buttons = []
    filter_configs = [
        ("✅ 正常", "sub_filter_active"),
        ("❌ 已过期", "sub_filter_expired"),
        ("📋 全部", "sub_filter_all"),
    ]
    for name, cb in filter_configs:
        prefix = "🔹 " if cb == f"sub_filter_{filter_type}" else ""
        filter_buttons.append(InlineKeyboardButton(f"{prefix}{name}", callback_data=cb))
    keyboard.append(filter_buttons)
    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="subscription_manage")])

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


def get_all_subscriptions_full():
    """获取所有会员信息（包括过期的）"""
    with get_db(0) as conn:
        now = int(time.time())
        rows = conn.execute(
            """SELECT u.user_id, u.plan_id, u.start_date, u.expire_date, u.status, p.name as plan_name,
                      a.deleted_at
               FROM user_subscriptions u
               LEFT JOIN subscription_plans p ON u.plan_id = p.plan_id
               LEFT JOIN admins a ON u.user_id = a.admin_id
               ORDER BY u.expire_date DESC"""
        ).fetchall()

        result = []
        for r in rows:
            sub = dict(r)
            # 自动标记过期
            if sub["status"] == "active" and sub["expire_date"] <= now:
                conn.execute("UPDATE user_subscriptions SET status = 'expired' WHERE user_id = ?", (sub["user_id"],))
                if not sub["deleted_at"] or sub["deleted_at"] == 0:
                    conn.execute("UPDATE admins SET deleted_at = ? WHERE admin_id = ?", (now, sub["user_id"]))
                sub["status"] = "expired"
                sub["deleted_at"] = now

            if sub.get("deleted_at") is None or sub["deleted_at"] == 0:
                sub["deleted_at"] = 0

            result.append(sub)

    return result

async def sub_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """付费用户筛选"""
    query = update.callback_query
    await query.answer()
    filter_type = query.data.replace("sub_filter_", "")
    context.user_data['sub_filter'] = filter_type
    await manage_users(update, context)

async def cmd_add_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动添加会员：/addsub 用户ID 套餐ID"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text("用法：/addsub 用户ID 套餐ID\n例如：/addsub 123456789 1")
        return

    try:
        target_uid = int(args[0])
        plan_id = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ 用户ID和套餐ID必须是数字")
        return

    conn = get_conn(0)
    plan = conn.execute("SELECT * FROM subscription_plans WHERE plan_id = ?", (plan_id,)).fetchone()
    if not plan:
        await update.message.reply_text("❌ 套餐不存在")
        return

    activate_subscription(target_uid, plan_id, plan["duration_days"])
    from auth import add_admin
    add_admin(target_uid)

    await update.message.reply_text(f"✅ 已为用户 {target_uid} 开通 {plan['name']}，有效期 {plan['duration_days']} 天")


async def cmd_remove_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """删除会员：/delsub 用户ID"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/delsub 用户ID")
        return

    try:
        target_uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
        return

    now = int(time.time())
    with get_db(0) as conn:
        conn.execute("UPDATE user_subscriptions SET status = 'cancelled' WHERE user_id = ?", (target_uid,))
        conn.execute("UPDATE admins SET deleted_at = ? WHERE admin_id = ?", (now, target_uid))

    from auth import admins, operators, temp_operators
    admins.pop(target_uid, None)
    to_remove_ops = [uid for uid, info in operators.items() if info.get("added_by") == target_uid]
    for uid in to_remove_ops:
        operators.pop(uid, None)
    to_remove_temps = [uid for uid, info in temp_operators.items() if info.get("added_by") == target_uid]
    for uid in to_remove_temps:
        temp_operators.pop(uid, None)

    await update.message.reply_text(f"✅ 已取消用户 {target_uid} 的会员，数据保留 7 天")

async def cmd_extend_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """延长会员：/extendsub 用户ID 天数"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if len(args) < 2:
        await update.message.reply_text("用法：/extendsub 用户ID 延长的天数\n例如：/extendsub 123456789 30")
        return

    try:
        target_uid = int(args[0])
        days = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ 用户ID和天数必须是数字")
        return

    with get_db(0) as conn:
        sub = conn.execute("SELECT * FROM user_subscriptions WHERE user_id = ? AND status = 'active'", (target_uid,)).fetchone()
        if not sub:
            await update.message.reply_text("❌ 该用户没有活跃的会员")
            return

        new_expire = sub["expire_date"] + days * 86400
        conn.execute("UPDATE user_subscriptions SET expire_date = ?, status = 'active' WHERE user_id = ?", (new_expire, target_uid))
        conn.execute("UPDATE admins SET expire_date = ?, deleted_at = 0 WHERE admin_id = ?", (new_expire, target_uid))

    await update.message.reply_text(f"✅ 已延长用户 {target_uid} 的会员 {days} 天")


async def cmd_toggle_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """停用/启用会员：/togglesub 用户ID"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/togglesub 用户ID")
        return

    try:
        target_uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ 用户ID必须是数字")
        return

    with get_db(0) as conn:
        sub = conn.execute("SELECT * FROM user_subscriptions WHERE user_id = ?", (target_uid,)).fetchone()
        if not sub:
            await update.message.reply_text("❌ 该用户没有会员记录")
            return

        now = int(time.time())

        if sub["status"] == "active":
            remaining = max(0, sub["expire_date"] - now)
            conn.execute(
                "UPDATE user_subscriptions SET status = 'suspended', expire_date = ? WHERE user_id = ?",
                (now + remaining, target_uid)
            )
            conn.execute("UPDATE admins SET deleted_at = ? WHERE admin_id = ?", (now, target_uid))
            from auth import admins
            admins.pop(target_uid, None)
            await update.message.reply_text(f"✅ 已停用用户 {target_uid} 的会员（剩余 {remaining // 86400} 天）")
        elif sub["status"] == "suspended":
            remaining = max(0, sub["expire_date"] - now)
            conn.execute("UPDATE user_subscriptions SET status = 'active' WHERE user_id = ?", (target_uid,))
            conn.execute("UPDATE admins SET deleted_at = 0, expire_date = ? WHERE admin_id = ?", (now + remaining, target_uid))
            from auth import load_admins_from_db
            load_admins_from_db()
            await update.message.reply_text(f"✅ 已启用用户 {target_uid} 的会员（剩余 {remaining // 86400} 天）")
        else:
            await update.message.reply_text(f"❌ 用户 {target_uid} 的会员已过期，无法操作")
            return

async def manage_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """订单管理"""
    query = update.callback_query
    await query.answer()

    filter_type = context.user_data.get('order_filter', 'all')

    orders = get_all_orders(200)

    if filter_type == 'pending':
        orders = [o for o in orders if o['status'] == 'pending']
    elif filter_type == 'paid':
        orders = [o for o in orders if o['status'] == 'paid']
    elif filter_type == 'expired':
        orders = [o for o in orders if o['status'] in ('expired', 'cancelled')]

    if not orders:
        keyboard = [
            [
                InlineKeyboardButton("⏳ 待支付", callback_data="order_filter_pending"),
                InlineKeyboardButton("✅ 已支付", callback_data="order_filter_paid"),
                InlineKeyboardButton("❌ 已取消", callback_data="order_filter_expired"),
                InlineKeyboardButton("📋 全部", callback_data="order_filter_all"),
            ],
            [InlineKeyboardButton("◀️ 返回", callback_data="subscription_manage")]
        ]
        await query.message.edit_text(
            f"📭 暂无{_get_filter_name(filter_type)}订单",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    page = context.user_data.get('order_page', 0)
    items_per_page = 5
    total_pages = (len(orders) + items_per_page - 1) // items_per_page
    start = page * items_per_page
    end = min(start + items_per_page, len(orders))
    page_orders = orders[start:end]

    text = f"📋 **{_get_filter_name(filter_type)}订单**（第 {page+1}/{total_pages} 页）\n"
    text += f"共 {len(orders)} 条\n\n"

    for o in page_orders:
        status_map = {"pending": "⏳ 待支付", "paid": "✅ 已支付", "expired": "❌ 已过期", "cancelled": "🚫 已取消"}
        status_text = status_map.get(o["status"], "❓ 未知")

        created = datetime.fromtimestamp(o["created_at"], tz=BEIJING_TZ).strftime('%m-%d %H:%M')

        text += f"📦 `{o['order_id']}`\n"
        text += f"   👤 用户: `{o['user_id']}` | 💰 {o['amount_usdt']} USDT\n"
        text += f"   📅 {created} | {status_text}\n\n"

    keyboard = []

    filter_buttons = []
    filter_names = [
        ("⏳ 待支付", "order_filter_pending"),
        ("✅ 已支付", "order_filter_paid"),
        ("❌ 已取消", "order_filter_expired"),
        ("📋 全部", "order_filter_all"),
    ]
    for name, cb in filter_names:
        prefix = "🔹 " if cb == f"order_filter_{filter_type}" else ""
        filter_buttons.append(InlineKeyboardButton(f"{prefix}{name}", callback_data=cb))
    keyboard.append(filter_buttons)

    for o in page_orders:
        if o["status"] == "pending":
            short_id = o['order_id'][-10:]
            keyboard.append([
                InlineKeyboardButton(f"✅ 确认支付 ID:{short_id}", callback_data=f"sub_confirm_{o['order_id']}"),
            ])
            keyboard.append([
                InlineKeyboardButton(f"❌ 取消订单 ID:{short_id}", callback_data=f"sub_cancel_admin_{o['order_id']}"),
            ])

    expired_count = len([o for o in orders if o['status'] in ('expired', 'cancelled')])
    if expired_count > 0:
        keyboard.append([
            InlineKeyboardButton(f"🗑️ 删除已取消/过期订单 ({expired_count}条)", callback_data="sub_delete_expired"),
        ])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ 上一页", callback_data="order_page_prev"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("下一页 ➡️", callback_data="order_page_next"))
    if nav:
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("◀️ 返回", callback_data="subscription_manage")])

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


def _get_filter_name(filter_type: str) -> str:
    names = {
        'all': '全部',
        'pending': '待支付',
        'paid': '已支付',
        'expired': '已取消/过期',
    }
    return names.get(filter_type, '全部')

async def order_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """订单筛选"""
    query = update.callback_query
    await query.answer()
    filter_type = query.data.replace("order_filter_", "")
    context.user_data['order_filter'] = filter_type
    context.user_data['order_page'] = 0
    await manage_orders(update, context)


async def delete_expired_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """删除已取消/过期的订单"""
    query = update.callback_query
    user_id = query.from_user.id

    if user_id != OWNER_ID:
        await query.answer("❌ 无权限", show_alert=True)
        return

    await query.answer()
    with get_db(0) as conn:
        conn.execute("DELETE FROM payment_orders WHERE status IN ('expired', 'cancelled')")

    await query.answer("✅ 已清理过期和取消的订单", show_alert=True)
    await manage_orders(update, context)

async def manual_confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员手动确认支付"""
    query = update.callback_query
    user_id = query.from_user.id

    if user_id != OWNER_ID:
        await query.answer("❌ 只有超级管理员可以操作", show_alert=True)
        return

    await query.answer()
    order_id = query.data.replace("sub_confirm_", "")

    with get_db(0) as conn:
        order = conn.execute("SELECT * FROM payment_orders WHERE order_id = ?", (order_id,)).fetchone()

        if not order:
            await query.message.edit_text("❌ 订单不存在")
            return

        now = int(time.time())

        # 更新订单状态
        conn.execute(
            "UPDATE payment_orders SET status = 'paid', paid_at = ? WHERE order_id = ?",
            (now, order_id)
        )

        # ✅ 释放地址回空闲状态
        conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (order["payment_address"],))

        plan = conn.execute("SELECT * FROM subscription_plans WHERE plan_id = ?", (order["plan_id"],)).fetchone()

        if plan:
            # 恢复停用状态
            conn.execute(
                "UPDATE user_subscriptions SET status = 'active' WHERE user_id = ? AND status = 'suspended'",
                (order["user_id"],)
            )

            # 开通/续费会员
            existing_sub = conn.execute("SELECT * FROM user_subscriptions WHERE user_id = ?", (order["user_id"],)).fetchone()
            if existing_sub and existing_sub["status"] == "active" and existing_sub["expire_date"] > now:
                new_expire = existing_sub["expire_date"] + plan["duration_days"] * 86400
            else:
                new_expire = now + plan["duration_days"] * 86400

            conn.execute(
                """INSERT OR REPLACE INTO user_subscriptions (user_id, plan_id, start_date, expire_date, status)
                   VALUES (?, ?, ?, ?, 'active')""",
                (order["user_id"], order["plan_id"], now, new_expire)
            )

            conn.execute(
                "UPDATE admins SET source = 'subscription', expire_date = ? WHERE admin_id = ?",
                (new_expire, order["user_id"])
            )

            # ✅ 释放该用户所有其他待支付订单的地址
            all_pending = conn.execute(
                "SELECT order_id, payment_address FROM payment_orders WHERE user_id = ? AND status = 'pending' AND order_id != ?",
                (order["user_id"], order_id)
            ).fetchall()
            for po in all_pending:
                conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (po["payment_address"],))
                conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (po["order_id"],))

    # 在 with 块外部调用
    from auth import add_admin, load_admins_from_db
    add_admin(order["user_id"])
    load_admins_from_db()

    await query.message.edit_text(
        f"✅ 已手动确认支付\n\n"
        f"📦 订单：`{order_id}`\n"
        f"👤 用户：{order['user_id']}\n"
        f"💰 金额：{order['amount_usdt']} USDT\n\n"
        f"会员已开通。",
        parse_mode="Markdown"
    )

async def manage_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """套餐管理"""
    query = update.callback_query
    await query.answer()

    plans = get_all_plans()

    text = "📦 **套餐管理**\n\n"
    if plans:
        for p in plans:
            status = "✅" if p["is_active"] else "❌"
            text += f"{status} ID: `{p['plan_id']}` | {p['name']}：{p['price_usdt']} USDT / {p['duration_days']}天\n\n"
    else:
        text += "📭 暂无套餐\n"

    text += "💡 使用命令管理套餐：\n"
    text += "`/addplan 名称 价格 天数` - 添加套餐\n"
    text += "`/editplan 套餐ID 名称 价格 天数` - 编辑套餐\n"
    text += "`/delplan 套餐ID` - 删除套餐\n"
    text += "`/toggleplan 套餐ID` - 启用/禁用套餐"

    keyboard = [[InlineKeyboardButton("◀️ 返回", callback_data="subscription_manage")]]

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def admin_cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员手动取消订单"""
    query = update.callback_query
    user_id = query.from_user.id

    if user_id != OWNER_ID:
        await query.answer("❌ 只有超级管理员可以操作", show_alert=True)
        return

    await query.answer()
    order_id = query.data.replace("sub_cancel_admin_", "")

    with get_db(0) as conn:
        order = conn.execute("SELECT * FROM payment_orders WHERE order_id = ?", (order_id,)).fetchone()

        if not order:
            await query.answer("订单不存在", show_alert=True)
            return

        conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (order["payment_address"],))
        conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (order_id,))

    await manage_orders(update, context)

async def cmd_add_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加套餐：/addplan 名称 价格 天数"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text("用法：/addplan 名称 价格(USDT) 天数\n例如：/addplan 月度 10 30")
        return

    try:
        name = args[0]
        price = float(args[1])
        days = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ 价格和天数必须是数字")
        return

    with get_db(0) as conn:
        conn.execute(
            "INSERT INTO subscription_plans (name, price_usdt, duration_days, created_at) VALUES (?, ?, ?, ?)",
            (name, price, days, int(time.time()))
        )

    await update.message.reply_text(f"✅ 已添加套餐：{name} - {price} USDT / {days}天")

async def cmd_edit_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """编辑套餐：/editplan 套餐ID 名称 价格 天数"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if len(args) < 4:
        await update.message.reply_text("用法：/editplan 套餐ID 名称 价格(USDT) 天数\n例如：/editplan 1 月度会员 15 30")
        return

    try:
        plan_id = int(args[0])
        name = args[1]
        price = float(args[2])
        days = int(args[3])
    except ValueError:
        await update.message.reply_text("❌ 套餐ID、价格和天数必须是数字")
        return

    with get_db(0) as conn:
        existing = conn.execute("SELECT * FROM subscription_plans WHERE plan_id = ?", (plan_id,)).fetchone()
        if not existing:
            await update.message.reply_text("❌ 套餐不存在")
            return

        conn.execute(
            "UPDATE subscription_plans SET name = ?, price_usdt = ?, duration_days = ? WHERE plan_id = ?",
            (name, price, days, plan_id)
        )

    await update.message.reply_text(f"✅ 已更新套餐 {plan_id}：{name} - {price} USDT / {days}天")

async def cmd_del_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """删除套餐：/delplan 套餐ID"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/delplan 套餐ID")
        return

    try:
        plan_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ 套餐ID必须是数字")
        return

    with get_db(0) as conn:
        conn.execute("DELETE FROM subscription_plans WHERE plan_id = ?", (plan_id,))

    await update.message.reply_text(f"✅ 已删除套餐 ID: {plan_id}")


async def cmd_toggle_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """启用/禁用套餐：/toggleplan 套餐ID"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/toggleplan 套餐ID")
        return

    try:
        plan_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ 套餐ID必须是数字")
        return

    with get_db(0) as conn:
        row = conn.execute("SELECT is_active FROM subscription_plans WHERE plan_id = ?", (plan_id,)).fetchone()
        if not row:
            await update.message.reply_text("❌ 套餐不存在")
            return

        new_state = 0 if row[0] else 1
        conn.execute("UPDATE subscription_plans SET is_active = ? WHERE plan_id = ?", (new_state, plan_id))

    status = "启用" if new_state else "禁用"
    await update.message.reply_text(f"✅ 套餐 {plan_id} 已{status}")

async def manage_addresses(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """地址池管理"""
    query = update.callback_query
    await query.answer()

    addresses = get_all_addresses()

    text = "🏦 **收款地址池**\n\n"
    idle_count = 0
    for a in addresses:
        status = "🟢 空闲" if a["status"] == "idle" else "🔴 使用中"
        if a["status"] == "idle":
            idle_count += 1
        text += f"• `{a['address']}` - {status}\n"

    text += f"\n空闲地址：{idle_count} 个\n\n"
    text += "💡 使用命令管理地址：\n"
    text += "`/addaddr TRC20地址` - 添加收款地址\n"
    text += "`/deladdr TRC20地址` - 删除收款地址"

    keyboard = [[InlineKeyboardButton("◀️ 返回", callback_data="subscription_manage")]]

    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def cmd_add_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """添加收款地址：/addaddr TRC20地址"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/addaddr TRC20地址")
        return

    address = args[0]
    if not (address.startswith('T') and len(address) == 34):
        await update.message.reply_text("❌ 地址格式不正确（T开头，34位）")
        return

    if add_payment_address(address):
        await update.message.reply_text(f"✅ 已添加收款地址：`{address}`", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ 添加失败，地址可能已存在")


async def cmd_del_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """删除收款地址：/deladdr TRC20地址"""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    args = context.args
    if not args:
        await update.message.reply_text("用法：/deladdr TRC20地址")
        return

    address = args[0]
    with get_db(0) as conn:
        conn.execute("DELETE FROM payment_addresses WHERE address = ?", (address,))

    await update.message.reply_text(f"✅ 已删除收款地址：`{address}`", parse_mode="Markdown")


# ==================== 定时任务：支付检测 ====================
async def check_subscription_payments(app):
    """检查待支付订单是否到账"""
    from handlers.monitor import get_trc20_transactions
    from auth import add_admin

    with get_db(0) as conn:
        now = int(time.time())

        orders = conn.execute(
            "SELECT * FROM payment_orders WHERE status = 'pending' AND expire_at > ?",
            (now,)
        ).fetchall()

        for order_dict in orders:
            order = dict(order_dict)
            address = order["payment_address"]

            txs = await get_trc20_transactions(address, min_timestamp=(order["created_at"] - 300) * 1000, limit=10)

            for tx in txs:
                to_addr = tx.get("to", "")
                raw_amount = tx.get("value", 0)
                amount = int(raw_amount) / 1_000_000 if raw_amount else 0
                tx_id = tx.get("transaction_id", "")

                if to_addr == address and amount >= order["amount_usdt"]:
                    existing = conn.execute(
                        "SELECT order_id FROM payment_orders WHERE tx_id = ? AND status = 'paid'",
                        (tx_id,)
                    ).fetchone()

                    if existing:
                        continue

                    conn.execute(
                        "UPDATE payment_orders SET status = 'paid', tx_id = ?, paid_at = ? WHERE order_id = ?",
                        (tx_id, now, order["order_id"])
                    )

                    plan = conn.execute(
                        "SELECT * FROM subscription_plans WHERE plan_id = ?", (order["plan_id"],)
                    ).fetchone()

                    if plan:
                        conn.execute(
                            "UPDATE user_subscriptions SET status = 'active' WHERE user_id = ? AND status = 'suspended'",
                            (order["user_id"],)
                        )
                        activate_subscription(order["user_id"], order["plan_id"], plan["duration_days"])
                        from auth import add_admin, load_admins_from_db
                        add_admin(order["user_id"])
                        load_admins_from_db()

                    # 释放该用户所有待支付订单的地址
                    all_pending = conn.execute(
                        "SELECT order_id, payment_address FROM payment_orders WHERE user_id = ? AND status = 'pending'",
                        (order["user_id"],)
                    ).fetchall()
                    for po in all_pending:
                        conn.execute("UPDATE payment_addresses SET status = 'idle' WHERE address = ?", (po["payment_address"],))
                        conn.execute("UPDATE payment_orders SET status = 'cancelled' WHERE order_id = ?", (po["order_id"],))

                    try:
                        await app.bot.send_message(
                            chat_id=order["user_id"],
                            text=f"🎉 **支付成功！**\n\n"
                                 f"会员已开通，有效期 {plan['duration_days']} 天。\n"
                                 f"您现在可以使用机器人的全部功能了！",
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.error(f"通知用户 {order['user_id']} 支付成功失败: {e}")

                    break

    released = release_expired_orders()
    if released > 0:
        logger.info(f"释放了 {released} 个过期订单的地址")