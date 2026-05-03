# handlers/data_provider.py - 适配物理隔离

import sqlite3
import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from db_manager import get_conn as get_admin_conn
from handlers.accounting import get_accounting_manager
from auth import list_operators, OWNER_ID, get_user_admin_id, temp_operators

# 北京时间时区
BEIJING_TZ = timezone(timedelta(hours=8))

def beijing_now():
    return datetime.now(BEIJING_TZ)

def timestamp_to_beijing_str(ts: int) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')

def timestamp_to_date(ts: int) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=BEIJING_TZ).strftime('%Y-%m-%d')


class DataProvider:
    def __init__(self):
        pass

    # ---------- 内部工具 ----------
    def _get_admin_conn(self, admin_id: int):
        """获取指定管理员的独立数据库连接"""
        return get_admin_conn(admin_id)

    def _get_visible_groups(self, admin_id: int, user_id: int = None) -> List[Dict]:
        # ✅ 新增：严格校验 admin_id
        if admin_id is None or admin_id <= 0:
            print(f"[DataProvider] 警告：无效的 admin_id={admin_id}，返回空列表")
            return []

        import time
        import random

        # ✅ 新增：重试机制，防止数据库被锁
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self._get_admin_conn(admin_id)
                rows = conn.execute("SELECT group_id, title, last_seen, category, joined_at, admin_id FROM groups").fetchall()
                groups = []
                for row in rows:
                    groups.append({
                        "id": row["group_id"],
                        "title": row["title"],
                        "last_seen": row["last_seen"],
                        "category": row["category"],
                        "joined_at": row["joined_at"] or row["last_seen"] or 0,
                        "admin_id": row["admin_id"] or 0
                    })
                return groups
            except Exception as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    time.sleep(random.uniform(0.1, 0.3))
                    continue
                print(f"[DataProvider] 获取群组列表失败 (admin_id={admin_id}): {e}")
                return []
        return []

    # ==================== 1. 群组相关数据 ====================
    def get_all_groups(self, admin_id: int, user_id: int = None, limit: int = 100) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        return {
            "total": len(groups),
            "groups": [
                {
                    "id": g['id'],
                    "name": g['title'],
                    "category": g.get('category', '未分类'),
                    "joined_at": timestamp_to_beijing_str(g.get('joined_at', 0)),
                    "last_seen": timestamp_to_beijing_str(g.get('last_seen', 0))
                } for g in groups[:limit]
            ]
        }

    def get_group_count(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        return {"group_count": len(groups), "message": f"当前共加入 {len(groups)} 个群组"}

    def get_group_categories(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        cat_count = {}
        for g in groups:
            cat = g.get('category', '未分类')
            cat_count[cat] = cat_count.get(cat, 0) + 1
        # 获取该管理员独立库中的所有分类定义
        conn = self._get_admin_conn(admin_id)
        all_cats_rows = conn.execute("SELECT category_name, description FROM group_categories ORDER BY category_id").fetchall()
        all_cats = [{"name": r[0], "description": r[1]} for r in all_cats_rows]
        if not cat_count:
            return {"message": "暂无群组分类数据", "categories": {}, "category_list": [], "total": 0}
        cat_list = "\n".join([f"• {cat}：{cnt}个" for cat, cnt in cat_count.items()])
        return {
            "categories": cat_count,
            "category_list": [c['name'] for c in all_cats],
            "total": sum(cat_count.values()),
            "summary": f"📊 群组分类统计：\n{cat_list}\n\n总计：{sum(cat_count.values())} 个群组"
        }

    def get_groups_by_category(self, admin_id: int, category_name: str = None, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        if category_name:
            filtered = [g for g in groups if g.get('category', '未分类') == category_name]
        else:
            filtered = groups
        if not filtered and category_name:
            return {"error": f"未找到分类「{category_name}」下的群组", "groups": [], "count": 0}
        return {
            "category": category_name or "全部",
            "count": len(filtered),
            "groups": [{"name": g['title'], "id": g['id']} for g in filtered[:50]]
        }

    def get_all_groups_by_category(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        # 获取分类列表
        conn = self._get_admin_conn(admin_id)
        categories_rows = conn.execute("SELECT category_name FROM group_categories ORDER BY category_id").fetchall()
        categories = [r[0] for r in categories_rows]
        result = {}
        for cat_name in categories:
            cat_groups = [g for g in groups if g.get('category', '未分类') == cat_name]
            if cat_groups:
                result[cat_name] = [{"name": g['title'], "id": g['id']} for g in cat_groups]
        summary = "📁 **所有分类及群组列表**\n\n"
        for cat_name, cat_groups in result.items():
            summary += f"📂 **{cat_name}** ({len(cat_groups)}个)\n"
            for g in cat_groups[:10]:
                summary += f"  • {g['name']}\n"
            if len(cat_groups) > 10:
                summary += f"  ... 还有 {len(cat_groups) - 10} 个\n"
            summary += "\n"
        return {
            "categories": result,
            "total_categories": len(categories),
            "total_groups": len(groups),
            "summary": summary
        }

    # ==================== 2. 新加入群组相关 ====================
    def get_today_joined_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份", "groups": [], "count": 0}

        groups = self._get_visible_groups(admin_id, user_id)
        now = beijing_now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        today_joined = []
        for g in groups:
            joined_at = g.get('joined_at', 0)
            if joined_at >= today_start:
                today_joined.append({
                    "name": g['title'],
                    "joined_at": timestamp_to_beijing_str(joined_at),
                    "category": g.get('category', '未分类')
                })
        today_joined.sort(key=lambda x: x['joined_at'])
        if not today_joined:
            return {"message": "今天没有新加入的群组", "groups": [], "count": 0}
        return {"date": "今天", "groups": today_joined, "count": len(today_joined)}

    def get_yesterday_joined_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        now = beijing_now()
        yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        yesterday_end = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        yesterday_joined = []
        for g in groups:
            joined_at = g.get('joined_at', 0)
            if yesterday_start <= joined_at < yesterday_end:
                yesterday_joined.append({
                    "name": g['title'],
                    "joined_at": timestamp_to_beijing_str(joined_at),
                    "category": g.get('category', '未分类')
                })
        yesterday_joined.sort(key=lambda x: x['joined_at'])
        if not yesterday_joined:
            return {"message": "昨天没有新加入的群组", "groups": [], "count": 0}
        return {"date": "昨天", "groups": yesterday_joined, "count": len(yesterday_joined)}

    def get_weekly_joined_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        now = beijing_now()
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        daily_groups = {}
        for i in range(7):
            day = week_start + timedelta(days=i)
            day_start = day.timestamp()
            day_end = (day + timedelta(days=1)).timestamp()
            day_str = day.strftime('%Y-%m-%d')
            daily_groups[day_str] = []
            for g in groups:
                joined_at = g.get('joined_at', 0)
                if day_start <= joined_at < day_end:
                    daily_groups[day_str].append({
                        "name": g['title'],
                        "time": timestamp_to_beijing_str(joined_at)
                    })
        result = []
        for date, group_list in daily_groups.items():
            if group_list:
                result.append({"date": date, "count": len(group_list), "groups": group_list})
        if not result:
            return {"message": "本周没有新加入的群组", "daily_groups": [], "total": 0}
        summary = "📅 **本周每天新加入的群组**\n\n"
        for day in result:
            summary += f"📌 {day['date']}：{day['count']}个\n"
            for g in day['groups'][:5]:
                summary += f"  • {g['name']}（{g['time']}）\n"
            if day['count'] > 5:
                summary += f"  ... 还有 {day['count'] - 5} 个\n"
            summary += "\n"
        return {"daily_groups": result, "total": sum(d['count'] for d in result), "summary": summary}

    def get_monthly_joined_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        now = beijing_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            next_month = now.replace(year=now.year+1, month=1, day=1)
        else:
            next_month = now.replace(month=now.month+1, day=1)
        days_in_month = (next_month - month_start).days
        daily_groups = {}
        for i in range(days_in_month):
            day = month_start + timedelta(days=i)
            day_start = day.timestamp()
            day_end = (day + timedelta(days=1)).timestamp()
            day_str = day.strftime('%Y-%m-%d')
            daily_groups[day_str] = []
            for g in groups:
                joined_at = g.get('joined_at', 0)
                if day_start <= joined_at < day_end:
                    daily_groups[day_str].append({
                        "name": g['title'],
                        "time": timestamp_to_beijing_str(joined_at)
                    })
        result = []
        for date, group_list in daily_groups.items():
            if group_list:
                result.append({"date": date, "count": len(group_list), "groups": group_list})
        if not result:
            return {"message": "本月没有新加入的群组", "daily_groups": [], "total": 0}
        summary = f"📅 **{now.strftime('%Y年%m月')}每天新加入的群组**\n\n"
        for day in result[:15]:
            summary += f"📌 {day['date']}：{day['count']}个\n"
            for g in day['groups'][:3]:
                summary += f"  • {g['name']}（{g['time']}）\n"
            if day['count'] > 3:
                summary += f"  ... 还有 {day['count'] - 3} 个\n"
            summary += "\n"
        if len(result) > 15:
            summary += f"... 还有 {len(result) - 15} 天有新增群组\n"
        return {"daily_groups": result, "total": sum(d['count'] for d in result), "summary": summary}

    def get_joined_groups_by_date(self, admin_id: int, date_str: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        try:
            if '-' in date_str:
                target_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=BEIJING_TZ)  # ✅ 添加时区
            elif '年' in date_str:
                match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    day = int(match.group(3))
                    target_date = datetime(year, month, day, tzinfo=BEIJING_TZ)
                else:
                    return {"error": f"日期格式错误: {date_str}"}
            else:
                target_date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=BEIJING_TZ)
            day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
            day_end = (target_date + timedelta(days=1)).timestamp()
        except Exception as e:
            return {"error": f"日期格式错误: {date_str}, {e}"}
        joined = []
        for g in groups:
            joined_at = g.get('joined_at', 0)
            if day_start <= joined_at < day_end:
                joined.append({
                    "name": g['title'],
                    "joined_at": timestamp_to_beijing_str(joined_at),
                    "category": g.get('category', '未分类')
                })
        joined.sort(key=lambda x: x['joined_at'])
        if not joined:
            return {"message": f"{date_str} 没有新加入的群组", "groups": [], "count": 0}
        return {"date": date_str, "groups": joined, "count": len(joined)}

    # ==================== 3. 活跃度相关 ====================
    def get_group_activity_ranking(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        group_stats = []
        for group in groups:
            try:
                stats = am.get_total_stats(group['id'], admin_id=admin_id)
                if stats['income_count'] > 0 or stats['expense_count'] > 0:
                    group_stats.append({
                        "name": group['title'],
                        "total_income_usdt": round(stats['income_usdt'], 2),
                        "total_count": stats['income_count'] + stats['expense_count'],
                        "income_count": stats['income_count'],
                        "expense_count": stats['expense_count']
                    })
            except:
                pass
        group_stats.sort(key=lambda x: x['total_count'], reverse=True)
        if not group_stats:
            return {"message": "暂无群组活跃数据", "ranking": [], "count": 0}
        summary = "📊 **群组活跃度排行**\n\n"
        for i, g in enumerate(group_stats[:20], 1):
            summary += f"{i}. {g['name']}\n   总交易：{g['total_count']}笔（入款{g['income_count']}笔，出款{g['expense_count']}笔）\n   总入款：{g['total_income_usdt']:.2f} USDT\n\n"
        return {"ranking": group_stats[:20], "count": len(group_stats), "summary": summary}

    def get_today_active_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        active_groups = []
        today = beijing_now().strftime('%Y-%m-%d')
        for group in groups:
            try:
                records = am.get_today_records(group['id'], admin_id=admin_id)
                income_count = len([r for r in records if r['type'] == 'income'])
                expense_count = len([r for r in records if r['type'] == 'expense'])
                if income_count > 0 or expense_count > 0:
                    stats = am.get_today_stats(group['id'], admin_id=admin_id)
                    active_groups.append({
                        "name": group['title'],
                        "income_usdt": round(stats['income_usdt'], 2),
                        "income_cny": round(stats['income_total'], 2),
                        "income_count": income_count,
                        "expense_count": expense_count
                    })
            except:
                pass
        active_groups.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not active_groups:
            return {"date": today, "message": f"今天（{today}）没有群组使用记账功能", "active_groups": [], "total_active": 0}
        group_list = []
        for g in active_groups:
            income_cny_str = f"{int(g['income_cny'])}" if g['income_cny'] == int(g['income_cny']) else f"{g['income_cny']:.2f}"
            income_usdt_str = f"{int(g['income_usdt'])}" if g['income_usdt'] == int(g['income_usdt']) else f"{g['income_usdt']:.2f}"
            group_list.append(f"• {g['name']}：{income_cny_str}元 = {income_usdt_str} USDT（入款{g['income_count']}笔，出款{g['expense_count']}笔）")
        summary = f"📊 **今天（{today}）使用记账功能的群组**\n\n" + "\n".join(group_list)
        return {"date": today, "active_groups": active_groups, "total_active": len(active_groups), "summary": summary}

    def get_yesterday_active_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        active_groups = []
        yesterday = (beijing_now() - timedelta(days=1)).strftime('%Y-%m-%d')
        for group in groups:
            try:
                records = am.get_records_by_date(group['id'], yesterday, admin_id=admin_id)
                income_count = len([r for r in records if r['type'] == 'income'])
                expense_count = len([r for r in records if r['type'] == 'expense'])
                if income_count > 0 or expense_count > 0:
                    total_income_usdt = sum(r['amount_usdt'] for r in records if r['type'] == 'income')
                    total_income_cny = sum(r['amount'] for r in records if r['type'] == 'income')
                    active_groups.append({
                        "name": group['title'],
                        "income_usdt": round(total_income_usdt, 2),
                        "income_cny": round(total_income_cny, 2),
                        "income_count": income_count,
                        "expense_count": expense_count
                    })
            except:
                pass
        active_groups.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not active_groups:
            return {"date": yesterday, "message": f"昨日（{yesterday}）没有群组使用记账功能", "active_groups": [], "total_active": 0}
        group_list = [f"• {g['name']}：{g['income_cny']:.2f}元 = {g['income_usdt']:.2f} USDT" for g in active_groups]
        summary = f"📊 **昨日（{yesterday}）使用记账功能的群组**\n\n" + "\n".join(group_list)
        return {"date": yesterday, "active_groups": active_groups, "total_active": len(active_groups), "summary": summary}

    def get_week_active_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        active_groups = []
        now = beijing_now()
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        start_ts = int(week_start.timestamp())
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                week_records = [r for r in records if r.get('created_at', 0) >= start_ts]
                income_count = len([r for r in week_records if r['type'] == 'income'])
                expense_count = len([r for r in week_records if r['type'] == 'expense'])
                if income_count > 0 or expense_count > 0:
                    total_income_usdt = sum(r['amount_usdt'] for r in week_records if r['type'] == 'income')
                    total_income_cny = sum(r['amount'] for r in week_records if r['type'] == 'income')
                    active_groups.append({
                        "name": group['title'],
                        "income_usdt": round(total_income_usdt, 2),
                        "income_cny": round(total_income_cny, 2),
                        "income_count": income_count,
                        "expense_count": expense_count
                    })
            except:
                pass
        active_groups.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not active_groups:
            return {"date": "本周", "message": "本周没有群组使用记账功能", "active_groups": [], "total_active": 0}
        group_list = [f"• {g['name']}：{g['income_cny']:.2f}元 = {g['income_usdt']:.2f} USDT" for g in active_groups]
        summary = f"📊 **本周使用记账功能的群组**\n\n" + "\n".join(group_list)
        return {"date": "本周", "active_groups": active_groups, "total_active": len(active_groups), "summary": summary}

    def get_month_active_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        active_groups = []
        now = beijing_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        start_ts = int(month_start.timestamp())
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                month_records = [r for r in records if r.get('created_at', 0) >= start_ts]
                income_count = len([r for r in month_records if r['type'] == 'income'])
                expense_count = len([r for r in month_records if r['type'] == 'expense'])
                if income_count > 0 or expense_count > 0:
                    total_income_usdt = sum(r['amount_usdt'] for r in month_records if r['type'] == 'income')
                    total_income_cny = sum(r['amount'] for r in month_records if r['type'] == 'income')
                    active_groups.append({
                        "name": group['title'],
                        "income_usdt": round(total_income_usdt, 2),
                        "income_cny": round(total_income_cny, 2),
                        "income_count": income_count,
                        "expense_count": expense_count
                    })
            except:
                pass
        active_groups.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not active_groups:
            return {"date": "本月", "message": "本月没有群组使用记账功能", "active_groups": [], "total_active": 0}
        group_list = [f"• {g['name']}：{g['income_cny']:.2f}元 = {g['income_usdt']:.2f} USDT" for g in active_groups]
        summary = f"📊 **本月使用记账功能的群组**\n\n" + "\n".join(group_list)
        return {"date": "本月", "active_groups": active_groups, "total_active": len(active_groups), "summary": summary}

    def get_today_top_group(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        top_group = None
        max_income = 0
        top_stats = None
        for group in groups:
            try:
                stats = am.get_today_stats(group['id'], admin_id=admin_id)
                if stats['income_usdt'] > max_income:
                    max_income = stats['income_usdt']
                    top_group = group
                    top_stats = stats
            except:
                pass
        if top_group:
            return {
                "group_name": top_group['title'],
                "category": top_group.get('category', '未分类'),
                "income_usdt": round(top_stats['income_usdt'], 2),
                "income_cny": round(top_stats['income_total'], 2),
                "income_count": top_stats['income_count']
            }
        return {"message": "今日没有交易记录", "group_name": None}

    # ==================== 4. 用户相关数据 ====================
    def get_today_top_users(self, admin_id: int, limit: int = 10, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        user_income = {}
        user_name_map = {}
        for group in groups:
            try:
                records = am.get_today_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income':
                        user_id_key = record.get('user_id')
                        if user_id_key:
                            user_name_map[user_id_key] = record.get('display_name', str(user_id_key))
                            user_income[user_id_key] = user_income.get(user_id_key, 0) + record['amount']
            except:
                pass
        sorted_users = sorted(user_income.items(), key=lambda x: x[1], reverse=True)[:limit]
        top_users_list = [
            {"name": user_name_map.get(uid, str(uid)), "income_cny": round(amount, 2)}
            for uid, amount in sorted_users
        ]
        if not top_users_list:
            return {"message": "今日没有入款记录", "top_users": []}
        return {"date": beijing_now().strftime('%Y-%m-%d'), "top_users": top_users_list}

    def get_today_active_users(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        user_activity = {}
        for group in groups:
            try:
                records = am.get_today_records(group['id'], admin_id=admin_id)
                for record in records:
                    user_id_key = record.get('user_id')
                    if user_id_key:
                        if user_id_key not in user_activity:
                            user_activity[user_id_key] = {
                                "name": record.get('display_name', str(user_id_key)),
                                "count": 0,
                                "income_usdt": 0,
                                "expense_usdt": 0
                            }
                        user_activity[user_id_key]["count"] += 1
                        if record['type'] == 'income':
                            user_activity[user_id_key]["income_usdt"] += record['amount_usdt']
                        else:
                            user_activity[user_id_key]["expense_usdt"] += record['amount_usdt']
            except:
                pass
        if not user_activity:
            return {"message": "今日没有用户使用记账功能", "active_users": [], "total_users": 0}
        return {"date": beijing_now().strftime('%Y-%m-%d'), "active_users": list(user_activity.values())[:30], "total_users": len(user_activity)}

    # ==================== 5. 记账统计相关 ====================
    def get_group_today_bill(self, admin_id: int, group_name: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        stats = am.get_today_stats(target['id'], admin_id=admin_id)
        records = am.get_today_records(target['id'], admin_id=admin_id)
        income_records = [r for r in records if r['type'] == 'income']
        expense_records = [r for r in records if r['type'] == 'expense']
        categories = {}
        for r in income_records:
            cat = r.get('category', '') or '未分类'
            if cat not in categories:
                categories[cat] = {"cny": 0, "usdt": 0, "count": 0}
            categories[cat]["cny"] += r['amount']
            categories[cat]["usdt"] += r['amount_usdt']
            categories[cat]["count"] += 1
        return {
            "group_name": target['title'],
            "category": target.get('category', '未分类'),
            "date": beijing_now().strftime('%Y-%m-%d'),
            "fee_rate": stats.get('fee_rate', 0),
            "exchange_rate": stats.get('exchange_rate', 1),
            "per_transaction_fee": stats.get('per_transaction_fee', 0),
            "income_usdt": round(stats['income_usdt'], 2),
            "income_cny": round(stats['income_total'], 2),
            "income_count": stats['income_count'],
            "expense_usdt": round(stats['expense_usdt'], 2),
            "expense_count": stats['expense_count'],
            "pending_usdt": round(stats['pending_usdt'], 2),
            "categories": categories,
            "recent_income": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_cny": round(r['amount'], 2), "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', ''), "category": r.get('category', '')}
                for r in income_records[:10]
            ],
            "recent_expense": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', '')}
                for r in expense_records[:10]
            ]
        }

    def get_group_yesterday_bill(self, admin_id: int, group_name: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        yesterday = (beijing_now() - timedelta(days=1)).strftime('%Y-%m-%d')
        return self.get_group_bill_by_date(admin_id, group_name, yesterday, user_id)

    def get_group_week_bill(self, admin_id: int, group_name: str, user_id: int = None) -> Dict:
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        now = beijing_now()
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        start_ts = int(week_start.timestamp())
        records = am.get_total_records(target['id'], admin_id=admin_id)
        week_records = [r for r in records if r.get('created_at', 0) >= start_ts]
        income_records = [r for r in week_records if r['type'] == 'income']
        expense_records = [r for r in week_records if r['type'] == 'expense']
        total_income_usdt = sum(r['amount_usdt'] for r in income_records)
        total_income_cny = sum(r['amount'] for r in income_records)
        total_expense_usdt = sum(r['amount_usdt'] for r in expense_records)
        categories = {}
        for r in income_records:
            cat = r.get('category', '') or '未分类'
            if cat not in categories:
                categories[cat] = {"cny": 0, "usdt": 0, "count": 0}
            categories[cat]["cny"] += r['amount']
            categories[cat]["usdt"] += r['amount_usdt']
            categories[cat]["count"] += 1
        return {
            "group_name": target['title'],
            "date": "本周",
            "income_usdt": round(total_income_usdt, 2),
            "income_cny": round(total_income_cny, 2),
            "income_count": len(income_records),
            "expense_usdt": round(total_expense_usdt, 2),
            "expense_count": len(expense_records),
            "pending_usdt": round(total_income_usdt - total_expense_usdt, 2),
            "categories": categories,
            "recent_income": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_cny": round(r['amount'], 2), "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', ''), "category": r.get('category', '')}
                for r in income_records[:10]
            ],
            "recent_expense": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', '')}
                for r in expense_records[:10]
            ]
        }

    def get_group_month_bill(self, admin_id: int, group_name: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        now = beijing_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        start_ts = int(month_start.timestamp())
        records = am.get_total_records(target['id'], admin_id=admin_id)
        month_records = [r for r in records if r.get('created_at', 0) >= start_ts]
        income_records = [r for r in month_records if r['type'] == 'income']
        expense_records = [r for r in month_records if r['type'] == 'expense']
        total_income_usdt = sum(r['amount_usdt'] for r in income_records)
        total_income_cny = sum(r['amount'] for r in income_records)
        total_expense_usdt = sum(r['amount_usdt'] for r in expense_records)
        categories = {}
        for r in income_records:
            cat = r.get('category', '') or '未分类'
            if cat not in categories:
                categories[cat] = {"cny": 0, "usdt": 0, "count": 0}
            categories[cat]["cny"] += r['amount']
            categories[cat]["usdt"] += r['amount_usdt']
            categories[cat]["count"] += 1
        return {
            "group_name": target['title'],
            "date": "本月",
            "income_usdt": round(total_income_usdt, 2),
            "income_cny": round(total_income_cny, 2),
            "income_count": len(income_records),
            "expense_usdt": round(total_expense_usdt, 2),
            "expense_count": len(expense_records),
            "pending_usdt": round(total_income_usdt - total_expense_usdt, 2),
            "categories": categories,
            "recent_income": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_cny": round(r['amount'], 2), "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', ''), "category": r.get('category', '')}
                for r in income_records[:10]
            ],
            "recent_expense": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "amount_usdt": round(r['amount_usdt'], 2), "user": r.get('display_name', '')}
                for r in expense_records[:10]
            ]
        }

    def get_group_bill_by_date(self, admin_id: int, group_name: str, date_str: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        records = am.get_records_by_date(target['id'], date_str, admin_id=admin_id)
        if not records:
            return {"group_name": target['title'], "date": date_str, "message": f"{date_str} 没有记账记录", "income_usdt": 0, "expense_usdt": 0, "pending_usdt": 0, "income_count": 0, "expense_count": 0}
        income_records = [r for r in records if r['type'] == 'income']
        expense_records = [r for r in records if r['type'] == 'expense']
        total_income_usdt = sum(r['amount_usdt'] for r in income_records)
        total_income_cny = sum(r['amount'] for r in income_records)
        total_expense_usdt = sum(r['amount_usdt'] for r in expense_records)
        return {
            "group_name": target['title'],
            "date": date_str,
            "income_usdt": round(total_income_usdt, 2),
            "income_cny": round(total_income_cny, 2),
            "income_count": len(income_records),
            "expense_usdt": round(total_expense_usdt, 2),
            "expense_count": len(expense_records),
            "pending_usdt": round(total_income_usdt - total_expense_usdt, 2),
            "records": [
                {"time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "type": r['type'], "amount_usdt": round(r['amount_usdt'], 2), "amount_cny": round(r['amount'], 2) if r['type']=='income' else None, "user": r.get('display_name',''), "category": r.get('category','')}
                for r in records[:20]
            ]
        }

    def get_group_bill_range(self, admin_id: int, group_name: str, start_date: str, end_date: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        try:
            start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
            end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()) + 86400
        except:
            return {"error": f"日期格式错误: {start_date} - {end_date}"}
        all_records = am.get_total_records(target['id'], admin_id=admin_id)
        range_records = [r for r in all_records if start_ts <= r.get('created_at', 0) < end_ts]
        if not range_records:
            return {"group_name": target['title'], "date": f"{start_date} 至 {end_date}", "message": "该时间段没有记账记录", "income_usdt": 0, "expense_usdt": 0, "pending_usdt": 0, "income_count": 0, "expense_count": 0}
        income_records = [r for r in range_records if r['type'] == 'income']
        expense_records = [r for r in range_records if r['type'] == 'expense']
        total_income_usdt = sum(r['amount_usdt'] for r in income_records)
        total_income_cny = sum(r['amount'] for r in income_records)
        total_expense_usdt = sum(r['amount_usdt'] for r in expense_records)
        return {
            "group_name": target['title'],
            "date": f"{start_date} 至 {end_date}",
            "income_usdt": round(total_income_usdt, 2),
            "income_cny": round(total_income_cny, 2),
            "income_count": len(income_records),
            "expense_usdt": round(total_expense_usdt, 2),
            "expense_count": len(expense_records),
            "pending_usdt": round(total_income_usdt - total_expense_usdt, 2),
            "records": [
                {"date": timestamp_to_date(r['created_at']), "time": timestamp_to_beijing_str(r['created_at'])[-8:-3], "type": r['type'], "amount_usdt": round(r['amount_usdt'], 2), "amount_cny": round(r['amount'], 2) if r['type']=='income' else None, "user": r.get('display_name',''), "category": r.get('category','')}
                for r in range_records[:30]
            ]
        }

    # ==================== 6. 所有群组收入 ====================
    def get_today_all_income(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份", "groups": [], "total_income_usdt": 0}

        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        group_details = []
        total_income_usdt = 0
        total_income_cny = 0
        for group in groups:
            try:
                stats = am.get_today_stats(group['id'], admin_id=admin_id)
                if stats['income_usdt'] > 0:
                    income_usdt = stats['income_usdt']
                    income_cny = stats['income_total']
                    total_income_usdt += income_usdt
                    total_income_cny += income_cny
                    pending_stats = am.get_current_stats(group['id'], admin_id=admin_id)
                    pending_usdt = pending_stats.get('pending_usdt', 0)
                    group_details.append({
                        "name": group['title'],
                        "category": group.get('category', '未分类'),
                        "income_usdt": round(income_usdt, 2),
                        "income_cny": round(income_cny, 2),
                        "income_count": stats['income_count'],
                        "pending_usdt": round(pending_usdt, 2)
                    })
            except:
                pass
        group_details.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not group_details:
            return {"message": "今日没有群组有收入记录", "groups": [], "total_income_usdt": 0}
        group_list = []
        for g in group_details:
            income_cny_str = f"{int(g['income_cny'])}" if g['income_cny'] == int(g['income_cny']) else f"{g['income_cny']:.2f}"
            income_usdt_str = f"{int(g['income_usdt'])}" if g['income_usdt'] == int(g['income_usdt']) else f"{g['income_usdt']:.2f}"
            pending_str = f"{int(g['pending_usdt'])}" if g['pending_usdt'] == int(g['pending_usdt']) else f"{g['pending_usdt']:.2f}"
            group_list.append(f"• {g['name']}：{income_cny_str}元 = {income_usdt_str} USDT（待下发 {pending_str} USDT）")
        total_cny_str = f"{int(total_income_cny)}" if total_income_cny == int(total_income_cny) else f"{total_income_cny:.2f}"
        total_usdt_str = f"{int(total_income_usdt)}" if total_income_usdt == int(total_income_usdt) else f"{total_income_usdt:.2f}"
        summary = f"📊 **今日所有群组收入**\n\n💰 总收入：{total_cny_str}元 = {total_usdt_str} USDT\n📈 有收入群组：{len(group_details)}个\n\n" + "\n".join(group_list)
        return {
            "date": beijing_now().strftime('%Y-%m-%d'),
            "groups": group_details,
            "total_income_usdt": round(total_income_usdt, 2),
            "total_income_cny": round(total_income_cny, 2),
            "active_group_count": len(group_details),
            "summary": summary
        }

    def get_yesterday_all_income(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        yesterday = (beijing_now() - timedelta(days=1)).strftime('%Y-%m-%d')
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        group_details = []
        total_income_usdt = 0
        total_income_cny = 0
        for group in groups:
            try:
                records = am.get_records_by_date(group['id'], yesterday, admin_id=admin_id)
                income_records = [r for r in records if r['type'] == 'income']
                if income_records:
                    income_usdt = sum(r['amount_usdt'] for r in income_records)
                    income_cny = sum(r['amount'] for r in income_records)
                    total_income_usdt += income_usdt
                    total_income_cny += income_cny
                    pending_stats = am.get_current_stats(group['id'], admin_id=admin_id)
                    pending_usdt = pending_stats.get('pending_usdt', 0)
                    group_details.append({
                        "name": group['title'],
                        "income_usdt": round(income_usdt, 2),
                        "income_cny": round(income_cny, 2),
                        "income_count": len(income_records),
                        "pending_usdt": round(pending_usdt, 2)
                    })
            except:
                pass
        group_details.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not group_details:
            return {"message": "昨日没有群组有收入记录", "groups": [], "total_income_usdt": 0}
        group_list = []
        for g in group_details:
            income_cny_str = f"{int(g['income_cny'])}" if g['income_cny'] == int(g['income_cny']) else f"{g['income_cny']:.2f}"
            income_usdt_str = f"{int(g['income_usdt'])}" if g['income_usdt'] == int(g['income_usdt']) else f"{g['income_usdt']:.2f}"
            pending_str = f"{int(g['pending_usdt'])}" if g['pending_usdt'] == int(g['pending_usdt']) else f"{g['pending_usdt']:.2f}"
            group_list.append(f"• {g['name']}：{income_cny_str}元 = {income_usdt_str} USDT（待下发 {pending_str} USDT）")
        total_cny_str = f"{int(total_income_cny)}" if total_income_cny == int(total_income_cny) else f"{total_income_cny:.2f}"
        total_usdt_str = f"{int(total_income_usdt)}" if total_income_usdt == int(total_income_usdt) else f"{total_income_usdt:.2f}"
        summary = f"📊 **昨日所有群组收入**\n\n💰 总收入：{total_cny_str}元 = {total_usdt_str} USDT\n📈 有收入群组：{len(group_details)}个\n\n" + "\n".join(group_list)
        return {"date": yesterday, "groups": group_details, "total_income_usdt": round(total_income_usdt, 2), "total_income_cny": round(total_income_cny, 2), "active_group_count": len(group_details), "summary": summary}

    def get_week_all_income(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        now = beijing_now()
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        start_ts = int(week_start.timestamp())
        group_details = []
        total_income_usdt = 0
        total_income_cny = 0
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                week_records = [r for r in records if r.get('created_at', 0) >= start_ts and r['type'] == 'income']
                if week_records:
                    income_usdt = sum(r['amount_usdt'] for r in week_records)
                    income_cny = sum(r['amount'] for r in week_records)
                    total_income_usdt += income_usdt
                    total_income_cny += income_cny
                    pending_stats = am.get_current_stats(group['id'], admin_id=admin_id)
                    pending_usdt = pending_stats.get('pending_usdt', 0)
                    group_details.append({
                        "name": group['title'],
                        "income_usdt": round(income_usdt, 2),
                        "income_cny": round(income_cny, 2),
                        "income_count": len(week_records),
                        "pending_usdt": round(pending_usdt, 2)
                    })
            except:
                pass
        group_details.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not group_details:
            return {"message": "本周没有群组有收入记录", "groups": [], "total_income_usdt": 0}
        group_list = []
        for g in group_details:
            income_cny_str = f"{int(g['income_cny'])}" if g['income_cny'] == int(g['income_cny']) else f"{g['income_cny']:.2f}"
            income_usdt_str = f"{int(g['income_usdt'])}" if g['income_usdt'] == int(g['income_usdt']) else f"{g['income_usdt']:.2f}"
            pending_str = f"{int(g['pending_usdt'])}" if g['pending_usdt'] == int(g['pending_usdt']) else f"{g['pending_usdt']:.2f}"
            group_list.append(f"• {g['name']}：{income_cny_str}元 = {income_usdt_str} USDT（待下发 {pending_str} USDT）")
        total_cny_str = f"{int(total_income_cny)}" if total_income_cny == int(total_income_cny) else f"{total_income_cny:.2f}"
        total_usdt_str = f"{int(total_income_usdt)}" if total_income_usdt == int(total_income_usdt) else f"{total_income_usdt:.2f}"
        summary = f"📊 **本周所有群组收入**\n\n💰 总收入：{total_cny_str}元 = {total_usdt_str} USDT\n📈 有收入群组：{len(group_details)}个\n\n" + "\n".join(group_list)
        return {"date": "本周", "groups": group_details, "total_income_usdt": round(total_income_usdt, 2), "total_income_cny": round(total_income_cny, 2), "active_group_count": len(group_details), "summary": summary}

    def get_month_all_income(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        now = beijing_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        start_ts = int(month_start.timestamp())
        group_details = []
        total_income_usdt = 0
        total_income_cny = 0
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                month_records = [r for r in records if r.get('created_at', 0) >= start_ts and r['type'] == 'income']
                if month_records:
                    income_usdt = sum(r['amount_usdt'] for r in month_records)
                    income_cny = sum(r['amount'] for r in month_records)
                    total_income_usdt += income_usdt
                    total_income_cny += income_cny
                    pending_stats = am.get_current_stats(group['id'], admin_id=admin_id)
                    pending_usdt = pending_stats.get('pending_usdt', 0)
                    group_details.append({
                        "name": group['title'],
                        "category": group.get('category', '未分类'),
                        "income_usdt": round(income_usdt, 2),
                        "income_cny": round(income_cny, 2),
                        "income_count": len(month_records),
                        "pending_usdt": round(pending_usdt, 2)
                    })
            except:
                pass
        group_details.sort(key=lambda x: x['income_usdt'], reverse=True)
        if not group_details:
            return {"message": "本月没有群组有收入记录", "groups": [], "total_income_usdt": 0}
        group_list = []
        for g in group_details:
            income_cny_str = f"{int(g['income_cny'])}" if g['income_cny'] == int(g['income_cny']) else f"{g['income_cny']:.2f}"
            income_usdt_str = f"{int(g['income_usdt'])}" if g['income_usdt'] == int(g['income_usdt']) else f"{g['income_usdt']:.2f}"
            pending_str = f"{int(g['pending_usdt'])}" if g['pending_usdt'] == int(g['pending_usdt']) else f"{g['pending_usdt']:.2f}"
            group_list.append(f"• {g['name']}：{income_cny_str}元 = {income_usdt_str} USDT（待下发 {pending_str} USDT）")
        total_cny_str = f"{int(total_income_cny)}" if total_income_cny == int(total_income_cny) else f"{total_income_cny:.2f}"
        total_usdt_str = f"{int(total_income_usdt)}" if total_income_usdt == int(total_income_usdt) else f"{total_income_usdt:.2f}"
        summary = f"📊 **本月所有群组收入**\n\n💰 总收入：{total_cny_str}元 = {total_usdt_str} USDT\n📈 有收入群组：{len(group_details)}个\n\n" + "\n".join(group_list)
        return {"date": "本月", "groups": group_details, "total_income_usdt": round(total_income_usdt, 2), "total_income_cny": round(total_income_cny, 2), "active_group_count": len(group_details), "summary": summary}

    # ==================== 7. 对比分析 ====================
    def get_group_compare(self, admin_id: int, group_name: str, period: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        target = self._find_group(groups, group_name)
        if not target:
            return {"error": f"未找到群组「{group_name}」"}
        if period == "today_vs_yesterday":
            today_stats = am.get_today_stats(target['id'], admin_id=admin_id)
            yesterday = (beijing_now() - timedelta(days=1)).strftime('%Y-%m-%d')
            yesterday_records = am.get_records_by_date(target['id'], yesterday, admin_id=admin_id)
            yesterday_income = sum(r['amount_usdt'] for r in yesterday_records if r['type'] == 'income')
            change = today_stats['income_usdt'] - yesterday_income
            change_percent = (change / yesterday_income * 100) if yesterday_income > 0 else 100 if today_stats['income_usdt'] > 0 else 0
            return {"group_name": target['title'], "period": "昨天 vs 今天", "today_income": round(today_stats['income_usdt'], 2), "yesterday_income": round(yesterday_income, 2), "change": round(change, 2), "change_percent": round(change_percent, 1), "trend": "上涨" if change >= 0 else "下跌"}
        return {"error": "暂不支持该对比方式"}

    def get_all_compare(self, admin_id: int, period: str, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        if period == "today_vs_yesterday":
            today_total = 0
            yesterday_total = 0
            yesterday = (beijing_now() - timedelta(days=1)).strftime('%Y-%m-%d')
            for group in groups:
                try:
                    today_stats = am.get_today_stats(group['id'], admin_id=admin_id)
                    today_total += today_stats['income_usdt']
                    yesterday_records = am.get_records_by_date(group['id'], yesterday, admin_id=admin_id)
                    yesterday_total += sum(r['amount_usdt'] for r in yesterday_records if r['type'] == 'income')
                except:
                    pass
            change = today_total - yesterday_total
            change_percent = (change / yesterday_total * 100) if yesterday_total > 0 else 100 if today_total > 0 else 0
            return {
                "period": "昨天 vs 今天",
                "today_total": round(today_total, 2),
                "yesterday_total": round(yesterday_total, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 1),
                "trend": "上涨" if change >= 0 else "下跌"
            }
        elif period == "week_vs_lastweek":
            now = beijing_now()
            this_week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
            last_week_start = this_week_start - timedelta(days=7)
            this_week_ts = int(this_week_start.timestamp())
            last_week_ts = int(last_week_start.timestamp())
            last_week_end = this_week_ts
            this_week_total = 0
            last_week_total = 0
            for group in groups:
                try:
                    records = am.get_total_records(group['id'], admin_id=admin_id)
                    this_week_total += sum(r['amount_usdt'] for r in records if r['type'] == 'income' and r.get('created_at', 0) >= this_week_ts)
                    last_week_total += sum(r['amount_usdt'] for r in records if r['type'] == 'income' and last_week_ts <= r.get('created_at', 0) < last_week_end)
                except:
                    pass
            change = this_week_total - last_week_total
            change_percent = (change / last_week_total * 100) if last_week_total > 0 else 100 if this_week_total > 0 else 0
            return {
                "period": "上周 vs 本周",
                "this_week_total": round(this_week_total, 2),
                "last_week_total": round(last_week_total, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 1),
                "trend": "上涨" if change >= 0 else "下跌"
            }
        elif period == "month_vs_lastmonth":
            now = beijing_now()
            this_month_start = now.replace(day=1, hour=0, minute=0, second=0)
            if now.month == 1:
                last_month_start = now.replace(year=now.year-1, month=12, day=1)
            else:
                last_month_start = now.replace(month=now.month-1, day=1)
            this_month_ts = int(this_month_start.timestamp())
            last_month_ts = int(last_month_start.timestamp())
            last_month_end = this_month_ts
            this_month_total = 0
            last_month_total = 0
            for group in groups:
                try:
                    records = am.get_total_records(group['id'], admin_id=admin_id)
                    this_month_total += sum(r['amount_usdt'] for r in records if r['type'] == 'income' and r.get('created_at', 0) >= this_month_ts)
                    last_month_total += sum(r['amount_usdt'] for r in records if r['type'] == 'income' and last_month_ts <= r.get('created_at', 0) < last_month_end)
                except:
                    pass
            change = this_month_total - last_month_total
            change_percent = (change / last_month_total * 100) if last_month_total > 0 else 100 if this_month_total > 0 else 0
            return {
                "period": "上月 vs 本月",
                "this_month_total": round(this_month_total, 2),
                "last_month_total": round(last_month_total, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 1),
                "trend": "上涨" if change >= 0 else "下跌"
            }
        return {"error": f"无法识别的对比周期: {period}"}

    # ==================== 8. 待下发 ====================
    def get_pending_usdt_groups(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        pending_groups = []
        total_pending = 0
        for group in groups:
            try:
                stats = am.get_total_pending_stats(group['id'], admin_id=admin_id)
                if stats['pending_usdt'] > 0:
                    pending_groups.append({
                        "name": group['title'],
                        "pending_usdt": round(stats['pending_usdt'], 2)
                    })
                    total_pending += stats['pending_usdt']
            except:
                pass
        pending_groups.sort(key=lambda x: x['pending_usdt'], reverse=True)
        if not pending_groups:
            return {
                "message": "✅ 所有群组都没有待下发的 USDT",
                "pending_groups": [],
                "total_pending_usdt": 0,
                "count": 0
            }
        group_list = [f"{g['name']}：{g['pending_usdt']:.0f} USDT" for g in pending_groups]
        summary = f"共有 {len(pending_groups)} 个群组有待下发，总计 {total_pending:.0f} USDT\n\n" + "\n".join(group_list)
        return {
            "pending_groups": pending_groups,
            "total_pending_usdt": round(total_pending, 2),
            "count": len(pending_groups),
            "summary": summary
        }

    # ==================== 9. 操作员 ====================
    def get_operators(self, admin_id: int, user_id: int = None) -> Dict:
        """获取操作员列表（只显示当前管理员下属的操作员）"""
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        # 从主库获取归属关系
        ops = list_operators(added_by=admin_id)
        operator_details = []
        for op_id in ops:
            details = self._get_user_details(op_id, admin_id)
            operator_details.append(details)
        temp_operator_details = []
        # 从主库获取临时操作员归属（也需要 admin_id 过滤）
        temps = {uid: info for uid, info in temp_operators.items() if info.get('added_by') == admin_id}
        for temp_id, info in temps.items():
            details = self._get_user_details(temp_id, admin_id)
            # 补充临时操作员的显示名（如果 group_users 中没有，用 info 中的）
            if not details.get('full_name') and info.get('first_name'):
                details['full_name'] = info.get('first_name', '')
                if info.get('username'):
                    details['display_name'] = f"{info.get('first_name')} (@{info.get('username')})"
                else:
                    details['display_name'] = info.get('first_name', str(temp_id))
            temp_operator_details.append(details)
        owner_details = self._get_user_details(OWNER_ID, admin_id)
        if owner_details.get('display_name') == str(OWNER_ID):
            owner_details['display_name'] = "👑 超级管理员"
        text = f"👑 **控制人**：{owner_details.get('display_name', OWNER_ID)}\n\n"
        if operator_details:
            text += "👥 **正式操作人**：\n"
            for op in operator_details:
                text += f"  • {op.get('display_name', op.get('user_id'))}\n"
        else:
            text += "👥 **正式操作人**：暂无\n"
        if temp_operator_details:
            text += "\n👤 **临时操作人**（仅记账权限）：\n"
            for temp in temp_operator_details:
                text += f"  • {temp.get('display_name', temp.get('user_id'))}\n"
        else:
            text += "\n👤 **临时操作人**：暂无\n"
        return {
            "owner": owner_details,
            "operators": operator_details,
            "temp_operators": temp_operator_details,
            "operator_count": len(ops),
            "temp_count": len(temps),
            "summary": text
        }

    def _get_user_details(self, user_id: int, admin_id: int) -> Dict:
        """从独立库的 group_users 表获取用户详细信息"""
        try:
            conn = self._get_admin_conn(admin_id)
            row = conn.execute("""
                SELECT username, first_name, last_name 
                FROM group_users 
                WHERE user_id = ? 
                LIMIT 1
            """, (user_id,)).fetchone()
            if row:
                username = row[0] or ""
                first_name = row[1] or ""
                last_name = row[2] or ""
                full_name = f"{first_name} {last_name}".strip()
                if username:
                    display_name = f"{first_name} (@{username})" if first_name else f"@{username}"
                else:
                    display_name = full_name if full_name else str(user_id)
                return {
                    "user_id": user_id,
                    "username": username,
                    "full_name": full_name,
                    "display_name": display_name
                }
        except:
            pass
        return {
            "user_id": user_id,
            "username": None,
            "full_name": None,
            "display_name": str(user_id)
        }

    # ==================== 10. 地址相关 ====================
    async def get_address_stats(self, address: str, date_range: str, user_id: int = None, admin_id: int = 0) -> Dict:
        """获取地址的收支统计（只查询当前管理员独立库中的地址）"""
        from handlers.monitor import get_trc20_transactions, get_address_balance
        from db import get_monitored_addresses as db_get_addrs
        # 检查地址是否属于当前管理员添加
        my_addresses = db_get_addrs(admin_id, user_id=user_id)
        address_notes = {a['address']: a.get('note', '') for a in my_addresses}
        if address not in address_notes:
            return {"error": "您没有权限查询此地址", "suggestion": "只能查询自己添加的监控地址"}
        now = beijing_now()
        if date_range == "today":
            start_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_ts = None
            period_name = "今日"
        elif date_range == "yesterday":
            yesterday = now - timedelta(days=1)
            start_ts = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            period_name = "昨日"
        elif date_range == "week":
            start_ts = int((now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_ts = None
            period_name = "本周"
        elif date_range == "month":
            start_ts = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_ts = None
            period_name = "本月"
        elif date_range == "last2days":
            start_ts = int((now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_ts = None
            period_name = "最近两天"
        else:
            start_ts = 0
            end_ts = None
            period_name = "全部"
        all_txs = []
        page = 0
        limit = 200
        while True:
            txs = await get_trc20_transactions(address, start_ts, limit=limit, offset=page * limit)
            if not txs:
                break
            if end_ts:
                txs = [tx for tx in txs if tx.get("block_timestamp", 0) < end_ts]
            all_txs.extend(txs)
            if len(txs) < limit:
                break
            page += 1
        received = 0.0
        sent = 0.0
        for tx in all_txs:
            to_addr = tx.get("to", "")
            raw_amount = tx.get("value", 0)
            amount = int(raw_amount) / 1_000_000 if raw_amount else 0
            if to_addr == address:
                received += amount
            else:
                sent += amount
        balance = await get_address_balance(address)
        note = address_notes.get(address, "")
        return {
            "address": address[:12] + "..." + address[-8:],
            "full_address": address,
            "note": note,
            "period": period_name,
            "received_usdt": round(received, 2),
            "sent_usdt": round(sent, 2),
            "net_usdt": round(received - sent, 2),
            "balance_usdt": round(balance, 2),
            "transaction_count": len(all_txs)
        }

    async def get_address_monthly_stats(self, address: str, user_id: int = None, admin_id: int = 0) -> Dict:
        from handlers.monitor import get_address_balance, get_monthly_stats
        from db import get_monitored_addresses as db_get_addrs
        my_addresses = db_get_addrs(admin_id, user_id=user_id)
        if address not in [a['address'] for a in my_addresses]:
            return {"error": "您没有权限查询此地址"}
        balance = await get_address_balance(address)
        monthly_stats = await get_monthly_stats(address)
        note = ""
        for a in my_addresses:
            if a['address'] == address:
                note = a.get('note', '')
                break
        return {
            "address": address[:12] + "..." + address[-8:],
            "full_address": address,
            "note": note,
            "current_balance": round(balance, 2),
            "monthly_received": round(monthly_stats.get('received', 0), 2),
            "monthly_sent": round(monthly_stats.get('sent', 0), 2),
            "monthly_net": round(monthly_stats.get('net', 0), 2)
        }

    # ==================== 11. 数据分析 ====================
    def get_hourly_distribution(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        hourly_data = [0] * 24
        for group in groups:
            try:
                records = am.get_today_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income':
                        hour = datetime.fromtimestamp(record['created_at'], tz=BEIJING_TZ).hour
                        hourly_data[hour] += record['amount_usdt']
            except:
                pass
        peak_hour = max(range(24), key=lambda x: hourly_data[x])
        active_hours = [(h, hourly_data[h]) for h in range(24) if hourly_data[h] > 0]
        if not active_hours:
            return {"message": "今日没有入款记录", "hourly": [], "peak_hour": None}
        return {
            "date": beijing_now().strftime('%Y-%m-%d'),
            "hourly": [{"hour": h, "usdt": round(hourly_data[h], 2)} for h, _ in active_hours],
            "peak_hour": peak_hour,
            "peak_usdt": round(hourly_data[peak_hour], 2)
        }

    def get_category_income_percentage(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        category_income = {}
        total = 0
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income':
                        category = record.get('category', '未分类')
                        if not category:
                            category = '未分类'
                        category_income[category] = category_income.get(category, 0) + record['amount_usdt']
                        total += record['amount_usdt']
            except:
                pass
        if total == 0:
            return {"message": "暂无入款记录", "categories": [], "total_usdt": 0}
        categories = []
        for cat, amount in sorted(category_income.items(), key=lambda x: x[1], reverse=True):
            categories.append({
                "name": cat,
                "usdt": round(amount, 2),
                "percentage": round(amount / total * 100, 1)
            })
        return {"categories": categories, "total_usdt": round(total, 2)}

    def get_weekly_trend(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        now = beijing_now()
        daily_data = {}
        for i in range(7):
            date = (now - timedelta(days=i)).strftime('%Y-%m-%d')
            daily_data[date] = 0
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income':
                        date = timestamp_to_date(record['created_at'])
                        if date in daily_data:
                            daily_data[date] += record['amount_usdt']
            except:
                pass
        trend = []
        for date in sorted(daily_data.keys()):
            trend.append({
                "date": date,
                "usdt": round(daily_data[date], 2)
            })
        if all(t['usdt'] == 0 for t in trend):
            return {"message": "最近7天没有收入记录", "trend": [], "days": 7}
        return {"trend": trend, "days": 7}

    def get_month_total_income(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        now = beijing_now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
        total_income_cny = 0
        total_income_usdt = 0
        for group in groups:
            try:
                records = am.get_total_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income' and record.get('created_at', 0) >= month_start:
                        total_income_cny += record['amount']
                        total_income_usdt += record['amount_usdt']
            except:
                pass
        return {
            "month": now.strftime('%Y年%m月'),
            "total_income_cny": round(total_income_cny, 2),
            "total_income_usdt": round(total_income_usdt, 2)
        }

    def get_large_transactions(self, admin_id: int, threshold: int = 5000, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        large_transactions = []
        for group in groups:
            try:
                records = am.get_today_records(group['id'], admin_id=admin_id)
                for record in records:
                    if record['type'] == 'income' and record['amount'] >= threshold:
                        large_transactions.append({
                            "group": group['title'],
                            "user": record.get('display_name', '未知'),
                            "amount_cny": round(record['amount'], 2),
                            "amount_usdt": round(record['amount_usdt'], 2),
                            "time": timestamp_to_beijing_str(record['created_at'])[-8:-3]
                        })
            except:
                pass
        large_transactions.sort(key=lambda x: x['amount_cny'], reverse=True)
        if not large_transactions:
            return {"message": f"今日没有超过 {threshold} 元的大额交易", "transactions": [], "count": 0}
        return {
            "date": beijing_now().strftime('%Y-%m-%d'),
            "threshold": threshold,
            "transactions": large_transactions,
            "count": len(large_transactions)
        }

    def get_today_summary(self, admin_id: int, user_id: int = None) -> Dict:
        if admin_id is None or admin_id <= 0:
            return {"error": "Invalid admin_id", "message": "无法确定管理员身份"}
        groups = self._get_visible_groups(admin_id, user_id)
        am = get_accounting_manager(admin_id)
        total_income_usdt = 0
        total_pending_usdt = 0
        for group in groups:
            try:
                stats = am.get_today_stats(group['id'], admin_id=admin_id)
                total_income_usdt += stats['income_usdt']
                current_stats = am.get_current_stats(group['id'], admin_id=admin_id)
                total_pending_usdt += current_stats['pending_usdt']
            except:
                pass
        return {
            "date": beijing_now().strftime('%Y-%m-%d'),
            "total_income_usdt": round(total_income_usdt, 2),
            "total_pending_usdt": round(total_pending_usdt, 2)
        }

    # ==================== 辅助方法 ====================
    def _find_group(self, groups: List[Dict], group_name: str) -> Optional[Dict]:
        group_name_lower = group_name.lower()
        for group in groups:
            title_lower = group['title'].lower()
            if group_name_lower == title_lower:
                return group
            if group_name_lower in title_lower or title_lower in group_name_lower:
                return group
        numbers = re.findall(r'\d+', group_name)
        for num in numbers:
            for group in groups:
                if num in group['title']:
                    return group
        return None


# 全局单例实例
data_provider = DataProvider()
