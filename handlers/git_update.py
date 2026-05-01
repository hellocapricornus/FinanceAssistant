# handlers/git_update.py
import subprocess
import os
import sys
import asyncio
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from config import OWNER_ID

DEFAULT_REMOTE = "origin"
DEFAULT_BRANCH = "main"

def get_git_root():
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except:
        pass
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def restart_bot():
    """重启机器人 - 使用 systemctl"""
    try:
        print(f"[重启] 正在重启机器人...")
        result = subprocess.run(
            ['systemctl', 'restart', 'finance-bot'],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            print(f"[重启] 重启成功")
            sys.exit(0)
        else:
            print(f"[重启] systemctl 失败: {result.stderr}")
            sys.exit(1)
    except Exception as e:
        print(f"[重启] 重启失败: {e}")
        sys.exit(1)


async def git_pull(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    status_msg = await update.message.reply_text("🔄 正在拉取最新代码...")
    git_root = get_git_root()

    try:
        result = subprocess.run(
            ['git', 'pull'],
            cwd=git_root,
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            if "Already up to date" in output:
                await status_msg.edit_text("✅ 代码已是最新，无需更新")
            else:
                await status_msg.edit_text("✅ 代码更新成功！\n\n🔄 正在重启机器人...", parse_mode='Markdown')
                await asyncio.sleep(2)
                restart_bot()
        else:
            await status_msg.edit_text(f"❌ 更新失败：\n```\n{result.stderr[:500]}\n```", parse_mode='Markdown')
    except subprocess.TimeoutExpired:
        await status_msg.edit_text("❌ 更新超时，请稍后重试")
    except Exception as e:
        await status_msg.edit_text(f"❌ 执行出错：{str(e)}")


async def git_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return

    status_msg = await update.message.reply_text("🔍 正在检查状态...")
    git_root = get_git_root()
    try:
        branch = subprocess.run(['git', 'branch', '--show-current'], cwd=git_root, capture_output=True, text=True)
        status = subprocess.run(['git', 'status', '--short'], cwd=git_root, capture_output=True, text=True)
        
        message = f"📊 **Git 状态**\n\n🌿 分支：`{branch.stdout.strip()}`\n\n"
        if status.stdout.strip():
            message += f"📝 本地修改：\n```\n{status.stdout[:300]}\n```"
        else:
            message += "✅ 工作区干净"
        await status_msg.edit_text(message, parse_mode='Markdown')
    except Exception as e:
        await status_msg.edit_text(f"❌ 检查失败：{str(e)}")


async def git_branch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ 只有超级管理员可以使用此命令")
        return
    status_msg = await update.message.reply_text("🔍 正在获取分支列表...")
    git_root = get_git_root()
    try:
        result = subprocess.run(['git', 'branch', '-a'], cwd=git_root, capture_output=True, text=True)
        branches = result.stdout.strip().split('\n')[:20]
        message = "📊 **Git 分支列表**\n\n"
        for b in branches:
            if b.startswith('*'):
                message += f"✅ `{b[1:].strip()}` (当前)\n"
            else:
                message += f"   `{b.strip()}`\n"
        await status_msg.edit_text(message, parse_mode='Markdown')
    except Exception as e:
        await status_msg.edit_text(f"❌ 获取分支失败：{str(e)}")


def get_git_handlers():
    return [
        CommandHandler("gitpull", git_pull),
        CommandHandler("gitstatus", git_status),
        CommandHandler("gitbranch", git_branch),
    ]
