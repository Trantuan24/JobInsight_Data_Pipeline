"""
Discord Bot module cho JobInsight.
Module này xử lý việc gửi thông báo và báo cáo tới Discord.
"""

try:
    from src.discord_bot.send_discord import send_message, send_report, send_alert
    
    __all__ = [
        'send_message',
        'send_report',
        'send_alert'
    ]
except ImportError:
    # Khi một số module chưa được phát triển
    pass
