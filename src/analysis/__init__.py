"""
Analysis module cho JobInsight.
Module này xử lý việc phân tích dữ liệu và tạo báo cáo từ dữ liệu đã được ETL.
"""

try:
    from src.analysis.analysis_queries import get_popular_skills, get_salary_trends
    from src.analysis.create_analysis_views import create_all_views, refresh_all_views
    from src.analysis.generate_report import generate_monthly_report, generate_trend_report
    
    __all__ = [
        'get_popular_skills',
        'get_salary_trends',
        'create_all_views',
        'refresh_all_views',
        'generate_monthly_report',
        'generate_trend_report'
    ]
except ImportError:
    # Khi một số module chưa được phát triển
    pass
