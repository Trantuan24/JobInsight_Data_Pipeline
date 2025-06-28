#!/usr/bin/env python3
"""
JobInsight Report Generator

Module này tạo báo cáo từ các view phân tích đã được tạo trước đó.
Báo cáo tập trung vào phân tích các công việc có mức lương 10-15 triệu.

Author: JobInsight Team
"""

import os
import sys
import pandas as pd
import duckdb
import datetime
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any, Optional

# Thêm thư mục gốc vào path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))

# Import utils
try:
    from src.utils.logger import get_logger
    from src.utils.config import DUCKDB_PATH, FILTER_MIN_SALARY, FILTER_MAX_SALARY, FILTER_LOCATION
except ImportError:
    try:
        from src.utils.logger import get_logger
        from src.utils.config import DUCKDB_PATH, FILTER_MIN_SALARY, FILTER_MAX_SALARY, FILTER_LOCATION
    except ImportError:
        import logging
        logging.basicConfig(level=logging.INFO)
        def get_logger(name):
            return logging.getLogger(name)
        
        # Default config values
        DUCKDB_PATH = "data/duck_db/jobinsight_warehouse.duckdb"
        FILTER_MIN_SALARY = 10000000
        FILTER_MAX_SALARY = 15000000
        FILTER_LOCATION = "Hà Nội"

logger = get_logger("report_generator")

# Đảm bảo thư mục output tồn tại
REPORTS_DIR = os.path.join(project_root, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

class ReportGenerator:
    """Lớp tạo báo cáo từ dữ liệu warehouse."""
    
    def __init__(self, db_path: str = DUCKDB_PATH):
        """
        Khởi tạo đối tượng ReportGenerator.
        
        Args:
            db_path (str): Đường dẫn đến file DuckDB
        """
        self.db_path = db_path
        self.conn = None
        self.report_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.output_dir = os.path.join(REPORTS_DIR, self.report_date)
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Khởi tạo ReportGenerator với DB: {db_path}")
        logger.info(f"Báo cáo sẽ được lưu tại: {self.output_dir}")
    
    def connect(self) -> bool:
        """
        Kết nối đến DuckDB.
        
        Returns:
            bool: True nếu kết nối thành công
        """
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info("Đã kết nối thành công đến DuckDB")
            return True
        except Exception as e:
            logger.error(f"Lỗi kết nối đến DuckDB: {e}")
            return False
    
    def close(self):
        """Đóng kết nối DuckDB."""
        if self.conn:
            self.conn.close()
            logger.info("Đã đóng kết nối DuckDB")
    
    def query_to_df(self, query: str) -> pd.DataFrame:
        """
        Thực thi truy vấn và trả về DataFrame.
        
        Args:
            query (str): Câu truy vấn SQL
            
        Returns:
            pd.DataFrame: Kết quả truy vấn
        """
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"Lỗi truy vấn: {e}")
            return pd.DataFrame()
    
    def save_dataframe(self, df: pd.DataFrame, name: str, format: str = "csv") -> str:
        """
        Lưu DataFrame ra file.
        
        Args:
            df (pd.DataFrame): DataFrame cần lưu
            name (str): Tên file (không bao gồm phần mở rộng)
            format (str): Định dạng file (csv, xlsx, html)
            
        Returns:
            str: Đường dẫn đến file đã lưu
        """
        if df.empty:
            logger.warning(f"DataFrame {name} rỗng, không lưu")
            return ""
        
        try:
            file_path = os.path.join(self.output_dir, f"{name}.{format}")
            
            if format == "csv":
                df.to_csv(file_path, index=False, encoding="utf-8-sig")
            elif format == "xlsx":
                df.to_excel(file_path, index=False)
            elif format == "html":
                df.to_html(file_path, index=False)
            else:
                logger.warning(f"Định dạng {format} không được hỗ trợ")
                return ""
            
            logger.info(f"Đã lưu {name}.{format}")
            return file_path
        except Exception as e:
            logger.error(f"Lỗi khi lưu {name}.{format}: {e}")
            return ""
    
    def generate_city_chart(self, df: pd.DataFrame, output_file: str = "city_distribution.png"):
        """
        Tạo biểu đồ phân bố công việc theo thành phố.
        
        Args:
            df (pd.DataFrame): DataFrame chứa dữ liệu
            output_file (str): Tên file output
            
        Returns:
            str: Đường dẫn đến file biểu đồ
        """
        try:
            plt.figure(figsize=(12, 6))
            sns.barplot(x="job_count", y="city", data=df.head(10))
            plt.title("Top 10 thành phố có nhiều công việc nhất (Lương 10-15 triệu)")
            plt.xlabel("Số lượng công việc")
            plt.ylabel("Thành phố")
            plt.tight_layout()
            
            file_path = os.path.join(self.output_dir, output_file)
            plt.savefig(file_path)
            plt.close()
            
            logger.info(f"Đã tạo biểu đồ: {output_file}")
            return file_path
        except Exception as e:
            logger.error(f"Lỗi khi tạo biểu đồ thành phố: {e}")
            return ""
    
    def generate_skill_chart(self, df: pd.DataFrame, output_file: str = "skill_distribution.png"):
        """
        Tạo biểu đồ phân bố kỹ năng.
        
        Args:
            df (pd.DataFrame): DataFrame chứa dữ liệu
            output_file (str): Tên file output
            
        Returns:
            str: Đường dẫn đến file biểu đồ
        """
        try:
            plt.figure(figsize=(12, 8))
            sns.barplot(x="job_count", y="skill", data=df.head(15))
            plt.title("Top 15 kỹ năng phổ biến nhất (Lương 10-15 triệu)")
            plt.xlabel("Số lượng công việc")
            plt.ylabel("Kỹ năng")
            plt.tight_layout()
            
            file_path = os.path.join(self.output_dir, output_file)
            plt.savefig(file_path)
            plt.close()
            
            logger.info(f"Đã tạo biểu đồ: {output_file}")
            return file_path
        except Exception as e:
            logger.error(f"Lỗi khi tạo biểu đồ kỹ năng: {e}")
            return ""
    
    def generate_deadline_chart(self, df: pd.DataFrame, output_file: str = "deadline_distribution.png"):
        """
        Tạo biểu đồ phân bố deadline.
        
        Args:
            df (pd.DataFrame): DataFrame chứa dữ liệu
            output_file (str): Tên file output
            
        Returns:
            str: Đường dẫn đến file biểu đồ
        """
        try:
            custom_order = ['1-7 ngày', '8-14 ngày', '15-30 ngày', '>30 ngày']
            df['deadline_range'] = pd.Categorical(df['deadline_range'], 
                                                 categories=custom_order, 
                                                 ordered=True)
            df = df.sort_values('deadline_range')
            
            plt.figure(figsize=(10, 6))
            sns.barplot(x="deadline_range", y="job_count", data=df)
            plt.title("Phân bố công việc theo thời gian deadline")
            plt.xlabel("Thời gian còn lại")
            plt.ylabel("Số lượng công việc")
            plt.tight_layout()
            
            file_path = os.path.join(self.output_dir, output_file)
            plt.savefig(file_path)
            plt.close()
            
            logger.info(f"Đã tạo biểu đồ: {output_file}")
            return file_path
        except Exception as e:
            logger.error(f"Lỗi khi tạo biểu đồ deadline: {e}")
            return ""
    
    def generate_full_report(self) -> Dict[str, str]:
        """
        Tạo báo cáo đầy đủ.
        
        Returns:
            Dict[str, str]: Dictionary chứa đường dẫn đến các file báo cáo
        """
        if not self.connect():
            return {}
        
        report_files = {}
        
        try:
            # 1. Top 10 công việc tại Hà Nội
            df_top10 = self.query_to_df("SELECT * FROM vw_top10_hn")
            if not df_top10.empty:
                report_files["top10_hn_csv"] = self.save_dataframe(df_top10, "top10_hn")
                report_files["top10_hn_html"] = self.save_dataframe(df_top10, "top10_hn", "html")
            
            # 2. Phân bố theo thành phố
            df_city = self.query_to_df("SELECT * FROM vw_city_distribution")
            if not df_city.empty:
                report_files["city_distribution_csv"] = self.save_dataframe(df_city, "city_distribution")
                report_files["city_chart"] = self.generate_city_chart(df_city)
            
            # 3. Phân bố theo deadline
            df_deadline = self.query_to_df("SELECT * FROM vw_deadline_distribution")
            if not df_deadline.empty:
                report_files["deadline_distribution_csv"] = self.save_dataframe(df_deadline, "deadline_distribution")
                report_files["deadline_chart"] = self.generate_deadline_chart(df_deadline)
            
            # 4. Kỹ năng phổ biến
            df_skills = self.query_to_df("SELECT * FROM vw_popular_skills")
            if not df_skills.empty:
                report_files["skills_csv"] = self.save_dataframe(df_skills, "popular_skills")
                report_files["skills_chart"] = self.generate_skill_chart(df_skills)
            
            # 5. Công ty đăng tuyển nhiều nhất
            df_companies = self.query_to_df("SELECT * FROM vw_top_companies")
            if not df_companies.empty:
                report_files["companies_csv"] = self.save_dataframe(df_companies, "top_companies")
            
            # 6. Xu hướng theo tháng
            df_trends = self.query_to_df("SELECT * FROM vw_monthly_trends")
            if not df_trends.empty:
                report_files["monthly_trends_csv"] = self.save_dataframe(df_trends, "monthly_trends")
            
            logger.info("Đã tạo báo cáo đầy đủ thành công!")
            
        except Exception as e:
            logger.error(f"Lỗi khi tạo báo cáo: {e}")
        
        finally:
            self.close()
        
        return report_files

def main():
    """Hàm main để chạy trực tiếp."""
    try:
        logger.info("Bắt đầu tạo báo cáo...")
        
        generator = ReportGenerator()
        report_files = generator.generate_full_report()
        
        if report_files:
            logger.info(f"Đã tạo {len(report_files)} file báo cáo")
            for file_type, file_path in report_files.items():
                if file_path:
                    logger.info(f"- {file_type}: {file_path}")
        else:
            logger.warning("Không tạo được báo cáo nào")
        
        logger.info("Kết thúc tạo báo cáo!")
        
    except Exception as e:
        logger.error(f"Lỗi không mong đợi: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
