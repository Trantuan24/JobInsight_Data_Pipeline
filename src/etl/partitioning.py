#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module quản lý partitioning và lưu trữ dạng Parquet
"""
import pandas as pd
import logging
import os
import sys
import duckdb
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Thiết lập logging
logger = logging.getLogger(__name__)

class PartitionManager:
    """
    Class quản lý partitioning và lưu trữ dạng Parquet
    """
    
    def __init__(self, duck_conn: duckdb.DuckDBPyConnection, parquet_dir: str = None):
        """
        Khởi tạo PartitionManager
        
        Args:
            duck_conn: Kết nối DuckDB
            parquet_dir: Thư mục lưu file Parquet
        """
        self.duck_conn = duck_conn
        
        # Thiết lập thư mục lưu file Parquet
        if parquet_dir is None:
            self.parquet_dir = os.path.join(PROJECT_ROOT, "data", "parquet")
        else:
            self.parquet_dir = parquet_dir
            
        # Đảm bảo thư mục tồn tại
        os.makedirs(self.parquet_dir, exist_ok=True)
    
    def create_partition(self, table_name: str, partition_column: str, partition_value: str) -> bool:
        """
        Tạo partition cho một bảng
        
        Args:
            table_name: Tên bảng
            partition_column: Tên cột partition
            partition_value: Giá trị partition
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Tạo thư mục cho partition
            partition_dir = os.path.join(self.parquet_dir, table_name, f"{partition_column}={partition_value}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Tạo view cho partition - thay thế dấu gạch ngang bằng dấu gạch dưới trong tên view
            safe_partition_value = partition_value.replace('-', '_')
            view_name = f"{table_name}_{partition_column}_{safe_partition_value}"
            
            view_query = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM {table_name}
                WHERE {partition_column} = '{partition_value}'
            """
            self.duck_conn.execute(view_query)
            
            logger.info(f"Đã tạo partition {partition_column}={partition_value} cho bảng {table_name}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tạo partition {partition_column}={partition_value} cho bảng {table_name}: {e}")
            return False
    
    def export_to_parquet(self, table_name: str, partition_column: str, partition_value: str) -> bool:
        """
        Export dữ liệu từ partition sang file Parquet
        
        Args:
            table_name: Tên bảng
            partition_column: Tên cột partition
            partition_value: Giá trị partition
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Tạo thư mục cho partition
            partition_dir = os.path.join(self.parquet_dir, table_name, f"{partition_column}={partition_value}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Tên file Parquet
            parquet_file = os.path.join(partition_dir, f"{table_name}_{partition_value}.parquet")
            
            # Export dữ liệu sang Parquet
            export_query = f"""
                COPY (
                    SELECT * FROM {table_name}
                    WHERE {partition_column} = '{partition_value}'
                ) TO '{parquet_file}' (FORMAT 'parquet')
            """
            self.duck_conn.execute(export_query)
            
            # Kiểm tra file đã được tạo chưa
            if os.path.exists(parquet_file):
                file_size = os.path.getsize(parquet_file) / (1024 * 1024)  # MB
                logger.info(f"Đã export partition {partition_column}={partition_value} sang {parquet_file} ({file_size:.2f} MB)")
                return True
            else:
                logger.warning(f"Không tìm thấy file Parquet sau khi export: {parquet_file}")
                return False
        except Exception as e:
            logger.error(f"Lỗi khi export partition {partition_column}={partition_value} sang Parquet: {e}")
            return False
    
    def manage_partitions(self, table_name: str = 'FactJobPostingDaily', partition_column: str = 'load_month') -> Dict[str, Any]:
        """
        Quản lý tất cả các partition của một bảng
        
        Args:
            table_name: Tên bảng
            partition_column: Tên cột partition
            
        Returns:
            Dict kết quả
        """
        try:
            # Lấy danh sách các giá trị partition
            query = f"""
                SELECT DISTINCT {partition_column}
                FROM {table_name}
                ORDER BY {partition_column}
            """
            partition_values = [row[0] for row in self.duck_conn.execute(query).fetchall()]
            
            if not partition_values:
                logger.warning(f"Không tìm thấy giá trị partition nào cho {table_name}.{partition_column}")
                return {
                    'success': False,
                    'message': f"Không tìm thấy giá trị partition nào cho {table_name}.{partition_column}",
                    'partitions': []
                }
            
            # Tạo và export partition
            results = []
            for value in partition_values:
                # Tạo partition
                create_result = self.create_partition(table_name, partition_column, value)
                
                # Export sang Parquet
                export_result = self.export_to_parquet(table_name, partition_column, value)
                
                results.append({
                    'partition_value': value,
                    'create_success': create_result,
                    'export_success': export_result
                })
            
            # Tóm tắt kết quả
            success_count = sum(1 for r in results if r['create_success'] and r['export_success'])
            
            logger.info(f"Đã quản lý {success_count}/{len(results)} partition cho {table_name}.{partition_column}")
            
            return {
                'success': success_count == len(results),
                'message': f"Đã quản lý {success_count}/{len(results)} partition cho {table_name}.{partition_column}",
                'partitions': results
            }
        except Exception as e:
            logger.error(f"Lỗi khi quản lý partition cho {table_name}.{partition_column}: {e}")
            return {
                'success': False,
                'message': f"Lỗi khi quản lý partition: {str(e)}",
                'partitions': []
            }
    
    def load_from_parquet(self, table_name: str, partition_column: str, partition_value: str) -> bool:
        """
        Load dữ liệu từ file Parquet vào DuckDB
        
        Args:
            table_name: Tên bảng
            partition_column: Tên cột partition
            partition_value: Giá trị partition
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Đường dẫn đến file Parquet
            partition_dir = os.path.join(self.parquet_dir, table_name, f"{partition_column}={partition_value}")
            parquet_file = os.path.join(partition_dir, f"{table_name}_{partition_value}.parquet")
            
            # Kiểm tra file tồn tại
            if not os.path.exists(parquet_file):
                logger.warning(f"Không tìm thấy file Parquet: {parquet_file}")
                return False
            
            # Tạo bảng tạm thời
            temp_table = f"{table_name}_temp_{partition_value}"
            create_temp_query = f"""
                CREATE OR REPLACE TABLE {temp_table} AS
                SELECT * FROM read_parquet('{parquet_file}')
            """
            self.duck_conn.execute(create_temp_query)
            
            # Xóa dữ liệu cũ
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE {partition_column} = '{partition_value}'
            """
            self.duck_conn.execute(delete_query)
            
            # Insert dữ liệu mới
            insert_query = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table}
            """
            self.duck_conn.execute(insert_query)
            
            # Xóa bảng tạm thời
            self.duck_conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
            # Đếm số bản ghi đã load
            count_query = f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {partition_column} = '{partition_value}'
            """
            count = self.duck_conn.execute(count_query).fetchone()[0]
            
            logger.info(f"Đã load {count} bản ghi từ {parquet_file} vào {table_name}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi load dữ liệu từ Parquet: {e}")
            return False
            
    def cleanup_old_partitions(self, months_to_keep: int = 12) -> Dict[str, Any]:
        """
        Dọn dẹp các file Parquet cũ hơn số tháng chỉ định
        
        Args:
            months_to_keep: Số tháng dữ liệu Parquet cần giữ lại, mặc định là 12 tháng
            
        Returns:
            Dict[str, Any]: Thống kê về số lượng thư mục, file đã xóa và lỗi nếu có
        """
        logger.info(f"Bắt đầu dọn dẹp các file Parquet cũ hơn {months_to_keep} tháng")
        
        stats = {
            'dirs_removed': 0, 
            'files_removed': 0, 
            'errors': 0,
            'bytes_freed': 0,
            'tables_processed': 0
        }
        
        try:
            # Tính thời điểm giới hạn
            cutoff_date = datetime.now() - timedelta(days=months_to_keep * 30)  # Xấp xỉ số ngày
            
            # Quét qua các thư mục bảng
            if os.path.exists(self.parquet_dir):
                for table_name in os.listdir(self.parquet_dir):
                    table_path = os.path.join(self.parquet_dir, table_name)
                    
                    # Bỏ qua nếu không phải thư mục
                    if not os.path.isdir(table_path):
                        continue
                    
                    stats['tables_processed'] += 1
                    
                    # Quét qua các thư mục partition
                    for partition_dir in os.listdir(table_path):
                        partition_path = os.path.join(table_path, partition_dir)
                        
                        # Bỏ qua nếu không phải thư mục
                        if not os.path.isdir(partition_path):
                            continue
                        
                        try:
                            # Kiểm tra xem có phải partition theo tháng không
                            if partition_dir.startswith('load_month='):
                                # Lấy giá trị partition (YYYY-MM)
                                partition_value = partition_dir.split('=')[1]
                                
                                try:
                                    # Parse ngày từ giá trị partition
                                    partition_date = datetime.strptime(partition_value, "%Y-%m")
                                    
                                    # Nếu partition cũ hơn thời điểm giới hạn, xóa toàn bộ
                                    if partition_date < cutoff_date:
                                        # Tính kích thước thư mục trước khi xóa
                                        dir_size = sum(os.path.getsize(os.path.join(root, file)) 
                                                for root, dirs, files in os.walk(partition_path) 
                                                for file in files)
                                        
                                        # Đếm số file bị xóa
                                        file_count = sum(len(files) for _, _, files in os.walk(partition_path))
                                        
                                        # Xóa thư mục và toàn bộ nội dung
                                        shutil.rmtree(partition_path)
                                        
                                        stats['dirs_removed'] += 1
                                        stats['files_removed'] += file_count
                                        stats['bytes_freed'] += dir_size
                                        
                                        logger.info(f"Đã xóa partition Parquet: {partition_path} ({file_count} files, {dir_size} bytes)")
                                except ValueError as e:
                                    # Lỗi parse ngày từ giá trị partition
                                    logger.warning(f"Không thể xác định ngày từ partition: {partition_path}, lỗi: {str(e)}")
                                    stats['errors'] += 1
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý partition {partition_path}: {str(e)}")
                            stats['errors'] += 1
                    
                    # Kiểm tra và xóa các thư mục bảng rỗng
                    try:
                        if os.path.exists(table_path) and not os.listdir(table_path):
                            os.rmdir(table_path)
                            logger.info(f"Đã xóa thư mục bảng rỗng: {table_path}")
                    except Exception as e:
                        logger.error(f"Lỗi khi xóa thư mục bảng rỗng {table_path}: {str(e)}")
                        stats['errors'] += 1
            
            # Format kích thước để dễ đọc
            freed_mb = stats['bytes_freed'] / (1024 * 1024)
            logger.info(f"Hoàn tất dọn dẹp Parquet: Đã xử lý {stats['tables_processed']} bảng, xóa {stats['dirs_removed']} partition, "
                       f"{stats['files_removed']} files ({freed_mb:.2f} MB), {stats['errors']} lỗi")
            
            return stats
        except Exception as e:
            logger.error(f"Lỗi trong quá trình dọn dẹp Parquet: {str(e)}")
            stats['errors'] += 1
            return stats 