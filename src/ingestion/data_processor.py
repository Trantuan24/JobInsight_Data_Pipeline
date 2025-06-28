import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, Optional
import os
import sys

# Import modules
try:
    from src.utils.logger import get_logger
    from src.crawler.crawler_utils import parse_last_update
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from src.crawler.crawler_utils import parse_last_update

logger = get_logger("ingestion.data_processor")

def validate_utf8(text: Any) -> Optional[str]:
    """Validate và clean UTF-8 text"""
    if pd.isna(text) or text is None:
        return None
    
    text = str(text)
    try:
        text.encode('utf-8')
        return text
    except UnicodeError:
        # Clean invalid UTF-8
        return text.encode('utf-8', errors='ignore').decode('utf-8')

def prepare_job_data(row: pd.Series, is_update: bool = False) -> Dict[str, Any]:
    """Chuẩn bị dữ liệu job để insert/update"""
    # Basic fields với validation theo thứ tự đúng của schema
    data = {
        'job_id': str(row['job_id']),
        'title': validate_utf8(row.get('title')),
        'job_url': validate_utf8(row.get('job_url')),
        'company_name': validate_utf8(row.get('company_name')),
        'company_url': validate_utf8(row.get('company_url')),
        'salary': validate_utf8(row.get('salary')),
        'skills': [],  # Sẽ được điền sau
        'location': validate_utf8(row.get('location')),
        'location_detail': validate_utf8(row.get('location_detail')),
        'deadline': validate_utf8(row.get('deadline')),
        'verified_employer': bool(row.get('verified_employer', False)),
        'last_update': validate_utf8(row.get('last_update')),
        'logo_url': validate_utf8(row.get('logo_url')),
        'posted_time': None,  # Sẽ được điền sau
        'crawled_at': None    # Sẽ được điền sau
    }
    
    # 1. Skills handling - đảm bảo là một list
    skills = row.get('skills', [])
    if isinstance(skills, str):
        try:
            skills = json.loads(skills) if skills else []
        except Exception as e:
            logger.warning(f"Không thể parse skills JSON từ {skills}: {str(e)}")
            skills = []
    
    # Loại bỏ các skill trống và làm sạch dữ liệu
    data['skills'] = [validate_utf8(s) for s in skills if s and validate_utf8(s)]
    
    # 2. Crawled_at handling
    if 'crawled_at' in row and pd.notna(row['crawled_at']):
        if isinstance(row['crawled_at'], str):
            try:
                data['crawled_at'] = datetime.fromisoformat(row['crawled_at'])
            except Exception as e:
                logger.warning(f"Không thể parse crawled_at từ {row['crawled_at']}: {str(e)}")
                data['crawled_at'] = datetime.now()
        else:
            data['crawled_at'] = row['crawled_at']
    else:
        data['crawled_at'] = datetime.now()
    
    # 3. Posted_time handling - golden source principle: chỉ set khi là job mới
    if not is_update:
        # Trường hợp 1: Đã có posted_time trong data
        if pd.notna(row.get('posted_time')):
            if isinstance(row['posted_time'], str):
                try:
                    data['posted_time'] = datetime.fromisoformat(row['posted_time'])
                except Exception as e:
                    logger.warning(f"Không thể parse posted_time từ {row['posted_time']}: {str(e)}")
                    # Fallback: tính từ last_update
                    if pd.notna(row.get('last_update')):
                        seconds_ago = parse_last_update(row['last_update'])
                        data['posted_time'] = datetime.fromtimestamp(
                            datetime.now().timestamp() - seconds_ago
                        )
                    else:
                        data['posted_time'] = data['crawled_at']
            else:
                data['posted_time'] = row['posted_time']
        # Trường hợp 2: Không có posted_time, tính từ last_update
        elif pd.notna(row.get('last_update')):
            try:
                seconds_ago = parse_last_update(row['last_update'])
                data['posted_time'] = datetime.fromtimestamp(
                    datetime.now().timestamp() - seconds_ago
                )
            except Exception as e:
                logger.warning(f"Không thể tính posted_time từ last_update {row.get('last_update')}: {str(e)}")
                data['posted_time'] = data['crawled_at']
        # Trường hợp 3: Không có thông tin, dùng crawled_at
        else:
            data['posted_time'] = data['crawled_at']
    
    return data 