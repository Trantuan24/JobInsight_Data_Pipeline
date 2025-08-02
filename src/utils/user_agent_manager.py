#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module quản lý User-Agent cho crawler
"""

import random
import os
import sys


# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config, CRAWLER_CONFIG
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("utils.user_agent_manager")

class UserAgentManager:
    """
    Class quản lý User-Agent cho crawler
    - Tách logic user-agent ra thành một class riêng
    - Hỗ trợ random UA theo device type
    - Hỗ trợ tải từ config
    """
    
    def __init__(self, desktop_agents=None, mobile_agents=None):
        """
        Khởi tạo UserAgentManager
        
        Args:
            desktop_agents: List các User-Agent desktop
            mobile_agents: List các User-Agent mobile
        """
        self.desktop_agents = desktop_agents or []
        self.mobile_agents = mobile_agents or []
        
        if not self.desktop_agents:
            self._add_default_desktop_agents()
            
        if not self.mobile_agents:
            self._add_default_mobile_agents()
            
        logger.info(f"Khởi tạo UserAgentManager với {len(self.desktop_agents)} desktop agents và {len(self.mobile_agents)} mobile agents")
    
    def get_random_agent(self, device_type=None):
        """
        Lấy User-Agent ngẫu nhiên theo loại thiết bị
        
        Args:
            device_type: Loại thiết bị ('desktop', 'mobile' hoặc None để random)
            
        Returns:
            str: User-Agent string
        """
        if device_type is None:
            # 80% desktop, 20% mobile để giảm detection (mobile dễ bị phát hiện hơn)
            # Thêm randomness cao hơn để tránh pattern
            if random.random() < 0.8:
                return random.choice(self.desktop_agents) if self.desktop_agents else ""
            else:
                return random.choice(self.mobile_agents) if self.mobile_agents else ""
        elif device_type.lower() == 'desktop':
            return random.choice(self.desktop_agents) if self.desktop_agents else ""
        elif device_type.lower() == 'mobile':
            return random.choice(self.mobile_agents) if self.mobile_agents else ""
        else:
            logger.warning(f"Device type không hợp lệ: {device_type}, sử dụng random")
            all_agents = self.desktop_agents + self.mobile_agents
            return random.choice(all_agents) if all_agents else ""
    
    def is_mobile(self, user_agent):
        """
        Kiểm tra User-Agent có phải mobile không
        
        Args:
            user_agent: User-Agent string cần kiểm tra
            
        Returns:
            bool: True nếu là mobile User-Agent
        """
        return "Mobile" in user_agent or "Android" in user_agent or "iPhone" in user_agent
    
    def get_viewport(self, user_agent):
        """
        Lấy viewport size phù hợp với User-Agent với randomization để tránh fingerprinting

        Args:
            user_agent: User-Agent string

        Returns:
            dict: Viewport size {'width': width, 'height': height}
        """
        if self.is_mobile(user_agent):
            # Random mobile viewport sizes để tránh detection
            mobile_sizes = [
                {'width': 390, 'height': 844},  # iPhone 12/13
                {'width': 414, 'height': 896},  # iPhone 11/XR
                {'width': 375, 'height': 812},  # iPhone X/XS
                {'width': 360, 'height': 800},  # Samsung Galaxy
            ]
            return random.choice(mobile_sizes)
        else:
            # Random desktop viewport sizes để tránh detection
            desktop_sizes = [
                {'width': 1920, 'height': 1080},  # Full HD
                {'width': 1366, 'height': 768},   # Common laptop
                {'width': 1440, 'height': 900},   # MacBook
                {'width': 1536, 'height': 864},   # Surface
                {'width': 1280, 'height': 720},   # HD
            ]
            return random.choice(desktop_sizes)
    
    def _add_default_desktop_agents(self):
        """Thêm các desktop User-Agent mặc định"""
        self.desktop_agents.extend([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        ])
    
    def _add_default_mobile_agents(self):
        """Thêm các mobile User-Agent mặc định"""
        self.mobile_agents.extend([
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
        ])
    
    @classmethod
    def from_config(cls, user_agents_config=None):
        """
        Tạo UserAgentManager từ config
        
        Args:
            user_agents_config: Dict config có chứa desktop_agents và mobile_agents
            
        Returns:
            UserAgentManager: Instance mới
        """
        if user_agents_config is None:
            try:
                # Thử sử dụng Config mới
                user_agents = {
                    'desktop': [],
                    'mobile': []
                }
                
                # Kiểm tra Config mới
                try:
                    # TODO: Bổ sung Config.Crawler.USER_AGENTS trong tương lai
                    pass
                except (NameError, AttributeError):
                    # Fallback về CRAWLER_CONFIG cũ
                    try:
                        from src.utils.config import CRAWLER_CONFIG
                        
                        # Kiểm tra nếu có user agents trong config
                        if 'user_agents' in CRAWLER_CONFIG:
                            ua_config = CRAWLER_CONFIG['user_agents']
                            user_agents['desktop'] = ua_config.get('desktop', [])
                            user_agents['mobile'] = ua_config.get('mobile', [])
                    except ImportError:
                        logger.warning("Không thể import CRAWLER_CONFIG, sử dụng UA mặc định")
                
                # Nếu có user agents, tạo instance
                if user_agents['desktop'] or user_agents['mobile']:
                    return cls(user_agents['desktop'], user_agents['mobile'])
                
            except Exception as e:
                logger.warning(f"Lỗi khi tải user agents từ config: {str(e)}")
        
        # Nếu không có config, dùng default
        return cls()


if __name__ == "__main__":
    # Test UserAgentManager
    ua_manager = UserAgentManager()
    print("Random UA:", ua_manager.get_random_agent())
    print("Desktop UA:", ua_manager.get_random_agent('desktop'))
    print("Mobile UA:", ua_manager.get_random_agent('mobile')) 