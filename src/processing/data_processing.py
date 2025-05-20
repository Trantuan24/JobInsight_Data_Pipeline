from bs4 import BeautifulSoup
import pandas as pd
import re

# Xử lý cột location_detail
def extract_location_info(html_content):
    """
    Phân tích thông tin location_detail từ HTML
    
    Args:
        html_content: Nội dung HTML của location_detail
        
    Returns:
        List: Danh sách thông tin địa điểm đã trích xuất
    """
    if pd.isna(html_content):
        return []

    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n")

    results = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            parts = line.split(":", 1)
            key = parts[0].strip()
            value = parts[1].strip()
            if key and value:
                results.append(f"{key}: {value}")
        else:
            results.append(line)
    return results

# Xử lý cột location
def refine_location(row):
    """
    Tinh chỉnh thông tin location dựa trên location_pairs
    
    Args:
        row: Dòng dữ liệu từ DataFrame
        
    Returns:
        str: Thông tin location đã tinh chỉnh
    """
    location = row["location"]
    pairs = row["location_pairs"]

    # Kiểm tra nếu chứa dấu "&" và có location_pairs
    if "&" in str(location) and isinstance(pairs, list) and pairs:
        refined = []
        seen = set()

        for item in pairs:
            if ":" in item:
                city = item.split(":", 1)[0].strip()
            else:
                city = item.strip()

            if city and city not in seen:
                refined.append(city)
                seen.add(city)

        return ", ".join(refined)
    
    # Không chứa dấu "&" thì giữ nguyên
    return location

# Xử lý cột title
def clean_title(title):
    """
    Làm sạch tiêu đề công việc
    
    Args:
        title: Tiêu đề công việc gốc
        
    Returns:
        str: Tiêu đề đã làm sạch
    """
    if pd.isna(title):
        return ""
        
    matches = re.search(r'([\w\s./-]+(?:\s*(?:\/|-)\s*[\w\s./-]*)*)', title)
    if matches:
        cleaned_title = matches.group(1).strip()
        cleaned_title = cleaned_title.split(' - ')[0].strip()
    else:
        cleaned_title = title.strip()

    return cleaned_title

def clean_company_name(title):
    """
    Chuẩn hóa tiêu đề công việc, giữ lại thông tin công nghệ quan trọng
    
    Args:
        title: Tiêu đề công việc gốc
        
    Returns:
        str: Tiêu đề đã chuẩn hóa
    """
    if pd.isna(title):
        return ""
    
    # Loại bỏ các ký tự đặc biệt không cần thiết nhưng giữ lại dấu ngoặc và gạch nối
    title = re.sub(r'[^\w\s\(\)\[\]\-\/\.,&+#]', ' ', title)
    
    # Loại bỏ các khoảng trắng thừa
    title = re.sub(r'\s+', ' ', title).strip()
    
    # Xóa các từ khóa tuyển dụng không cần thiết
    remove_patterns = [
        r'tuyển\s+dụng',
        r'cần\s+tuyển', 
        r'đang\s+tuyển',
        r'hot',
        r'gấp',
        r'\bhr\b',
    ]
    
    for pattern in remove_patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)
    
    # Chuẩn hóa viết hoa cho từ đầu tiên và sau dấu gạch nối
    parts = re.split(r'(\s*[\-\/]\s*)', title)
    result = []
    
    for i, part in enumerate(parts):
        if i % 2 == 0:  # Phần text, không phải separator
            # Giữ nguyên các từ viết hoa (ngôn ngữ lập trình, công nghệ)
            words = part.split()
            for j, word in enumerate(words):
                # Kiểm tra xem có phải từ công nghệ đặc biệt không
                tech_words = ['PHP', 'Java', 'Python', 'AWS', 'SQL', 'C#', 'C++', '.NET', 
                             'HTML', 'CSS', 'JS', 'UI', 'UX', 'AI', 'ML', 'iOS', 'API',
                             'React', 'Vue', 'Angular', 'Node', 'DevOps', 'QA', 'BA']
                
                if word.upper() in tech_words:
                    words[j] = word.upper()
                elif j == 0:  # Từ đầu tiên
                    words[j] = word.capitalize()
            
            result.append(' '.join(words))
        else:  # Giữ nguyên separator
            result.append(part)
    
    title = ''.join(result).strip()
    
    # Xóa các khoảng trắng thừa cuối cùng
    title = re.sub(r'\s+', ' ', title)
    
    return title.strip()