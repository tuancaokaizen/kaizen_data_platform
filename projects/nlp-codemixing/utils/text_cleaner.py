import re

def clean_text(text: str) -> str:
    if not text:
        return ""
    # Remove excessive whitespaces
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove HTML tags if any
    text = re.sub(r'<[^>]*>', '', text)
    return text
