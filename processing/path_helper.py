import os


def get_price_file_path(filename: str) -> str:
    """
    Повертає абсолютний шлях до файлу прайсу в папці price/current
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "../price/current", filename)
