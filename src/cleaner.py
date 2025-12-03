"""
cleaner.py - Очистка данных манги (исправленная версия с обработкой обрывов)
"""
import pandas as pd
import json
import re
import os
from datetime import datetime

def fix_truncated_description(desc):
    """Исправляет обрезанные описания"""
    if not isinstance(desc, str):
        return ""
    
    desc = desc.strip()
    if len(desc) < 100:
        return desc

    if desc and desc[-1] in ['.', '!', '?', '…']:
        return desc
    
    for i in range(len(desc) - 1, -1, -1):
        if desc[i] in ['.', '!', '?', '…']:
            # Проверяем, что после знака препинания идет пробел или конец строки
            if i == len(desc) - 1 or desc[i+1] in [' ', '\n', '"', "'"]:
                return desc[:i+1]

    search_area = desc[max(0, len(desc)-50):]
    last_space = search_area.rfind(' ')
    
    if last_space != -1:
        abs_pos = max(0, len(desc)-50) + last_space
        return desc[:abs_pos] + "..."
    
    return desc

def clean_manga_data():

    with open("data/raw_manga.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    print(f"📥 Загружено: {len(df)} записей")
    
    before = len(df)
    df = df.drop_duplicates(subset=["url"])
    after = len(df)
    print(f"🧹 Удалено дубликатов: {before - after}")

    df["description"] = df["description"].fillna("")
    df["year"] = df["year"].fillna("")
    df["rating"] = df["rating"].fillna("0.0")
    df["cover_url"] = df["cover_url"].fillna("")

    str_cols = ["title", "description"]
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    
    
    df["title"] = df["title"].str.title()
    
  
    df["description"] = df["description"].str.replace(r'^"|"$', '', regex=True)
    
  
    df["description"] = df["description"].apply(fix_truncated_description)

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    
 
    df["has_cover"] = df["cover_url"] != ""
    
    def extract_id(url):
        match = re.search(r'/manga/([^/]+)/', url)
        return match.group(1) if match else None
    
    df["manga_id"] = df["url"].apply(extract_id)

    df = df[df["title"].notna() & (df["title"] != "")]
    df = df[df["url"].str.startswith("https://remanga.org/")]

    print("\nАнализ описаний:")
    df["desc_length"] = df["description"].str.len()
    avg_len = df["desc_length"].mean()
    print(f"   Средняя длина: {avg_len:.0f} символов")

    df["is_truncated"] = df["description"].apply(
        lambda x: isinstance(x, str) and len(x) > 100 and x[-1] not in ['.', '!', '?', '…']
    )
    truncated = df["is_truncated"].sum()
    print(f"   Возможно обрезанных: {truncated} из {len(df)}")

    df = df.drop(columns=["desc_length", "is_truncated"])

    os.makedirs("data", exist_ok=True)
    
    output_cols = ["manga_id", "title", "description", "year", "rating", 
                   "has_cover", "cover_url", "url", "scraped_at"]
    output_cols = [col for col in output_cols if col in df.columns]
    
    with open("data/cleaned_manga.json", "w", encoding="utf-8") as f:
        json.dump(df[output_cols].to_dict(orient="records"), 
                  f, ensure_ascii=False, indent=2, default=str)
    
    df[output_cols].to_csv("data/cleaned_manga.csv", index=False, encoding="utf-8")
    
    print(f"\nСтатистика после очистки:")
    print(f"   Сохранено записей: {len(df)}")
    print(f"   Годы: {df['year'].min()} - {df['year'].max()}")
    print(f"   Средний рейтинг: {df['rating'].mean():.2f}")
    print(f"   Есть обложки: {df['has_cover'].sum()} из {len(df)}")
    
    return df

if __name__ == "__main__":
    print("🚀 ЗАПУСК ОЧИСТКИ ДАННЫХ")
    print("="*50)
    
    if not os.path.exists("data/raw_manga.json"):
        print(" Файл data/raw_manga.json не найден")
        print("   Запустите сначала: python scraper.py")
    else:
        clean_manga_data()
        print("\n ОЧИСТКА ЗАВЕРШЕНА!")