import sqlite3
import pandas as pd
import random
import os

DB_NAME = 'music_app.db'
CSV_FILE = 'dataset.csv'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Khởi tạo cấu trúc bảng"""
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Bảng Users
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        )
    ''')
    
    # 2. Bảng Songs
    c.execute('''
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            artist TEXT NOT NULL,
            genre TEXT,
            valence REAL DEFAULT 0.5,
            energy REAL DEFAULT 0.5,
            danceability REAL DEFAULT 0.5,
            acousticness REAL DEFAULT 0.5
        )
    ''')
    
    # 3. Bảng Ratings
    c.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            user_id INTEGER,
            song_id INTEGER,
            rating INTEGER CHECK(rating >= 1 AND rating <= 5),
            PRIMARY KEY (user_id, song_id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (song_id) REFERENCES songs (id)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized.")

def seed_from_csv():
    """Đọc dataset.csv và nạp vào Database (đã xử lý lỗi dữ liệu trống)"""
    if not os.path.exists(CSV_FILE):
        print(f"CẢNH BÁO: Không tìm thấy file {CSV_FILE}. Hãy copy file vào thư mục dự án.")
        return

    conn = get_db_connection()
    c = conn.cursor()
    
    # Kiểm tra nếu DB đã có dữ liệu thì thôi
    c.execute('SELECT count(*) FROM songs')
    if c.fetchone()[0] > 0:
        conn.close()
        return

    print("Đang đọc và làm sạch dữ liệu từ CSV...")
    try:
        df = pd.read_csv(CSV_FILE)
        
        # --- SỬA LỖI: Xử lý dữ liệu trống (NaN) ---
        # Điền 'Unknown' vào các ô chữ bị trống
        str_cols = ['track_name', 'song_name', 'artists', 'artist_name', 'track_genre', 'genre']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')

        # Điền 0.5 vào các ô số bị trống
        num_cols = ['valence', 'energy', 'danceability', 'acousticness']
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0.5)
        # ------------------------------------------

        songs_to_insert = []
        for _, row in df.iterrows():
            # Lấy tên cột linh hoạt (ưu tiên track_name, nếu không có thì tìm song_name...)
            title = row.get('track_name') if 'track_name' in df.columns else row.get('song_name', 'Unknown Title')
            artist = row.get('artists') if 'artists' in df.columns else row.get('artist_name', 'Unknown Artist')
            genre = row.get('track_genre') if 'track_genre' in df.columns else row.get('genre', 'Pop')
            
            # Chỉ số mood
            valence = row.get('valence', 0.5)
            energy = row.get('energy', 0.5)
            dance = row.get('danceability', 0.5)
            acoustic = row.get('acousticness', 0.5)
            
            # Kiểm tra lần cuối để chắc chắn title không bị rỗng
            if not title or str(title).strip() == '':
                title = 'Unknown Title'

            songs_to_insert.append((str(title), str(artist), str(genre), valence, energy, dance, acoustic))
            
        print(f"Đang nạp {len(songs_to_insert)} bài hát vào Database...")
        c.executemany('''
            INSERT INTO songs (title, artist, genre, valence, energy, danceability, acousticness) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', songs_to_insert)
        
        # --- Tạo User và Rating giả lập ---
        print("Đang tạo dữ liệu giả lập để test...")
        dummy_users = [('user1', '123'), ('user2', '123'), ('user3', '123')]
        c.executemany('INSERT OR IGNORE INTO users (username, password) VALUES (?, ?)', dummy_users)
        
        c.execute('SELECT id FROM songs')
        all_ids = [r[0] for r in c.fetchall()]
        
        if all_ids:
            dummy_ratings = []
            # Tạo 100 rating ngẫu nhiên
            for _ in range(100):
                uid = random.randint(1, 3)
                sid = random.choice(all_ids)
                rate = random.randint(3, 5)
                dummy_ratings.append((uid, sid, rate))
            c.executemany('INSERT OR REPLACE INTO ratings (user_id, song_id, rating) VALUES (?, ?, ?)', dummy_ratings)
            
        conn.commit()
        print(">>> THÀNH CÔNG: Đã nạp dữ liệu xong!")
        
    except Exception as e:
        print(f"LỖI NGHIÊM TRỌNG KHI ĐỌC CSV: {e}")
    finally:
        conn.close()

# --- CÁC HÀM TRUY VẤN GIỮ NGUYÊN ---
def check_login(username, password):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
    conn.close()
    return user

def create_user(username, password):
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO users (username, password) VALUES (?, ?)', (username, password))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_all_songs():
    conn = get_db_connection()
    songs = conn.execute('SELECT * FROM songs LIMIT 100').fetchall() # Lấy 100 bài thôi cho nhẹ
    conn.close()
    return songs

def get_song_by_id(song_id):
    conn = get_db_connection()
    song = conn.execute('SELECT * FROM songs WHERE id = ?', (song_id,)).fetchone()
    conn.close()
    return song

def get_all_ratings():
    conn = get_db_connection()
    ratings = conn.execute('SELECT user_id, song_id, rating FROM ratings').fetchall()
    conn.close()
    return [(r['user_id'], r['song_id'], r['rating']) for r in ratings]

def add_rating(user_id, song_id, rating):
    conn = get_db_connection()
    conn.execute('INSERT OR REPLACE INTO ratings (user_id, song_id, rating) VALUES (?, ?, ?)', (user_id, song_id, rating))
    conn.commit()
    conn.close()

def get_songs_by_mood_criteria(min_val=0, max_val=1, min_energy=0, max_energy=1, limit=10):
    conn = get_db_connection()
    query = '''
        SELECT * FROM songs 
        WHERE valence BETWEEN ? AND ? 
        AND energy BETWEEN ? AND ?
        ORDER BY RANDOM() LIMIT ?
    '''
    songs = conn.execute(query, (min_val, max_val, min_energy, max_energy, limit)).fetchall()
    conn.close()
    return songs