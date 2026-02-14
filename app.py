from flask import Flask, render_template, request, redirect, url_for, session, flash
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import db_manager

app = Flask(__name__)
app.secret_key = 'secret_key_nhom_nhac'

# ----------------------------------------------------------------
# LOGIC 1: COLLABORATIVE FILTERING (MỚI - Dựa trên Rating thật)
# ----------------------------------------------------------------
def get_collaborative_recommendations(current_user_id, n_recommendations=5):
    ratings_data = db_manager.get_all_ratings()
    if not ratings_data:
        return []

    df_ratings = pd.DataFrame(ratings_data, columns=['user_id', 'song_id', 'rating'])

    if current_user_id not in df_ratings['user_id'].values:
        return [] # User mới chưa có rating

    # Tạo User-Item Matrix
    user_item_matrix = df_ratings.pivot_table(index='user_id', columns='song_id', values='rating').fillna(0)
    
    # Tính Cosine Similarity
    user_similarity = cosine_similarity(user_item_matrix)
    user_sim_df = pd.DataFrame(user_similarity, index=user_item_matrix.index, columns=user_item_matrix.index)

    # Tìm user giống nhất
    similar_users = user_sim_df[current_user_id].sort_values(ascending=False).drop(current_user_id)
    top_similar_users = similar_users.head(5)

    recommended_songs = {}
    user_rated_songs = user_item_matrix.loc[current_user_id]
    songs_already_listened = user_rated_songs[user_rated_songs > 0].index.tolist()

    for other_user_id, score in top_similar_users.items():
        other_user_ratings = user_item_matrix.loc[other_user_id]
        good_songs = other_user_ratings[other_user_ratings >= 4].index.tolist() # Lấy bài rating >= 4
        
        for song_id in good_songs:
            if song_id not in songs_already_listened:
                if song_id not in recommended_songs:
                    recommended_songs[song_id] = score * other_user_ratings[song_id]
                else:
                    recommended_songs[song_id] += score * other_user_ratings[song_id]

    # Sort và lấy thông tin bài hát
    sorted_recs = sorted(recommended_songs.items(), key=lambda x: x[1], reverse=True)[:n_recommendations]
    
    results = []
    for song_id, _ in sorted_recs:
        song = db_manager.get_song_by_id(song_id)
        if song:
            results.append(song)
    return results

# ----------------------------------------------------------------
# LOGIC 2: MOOD BASED RECOMMENDATION (NHƯ CŨ - Dựa trên CSV features)
# ----------------------------------------------------------------
def recommend_by_mood(mood):
    """
    Ánh xạ tâm trạng sang các chỉ số Valence (tích cực) và Energy (năng lượng)
    Dữ liệu này đã được nạp từ CSV vào DB ở cột valence, energy.
    """
    if mood == 'Happy':     # Vui: Tích cực cao, Năng lượng cao
        return db_manager.get_songs_by_mood_criteria(min_val=0.6, max_val=1.0, min_energy=0.6, max_energy=1.0)
    
    elif mood == 'Sad':     # Buồn: Tích cực thấp, Năng lượng thấp
        return db_manager.get_songs_by_mood_criteria(min_val=0.0, max_val=0.4, min_energy=0.0, max_energy=0.4)
    
    elif mood == 'Chill':   # Thư giãn: Tích cực trung bình/cao, Năng lượng thấp
        return db_manager.get_songs_by_mood_criteria(min_val=0.5, max_val=1.0, min_energy=0.0, max_energy=0.5)
    
    elif mood == 'Energetic': # Sôi động: Năng lượng rất cao
        return db_manager.get_songs_by_mood_criteria(min_val=0.0, max_val=1.0, min_energy=0.7, max_energy=1.0)
    
    else: # Mặc định (Random)
        return db_manager.get_songs_by_mood_criteria(min_val=0, max_val=1, min_energy=0, max_energy=1)

# ----------------------------------------------------------------
# ROUTES
# ----------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def home():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    username = session['username']
    
    # 1. Mặc định hiển thị gợi ý Collaborative Filtering
    collab_recommendations = get_collaborative_recommendations(user_id)
    mood_recommendations = []
    selected_mood = None

    # 2. Nếu người dùng chọn Mood từ form
    if request.method == 'POST':
        selected_mood = request.form.get('mood')
        if selected_mood:
            mood_recommendations = recommend_by_mood(selected_mood)

    # 3. Danh sách tất cả bài hát (để khám phá/đánh giá thêm)
    all_songs = db_manager.get_all_songs()

    return render_template('index.html', 
                           username=username,
                           collab_recommendations=collab_recommendations,
                           mood_recommendations=mood_recommendations,
                           selected_mood=selected_mood,
                           all_songs=all_songs)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = db_manager.check_login(username, password)
        if user:
            session['user_id'] = user[0]
            session['username'] = user[1]
            return redirect(url_for('home'))
        else:
            flash('Sai thông tin đăng nhập')
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if db_manager.create_user(username, password):
            flash('Đăng ký thành công')
            return redirect(url_for('login'))
        else:
            flash('Tên đăng nhập đã tồn tại')
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/rate_song/<int:song_id>', methods=['POST'])
def rate_song(song_id):
    if 'user_id' in session:
        rating = int(request.form['rating'])
        db_manager.add_rating(session['user_id'], song_id, rating)
        flash('Đã lưu đánh giá!')
    return redirect(url_for('home'))

if __name__ == '__main__':
    # Xóa file music_app.db CŨ trước khi chạy lần đầu để đảm bảo cột mood được tạo mới
    db_manager.init_db()
    db_manager.seed_from_csv() # Đọc dataset.csv vào DB
    app.run(debug=True)