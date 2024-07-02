from flask import Flask, request, jsonify
import pickle
import requests

app = Flask(__name__)

# Load data
movies = pickle.load(open("movies_list.pkl", 'rb'))
similarity = pickle.load(open("similarity.pkl", 'rb'))

# TMDB API key
API_KEY = "b8b76ccfa61c6e85ca7e096d905a7d63"


def fetch_movie_data(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&language=en-US"
    response = requests.get(url)
    data = response.json()

    credits_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={API_KEY}&language=en-US"
    credits_response = requests.get(credits_url)
    credits_data = credits_response.json()

    return {
        "title": data.get('title'),
        "poster": f"https://image.tmdb.org/t/p/w500{data.get('poster_path')}",
        "overview": data.get('overview', 'Overview not available'),
        "rating": f"{data.get('vote_average', 'Not rated')}/10 ({data.get('vote_count', 0)} votes)",
        "cast": ", ".join([actor['name'] for actor in credits_data.get('cast', [])[:5]])
    }


def recommend(movie):
    index = movies[movies['title'] == movie].index[0]
    distances = sorted(
        list(enumerate(similarity[index])), reverse=True, key=lambda x: x[1])
    recommended_movies = []
    for i in distances[1:6]:
        movie_id = movies.iloc[i[0]].id
        recommended_movies.append(fetch_movie_data(movie_id))
    return recommended_movies


@app.route('/')
def home():
    return "Now Showing Movie Recommender"


@app.route('/recommend', methods=['POST'])
def get_recommendations():
    data = request.json
    movie = data.get('movie')
    if not movie:
        return jsonify({"error": "No movie provided"}), 400

    try:
        recommendations = recommend(movie)
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/movie/<int:movie_id>', methods=['GET'])
def get_movie_details(movie_id):
    try:
        movie_data = fetch_movie_data(movie_id)
        return jsonify(movie_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/movies', methods=['GET'])
def get_movies_list():
    return jsonify(movies['title'].tolist())


if __name__ == '__main__':
    app.run(debug=True)
