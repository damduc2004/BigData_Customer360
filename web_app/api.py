from flask import Flask, jsonify
from flask_cors import CORS
import pymysql

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

DB = dict(host="localhost", port=3306, user="root",
          password="123456", database="bigdata",
          cursorclass=pymysql.cursors.DictCursor, connect_timeout=5)

def query(sql, args=None):
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        conn.close()

@app.route("/")
def index():
    return app.send_static_file("index.html")

# CONTENT APIs ──────────────────────────────────────────────────────────────────
@app.route("/api/content/kpi")
def content_kpi():
    rows = query("""
        SELECT
            COUNT(*) AS total_contracts,
            SUM(Active = 'High') AS active_high,
            ROUND(AVG(Total_Devices), 1) AS avg_devices,
            (SELECT MostWatch FROM customer_content_stats GROUP BY MostWatch ORDER BY COUNT(*) DESC LIMIT 1) AS top_mostwatch
        FROM customer_content_stats
    """)
    r = rows[0]
    r["active_high_pct"] = round(r["active_high"] / r["total_contracts"] * 100, 1)
    return jsonify(r)

@app.route("/api/content/mostwatch")
def content_mostwatch():
    rows = query("SELECT MostWatch AS label, COUNT(*) AS value FROM customer_content_stats GROUP BY MostWatch ORDER BY value DESC")
    return jsonify(rows)

@app.route("/api/content/watchtime")
def content_watchtime():
    rows = query("""
        SELECT
            ROUND(SUM(Total_Truyen_Hinh)/3600) AS truyen_hinh,
            ROUND(SUM(Total_Phim_Truyen)/3600) AS phim_truyen,
            ROUND(SUM(Total_Giai_Tri)/3600) AS giai_tri,
            ROUND(SUM(Total_The_Thao)/3600) AS the_thao,
            ROUND(SUM(Total_Thieu_Nhi)/3600) AS thieu_nhi
        FROM customer_content_stats
    """)
    r = rows[0]
    return jsonify([
        {"label": "Truyền Hình", "value": r["truyen_hinh"]},
        {"label": "Phim Truyện", "value": r["phim_truyen"]},
        {"label": "Giải Trí",    "value": r["giai_tri"]},
        {"label": "Thể Thao",    "value": r["the_thao"]},
        {"label": "Thiếu Nhi",   "value": r["thieu_nhi"]},
    ])

@app.route("/api/content/active")
def content_active():
    rows = query("SELECT Active AS label, COUNT(*) AS value FROM customer_content_stats GROUP BY Active")
    return jsonify(rows)

@app.route("/api/content/taste")
def content_taste():
    rows = query("SELECT Taste AS label, COUNT(*) AS value FROM customer_content_stats GROUP BY Taste ORDER BY value DESC LIMIT 5")
    return jsonify(rows)

@app.route("/api/content/segment")
def content_segment():
    rows = query("""
        SELECT
            CASE
                WHEN Total_Devices > 3 AND Total_Thieu_Nhi > 0 THEN 'Family'
                WHEN Active = 'High' AND (
                    (Total_Truyen_Hinh > 0) + (Total_Phim_Truyen > 0) +
                    (Total_The_Thao > 0) + (Total_Thieu_Nhi > 0) +
                    (Total_Giai_Tri > 0)
                ) >= 3 THEN 'Explorer'
                WHEN Active = 'High' THEN 'Heavy Viewer'
                ELSE 'Light User'
            END AS label,
            COUNT(*) AS value
        FROM customer_content_stats
        GROUP BY label ORDER BY value DESC
    """)
    return jsonify(rows)

# SEARCH APIs ───────────────────────────────────────────────────────────────────
@app.route("/api/search/kpi")
def search_kpi():
    rows = query("""
        SELECT
            COUNT(*) AS total_users,
            SUM(Genre_T6 = Genre_T7) AS unchanged,
            SUM(Genre_T6 != Genre_T7) AS changed
        FROM customer_search_stats
        WHERE Genre_T6 IS NOT NULL AND Genre_T7 IS NOT NULL
    """)
    r = rows[0]
    r["unchanged_pct"] = round(r["unchanged"] / r["total_users"] * 100, 1)
    r["changed_pct"] = round(r["changed"] / r["total_users"] * 100, 1)
    return jsonify(r)

@app.route("/api/search/genre")
def search_genre():
    t6 = query("SELECT Genre_T6 AS label, COUNT(*) AS value FROM customer_search_stats WHERE Genre_T6 IS NOT NULL GROUP BY Genre_T6 ORDER BY value DESC")
    t7 = query("SELECT Genre_T7 AS label, COUNT(*) AS value FROM customer_search_stats WHERE Genre_T7 IS NOT NULL GROUP BY Genre_T7 ORDER BY value DESC")
    return jsonify({"t6": t6, "t7": t7})

@app.route("/api/search/keywords")
def search_keywords():
    t6 = query("SELECT Most_Searched_T6 AS label, COUNT(*) AS value FROM customer_search_stats WHERE Most_Searched_T6 IS NOT NULL GROUP BY Most_Searched_T6 ORDER BY value DESC LIMIT 10")
    t7 = query("SELECT Most_Searched_T7 AS label, COUNT(*) AS value FROM customer_search_stats WHERE Most_Searched_T7 IS NOT NULL GROUP BY Most_Searched_T7 ORDER BY value DESC LIMIT 10")
    return jsonify({"t6": t6, "t7": t7})

# TREND APIs ────────────────────────────────────────────────────────────────────
@app.route("/api/trend/genre_comparison")
def trend_genre_comparison():
    rows = query("""
        SELECT 
            CASE WHEN period = 6 THEN 'Tháng 6' ELSE 'Tháng 7' END AS period,
            genre, value
        FROM (
            SELECT Genre_T6 AS genre, COUNT(*) AS value, 6 AS period FROM customer_search_stats WHERE Genre_T6 IS NOT NULL GROUP BY Genre_T6
            UNION ALL
            SELECT Genre_T7 AS genre, COUNT(*) AS value, 7 AS period FROM customer_search_stats WHERE Genre_T7 IS NOT NULL GROUP BY Genre_T7
        ) t
        WHERE genre IN (
            SELECT g.genre FROM (
                SELECT Genre_T6 AS genre FROM customer_search_stats WHERE Genre_T6 IS NOT NULL
                GROUP BY Genre_T6 ORDER BY COUNT(*) DESC LIMIT 12
            ) g
        )
        ORDER BY value DESC
    """)
    return jsonify(rows)

@app.route("/api/trend/heatmap")
def trend_heatmap():
    rows = query("""
        SELECT 
            Genre_T6 AS source, 
            Genre_T7 AS target, 
            COUNT(*) AS value
        FROM customer_search_stats
        WHERE Genre_T6 IN (
            SELECT g.Genre_T6 FROM (
                SELECT Genre_T6 FROM customer_search_stats 
                WHERE Genre_T6 IS NOT NULL GROUP BY Genre_T6 ORDER BY COUNT(*) DESC LIMIT 8
            ) g
        ) AND Genre_T7 IN (
            SELECT g.Genre_T7 FROM (
                SELECT Genre_T7 FROM customer_search_stats 
                WHERE Genre_T7 IS NOT NULL GROUP BY Genre_T7 ORDER BY COUNT(*) DESC LIMIT 8
            ) g
        )
        GROUP BY Genre_T6, Genre_T7
        ORDER BY source, target
    """)
    return jsonify(rows)

@app.route("/api/trend/changes")
def trend_changes():
    rows = query("""
        SELECT Changing AS label, COUNT(*) AS value
        FROM customer_search_stats
        WHERE Changing != 'No Change' AND Changing IS NOT NULL
        GROUP BY Changing ORDER BY value DESC LIMIT 15
    """)
    return jsonify(rows)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
