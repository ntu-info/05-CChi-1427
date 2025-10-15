# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    """
    Initializes and returns a global SQLAlchemy engine instance.
    Reads database URL from environment variables.
    """
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    """
    Flask application factory.
    """
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    # --- 主要 API 端點 ---

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_terms(term_a, term_b):
        """
        Returns studies that mention term_a but not term_b.
        Automatically handles the 'terms_abstract_tfidf__' prefix and underscores.
        """
        # 定義固定的前綴詞
        PREFIX = "terms_abstract_tfidf__"

        # 處理傳入的術語：加上前綴，並將使用者用來分隔單字的 '_' 換回空格
        # 例如：'autobiographical_memory' -> 'terms_abstract_tfidf__autobiographical memory'
        # 例如：'abuse' -> 'terms_abstract_tfidf__abuse'
        full_term_a = f"{PREFIX}{term_a.replace('_', ' ')}"
        full_term_b = f"{PREFIX}{term_b.replace('_', ' ')}"

        sql = text("""
            SELECT DISTINCT study_id FROM ns.annotations_terms WHERE term = :term_a
            EXCEPT
            SELECT DISTINCT study_id FROM ns.annotations_terms WHERE term = :term_b;
        """)

        try:
            with get_engine().connect() as conn:
                # 使用處理過後的完整術語進行查詢
                result = conn.execute(sql, {"term_a": full_term_a, "term_b": full_term_b}).fetchall()
                studies = [row[0] for row in result]
                return jsonify({
                    "term_a_not_b": {
                        "term_a": term_a, # 回傳給使用者時，仍然是簡潔的原始輸入
                        "term_b": term_b,
                        "count": len(studies),
                        "studies": studies
                    }
                })
        except Exception as e:
            return jsonify({"error": f"Database query failed: {e}"}), 500


    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_by_locations(coords_a, coords_b):
        """
        Returns studies with activations near coords_a but not near coords_b.
        A search radius of 10mm is used for matching coordinates.
        """
        try:
            x_a, y_a, z_a = map(int, coords_a.split("_"))
            x_b, y_b, z_b = map(int, coords_b.split("_"))
        except ValueError:
            abort(400, description="Invalid coordinate format. Expected x_y_z.")

        # 使用 ST_SetSRID 將即時建立的點的 SRID 設為 4326，以匹配 'geom' 欄位
        sql = text("""
            SELECT DISTINCT study_id FROM ns.coordinates
            WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(:x_a, :y_a, :z_a), 4326), :radius)
            EXCEPT
            SELECT DISTINCT study_id FROM ns.coordinates
            WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(:x_b, :y_b, :z_b), 4326), :radius);
        """)

        params = {
            "x_a": x_a, "y_a": y_a, "z_a": z_a,
            "x_b": x_b, "y_b": y_b, "z_b": z_b,
            "radius": 10
        }

        try:
            with get_engine().connect() as conn:
                result = conn.execute(sql, params).fetchall()
                studies = [row[0] for row in result]
                return jsonify({
                    "location_a_not_b": {
                        "location_a": f"{x_a}_{y_a}_{z_a}",
                        "location_b": f"{x_b}_{y_b}_{z_b}",
                        "radius_mm": params["radius"],
                        "count": len(studies),
                        "studies": studies
                    }
                })
        except Exception as e:
            return jsonify({"error": f"Database query failed: {e}"}), 500

    # --- 輔助與測試工具 ---

    @app.get("/find_terms/<keyword>")
    def find_terms(keyword):
        """
        [輔助工具] 尋找資料庫中包含特定關鍵字的術語。
        用法: /find_terms/memory 或 /find_terms/abuse
        """
        # 我們搜尋原始的 term，包含前綴
        sql = text("SELECT DISTINCT term FROM ns.annotations_terms WHERE term ILIKE :pattern ORDER BY term;")
        try:
            with get_engine().connect() as conn:
                pattern = f"%{keyword}%"
                result = conn.execute(sql, {"pattern": pattern}).fetchall()
                terms = [row[0] for row in result]
                return jsonify({
                    "keyword": keyword,
                    "match_count": len(terms),
                    "matching_terms": terms
                })
        except Exception as e:
            return jsonify({"error": f"Database query failed: {e}"}), 500

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        """
        Tests database connectivity and provides schema information.
        """
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()
                payload["ok"] = True
                return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point
app = create_app()