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
    # Normalize old 'postgres://' scheme to 'postgresql://'
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

    # --- 新增的 API 端點 ---

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_terms(term_a, term_b):
        """
        Returns studies that mention term_a but not term_b.
        """
        sql = text("""
            -- Select studies associated with the first term
            SELECT DISTINCT study_id FROM ns.annotations_terms WHERE term = :term_a
            -- Subtract studies also associated with the second term
            EXCEPT
            SELECT DISTINCT study_id FROM ns.annotations_terms WHERE term = :term_b;
        """)
        try:
            with get_engine().connect() as conn:
                result = conn.execute(sql, {"term_a": term_a, "term_b": term_b}).fetchall()
                # The result is a list of tuples, e.g., [('pmid1',), ('pmid2',)]
                # We extract the first element of each tuple to create a simple list.
                studies = [row[0] for row in result]
                return jsonify({
                    "term_a_not_b": {
                        "term_a": term_a,
                        "term_b": term_b,
                        "count": len(studies),
                        "studies": studies
                    }
                })
        except Exception as e:
            abort(500, description=f"Database query failed: {e}")

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

        # Using PostGIS for spatial queries. ST_DWithin checks if geometries are within a specified distance.
        # We create points from the input coordinates and search for study coordinates within a 10mm radius.
        sql = text("""
            -- Select studies with a coordinate near location A
            SELECT DISTINCT study_id FROM ns.coordinates
            WHERE ST_DWithin(geom, ST_MakePoint(:x_a, :y_a, :z_a), :radius)
            -- Subtract studies that also have a coordinate near location B
            EXCEPT
            SELECT DISTINCT study_id FROM ns.coordinates
            WHERE ST_DWithin(geom, ST_MakePoint(:x_b, :y_b, :z_b), :radius);
        """)

        params = {
            "x_a": x_a, "y_a": y_a, "z_a": z_a,
            "x_b": x_b, "y_b": y_b, "z_b": z_b,
            "radius": 10  # Search radius in mm
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
            abort(500, description=f"Database query failed: {e}")


    @app.get("/test_db", endpoint="test_db")
    def test_db():
        """
        Tests database connectivity and provides schema information.
        """
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
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