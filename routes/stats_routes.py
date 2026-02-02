from flask import Blueprint, jsonify

def create_stats_routes(stats_controller):
    stats_bp = Blueprint("stats", __name__)

    @stats_bp.route("/stats/<publication_id>", methods=["GET"])
    def publication_stats(publication_id):
        stats = stats_controller.get_publication_stats(publication_id)
        if not stats:
            return jsonify({"error": "No stats found"}), 404
        return jsonify(stats)

    return stats_bp
