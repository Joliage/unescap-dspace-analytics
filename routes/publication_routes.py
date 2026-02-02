from flask import Blueprint, request, render_template


def create_publication_routes(publication_controller, stats_controller):
    pub_bp = Blueprint("publication", __name__)

    @pub_bp.route("/", methods=["GET", "POST"])
    def publication_search():
        result = None
        stats = None
        error = None

        if request.method == "POST":
            pub_id = request.form.get("publication_id")
            if pub_id:
                result = publication_controller.fetch_publication_data(pub_id)
                if not result:
                    error = "Publication ID not found."
                else:
                    stats = stats_controller.get_publication_stats(pub_id)

        return render_template("index.html", result=result, stats=stats, error=error)

    return pub_bp
