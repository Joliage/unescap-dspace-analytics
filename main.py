from flask import Flask
from pymongo import MongoClient
import os
from dotenv import load_dotenv

from controllers.publication_controller import PublicationController
from controllers.stats_controller import StatsController
from routes.publication_routes import create_publication_routes
from routes.health_routes import health_bp

load_dotenv()

app = Flask(__name__)

client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("DB_NAME")]

REPO_API_BASE = os.getenv("REPOSITORY_API_BASE")

# Controllers
publication_controller = PublicationController(db, REPO_API_BASE)
stats_controller = StatsController(db)

# Run stats pipeline ONCE (safe, idempotent)
stats_controller.run_full_pipeline()

# Routes
app.register_blueprint(
    create_publication_routes(publication_controller, stats_controller)
)
app.register_blueprint(health_bp)



if __name__ == "__main__":
    app.run(debug=True)
