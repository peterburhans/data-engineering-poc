import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@control-db/superset"
