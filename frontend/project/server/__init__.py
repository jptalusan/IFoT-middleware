# project/server/__init__.py


import os

from flask import Flask
from flask_bootstrap import Bootstrap

#from flask_sqlalchemy import SQLAlchemy
#from flask_migrate import Migrate
from .database import db

# instantiate the extensions
bootstrap = Bootstrap()

def create_app(script_info=None):

    # instantiate the app
    app = Flask(
        __name__,
        template_folder='../client/templates',
        static_folder='../client/static'
    )

    # set config
    app_settings = os.getenv('APP_SETTINGS')
    app.config.from_object(app_settings)

    ## set up datooabase and structures
    db.init_app(app)
    #db = SQLAlchemy(app)
    #migrate = Migrate(app, db)

    # set up extensions
    bootstrap.init_app(app)

    # register blueprints
    from project.server.main.views import main_blueprint
    from project.server.api.views import api
    app.register_blueprint(main_blueprint)
    app.register_blueprint(api, url_prefix='/api')

    # shell context for flask cli
    app.shell_context_processor({'app': app})

    return app
