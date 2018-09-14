from flask_sqlalchemy import SQLAlchemy
#from flask_migrate import Migrate

db = SQLAlchemy()
#migrate = Migrate()

class Node(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)

    def __repr__(self):
        return '<Node {}>'.format(self.name)
