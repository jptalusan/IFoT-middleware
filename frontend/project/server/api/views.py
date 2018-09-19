from flask import current_app, Blueprint

api = Blueprint('api', __name__,)

@api.route('/', methods=['GET'])
def home():
  return "{'hello':'world'}"
