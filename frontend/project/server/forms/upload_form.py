from flask_wtf import FlaskForm
from wtforms import FileField
from wtforms.validators import InputRequired

class UploadForm(FlaskForm):
  file = FileField(u'File to upload:', validators=[InputRequired()])
