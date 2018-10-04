from flask_wtf import FlaskForm
from wtforms import FileField, IntegerField, SubmitField, SelectField
from wtforms.validators import InputRequired, NumberRange
from wtforms.csrf.core import CSRF
from hashlib import md5
from flask import current_app

SECRET_KEY = b"1234567890"

class IPAddressCSRF(CSRF):
    """
    Generate a CSRF token based on the user's IP. I am probably not very
    secure, so don't use me.
    """
    def setup_form(self, form):
        self.csrf_context = form.meta.csrf_context
        return super(IPAddressCSRF, self).setup_form(form)

    def generate_csrf_token(self, csrf_token):
        token = md5(SECRET_KEY + self.csrf_context.encode('utf-8')).hexdigest()
        return token

    def validate_csrf_token(self, form, field):
        if field.data != field.current_token:
            raise ValueError('Invalid CSRF')

class UploadForm(FlaskForm):
  file = FileField(u'File to upload:', validators=[InputRequired()])

class TextForm(FlaskForm):
  class Meta:
    csrf = True
    csrf_class = IPAddressCSRF

  node_count = IntegerField("Number of Nodes:", validators=[NumberRange(1, 3)])
  chunk_count = IntegerField("Number of 10 second chunks:", validators=[NumberRange(1, 200)])
  model_type = SelectField(
        u'Model type',
        choices = [('NN', 'nn'), ('SVM', 'svm')]
    )
  # submit = SubmitField("Process")

