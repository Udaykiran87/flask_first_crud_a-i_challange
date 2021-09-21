from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField, FloatField
from wtforms.validators import DataRequired

class InsertForm(FlaskForm):
    title = StringField('title', validators=[DataRequired()])
    release_year = IntegerField('release_year', validators=[DataRequired()])
    genre = StringField('genre', validators=[DataRequired()])
    imdb_rating = FloatField('imdb_rating', validators=[DataRequired()])

    submit = SubmitField('Insert')

class UpdateForm(FlaskForm):
    title = StringField('title', validators=[DataRequired()])
    column = StringField('column', validators=[DataRequired()])
    value = StringField('value', validators=[DataRequired()])

    submit = SubmitField('Update')

class DeleteForm(FlaskForm):
    title = StringField('title', validators=[DataRequired()])

    submit = SubmitField('Delete')