from flask import Blueprint

bp = Blueprint('baixabradesco', __name__)

from . import routes  # noqa: E402,F401
