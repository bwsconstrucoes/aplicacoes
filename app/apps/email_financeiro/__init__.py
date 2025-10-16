# -*- coding: utf-8 -*-
"""
Módulo Email Financeiro - inicialização do Blueprint
"""
from flask import Blueprint

bp = Blueprint("email_financeiro", __name__)

from . import routes  # noqa
