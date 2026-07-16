# -*- coding: utf-8 -*-
"""
Pacote app.apps.telegram — expõe o blueprint no padrão do monorepo.
Permite: from app.apps.telegram import bp
"""

from .telegram_bot import telegram_bp as bp

__all__ = ["bp"]
