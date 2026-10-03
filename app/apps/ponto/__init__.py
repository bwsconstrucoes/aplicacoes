# -*- coding: utf-8 -*-
"""Ponto eletrônico próprio (REP-P) — blueprint do monorepo, em /ponto.

Fase 1: schema, API REST e importadores. O celular (PWA), o iDFace e as telas
no ERP vêm nas fases seguintes.

O import é leve de propósito: só Flask e o módulo de rotas. Conexão de banco,
Pillow e openpyxl são carregados quando alguém chama uma rota ou um script —
é o que garante que um problema aqui não derrube os outros módulos no start
do gunicorn.
"""
from .routes import bp

__all__ = ["bp"]
