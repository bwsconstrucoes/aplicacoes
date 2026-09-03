# -*- coding: utf-8 -*-
"""Análise de SPs — blueprint do monorepo, em /analisesps.

Era um Streamlit que rodava no computador do dono, lendo uma base local de
60 MB. Isto aqui é a mesma coisa online, no serviço que já existe — sem
Streamlit, sem arquivo, com login.

O import é leve de propósito: aqui só entram Flask e o próprio módulo de rotas.
Nem `gspread`, nem conexão de banco — essas coisas só são carregadas quando
alguém abre uma tela ou dispara a sincronização.

É o que garante que um problema neste módulo (banco fora do ar, biblioteca
faltando) não derrube os outros 15 do monorepo no start do gunicorn.
"""
from .web import bp

__all__ = ["bp"]
