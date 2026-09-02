# -*- coding: utf-8 -*-
"""Painel Financeiro OMIE — blueprint do monorepo, em /painel.

O import e leve de proposito: aqui so entram Flask e o proprio modulo de rotas.
Nem `pandas`, nem `requests` da OMIE, nem conexao de banco — essas coisas so
sao carregadas quando alguem abre uma tela ou dispara a atualizacao.

E o que garante que um problema no painel (banco fora do ar, biblioteca
faltando) nao derrube os outros 14 modulos do monorepo no start do gunicorn.
"""
from .web import bp

__all__ = ["bp"]
