# -*- coding: utf-8 -*-
"""Os erros do ponto, cada um com o status HTTP que a rota responde.

A regra é a do ERP: o que não está no alcance de quem pergunta responde 404
"não encontrado", nunca 403 — dizer "sem permissão" para um número que existe
confirma a existência dele."""
from __future__ import annotations


class ErroDoPonto(Exception):
    status = 400

    def __init__(self, mensagem: str, **detalhes):
        super().__init__(mensagem)
        self.mensagem = mensagem
        self.detalhes = detalhes


class ErroDeValidacao(ErroDoPonto):
    """Entrada ruim: campo faltando, CPF inválido, data ilegível."""
    status = 400


class NaoAutenticado(ErroDoPonto):
    """Chave de API ou token do aparelho ausente ou errado."""
    status = 401


class Recusada(ErroDoPonto):
    """Batida recusada por identidade: aparelho não aprovado, pessoa não
    autorizada, obra não permitida ao aparelho. Fica em `ponto.recusas`."""
    status = 403


class NaoEncontrado(ErroDoPonto):
    status = 404


class NaoConfigurado(ErroDoPonto):
    """Falta variável de ambiente (PONTO_API_KEY). Falha fechado."""
    status = 503
