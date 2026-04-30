# -*- coding: utf-8 -*-
"""
chatbot/auth.py
Validação de acesso: CPF × telefone × planilha.

Níveis de acesso:
  MASTER  → telefone 5585987846225 — acessa qualquer CPF
  NORMAL  → CPF deve corresponder ao telefone na planilha
  NEGADO  → CPF não encontrado, não corresponde ao telefone, ou desligado
"""

import os
import re
import logging
from . import sheets_cache

logger = logging.getLogger(__name__)

TELEFONE_MASTER = os.getenv('CHATBOT_MASTER_PHONE', '5585987846225')


def _normalizar_cpf(cpf: str) -> str:
    return re.sub(r'\D', '', str(cpf or ''))


def _normalizar_tel(tel: str) -> str:
    digits = re.sub(r'\D', '', str(tel or ''))
    if digits and not digits.startswith('55'):
        digits = '55' + digits
    return digits


def _tels_equivalentes(tel1: str, tel2: str) -> bool:
    """
    Compara dois telefones ignorando o 9 extra do celular brasileiro.
    5585987846225 == 558587846225 → True
    """
    def sem_nono(t: str) -> str:
        # Remove prefixo 55 + DDD (4 dígitos) e verifica se tem 9 extra
        if len(t) == 13 and t.startswith('55'):
            # 55 + DDD(2) + 9 + numero(8) → remove o 9
            return t[:4] + t[5:]
        return t

    t1 = _normalizar_tel(tel1)
    t2 = _normalizar_tel(tel2)

    if t1 == t2:
        return True
    if sem_nono(t1) == t2 or t1 == sem_nono(t2):
        return True
    return False


def is_master(telefone: str) -> bool:
    return _tels_equivalentes(telefone, TELEFONE_MASTER)


def validar_acesso(cpf: str, telefone_solicitante: str) -> dict:
    """
    Valida se o solicitante pode acessar dados do CPF informado.

    Retorna dict com:
      ok: bool
      nivel: 'master' | 'normal' | None
      colaborador: dict | None
      motivo: str (em caso de falha)
    """
    cpf_norm = _normalizar_cpf(cpf)
    tel_norm = _normalizar_tel(telefone_solicitante)

    if not cpf_norm or len(cpf_norm) != 11:
        return {'ok': False, 'nivel': None, 'colaborador': None,
                'motivo': 'CPF inválido.'}

    colaborador = sheets_cache.buscar_por_cpf(cpf_norm)

    if not colaborador:
        return {'ok': False, 'nivel': None, 'colaborador': None,
                'motivo': 'CPF não encontrado na base de dados.'}

    if sheets_cache.esta_desligado(colaborador):
        return {'ok': False, 'nivel': None, 'colaborador': colaborador,
                'motivo': 'desligado'}

    # Acesso master — pode consultar qualquer CPF
    if is_master(tel_norm):
        logger.info(f"[auth] Acesso MASTER para CPF {cpf_norm[:3]}***")
        return {'ok': True, 'nivel': 'master', 'colaborador': colaborador, 'motivo': ''}

    # Acesso normal — telefone deve bater com o CPF (tolerando 9 extra)
    tel_planilha = colaborador.get('tel', '')
    if not _tels_equivalentes(tel_norm, tel_planilha):
        logger.warning(f"[auth] Telefone não corresponde ao CPF {cpf_norm[:3]}***")
        return {'ok': False, 'nivel': None, 'colaborador': None,
                'motivo': 'cpf_telefone_nao_corresponde'}

    logger.info(f"[auth] Acesso NORMAL validado para CPF {cpf_norm[:3]}***")
    return {'ok': True, 'nivel': 'normal', 'colaborador': colaborador, 'motivo': ''}