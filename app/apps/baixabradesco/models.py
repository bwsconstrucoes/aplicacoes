# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from decimal import Decimal
from typing import Any, Dict, List, Optional


@dataclass
class AttachmentInput:
    filename: str
    base64: str = ''
    url: str = ''


@dataclass
class ExtractedReceipt:
    filename: str
    page: int = 1
    text: str = ''
    drive_link: str = ''
    fingerprint: str = ''
    tipo_comprovante: str = 'nao_classificado'
    id_pipefy: str = ''
    data_pagamento: str = ''
    valor_pago: str = ''
    acrescimos: str = '0,00'
    tarifa: str = '0,00'
    forma_pagamento: str = ''
    conta_origem_raw: str = ''
    agencia_origem: str = ''
    conta_origem: str = ''
    conta_destino_raw: str = ''
    nome_recebedor: str = ''
    documento_recebedor: str = ''
    descricao: str = ''
    codigo_barras: str = ''
    confianca: Dict[str, float] = field(default_factory=dict)
    pendencias: List[str] = field(default_factory=list)


@dataclass
class SpRecord:
    row_number: int
    id: str
    nome_credor: str = ''
    cpf_cnpj: str = ''
    valor_total: str = ''
    info_pgt: str = ''
    numero_nf: str = ''
    tipo_pagamento: str = ''
    conta_pagamento: str = ''
    status_agendamento: str = ''
    centro_custo: str = ''
    vencimento: str = ''
    descricao: str = ''
    codigo_integracao_omie: str = ''
    status_pgt: str = ''
    status_aut: str = ''
    link_card: str = ''
    codigo_barras: str = ''
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BankAccount:
    row_number: int
    banco: str = ''
    agencia: str = ''
    conta: str = ''
    chave_normalizada: str = ''
    codigo_omie: str = ''
    codigo_pipefy: str = ''
    descricao: str = ''
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchResult:
    status: str = 'nao_localizado'  # localizado, pendente_validacao, nao_localizado, transferencia_sem_sp
    metodo: str = ''
    id: str = ''
    sp: Optional[SpRecord] = None
    candidatos: List[SpRecord] = field(default_factory=list)
    motivo: str = ''


@dataclass
class ExecutionPlan:
    receipt: ExtractedReceipt
    match: MatchResult
    banco: Optional[BankAccount] = None
    acao: str = 'pendente_validacao'
    pode_executar: bool = False
    motivos_bloqueio: List[str] = field(default_factory=list)
    pipefy_get_query: str = ''
    pipefy_update_mutation: str = ''
    omie_requests: List[Dict[str, Any]] = field(default_factory=list)
    sheets_updates: List[Dict[str, Any]] = field(default_factory=list)
    whatsapp_messages: List[Dict[str, Any]] = field(default_factory=list)
    responses: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        data = asdict(self)
        # Nunca expor credenciais Omie no output/Make.
        for item in data.get('omie_requests') or []:
            req = item.get('request') if isinstance(item, dict) else None
            if isinstance(req, dict):
                if 'app_key' in req:
                    req['app_key'] = '***REDACTED***'
                if 'app_secret' in req:
                    req['app_secret'] = '***REDACTED***'
        return data
