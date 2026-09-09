# ============================================================================
# ERP — core/cadastros/emissao.py
# Os dados de emissão de nota fiscal, POR EMPRESA.
#
# POR QUE ISTO EXISTE, E POR QUE VEIO CEDO
#
# Ia ficar para o fim. Mudou quando o dono respondeu, em 09/09/2026: *"a BWS
# não tem inscrição municipal em Petrolina, mas outra empresa que vamos operar
# sim"*, e *"uma por API e outra manual"*.
#
# Ou seja: emitir em mais de um município não é planejamento para depois, é
# requisito do primeiro dia. Hoje o município, o endereço do serviço e o código
# IBGE estão FIXOS no código do `emissaonf` (Eusébio/CE) — enquanto estiverem,
# a segunda empresa simplesmente não emite.
#
# TRÊS DEFESAS, e cada uma existe por um motivo:
#
#   1. MANUAL é o padrão. Empresa recém-cadastrada não sai emitindo nota
#      fiscal sozinha porque alguém esqueceu de configurar.
#   2. HOMOLOGAÇÃO é o padrão. Emitir é irreversível: cada emissão em produção
#      gera documento fiscal de verdade, com efeito tributário.
#   3. O TOKEN VAI CIFRADO, pelo mesmo caminho da senha de e-mail. Sem a chave
#      `ERP_CHAVE_SEGREDOS` o sistema RECUSA gravar, em vez de guardar aberto —
#      token de emissão assina em nome da empresa.
# ============================================================================
from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.comum.segredos import cifrar, decifrar, ha_chave
from app.apps.erp.db.models.cadastros import Empresa, Usuario

logger = logging.getLogger(__name__)

MODOS = ("MANUAL", "API")
CANAIS = ("NACIONAL", "ABRASF")
AMBIENTES = ("HOMOLOGACAO", "PRODUCAO")

# Municípios já conhecidos, para o dono não precisar caçar endereço.
#
# Os dois usam o MESMO provedor (E&L) e o endereço segue o molde
# `{uf}-{municipio}-pm-nfs-backend.cloud.el.com.br`. Foi isso que respondeu a
# pergunta dele sobre Petrolina: o emissor não precisa ser reescrito.
#
# ⚠️ O caminho depois do domínio NÃO é igual nos dois (`nfse40` no Eusébio,
# `nfse` em Petrolina) — por isso o endereço fica editável, e não montado.
MUNICIPIOS_CONHECIDOS = {
    "2304285": {
        "nome": "Eusébio/CE",
        "url_base": "https://ce-eusebio-pm-nfs-backend.cloud.el.com.br/nfse40",
    },
    "2611101": {
        "nome": "Petrolina/PE",
        "url_base": "https://pe-petrolina-pm-nfs-backend.cloud.el.com.br/nfse",
    },
}


def _texto(v: Any) -> str:
    return (str(v).strip() if v is not None else "")


def _decimal(v: Any, campo: str) -> Optional[Decimal]:
    bruto = _texto(v).replace(",", ".")
    if not bruto:
        return None
    try:
        return Decimal(bruto)
    except InvalidOperation:
        raise ErroValidacao(f"{campo} inválido: {v}.")


def ler(s: Session, empresa: Empresa) -> dict[str, Any]:
    """O que a tela mostra. O TOKEN NUNCA SAI — só se ele existe."""
    conhecido = MUNICIPIOS_CONHECIDOS.get(_texto(empresa.emissao_codigo_ibge))
    return {
        "modo": empresa.emissao_modo,
        "municipio": empresa.emissao_municipio or "",
        "codigo_ibge": empresa.emissao_codigo_ibge or "",
        "url_base": empresa.emissao_url_base or "",
        "canal": empresa.emissao_canal,
        "serie": empresa.emissao_serie or "",
        "aliquota_iss": (str(empresa.emissao_aliquota_iss)
                         if empresa.emissao_aliquota_iss is not None else ""),
        "codigo_servico": empresa.emissao_codigo_servico or "",
        "ambiente": empresa.emissao_ambiente,
        "tem_token": bool(empresa.emissao_token_cifrado),
        "municipio_conhecido": conhecido["nome"] if conhecido else "",
        "pode_emitir": pode_emitir(empresa)[0],
        "o_que_falta": pode_emitir(empresa)[1],
        "conhecidos": [{"ibge": k, **v} for k, v in MUNICIPIOS_CONHECIDOS.items()],
    }


def pode_emitir(empresa: Empresa) -> tuple[bool, list[str]]:
    """Dá para emitir por API hoje? E, se não, o que falta — item a item.

    Dizer só "não dá" faria a pessoa adivinhar. A lista é a diferença entre
    resolver em dois minutos e abrir um chamado.
    """
    if empresa.emissao_modo != "API":
        return False, ["a empresa está no modo MANUAL"]
    falta = []
    if not _texto(empresa.emissao_url_base):
        falta.append("o endereço do serviço do município")
    if not _texto(empresa.emissao_codigo_ibge):
        falta.append("o código IBGE do município")
    if not empresa.emissao_token_cifrado:
        falta.append("o token do canal")
    if not _texto(empresa.inscricao_municipal):
        falta.append("a inscrição municipal da empresa")
    return (not falta), falta


def definir(s: Session, empresa_id: int, dados: dict[str, Any],
            usuario: Optional[Usuario] = None) -> Empresa:
    """Guarda os dados de emissão. O token vai cifrado ou não vai."""
    empresa = s.get(Empresa, empresa_id)
    if empresa is None:
        raise ErroValidacao("Empresa não encontrada.")

    modo = _texto(dados.get("modo")).upper() or empresa.emissao_modo
    if modo not in MODOS:
        raise ErroValidacao(f"Modo de emissão inválido: {modo}.")
    canal = _texto(dados.get("canal")).upper() or empresa.emissao_canal
    if canal not in CANAIS:
        raise ErroValidacao(f"Canal inválido: {canal}.")
    ambiente = _texto(dados.get("ambiente")).upper() or empresa.emissao_ambiente
    if ambiente not in AMBIENTES:
        raise ErroValidacao(f"Ambiente inválido: {ambiente}.")

    ibge = re.sub(r"\D", "", _texto(dados.get("codigo_ibge")))
    if ibge and len(ibge) != 7:
        raise ErroValidacao("O código IBGE do município tem 7 dígitos.")

    url = _texto(dados.get("url_base"))
    if url and not url.startswith("https://"):
        # Token e nota assinada não viajam em claro. Aceitar http seria
        # convidar o vazamento no meio do caminho.
        raise ErroValidacao("O endereço do serviço precisa começar com https://.")

    empresa.emissao_modo = modo
    empresa.emissao_canal = canal
    empresa.emissao_ambiente = ambiente
    empresa.emissao_municipio = _texto(dados.get("municipio")) or None
    empresa.emissao_codigo_ibge = ibge or None
    empresa.emissao_url_base = url.rstrip("/") or None
    empresa.emissao_serie = _texto(dados.get("serie")) or None
    empresa.emissao_codigo_servico = _texto(dados.get("codigo_servico")) or None
    if "aliquota_iss" in dados:
        empresa.emissao_aliquota_iss = _decimal(dados.get("aliquota_iss"),
                                                "Alíquota de ISS")

    token = dados.get("token")
    if token:
        if not ha_chave():
            raise ErroValidacao(
                "O token não foi guardado: falta a chave ERP_CHAVE_SEGREDOS na "
                "Environment do Render. O resto foi salvo — assim que a chave "
                "existir, digite o token de novo.")
        empresa.emissao_token_cifrado = cifrar(str(token))

    # A trava do banco recusaria de qualquer jeito; recusar aqui deixa a
    # mensagem em português em vez de um erro de restrição.
    if modo == "API" and not (empresa.emissao_url_base and empresa.emissao_codigo_ibge):
        raise ErroValidacao(
            "Para emitir por API é preciso o endereço do serviço e o código "
            "IBGE do município. Escolha o município na lista ou preencha os dois.")

    s.flush()
    registrar_evento(s, "empresa", empresa.id, "EMISSAO_CONFIGURADA", {
        "modo": modo, "canal": canal, "ambiente": ambiente,
        "municipio": empresa.emissao_municipio, "ibge": empresa.emissao_codigo_ibge,
        "token_trocado": bool(token)}, usuario.id if usuario else None)
    logger.info("ERP/emissão: empresa %s em %s, modo %s, ambiente %s",
                empresa.cnpj, empresa.emissao_municipio or "?", modo, ambiente)
    return empresa


def token_de(empresa: Empresa) -> Optional[str]:
    """O token em claro, para quem vai chamar o serviço do município.

    Fica isolado numa função só para ser fácil de auditar quem usa: nenhuma
    tela chama isto, e nenhuma resposta JSON devolve o resultado.
    """
    return decifrar(empresa.emissao_token_cifrado)
