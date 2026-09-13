# ============================================================================
# ERP — core/cadastros/contas.py
# As contas bancárias da empresa e as chaves Pix delas.
#
# POR QUE AS CHAVES PIX MORAM AQUI
#
# O dono deu o uso, e ele não é pagar: *"eventualmente a gente precisa
# consultar, e tendo esse cadastro das contas é o local mais fácil da gente
# consultar."* É para COPIAR E MANDAR quando alguém pede os dados da empresa.
#
# Por isso este módulo faz uma coisa que parece boba e é o ponto todo: monta o
# BLOCO DE TEXTO pronto para colar num WhatsApp ou num e-mail. Quem copia campo
# por campo erra um dígito, e dígito errado em dados bancários é dinheiro indo
# para o lugar errado.
# ============================================================================
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (ContaBancaria, ContaChavePix,
                                              Empresa, Usuario)

logger = logging.getLogger(__name__)

TIPOS_PIX = ("CNPJ", "CPF", "EMAIL", "TELEFONE", "ALEATORIA")
ROTULO_PIX = {"CNPJ": "CNPJ", "CPF": "CPF", "EMAIL": "E-mail",
              "TELEFONE": "Telefone", "ALEATORIA": "Chave aleatória"}


def _texto(v: Any) -> str:
    return (str(v).strip() if v is not None else "")


def _formatar_cnpj(bruto: str) -> str:
    d = re.sub(r"\D", "", bruto or "")
    if len(d) != 14:
        return bruto or ""
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def validar_chave(tipo: str, chave: str) -> str:
    """Confere o formato e devolve a chave limpa.

    A conferência é de FORMATO, não de existência — o ERP não fala com o banco.
    Ainda assim vale: chave com o número errado de dígitos é o erro que passa
    despercebido e só aparece quando o dinheiro não chega.
    """
    tipo = _texto(tipo).upper()
    if tipo not in TIPOS_PIX:
        raise ErroValidacao(f"Tipo de chave Pix inválido: {tipo}.")
    chave = _texto(chave)
    if not chave:
        raise ErroValidacao("Digite a chave Pix.")

    if tipo in ("CNPJ", "CPF"):
        d = re.sub(r"\D", "", chave)
        esperado = 14 if tipo == "CNPJ" else 11
        if len(d) != esperado:
            raise ErroValidacao(f"{tipo} deve ter {esperado} dígitos — vieram {len(d)}.")
        return d
    if tipo == "TELEFONE":
        d = re.sub(r"\D", "", chave)
        if len(d) not in (10, 11, 12, 13):
            raise ErroValidacao("Telefone com quantidade de dígitos fora do esperado.")
        return f"+55{d[-11:]}" if not chave.startswith("+") else chave
    if tipo == "EMAIL":
        if "@" not in chave or "." not in chave.split("@")[-1]:
            raise ErroValidacao("E-mail inválido para chave Pix.")
        return chave.lower()
    # ALEATORIA: o padrão do Banco Central é um UUID de 36 caracteres
    if len(chave) != 36 or chave.count("-") != 4:
        raise ErroValidacao(
            "Chave aleatória do Pix tem 36 caracteres, no formato "
            "8-4-4-4-12 separado por hífens.")
    return chave.lower()


def acrescentar_pix(s: Session, conta_id: int, *, tipo: str, chave: str,
                    descricao: str = "",
                    usuario: Optional[Usuario] = None) -> ContaChavePix:
    conta = s.get(ContaBancaria, conta_id)
    if conta is None:
        raise ErroValidacao("Conta bancária não encontrada.")
    tipo = _texto(tipo).upper()
    limpa = validar_chave(tipo, chave)
    ja = s.scalars(select(ContaChavePix).where(
        ContaChavePix.conta_id == conta_id, ContaChavePix.chave == limpa)).first()
    if ja is not None:
        return ja
    nova = ContaChavePix(conta_id=conta_id, tipo=tipo, chave=limpa,
                         descricao=_texto(descricao) or None)
    s.add(nova)
    s.flush()
    registrar_evento(s, "conta_bancaria", conta_id, "CHAVE_PIX_ACRESCENTADA",
                     {"tipo": tipo}, usuario.id if usuario else None)
    return nova


def remover_pix(s: Session, chave_id: int, usuario: Optional[Usuario] = None) -> None:
    chave = s.get(ContaChavePix, chave_id)
    if chave is None:
        raise ErroValidacao("Chave Pix não encontrada.")
    registrar_evento(s, "conta_bancaria", chave.conta_id, "CHAVE_PIX_REMOVIDA",
                     {"tipo": chave.tipo}, usuario.id if usuario else None)
    s.delete(chave)
    s.flush()


def texto_para_copiar(conta: ContaBancaria, empresa: Optional[Empresa] = None) -> str:
    """O bloco pronto para colar num WhatsApp ou num e-mail.

    É o ponto do módulo. Copiar campo por campo é onde se erra um dígito — e
    dígito errado em dado bancário é dinheiro no lugar errado.
    """
    linhas = []
    if empresa is not None:
        linhas.append(empresa.razao_social)
        linhas.append(f"CNPJ: {_formatar_cnpj(empresa.cnpj)}")
        linhas.append("")
    linhas.append(f"Banco: {conta.banco_codigo} — {conta.descricao}")
    linhas.append(f"Agência: {conta.agencia}")
    linhas.append(f"Conta: {conta.conta}")
    chaves = list(conta.chaves_pix or [])
    if chaves:
        linhas.append("")
        for c in chaves:
            rotulo = ROTULO_PIX.get(c.tipo, c.tipo)
            mostrar = _formatar_cnpj(c.chave) if c.tipo == "CNPJ" else c.chave
            linhas.append(f"Pix ({rotulo}): {mostrar}")
    return "\n".join(linhas)


def criar(s: Session, dados: dict[str, Any],
          usuario: Optional[Usuario] = None) -> ContaBancaria:
    """Cadastra a conta da empresa. Uma chave Pix pode vir junto.

    A chave Pix vinha depois, por um botão à parte, e o formulário mostrava
    menos campos do que a tabela ao lado — foi o que o dono estranhou em
    10/09/2026. Quem cadastra a conta tem a chave na mão naquele momento; pedir
    para voltar depois é o jeito mais fácil de a conta ficar sem chave.
    """
    from app.apps.erp.core.cadastros import bancos

    descricao = _texto(dados.get("descricao"))
    banco = bancos.normalizar_codigo(dados.get("banco_codigo"))
    agencia = _texto(dados.get("agencia"))
    conta = _texto(dados.get("conta"))
    faltando = [rot for valor, rot in (
        (descricao, "a descrição"), (banco, "o banco"),
        (agencia, "a agência"), (conta, "a conta")) if not valor]
    if faltando:
        raise ErroValidacao(f"Preencha {', '.join(faltando)}.")

    linha = ContaBancaria(descricao=descricao, banco_codigo=banco,
                          agencia=agencia, conta=conta)
    s.add(linha)
    s.flush()
    registrar_evento(s, "conta_bancaria", linha.id, "CRIADA",
                     {"descricao": descricao, "banco": bancos.rotulo(banco, s),
                      "agencia": agencia, "conta": conta},
                     usuario.id if usuario else None)

    if _texto(dados.get("pix_chave")):
        acrescentar_pix(s, linha.id, tipo=_texto(dados.get("pix_tipo")),
                        chave=_texto(dados.get("pix_chave")),
                        descricao=_texto(dados.get("pix_descricao")),
                        usuario=usuario)
    return linha


def listar(s: Session, *, empresa_id: Optional[int] = None,
           incluir_inativas: bool = False) -> list[dict[str, Any]]:
    from app.apps.erp.core.cadastros import bancos

    stmt = (select(ContaBancaria).options(selectinload(ContaBancaria.chaves_pix))
            .order_by(ContaBancaria.descricao))
    if not incluir_inativas:
        stmt = stmt.where(ContaBancaria.ativo.is_(True))
    empresa = s.get(Empresa, empresa_id) if empresa_id else None
    if empresa is None:
        empresa = s.scalars(select(Empresa).where(Empresa.padrao.is_(True))).first()

    saida = []
    for c in s.scalars(stmt).all():
        saida.append({
            "id": c.id, "descricao": c.descricao,
            "banco_codigo": c.banco_codigo,
            "banco_nome": bancos.nome(c.banco_codigo, s),
            "agencia": c.agencia, "conta": c.conta,
            "ativo": c.ativo is not False,
            "chaves_pix": [{"id": k.id, "tipo": k.tipo,
                            "tipo_nome": ROTULO_PIX.get(k.tipo, k.tipo),
                            "chave": (_formatar_cnpj(k.chave) if k.tipo == "CNPJ"
                                      else k.chave),
                            "descricao": k.descricao or ""}
                           for k in (c.chaves_pix or [])],
            "texto_para_copiar": texto_para_copiar(c, empresa),
        })
    return saida
