# ============================================================================
# BWS ERP — core/cadastros/validadores.py
# Validações estruturais de documentos e chaves. Funções puras.
# ============================================================================
from __future__ import annotations

import re


def somente_digitos(valor: str) -> str:
    return re.sub(r"\D", "", valor or "")


def cpf_valido(cpf: str) -> bool:
    cpf = somente_digitos(cpf)
    if len(cpf) != 11 or cpf == cpf[0] * 11:
        return False
    for pos in (9, 10):
        soma = sum(int(cpf[i]) * ((pos + 1) - i) for i in range(pos))
        dv = (soma * 10) % 11
        dv = 0 if dv == 10 else dv
        if dv != int(cpf[pos]):
            return False
    return True


def cnpj_valido(cnpj: str) -> bool:
    cnpj = somente_digitos(cnpj)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6] + pesos1
    for pesos, pos in ((pesos1, 12), (pesos2, 13)):
        soma = sum(int(cnpj[i]) * pesos[i] for i in range(pos))
        dv = soma % 11
        dv = 0 if dv < 2 else 11 - dv
        if dv != int(cnpj[pos]):
            return False
    return True


def documento_valido(doc: str, tipo_pessoa: str) -> bool:
    if tipo_pessoa == "PF":
        return cpf_valido(doc)
    if tipo_pessoa == "PJ":
        return cnpj_valido(doc)
    return False


_RE_EMAIL = re.compile(r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
_RE_EVP = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def pix_chave_valida(tipo: str, chave: str) -> tuple[bool, str]:
    tipo = (tipo or "").strip().upper()
    chave = (chave or "").strip()
    if not chave:
        return False, "Chave Pix vazia."
    if tipo == "CPF":
        return (True, "") if cpf_valido(chave) else (False, "CPF da chave Pix inválido (dígito verificador).")
    if tipo == "CNPJ":
        return (True, "") if cnpj_valido(chave) else (False, "CNPJ da chave Pix inválido (dígito verificador).")
    if tipo == "EMAIL":
        return (True, "") if _RE_EMAIL.match(chave) else (False, "E-mail da chave Pix em formato inválido.")
    if tipo == "TELEFONE":
        dig = somente_digitos(chave)
        if len(dig) in (10, 11) or (dig.startswith("55") and len(dig) in (12, 13)):
            return True, ""
        return False, "Telefone da chave Pix inválido (esperado DDD + número)."
    if tipo == "EVP":
        return (True, "") if _RE_EVP.match(chave) else (False, "Chave aleatória (EVP) em formato inválido.")
    return False, f"Tipo de chave Pix desconhecido: {tipo!r} (use CPF/CNPJ/EMAIL/TELEFONE/EVP)."
