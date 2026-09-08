# ============================================================================
# ERP — core/comum/segredos.py
# Guardar no banco o que não pode ficar em claro.
#
# Hoje serve à senha da conta de e-mail de cada empresa. A senha PRECISA ser
# reversível — na hora de enviar, o servidor SMTP quer a senha, não um resumo
# dela —, então não dá para usar hash como se faz com senha de operador. O que
# dá é cifrar, com uma chave que não mora no banco.
#
# TRÊS REGRAS QUE VALEM A PENA ENTENDER ANTES DE MEXER:
#
#   1. A CHAVE MORA NA ENVIRONMENT DO RENDER (`ERP_CHAVE_SEGREDOS`), nunca no
#      banco e nunca no repositório. Quem tem uma cópia do banco não tem as
#      senhas.
#   2. SEM A CHAVE, NÃO SE GRAVA. Se a variável não existir, gravar levanta
#      erro com o recado do que falta. A alternativa — guardar em claro
#      "só desta vez" — é como uma senha vaza sem ninguém perceber.
#   3. TROCAR A CHAVE INVALIDA O QUE JÁ FOI GUARDADO. Não há como decifrar o
#      antigo com a chave nova. Trocar a chave = redigitar as senhas.
#
# Como gerar a chave (uma vez, e guardar na Environment do Render):
#
#     python -c "from cryptography.fernet import Fernet; \
#                print(Fernet.generate_key().decode())"
# ============================================================================
from __future__ import annotations

import logging
import os
from typing import Optional

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

VARIAVEL = "ERP_CHAVE_SEGREDOS"

_RECADO_SEM_CHAVE = (
    "A chave de segredos do ERP não está configurada, então a senha não pode "
    "ser guardada com segurança. Defina a variável ERP_CHAVE_SEGREDOS na "
    "Environment do Render e tente de novo. O resto do cadastro pode ser "
    "salvo normalmente — só a senha fica de fora."
)


def ha_chave() -> bool:
    """A tela usa isto para avisar ANTES de a pessoa digitar a senha."""
    return bool((os.environ.get(VARIAVEL) or "").strip())


def _cofre():
    from cryptography.fernet import Fernet
    bruta = (os.environ.get(VARIAVEL) or "").strip()
    if not bruta:
        raise ErroValidacao(_RECADO_SEM_CHAVE)
    try:
        return Fernet(bruta.encode())
    except Exception:
        raise ErroValidacao(
            "A chave em ERP_CHAVE_SEGREDOS não tem o formato esperado. Ela é "
            "gerada uma única vez com Fernet.generate_key() — ver o cabeçalho "
            "de core/comum/segredos.py.")


def cifrar(texto: str) -> str:
    """Texto em claro → texto cifrado, pronto para ir ao banco."""
    if texto is None or texto == "":
        raise ErroValidacao("Nada a guardar: o segredo veio em branco.")
    return _cofre().encrypt(texto.encode()).decode()


def decifrar(cifrado: Optional[str]) -> Optional[str]:
    """Texto cifrado → texto em claro. Devolve None quando não há nada.

    Se a chave mudou desde que o segredo foi guardado, isto levanta um erro
    com o recado em português — em vez de devolver lixo e o envio falhar
    depois, num lugar onde ninguém entende o motivo.
    """
    if not cifrado:
        return None
    from cryptography.fernet import InvalidToken
    try:
        return _cofre().decrypt(cifrado.encode()).decode()
    except InvalidToken:
        raise ErroValidacao(
            "A senha guardada não pôde ser lida com a chave atual. Isso "
            "acontece quando ERP_CHAVE_SEGREDOS é trocada: o que foi cifrado "
            "com a chave antiga não abre com a nova. Digite a senha de novo "
            "no cadastro da empresa.")
