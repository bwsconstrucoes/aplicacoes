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


# ⚑ O FORMATO DA CHAVE, escrito aqui porque é o que o diagnóstico usa: 32
# bytes sorteados, escritos em base64 "de URL" (o alfabeto que troca `+/` por
# `-_`). Dá sempre 44 caracteres, e o último é `=`. Qualquer outra coisa — uma
# senha inventada, um texto cortado, aspas que vieram coladas — não serve, e
# o erro só aparecia na hora de gravar.
TAMANHO_DA_CHAVE = 44


def gerar_chave() -> str:
    """Uma chave nova, válida. Usada pela tela de Configurações.

    ⚠️ QUEM CHAMA ISTO NÃO GRAVA NADA: a chave tem de ir para a Environment do
    Render à mão. Guardá-la no banco desfaria a razão de ela existir — quem
    tem uma cópia do banco passaria a ter as senhas junto.
    """
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode()


def _porque_nao_serve(bruta: str) -> str:
    """O que há de errado com a chave, SEM dizer qual ela é.

    ⚠️ SÓ É CHAMADA DEPOIS DE O COFRE RECUSAR a chave, e o diagnóstico fala do
    que de fato derruba: TAMANHO. Aspas em volta e lixo colado no fim o cofre
    costuma engolir (a decodificação ignora o que não é do alfabeto e para na
    "=" do fim) — então dizer "tirou as aspas que resolve" seria mandar
    arrumar o que não é o problema.

    ⚑ ESTA FUNÇÃO NUNCA DEVOLVE A CHAVE, nem um pedaço dela, e isso é
    deliberado: o recado vai para a tela e para o log, e segredo que aparece
    em log deixa de ser segredo. Ela fala de TAMANHO e de FORMATO.
    """
    if not bruta:
        return "a variável existe mas está vazia"
    n = len(bruta)
    if n != TAMANHO_DA_CHAVE:
        falta = TAMANHO_DA_CHAVE - n
        como = ("faltam" if falta > 0 else "sobram") + f" {abs(falta)}"
        causa = ("Normalmente é pedaço que ficou de fora ao copiar"
                 if falta > 0 else
                 "Normalmente são aspas em volta, ou texto colado junto")
        return (f"a chave tem {n} caracteres e o certo são "
                f"{TAMANHO_DA_CHAVE} — {como}. {causa}")
    return ("a chave tem o tamanho certo mas não é uma chave gerada pelo "
            "sistema — ela não pode ser inventada nem ser uma senha")


def situacao() -> dict:
    """Em que pé está a chave — para a tela avisar ANTES de alguém digitar uma
    senha que não vai ser guardada.

    ⚠️ NUNCA inclui o valor da chave. Só `tem`, `serve` e o motivo.
    """
    bruta = (os.environ.get(VARIAVEL) or "").strip()
    if not bruta:
        return {"tem": False, "serve": False,
                "problema": "a variável ERP_CHAVE_SEGREDOS não existe"}
    try:
        from cryptography.fernet import Fernet
        Fernet(bruta.encode())
        return {"tem": True, "serve": True, "problema": ""}
    except Exception:
        return {"tem": True, "serve": False, "problema": _porque_nao_serve(bruta)}


def recado_do_problema(o_que_nao_foi_guardado: str) -> str:
    # `o_que_nao_foi_guardado` vem com a frase inteira e concordada ("A senha
    # não foi guardada"): montá-la aqui a partir do substantivo daria "a senha
    # não foi guardado(a)", que é o português de formulário que ele não quer
    # ver na tela.
    """A frase que a tela mostra quando não dá para guardar um segredo.

    ⚑ MONTADA DA SITUAÇÃO REAL (17/11/2026). As três telas que guardam segredo
    diziam, as três, "falta a chave ERP_CHAVE_SEGREDOS" — mesmo quando a chave
    EXISTIA e estava torta. Mandar procurar o que já está lá é o jeito mais
    rápido de fazer alguém perder a tarde.
    """
    e = situacao()
    inicio = (f"{o_que_nao_foi_guardado}: "
              + ("a chave de segredos do ERP não serve — "
                 if e["tem"] else "falta a chave de segredos do ERP — ")
              + e["problema"] + ".")
    return (inicio + " Em Configurações › Chave de segredos há um botão que "
            "gera uma chave válida para copiar para a Environment do Render. "
            "O resto do cadastro foi salvo normalmente.")


def ha_chave() -> bool:
    """A tela usa isto para avisar ANTES de a pessoa digitar a senha.

    ⚑ MUDOU EM 17/11/2026, e o motivo foi um relato dele: *"fui gravar o email
    no cadastro da empresa e apareceu: A chave em ERP_CHAVE_SEGREDOS não tem o
    formato esperado."*

    Antes isto respondia só "a variável existe?". Com uma chave presente mas
    TORTA, a tela não avisava nada, ele preenchia a conta inteira, digitava a
    senha, clicava em Salvar — e só então levava o recado. Agora a pergunta é
    "a chave SERVE?", que é a que a tela realmente queria fazer.
    """
    return situacao()["serve"]


def _cofre():
    from cryptography.fernet import Fernet
    bruta = (os.environ.get(VARIAVEL) or "").strip()
    if not bruta:
        raise ErroValidacao(_RECADO_SEM_CHAVE)
    try:
        return Fernet(bruta.encode())
    except Exception:
        raise ErroValidacao(
            "A chave em ERP_CHAVE_SEGREDOS não serve: "
            f"{_porque_nao_serve(bruta)}. Ela não se escreve à mão — é "
            "sorteada uma vez. Em Configurações › Chave de segredos há um "
            "botão que gera uma válida para você copiar para a Environment "
            "do Render.")


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
