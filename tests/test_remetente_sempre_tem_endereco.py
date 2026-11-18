"""O REMETENTE SEMPRE SAI COM UM ENDEREÇO DENTRO.

⚑ Este arquivo nasceu de um defeito que parou o dono em 18/11/2026, e vale
registrar o sintoma porque ele apontava para o lado errado:

> *"Servidor de entrada deu certo, mas o de saída fica dando isso: O servidor
> recusou o(s) endereço(s): <o e-mail dele>."* E depois: *"esses e-mails usamos
> há muito tempo e não [há] restrição nenhuma."*

A suspeita natural era o provedor, ou o endereço de destino. Não era nenhum
dos dois: a tela pede **"como aparece para quem recebe"**, e quem lê isso
escreve um NOME ("DP — BWS Construções"). Esse texto virava o `From` cru da
mensagem. Um `From` sem `@` não é endereço — o `smtplib` tira dele o remetente
do envelope, manda um envelope torto, e o servidor recusa o **destinatário**.

Por isso a recusa falava do endereço de destino, que estava certo o tempo todo.

A regra que estes casos prendem: **o que sai no `From` tem sempre um endereço
dentro**, venha o campo preenchido como vier.
"""
from __future__ import annotations

from email.utils import parseaddr

import pytest

from app.apps.erp.core.comum.email import remetente_de


class Conta:
    """O mínimo que `remetente_de` olha — serve para empresa e para a conta
    de área (`ContaDoSistema`), que de propósito têm os mesmos nomes de campo."""

    def __init__(self, **campos):
        self.smtp_remetente = campos.get("remetente", "")
        self.smtp_usuario = campos.get("usuario", "")
        self.email = campos.get("email", "")
        self.nome_fantasia = campos.get("fantasia", "")
        self.razao_social = campos.get("razao", "")


@pytest.mark.parametrize("escrito", [
    "DP — BWS Construções",          # o caso dele, com travessão
    "DP BWS",
    "Departamento Pessoal",
    "ERP BWS",
    "  Financeiro  ",
])
def test_nome_sem_arroba_ganha_o_endereco_da_conta(escrito):
    r = remetente_de(Conta(remetente=escrito, usuario="dp@bws.com.br"))

    nome, endereco = parseaddr(r)
    assert endereco == "dp@bws.com.br", (
        f"“{escrito}” virou o From “{r}” — sem endereço, o servidor recusa o "
        f"destinatário e a culpa parece ser dele")
    # ⚑ NOME COM ACENTO VOLTA CODIFICADO, e tem de voltar mesmo: é a regra do
    # e-mail para cabeçalho fora do ASCII ("=?utf-8?b?...?="). Quem lê a
    # mensagem vê o nome certo; aqui a comparação decodifica antes.
    from email.header import decode_header, make_header
    assert str(make_header(decode_header(nome))).strip() == escrito.strip()


@pytest.mark.parametrize("escrito", [
    "dp@bws.com.br",
    "DP — BWS <dp@bws.com.br>",
    "BWS Construções <contato@bws.com.br>",
])
def test_quem_ja_escreveu_endereco_e_respeitado(escrito):
    """Quem preencheu certo não pode ter o que escreveu trocado."""
    r = remetente_de(Conta(remetente=escrito, usuario="outra@bws.com.br"))

    assert r == escrito
    assert "@" in parseaddr(r)[1]


def test_sem_remetente_escrito_usa_a_conta():
    r = remetente_de(Conta(usuario="sistema@bws.com.br", fantasia="ERP BWS"))

    nome, endereco = parseaddr(r)
    assert endereco == "sistema@bws.com.br" and nome == "ERP BWS"


def test_sem_nada_nao_inventa_endereco():
    """Sem conta nenhuma não há endereço a pôr — e inventar um seria pior."""
    assert "@" not in remetente_de(Conta(remetente="Só um nome"))


def test_o_MESSAGE_ID_sai_com_dominio_de_verdade():
    """O Message-ID é tirado do domínio do From, e é por ele que a resposta da
    contabilidade casa com o pedido. From sem `@` dava um Message-ID torto, e a
    resposta nunca casaria."""
    from email.utils import make_msgid

    r = remetente_de(Conta(remetente="DP — BWS Construções",
                           usuario="dp@bws.com.br"))
    dominio = parseaddr(r)[1].split("@")[-1]

    assert dominio == "bws.com.br"
    assert make_msgid(domain=dominio).endswith("@bws.com.br>")
