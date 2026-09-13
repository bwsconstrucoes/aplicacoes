# -*- coding: utf-8 -*-
"""Análise de SPs — o cofre dos certificados digitais.

⚠️ ESTA É A CREDENCIAL MAIS SENSÍVEL DO SISTEMA. Com o arquivo e a senha,
qualquer um emite nota em nome da empresa. Por isso estes testes olham menos o
"funciona" e mais o **o que não pode acontecer**: o arquivo legível no banco, o
conteúdo saindo por alguma rota, e o certificado vencido passando batido.

O certificado usado aqui é **fabricado no próprio teste** — não há A1 de
verdade fora do Render, e não deve haver.
"""
from __future__ import annotations

import datetime as dt

import pytest

from app.apps.analisesps import certificados


CNPJ = "10656452007869"
SENHA = "senha-de-teste"


def pfx(cnpj=CNPJ, nome="BWS CONSTRUCOES LTDA", senha=SENHA,
        vence=dt.datetime(2027, 1, 1)) -> bytes:
    """Um certificado de mentira, com a mesma forma de um A1 de verdade —
    inclusive o CNPJ dentro do nome do titular, como manda a ICP-Brasil."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    chave = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    titular = x509.Name([x509.NameAttribute(
        NameOID.COMMON_NAME, f"{nome}:{cnpj}" if cnpj else nome)])
    cert = (x509.CertificateBuilder().subject_name(titular).issuer_name(titular)
            .public_key(chave.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(dt.datetime(2026, 1, 1))
            .not_valid_after(vence).sign(chave, hashes.SHA256()))
    return pkcs12.serialize_key_and_certificates(
        b"teste", chave, cert, None,
        serialization.BestAvailableEncryption(senha.encode()))


@pytest.fixture
def cofre(monkeypatch):
    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "chave-de-teste")


# ---------------------------------------------------------------------------
# LER O QUE ESTÁ DENTRO DO ARQUIVO
# ---------------------------------------------------------------------------
def test_o_CNPJ_e_a_validade_saem_de_DENTRO_do_certificado(cofre):
    """Não são digitados. Pedir para a pessoa digitar o CNPJ deixaria subir o
    certificado de uma empresa dizendo que é de outra — e a data digitada erra,
    com o erro aparecendo só no dia em que a busca parar."""
    dados = certificados._abrir(pfx(), SENHA)
    assert dados["cnpj"] == CNPJ
    assert dados["valido_ate"] == dt.date(2027, 1, 1)
    assert "BWS" in dados["titular"]


def test_senha_errada_avisa_NA_HORA_DE_SUBIR(cofre):
    """Com a pessoa olhando — e não semanas depois, quando a busca parar de
    trazer nota e ninguém souber por quê."""
    with pytest.raises(certificados.ErroDeCertificado) as erro:
        certificados._abrir(pfx(), "senha-errada")
    assert "senha" in str(erro.value).lower()


def test_arquivo_que_nao_e_certificado_avisa_o_que_e_esperado(cofre):
    with pytest.raises(certificados.ErroDeCertificado) as erro:
        certificados._abrir(b"isto nao e um pfx", SENHA)
    assert ".pfx" in str(erro.value) or "A1" in str(erro.value)


# ---------------------------------------------------------------------------
# O COFRE — o que NÃO pode acontecer
# ---------------------------------------------------------------------------
def test_SEM_a_chave_do_cofre_o_sistema_RECUSA_guardar(monkeypatch):
    """A regra que não se negocia: guardar sem cifrar seria a conveniência da
    tela COM o risco de um certificado aberto no banco — pior do que os dois
    separados. Por isso a chave não tem padrão."""
    monkeypatch.delenv("ANALISESPS_CHAVE_COFRE", raising=False)
    assert certificados.cofre_configurado() is False
    with pytest.raises(certificados.SemCofre) as erro:
        certificados._cofre()
    assert "ANALISESPS_CHAVE_COFRE" in str(erro.value)


def test_o_arquivo_guardado_NAO_e_legivel(cofre):
    """Um vazamento do banco — backup esquecido, acesso indevido — tem de
    entregar bytes embaralhados, não o certificado."""
    original = pfx()
    guardado = certificados._cofre().encrypt(original)
    assert original not in guardado
    assert b"BWS" not in guardado
    assert certificados._cofre().decrypt(guardado) == original


def test_a_senha_tambem_e_cifrada(cofre):
    """De nada adiantaria cifrar o arquivo e deixar a senha ao lado, aberta."""
    guardada = certificados._cofre().encrypt(SENHA.encode())
    assert SENHA.encode() not in guardada


def test_chave_de_cofre_diferente_nao_abre(cofre, monkeypatch):
    """Trocar a chave sem trocar o conteúdo torna o certificado ilegível — é o
    esperado, e é por isso que a chave é do ambiente e não muda sozinha."""
    from cryptography.fernet import InvalidToken

    guardado = certificados._cofre().encrypt(b"conteudo")
    monkeypatch.setenv("ANALISESPS_CHAVE_COFRE", "outra-chave")
    with pytest.raises(InvalidToken):
        certificados._cofre().decrypt(guardado)


def test_NAO_existe_rota_que_devolva_o_certificado():
    """O conteúdo só sai do banco para dentro do próprio sistema. Se um dia
    alguém criar uma rota de download "para conferir", este teste acusa."""
    from pathlib import Path

    web = Path("app/apps/analisesps/web.py").read_text(encoding="utf-8")
    assert "abrir_para_uso" not in web, (
        "uma rota passou a decifrar o certificado — o conteúdo não pode "
        "trafegar para fora do servidor")


def test_a_listagem_nao_traz_arquivo_nem_senha():
    """O que a tela mostra é de quem é, até quando vale e quem subiu."""
    import inspect
    codigo = inspect.getsource(certificados.listar)
    assert "arquivo" not in codigo.split("SELECT")[1].split("FROM")[0]
    assert "senha" not in codigo.split("SELECT")[1].split("FROM")[0]


# ---------------------------------------------------------------------------
# O VENCIMENTO — o A1 para de funcionar calado
# ---------------------------------------------------------------------------
def test_o_certificado_de_pessoa_fisica_e_recusado(cofre, monkeypatch):
    """A busca na Receita precisa de e-CNPJ. Recusar aqui é melhor do que
    aceitar e falhar na primeira consulta, gastando cota."""
    monkeypatch.setattr(certificados, "_cofre", lambda: _CofreFalso())
    with pytest.raises(certificados.ErroDeCertificado) as erro:
        certificados.guardar(pfx(cnpj="", nome="FULANO DE TAL"), SENHA, "", "eu")
    assert "pessoa física" in str(erro.value)


class _CofreFalso:
    def encrypt(self, b):
        return b"cifrado:" + b


def test_arquivo_grande_demais_nao_e_certificado(cofre):
    with pytest.raises(certificados.ErroDeCertificado) as erro:
        certificados.guardar(b"x" * (certificados.MAXIMO_ARQUIVO + 1),
                             SENHA, "", "eu")
    assert "grande demais" in str(erro.value)


def test_senha_em_branco_e_recusada(cofre):
    with pytest.raises(certificados.ErroDeCertificado):
        certificados.guardar(pfx(), "", "", "eu")


def test_o_aviso_de_vencimento_existe_e_da_tempo_de_trocar():
    """Renovar A1 não é imediato. Avisar no dia do vencimento seria avisar
    tarde."""
    assert certificados.DIAS_DE_AVISO >= 15
