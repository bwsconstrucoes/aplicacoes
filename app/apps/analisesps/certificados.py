# -*- coding: utf-8 -*-
"""
Os certificados digitais A1, guardados pela tela em vez de por variável.

Pedido do dono em 12/09/2026: *"não daria pra adicionar o certificado a partir
da tela de configurações, inserir o arquivo, e adicionar lá? Que facilitaria
uma troca ou a inclusão de outros certificados de outras empresas."*

E o ganho é maior que a conveniência. Com o certificado em variável de
ambiente, **cada troca é mexer no Render e reiniciar o serviço** — e ele vence
todo ano. Cada empresa nova é uma variável nova. Pela tela, é subir um arquivo.

⚠️ ESTA É A CREDENCIAL MAIS SENSÍVEL DO SISTEMA. Com o arquivo e a senha,
qualquer um **emite nota em nome da empresa**. Três coisas decorrem disso, e
nenhuma é opcional:

  1. **O arquivo e a senha ficam CIFRADOS no banco.** Um vazamento do banco —
     backup esquecido, acesso indevido — entrega bytes embaralhados. A chave
     que decifra vive FORA do banco, no ambiente.
  2. **Não há como baixar de volta.** A tela mostra de quem é, até quando vale
     e quem subiu. O conteúdo só sai daqui para dentro do próprio sistema, na
     hora de falar com a Receita. Não existe rota que devolva o arquivo.
  3. **A validade é lida de DENTRO do arquivo**, não digitada. Data digitada à
     mão erra, e o erro só aparece no dia em que a busca para.

A CHAVE QUE CIFRA. Vem de `ANALISESPS_CHAVE_COFRE`, e não tem padrão: sem ela o
sistema **recusa guardar** em vez de guardar em texto puro. Guardar credencial
sem cifra porque a chave não foi configurada seria o pior dos dois mundos — a
conveniência da tela com o risco de um arquivo aberto no banco.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import re

logger = logging.getLogger("analisesps.certificados")

# Quantos dias antes do vencimento a tela começa a avisar. Trinta dá tempo de
# comprar e trocar sem correria — renovar A1 não é imediato.
DIAS_DE_AVISO = 30

# Teto do arquivo. Um .pfx tem poucos KB; qualquer coisa maior não é
# certificado, e não faz sentido carregar na memória para descobrir.
MAXIMO_ARQUIVO = 512 * 1024


class ErroDeCertificado(RuntimeError):
    """Falha com a mensagem já pronta para a tela."""


class SemCofre(RuntimeError):
    """Falta a chave que cifra — e sem ela não se guarda nada."""


# ---------------------------------------------------------------------------
# O cofre
# ---------------------------------------------------------------------------
def _cofre():
    """A cifra que protege arquivo e senha no banco.

    A CHAVE NÃO TEM PADRÃO DE PROPÓSITO. Sem ela, guardar seria guardar em
    texto puro — a conveniência da tela com o risco de um certificado aberto no
    banco, que é pior do que os dois separados."""
    from cryptography.fernet import Fernet

    segredo = os.getenv("ANALISESPS_CHAVE_COFRE", "").strip()
    if not segredo:
        raise SemCofre(
            "Falta a chave que protege o certificado (ANALISESPS_CHAVE_COFRE, "
            "no Render). Sem ela o certificado seria guardado aberto no banco, "
            "e isso não vai acontecer. Peça para gerar uma chave e configurar.")
    # Aceita qualquer texto como chave: o que a cifra exige são 32 bytes, e
    # derivar disso é melhor do que obrigar quem configura a gerar um formato
    # específico — configuração difícil é configuração que ninguém faz.
    derivada = base64.urlsafe_b64encode(hashlib.sha256(segredo.encode()).digest())
    return Fernet(derivada)


def cofre_configurado() -> bool:
    return bool(os.getenv("ANALISESPS_CHAVE_COFRE", "").strip())


# ---------------------------------------------------------------------------
# Ler o que está DENTRO do arquivo
# ---------------------------------------------------------------------------
def _abrir(conteudo: bytes, senha: str) -> dict:
    """Abre o .pfx e devolve titular, CNPJ e validade — tirados de dentro dele.

    ABRIR AQUI É A VALIDAÇÃO. Se a senha estiver errada ou o arquivo não for um
    certificado, o erro aparece NA HORA DE SUBIR, com a pessoa olhando — e não
    semanas depois, quando a busca parar de trazer nota e ninguém souber por
    quê."""
    from cryptography.hazmat.primitives.serialization import pkcs12

    try:
        chave, certificado, _ = pkcs12.load_key_and_certificates(
            conteudo, (senha or "").encode("utf-8"))
    except Exception as e:  # noqa: BLE001 — a mensagem tem de dizer o que fazer
        raise ErroDeCertificado(
            "Não consegui abrir o certificado. Confira a senha — e confira se "
            f"o arquivo é mesmo um A1 (.pfx ou .p12). Detalhe: {e}") from e
    if certificado is None or chave is None:
        raise ErroDeCertificado(
            "O arquivo abriu, mas não traz um certificado com chave privada. "
            "Um A1 tem os dois.")

    titular = ""
    for parte in certificado.subject:
        if parte.oid.dotted_string == "2.5.4.3":        # CN, o nome do titular
            titular = str(parte.value)
            break

    # O CNPJ está DENTRO do nome do titular, no padrão da ICP-Brasil:
    # "EMPRESA LTDA:12345678000199". Tirar dali é mais confiável do que pedir
    # para a pessoa digitar — e impede subir o certificado de uma empresa
    # dizendo que é de outra.
    digitos = re.findall(r"\d{14}", titular.replace(".", "").replace("/", "")
                         .replace("-", ""))
    return {
        "titular": titular,
        "cnpj": digitos[0] if digitos else "",
        "valido_ate": _validade(certificado),
    }


def _validade(certificado):
    """A data em que ele para de valer.

    A biblioteca trocou o nome desta propriedade (`not_valid_after` virou
    `not_valid_after_utc`). Tentar as duas é o que evita a busca parar por
    causa de uma atualização de biblioteca."""
    for nome in ("not_valid_after_utc", "not_valid_after"):
        valor = getattr(certificado, nome, None)
        if valor is not None:
            return valor.date()
    return None


# ---------------------------------------------------------------------------
# Guardar e ler
# ---------------------------------------------------------------------------
def guardar(conteudo: bytes, senha: str, apelido: str, quem: str) -> dict:
    """Confere, cifra e guarda. Devolve o que a tela mostra."""
    from .db import conexao

    if not conteudo:
        raise ErroDeCertificado("O arquivo chegou vazio.")
    if len(conteudo) > MAXIMO_ARQUIVO:
        raise ErroDeCertificado(
            "Arquivo grande demais para ser um certificado A1 — eles têm "
            "poucos KB. Confira se não mandou o arquivo errado.")
    if not (senha or "").strip():
        raise ErroDeCertificado("A senha do certificado é obrigatória.")

    cofre = _cofre()                 # antes de abrir: sem cofre, nem começa
    dados = _abrir(conteudo, senha)
    if not dados["cnpj"]:
        raise ErroDeCertificado(
            "Não achei o CNPJ dentro do certificado. Ele é de pessoa física? "
            "A busca na Receita precisa de um certificado de empresa (e-CNPJ).")

    with conexao() as conn:
        conn.execute(
            "INSERT INTO analisesps.certificados "
            "  (cnpj, apelido, titular, arquivo, senha, valido_ate, ativo, "
            "   subido_por, subido_em) "
            "VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, now()) "
            "ON CONFLICT (cnpj) DO UPDATE SET "
            "  apelido = EXCLUDED.apelido, titular = EXCLUDED.titular, "
            "  arquivo = EXCLUDED.arquivo, senha = EXCLUDED.senha, "
            "  valido_ate = EXCLUDED.valido_ate, ativo = TRUE, "
            "  subido_por = EXCLUDED.subido_por, subido_em = now()",
            (dados["cnpj"], (apelido or "").strip()[:120], dados["titular"][:200],
             cofre.encrypt(conteudo), cofre.encrypt((senha or "").encode()),
             dados["valido_ate"], (quem or "")[:120]))
        conn.commit()

    logger.info("Análise de SPs: certificado de %s guardado por %s, vale até %s.",
                dados["cnpj"], quem or "sem nome", dados["valido_ate"])
    return dados


def abrir_para_uso(cnpj: str) -> tuple:
    """O arquivo e a senha, decifrados. SÓ para falar com a Receita.

    Não existe rota que chame isto: o conteúdo nunca sai do servidor."""
    from .db import consultar_um

    linha = consultar_um(
        "SELECT arquivo, senha FROM analisesps.certificados "
        " WHERE cnpj = ? AND ativo", (re.sub(r"\D", "", cnpj),))
    if not linha:
        raise ErroDeCertificado(
            f"Não há certificado guardado para o CNPJ {cnpj}.")
    cofre = _cofre()
    return (cofre.decrypt(bytes(linha[0])),
            cofre.decrypt(bytes(linha[1])).decode("utf-8"))


def listar() -> list:
    """Os certificados guardados — SEM o arquivo e SEM a senha.

    O que a tela mostra é de quem é, até quando vale e quem subiu. O conteúdo
    não aparece em consulta nenhuma."""
    from .db import consultar

    try:
        linhas = consultar(
            "SELECT cnpj, apelido, titular, valido_ate, ativo, subido_por, "
            "       subido_em, "
            "       (valido_ate - (now() AT TIME ZONE 'America/Sao_Paulo')::date) "
            "  FROM analisesps.certificados ORDER BY valido_ate NULLS LAST")
    except Exception:  # noqa: BLE001 — migração 009 ainda não aplicada
        logger.exception("Análise de SPs: não consegui listar os certificados")
        return []

    saida = []
    for l in linhas:
        dias = l[7]
        saida.append({
            "cnpj": l[0], "apelido": l[1], "titular": l[2], "valido_ate": l[3],
            "ativo": l[4], "subido_por": l[5], "subido_em": l[6],
            "dias_para_vencer": dias,
            # O A1 PARA DE FUNCIONAR CALADO no dia seguinte ao vencimento.
            # Avisar antes é o que evita descobrir pela nota que não chegou.
            "vencido": dias is not None and dias < 0,
            "vencendo": dias is not None and 0 <= dias <= DIAS_DE_AVISO,
        })
    return saida


def cnpjs_ativos() -> list:
    """Os CNPJs que têm certificado válido — é por eles que a busca passa.

    NÃO INCLUI OS VENCIDOS. Consultar a Receita com certificado vencido só
    produz recusa, e a recusa gasta a cota de consultas."""
    from .db import consultar

    try:
        linhas = consultar(
            "SELECT cnpj FROM analisesps.certificados "
            " WHERE ativo AND (valido_ate IS NULL "
            "   OR valido_ate >= (now() AT TIME ZONE 'America/Sao_Paulo')::date)"
            " ORDER BY cnpj")
    except Exception:  # noqa: BLE001 — migração ainda não aplicada
        return []
    return [l[0] for l in linhas]


def remover(cnpj: str, quem: str) -> bool:
    """Tira o certificado de uso. APAGA de verdade, e é o certo aqui.

    Guardar credencial "desativada" é guardar credencial — e o motivo de tirar
    costuma ser justamente que ela não deveria mais existir."""
    from .db import conexao

    with conexao() as conn:
        cur = conn.execute(
            "DELETE FROM analisesps.certificados WHERE cnpj = ?",
            (re.sub(r"\D", "", cnpj),))
        quantos = cur.rowcount or 0
        cur.close()
        conn.commit()
    if quantos:
        logger.warning("Análise de SPs: certificado de %s REMOVIDO por %s.",
                       cnpj, quem or "sem nome")
    return bool(quantos)
