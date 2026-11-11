# -*- coding: utf-8 -*-
"""
ERP — LER O PAPEL DO COLABORADOR e preencher o cadastro com o que ele diz.

Ele, em 10/11/2026, cobrando pela enésima vez: *"no cadastro colaborador não
tem campo para anexar os documentos. Como é que vai ser cadastrado esse
colaborador? Como é que a IA vai fazer a leitura do código, ler as
informações? Não estou entendendo qual é essa dificuldade de colocar isso."*

E, sobre a conferência: *"não sei se na pré-análise assim, o formulário tem
que ser inteligente também. Se a pessoa mandar alguma coisa faltando, como é
que a gente vai tratar isso aí?"*

⚑ TRÊS REGRAS, e elas decidem o comportamento inteiro:

1. **AVISA, NÃO BARRA.** A leitura aponta o que parece errado e preenche o
   que está vazio. Ela NÃO impede o envio: a IA erra, e um aviso que barra
   vira parede justamente na hora em que ela errou. O bloqueio objetivo é
   outro, e vive em `pessoal_solicitacoes`: não anexou, não sai.

2. **NÃO SOBRESCREVE O QUE A PESSOA ESCREVEU.** Quem digita e depois anexa
   não pode ver o que digitou sumir. Quem decide é a tela, e ela só preenche
   campo vazio — mas a regra está escrita aqui porque é dela.

3. **A CONFERÊNCIA É NA HORA.** É o ganho real: a pessoa ainda está ao lado
   do colaborador, com os papéis na mão. Hoje o DP descobre a foto tremida
   três dias depois, quando a obra já dispersou.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Só o que o cadastro precisa. Pedir mais é pedir à IA que invente.
CAMPOS_QUE_O_PAPEL_PREENCHE = ("nome", "cpf", "data_nascimento", "telefone",
                               "num_ctps", "num_pis")

# O que cada papel DEVE ser. A leitura compara com o que achou e avisa quando
# não bate — sem impedir, porque documento de obra vem torto e a IA erra.
O_QUE_ESPERO = {
    "DOC-IDENTIDADE": ("um documento de identificação (RG, CNH ou CTPS)",
                       ("RG", "CNH", "IDENTIDADE", "CTPS", "CARTEIRA")),
    "CPF-DOC": ("o CPF", ("CPF",)),
    "COMPROVANTE-ENDERECO-PESSOA": ("um comprovante de endereço",
                                    ("COMPROVANTE", "ENDERECO", "FATURA",
                                     "CONTA", "CONCESSIONARIA")),
    "CTPS-FOTO": ("a página da CTPS com a foto", ("CTPS", "CARTEIRA")),
    "CTPS-INFORMACOES": ("a página da CTPS com as informações",
                         ("CTPS", "CARTEIRA")),
    "DOC-DEPENDENTE": ("a certidão ou o RG do dependente",
                       ("CERTIDAO", "NASCIMENTO", "RG", "IDENTIDADE")),
}

_INSTRUCAO = """Você lê documentos de identificação de trabalhadores
brasileiros, fotografados com celular em canteiro de obra — torto, com
sombra, às vezes cortado.

Devolva SOMENTE um JSON com estas chaves, usando "" no que não conseguir ler
com certeza:

{{
  "documento": "o que é este papel (RG, CNH, CTPS, CPF, comprovante de
                endereço, certidão de nascimento, outro)",
  "nome": "nome completo da pessoa",
  "cpf": "só os 11 dígitos",
  "data_nascimento": "AAAA-MM-DD",
  "telefone": "só dígitos, com DDD",
  "num_ctps": "número da carteira de trabalho",
  "num_pis": "número do PIS/PASEP/NIT",
  "legivel": true ou false,
  "problema": "o que atrapalha a leitura (cortado, tremido, sombra), ou \\"\\""
}}

⚠️ NUNCA INVENTE. Se o campo não estiver visível no documento, devolva "".
Um CPF errado num cadastro impede a pessoa de receber. Preferir "" a chutar.
Esperava-se {esperado}."""


def _so_digitos(v: Any) -> str:
    return "".join(c for c in str(v or "") if c.isdigit())


def _data(v: Any) -> str:
    texto = str(v or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto):
        return texto
    m = re.fullmatch(r"(\d{2})[/-](\d{2})[/-](\d{4})", texto)
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""


def ler(conteudo: bytes, nome_do_arquivo: str, tipo: str) -> dict[str, Any]:
    """O que este papel diz — e se ele é o papel que se pediu.

    Devolve `{"campos": {...}, "resumo": "...", "aviso": "..."}`. Nunca
    levanta por causa da leitura: quem chama trata a falha como "anexado, não
    consegui ler".
    """
    esperado, pistas = O_QUE_ESPERO.get(
        str(tipo or "").upper(), ("um documento da pessoa", ()))
    lido: dict[str, Any] = {}
    try:
        from app.apps.erp.core.documentos.leitor import _cliente, _extrair_json
        import base64 as _b64
        cliente = _cliente()
        import os as _os
        modelo = _os.getenv("ERP_MODELO_IA_VISAO", "gpt-4o")
        dado = _b64.b64encode(conteudo).decode()
        mime = ("application/pdf" if nome_do_arquivo.lower().endswith(".pdf")
                else "image/jpeg")
        resp = cliente.chat.completions.create(
            model=modelo, temperature=0, max_tokens=700,
            messages=[{"role": "user", "content": [
                {"type": "text",
                 "text": _INSTRUCAO.format(esperado=esperado)},
                {"type": "image_url",
                 "image_url": {"url": f"data:{mime};base64,{dado}"}}]}])
        lido = _extrair_json(resp.choices[0].message.content or "") or {}
    except Exception as e:                                # noqa: BLE001
        logger.warning("ERP/portal: não consegui ler o documento (%s)", e)
        return {"campos": {}, "resumo": "anexado (não consegui ler)",
                "aviso": ""}

    campos: dict[str, str] = {}
    if str(lido.get("nome") or "").strip():
        campos["nome"] = str(lido["nome"]).strip()
    cpf = _so_digitos(lido.get("cpf"))
    if len(cpf) == 11:
        campos["cpf"] = cpf
    nasc = _data(lido.get("data_nascimento"))
    if nasc:
        campos["data_nascimento"] = nasc
    for chave in ("telefone", "num_ctps", "num_pis"):
        d = _so_digitos(lido.get(chave))
        if d:
            campos[chave] = d
    campos = {k: v for k, v in campos.items()
              if k in CAMPOS_QUE_O_PAPEL_PREENCHE}

    achado = str(lido.get("documento") or "").strip()
    resumo = achado or "anexado"
    if campos.get("nome"):
        resumo += f" de {campos['nome']}"

    # ⚑ O AVISO. Ele não barra nada — é a frase que a pessoa lê ainda ao lado
    # do colaborador, que é quando dá para resolver.
    aviso = ""
    seco = "".join(c for c in achado.upper() if c.isalnum() or c == " ")
    if pistas and achado and not any(p in seco for p in pistas):
        aviso = (f"Isto parece {achado.lower()}, e aqui se esperava "
                 f"{esperado}. Confira se não trocou o lugar do arquivo.")
    elif lido.get("legivel") is False:
        problema = str(lido.get("problema") or "").strip()
        aviso = ("Não consegui ler direito este documento"
                 + (f" ({problema})" if problema else "")
                 + ". Vale tirar outra foto, com a página inteira e sem "
                   "sombra — o DP vai precisar ler isto.")
    return {"campos": campos, "resumo": resumo, "aviso": aviso}
