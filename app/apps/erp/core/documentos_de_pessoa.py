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


# ===========================================================================
# ⚑ VÁRIOS PAPÉIS DE UMA VEZ, NUMA LEITURA SÓ (19/11/2026)
#
# Ele: *"esse campo deveria ser um campo onde você pode jogar o documento
# dentro (…) e eu preciso poder colocar mais de um documento. (…) a pessoa
# coloca o RG e o RG é uma imagem da frente e uma imagem do verso. (…) eu
# quero jogar o RG, jogar a CNH, o comprovante de endereço, o CPF, e o sistema
# fazer a leitura de todas essas informações, preenchendo os dados da pessoa."*
#
# ⚑ CUSTO: UMA chamada de IA por lote, não uma por arquivo. A frente e o verso
# do RG vão juntos — e é assim que a IA junta o número (na frente) com a
# emissão e o órgão (no verso). Cinco fotos custam uma leitura.
#
# ⚑ A IA DIZ O QUE CADA ARQUIVO É, e um arquivo pode valer por mais de um
# papel: a CNH é identificação E traz o CPF. A tela marca os papéis exigidos
# pelo que a leitura reconheceu, e a pessoa pode corrigir o tipo à mão.
# ===========================================================================
TIPOS_RECONHECIDOS = ("DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA",
                      "CTPS-FOTO", "CTPS-INFORMACOES", "OUTRO")
TETO_DE_ARQUIVOS = 8
CAMPOS_DO_LOTE = ("nome", "cpf", "nome_mae", "data_nascimento", "rg",
                  "rg_emissao", "rg_orgao", "rg_uf", "cep", "logradouro",
                  "numero_endereco", "complemento", "bairro", "municipio", "uf",
                  "num_ctps", "num_pis")

_INSTRUCAO_LOTE = """Você recebe {n} arquivo(s), na ordem 0, 1, 2…, com os
documentos de UM trabalhador brasileiro, fotografados com celular em canteiro
de obra (torto, com sombra, às vezes frente e verso em fotos separadas).

Devolva SOMENTE um JSON:

{{
  "arquivos": [
    {{"indice": 0,
      "documento": "o que é (RG frente, RG verso, CNH, CPF, comprovante de
                    endereço, CTPS página da foto, CTPS página de informações,
                    outro)",
      "tipos": ["um ou mais entre: DOC-IDENTIDADE, CPF-DOC,
                 COMPROVANTE-ENDERECO-PESSOA, CTPS-FOTO, CTPS-INFORMACOES,
                 OUTRO — a CNH e o RG novo trazem o CPF, então valem por
                 DOC-IDENTIDADE e CPF-DOC"],
      "legivel": true ou false,
      "problema": "o que atrapalha a leitura, ou \\"\\""}}
  ],
  "campos": {{
    "nome": "", "cpf": "só os 11 dígitos", "nome_mae": "",
    "data_nascimento": "AAAA-MM-DD", "rg": "número do RG",
    "rg_emissao": "AAAA-MM-DD", "rg_orgao": "sigla do órgão (SSP, DETRAN…)",
    "rg_uf": "sigla do estado que emitiu o RG",
    "cep": "só dígitos", "logradouro": "rua/avenida", "numero_endereco": "",
    "complemento": "", "bairro": "", "municipio": "", "uf": "sigla",
    "num_ctps": "", "num_pis": "só dígitos"
  }}
}}

⚠️ NUNCA INVENTE. Campo que não estiver visível com certeza fica "". Um CPF
ou um RG errado impede a pessoa de receber. Preferir "" a chutar. O endereço
vem do comprovante de endereço; o RG, a emissão e o órgão vêm do RG (ou da
CNH, no campo de documento de identidade)."""


def _mime(nome: str) -> str:
    n = (nome or "").lower()
    if n.endswith(".pdf"):
        return "application/pdf"
    if n.endswith(".png"):
        return "image/png"
    if n.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def _uf(v: Any) -> str:
    t = "".join(c for c in str(v or "").upper() if c.isalpha())
    return t if len(t) == 2 else ""


def normalizar_campos(lido: dict[str, Any]) -> dict[str, str]:
    """O que a IA devolveu, no formato que o formulário guarda — e só o que
    passou na conferência de forma (CPF com 11 dígitos, data de verdade,
    UF com duas letras, CEP com oito)."""
    campos: dict[str, str] = {}
    for chave in ("nome", "nome_mae", "rg_orgao", "logradouro",
                  "numero_endereco", "complemento", "bairro", "municipio"):
        v = " ".join(str(lido.get(chave) or "").split())
        if v:
            campos[chave] = v
    cpf = _so_digitos(lido.get("cpf"))
    if len(cpf) == 11:
        campos["cpf"] = cpf
    for chave in ("data_nascimento", "rg_emissao"):
        d = _data(lido.get(chave))
        if d:
            campos[chave] = d
    rg = "".join(c for c in str(lido.get("rg") or "").upper()
                 if c.isalnum())
    if len(rg) >= 5:
        campos["rg"] = rg
    for chave in ("rg_uf", "uf"):
        u = _uf(lido.get(chave))
        if u:
            campos[chave] = u
    cep = _so_digitos(lido.get("cep"))
    if len(cep) == 8:
        campos["cep"] = cep
    for chave in ("num_ctps", "num_pis"):
        d = _so_digitos(lido.get(chave))
        if d:
            campos[chave] = d
    return campos


def ler_varios(arquivos: list[tuple[bytes, str]]) -> dict[str, Any]:
    """Lê o lote inteiro numa chamada. `arquivos`: [(conteudo, nome)].

    Devolve `{"arquivos": [{indice, nome, documento, tipos, aviso}],
    "campos": {...}}`. Nunca levanta por causa da leitura: falhou, volta
    vazio, e a tela pede para digitar.
    """
    arquivos = list(arquivos)[:TETO_DE_ARQUIVOS]
    vazio = {"arquivos": [{"indice": i, "nome": n, "documento": "",
                           "tipos": [], "aviso": "não consegui ler"}
                          for i, (_c, n) in enumerate(arquivos)],
             "campos": {}}
    if not arquivos:
        return {"arquivos": [], "campos": {}}
    try:
        from app.apps.erp.core.documentos.leitor import _cliente, _extrair_json
        import base64 as _b64
        import os as _os
        cliente = _cliente()
        modelo = _os.getenv("ERP_MODELO_IA_VISAO", "gpt-4o")
        partes: list[dict[str, Any]] = [
            {"type": "text", "text": _INSTRUCAO_LOTE.format(n=len(arquivos))}]
        for i, (conteudo, nome) in enumerate(arquivos):
            partes.append({"type": "text", "text": f"Arquivo {i}: {nome}"})
            partes.append({"type": "image_url", "image_url": {
                "url": f"data:{_mime(nome)};base64,"
                       + _b64.b64encode(conteudo).decode()}})
        resp = cliente.chat.completions.create(
            model=modelo, temperature=0, max_tokens=1500,
            messages=[{"role": "user", "content": partes}])
        lido = _extrair_json(resp.choices[0].message.content or "") or {}
    except Exception as e:                                # noqa: BLE001
        logger.warning("ERP/portal: não consegui ler o lote (%s)", e)
        return vazio

    por_indice = {}
    for a in (lido.get("arquivos") or []):
        try:
            por_indice[int(a.get("indice"))] = a
        except (TypeError, ValueError):
            continue
    saida = []
    for i, (_c, nome) in enumerate(arquivos):
        a = por_indice.get(i) or {}
        tipos = [str(x).strip().upper() for x in (a.get("tipos") or [])]
        tipos = [x for x in tipos if x in TIPOS_RECONHECIDOS]
        aviso = ""
        if a.get("legivel") is False:
            problema = str(a.get("problema") or "").strip()
            aviso = ("difícil de ler" + (f" ({problema})" if problema else "")
                     + " — vale fotografar de novo")
        saida.append({"indice": i, "nome": nome,
                      "documento": str(a.get("documento") or "").strip(),
                      "tipos": tipos or ["OUTRO"], "aviso": aviso})
    return {"arquivos": saida,
            "campos": normalizar_campos(lido.get("campos") or {})}
