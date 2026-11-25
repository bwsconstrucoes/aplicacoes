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
    # ⚑ 21/11/2026: o exame admissional vem junto, sem travar (ele: *"deixar
    # preparar para receber ele junto com os demais documentos (…) por
    # enquanto sem travar"*).
    "ASO": ("o ASO (atestado de saúde ocupacional)",
            ("ASO", "SAUDE OCUPACIONAL", "APTO", "EXAME")),
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
        imagens = _imagens_para_ia(conteudo, nome_do_arquivo)
        resp = cliente.chat.completions.create(
            model=modelo, temperature=0, max_tokens=700,
            messages=[{"role": "user", "content": [
                {"type": "text",
                 "text": _INSTRUCAO.format(esperado=esperado)}] + [
                {"type": "image_url", "image_url": {"url": u}} for u in imagens]}])
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
# ⚑ DOC-DEPENDENTE entrou em 25/11/2026. Ele, cadastrando uma criança: *"ele
# só tem certidão de nascimento e o sistema tá exigindo RG."* A leitura não
# conhecia a certidão: ela voltava "não reconhecida" e o pedido de dependente
# não saía.
TIPOS_RECONHECIDOS = ("DOC-IDENTIDADE", "CPF-DOC", "COMPROVANTE-ENDERECO-PESSOA",
                      "CTPS-FOTO", "CTPS-INFORMACOES", "ASO", "DOC-DEPENDENTE",
                      "OUTRO")
TETO_DE_ARQUIVOS = 8
CAMPOS_DO_LOTE = ("nome", "cpf", "nome_mae", "data_nascimento", "rg",
                  "rg_emissao", "rg_orgao", "rg_uf", "cep", "logradouro",
                  "numero_endereco", "complemento", "bairro", "municipio", "uf",
                  "num_ctps", "num_pis")

_INSTRUCAO_LOTE = """Você recebe {n} arquivo(s), na ordem 0, 1, 2…, com os
documentos de UM trabalhador brasileiro — ou de UM dependente dele —,
fotografados com celular em canteiro de obra (torto, com sombra, às vezes
frente e verso em fotos separadas).

Se for CERTIDÃO DE NASCIMENTO, "nome" e "data_nascimento" são os de quem
NASCEU (a criança), nunca os da mãe ou do pai; os pais vão em "nome_mae" e
"nome_pai". Se for CERTIDÃO DE CASAMENTO, "nome" é o do cônjuge que não é o
trabalhador, quando der para saber; se não der, deixe "".

Devolva SOMENTE um JSON:

{{
  "arquivos": [
    {{"indice": 0,
      "documento": "o que é (RG frente, RG verso, CNH, CPF, comprovante de
                    endereço, CTPS página da foto, CTPS página de informações,
                    certidão de nascimento, certidão de casamento, outro)",
      "tipos": ["um ou mais entre: DOC-IDENTIDADE, CPF-DOC,
                 COMPROVANTE-ENDERECO-PESSOA, CTPS-FOTO, CTPS-INFORMACOES,
                 ASO (atestado de saúde ocupacional, o exame admissional),
                 DOC-DEPENDENTE (certidão de nascimento ou de casamento),
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
    "num_ctps": "", "num_pis": "só dígitos",
    "nome_pai": "", "sexo": "M ou F", "estado_civil": "SOLTEIRO, CASADO,
    DIVORCIADO, VIUVO ou UNIAO_ESTAVEL", "naturalidade": "cidade onde nasceu",
    "naturalidade_uf": "sigla", "ctps_serie": "", "ctps_uf": "sigla",
    "ctps_emissao": "AAAA-MM-DD"
  }}
}}

⚠️ NUNCA INVENTE. Campo que não estiver visível com certeza fica "". Um CPF
ou um RG errado impede a pessoa de receber. Preferir "" a chutar. O endereço
vem do comprovante de endereço; o RG, a emissão e o órgão vêm do RG (ou da
CNH, no campo de documento de identidade)."""


# ⚑ PDF E FOTO GRANDE (25/11/2026). Ele, com uma certidão: *"a IA não tá
# conseguindo fazer a leitura."* As duas leituras deste arquivo mandavam o
# arquivo CRU como imagem. PDF cru a IA recusa — e certidão chega muito em
# PDF (a digital, do cartório) —, e foto de celular inteira (12 MP) estoura o
# tamanho; nos dois casos voltava "não consegui ler". Agora passam pelo mesmo
# preparo do leitor de documentos do ERP: PDF vira imagem página a página
# (até 2 por arquivo — documento de pessoa não passa disso) e a foto é
# reduzida para 1800 px.
PAGINAS_POR_ARQUIVO = 2


def _imagens_para_ia(conteudo: bytes, nome: str) -> list[str]:
    """As imagens deste arquivo, prontas para a IA (data URL)."""
    from app.apps.erp.core.documentos import leitor
    if (nome or "").lower().endswith(".pdf") or conteudo[:5] == b"%PDF-":
        paginas = leitor._paginas_como_png(conteudo)[:PAGINAS_POR_ARQUIVO]
        return [f"data:image/png;base64,{p}" for p in paginas]
    dado, mime = leitor._preparar_imagem(conteudo)
    return [f"data:{mime};base64,{dado}"]


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


# ⚑ CAIXA ALTA E SEM ACENTO (19/11/2026). Ele: *"eu queria padronizar a
# nomenclatura das informações. Eu queria tudo caixa alta, que na leitura da IA
# fosse removido a acentuação e ficasse tudo caixa alta."* Vale para os campos
# de TEXTO da pessoa — nome, mãe, órgão, endereço. Número, data e sigla já têm
# forma própria.
CAMPOS_DE_TEXTO = ("nome", "nome_mae", "rg_orgao", "logradouro",
                   "numero_endereco", "complemento", "bairro", "municipio",
                   "nome_pai", "naturalidade")
# ⚑ O QUE A LEITURA ACHAR ALÉM DO OBRIGATÓRIO TAMBÉM ENTRA (19/11/2026). Ele:
# *"outros dados que a leitura da IA conseguir coletar, mesmo que não sejam
# obrigatórios, inclusive o PIS, nome do pai e qualquer outra coisa a mais que
# tenha no nosso cadastro, pode preencher nos campos, mesmo que estejam
# ocultos."* Estes não têm campo no formulário: vão no pedido como "lido do
# documento" e chegam à ficha da pessoa.
EXTRAS_DA_LEITURA = ("nome_pai", "sexo", "estado_civil", "naturalidade",
                     "naturalidade_uf", "ctps_serie", "ctps_uf", "ctps_emissao")
_ORGAOS = ("SSP", "DETRAN", "MTE", "ME")


def caixa_alta(texto: Any) -> str:
    """"José da Conceição" → "JOSE DA CONCEICAO"."""
    import unicodedata
    t = unicodedata.normalize("NFKD", " ".join(str(texto or "").split()))
    return "".join(c for c in t if not unicodedata.combining(c)).upper()


def normalizar_campos(lido: dict[str, Any]) -> dict[str, str]:
    """O que a IA devolveu, no formato que o formulário guarda — e só o que
    passou na conferência de forma (CPF com 11 dígitos, data de verdade,
    UF com duas letras, CEP com oito)."""
    campos: dict[str, str] = {}
    for chave in CAMPOS_DE_TEXTO:
        v = caixa_alta(lido.get(chave))
        if v:
            campos[chave] = v
    cpf = _so_digitos(lido.get("cpf"))
    if len(cpf) == 11:
        campos["cpf"] = cpf
    # Órgão emissor é LISTA no cadastro: fora dela, fica para a pessoa
    # escolher (gravar "SSP/CE" quebraria a ficha).
    if campos.get("rg_orgao") and campos["rg_orgao"].split("/")[0].strip() in _ORGAOS:
        campos["rg_orgao"] = campos["rg_orgao"].split("/")[0].strip()
    else:
        campos.pop("rg_orgao", None)
    sexo = str(lido.get("sexo") or "").strip().upper()[:1]
    if sexo in ("M", "F"):
        campos["sexo"] = sexo
    civil = caixa_alta(lido.get("estado_civil")).replace(" ", "_")
    if civil in ("SOLTEIRO", "CASADO", "DIVORCIADO", "VIUVO", "UNIAO_ESTAVEL"):
        campos["estado_civil"] = civil
    serie = "".join(c for c in str(lido.get("ctps_serie") or "") if c.isalnum())
    if serie:
        campos["ctps_serie"] = serie.upper()
    for chave in ("data_nascimento", "rg_emissao", "ctps_emissao"):
        d = _data(lido.get(chave))
        if d:
            campos[chave] = d
    rg = "".join(c for c in str(lido.get("rg") or "").upper()
                 if c.isalnum())
    if len(rg) >= 5:
        campos["rg"] = rg
    for chave in ("rg_uf", "uf", "naturalidade_uf", "ctps_uf"):
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
            for u in _imagens_para_ia(conteudo, nome):
                partes.append({"type": "image_url", "image_url": {"url": u}})
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
