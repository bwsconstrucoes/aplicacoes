# -*- coding: utf-8 -*-
"""TRAZER DO PIPEFY O QUE FICOU LÁ: os documentos das pessoas, e o que falta
no cadastro delas.

O dono, em 10/11/2026:

> *"Olhando os cadastros, a planilha lá, dados e documentos, tem o número do
> card do pipe, certo? Então, tu não consegue fazer logo essa importação, não?
> Faz a leitura do card, baixa os documentos, importa para o ERP. Só das
> pessoas que você já sabe que tem que estar."*

Ele está certo nas duas pontas: o número do card já está no cadastro
(`colaboradores.ref_pipefy`), e o pipe guarda os documentos que o ERP não tem.

AS QUATRO REGRAS DESTE ARQUIVO, e cada uma existe por um motivo:

1. **CONFERIR SEM GRAVAR, DEPOIS GRAVAR.** Igual à importação dos diários:
   `simular=True` lê tudo, monta o relatório e NÃO toca em nada. É assim que
   se descobre que 40 cards não existem mais antes de baixar 4.000 arquivos.
2. **NUNCA SOBRESCREVER.** Campo que já tem valor no ERP fica como está. O
   cadastro daqui foi conferido por gente; o card pode ter dois anos e um
   endereço antigo. Na dúvida entre o que o DP digitou aqui e o que está no
   Pipefy, vale o daqui.
3. **SÓ QUEM TEM QUE ESTAR.** O padrão é **pessoa ATIVA com número de card**.
   Pipe tem 3.664 cards; o ERP não precisa do histórico de quem saiu há cinco
   anos para funcionar — e baixar isso são horas de rede e um monte de
   documento de gente que não trabalha mais aqui.
4. **O QUE NÃO DEU CERTO É DITO.** Arquivo que não desceu vira linha no
   relatório, com o motivo. "Importação concluída" escondendo trinta falhas é
   pior que o erro, porque ninguém volta para conferir.

⚑ O QUE ESTE ARQUIVO NÃO FAZ, de propósito:

- **Não cria pessoa.** Importa para quem JÁ está no cadastro do ERP. Criar
  gente a partir de card é outra decisão, e ela é dele.
- **Não traz cargo, obra, convenção nem empresa.** No Pipefy esses campos são
  CONECTORES — guardam o id de um registro do Pipefy, não o nome. Casar isso
  com o cadastro daqui é adivinhação, e adivinhação em cargo vira salário
  errado.
- **Não traz o ASO.** Ele VENCE, e o ERP exige a validade para poder avisar
  antes. O card não diz até quando o atestado vale; inventar essa data seria
  criar um aviso que não significa nada. O relatório conta quantos ficaram.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.pipefy import cliente
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

# Teto por arquivo. O mesmo do envio pela tela — o OOM de julho de 2026 veio
# justamente de download sem teto (`CONTEXTO.md` §9).
TETO_POR_ARQUIVO = 25 * 1024 * 1024

# Quantas pessoas de uma vez, quando ele não pede um número. Importação longa
# demais esbarra no reinício do serviço no Render; em vez de uma rodada de
# duas horas que morre no meio, são várias rodadas curtas que continuam de
# onde pararam (a tabela `pipefy_importado` é quem lembra).
POR_RODADA = 50


# ---------------------------------------------------------------------------
# DE-PARA DOS ANEXOS — campo do Pipefy → tipo do catálogo do ERP
#
# Os ids vêm do export real do pipe CADASTRO COLABORADORES (301487297), não de
# cabeça: estão em `app/apps/erp/DE_PARA_PIPEFY_DP.md`, e o script que gera
# aquele arquivo é `scripts/gerar_de_para_dp.py`.
# ---------------------------------------------------------------------------
PAPEIS: dict[str, str] = {
    "anexo_ctps_rg_cpf_comprovante_de_endere_o": "DOC-IDENTIDADE",
    "carteira_de_trabalho": "CTPS-FOTO",
    "ctps_p_gina_com_as_informa_es_do_colaborador": "CTPS-INFORMACOES",
    "attachment_t_tulo": "CPF-DOC",
    "ctps_1": "COMPROVANTE-ENDERECO-PESSOA",
    "foto": "FOTO-COLABORADOR",
    "certid_o_de_nascimento_dos_dependentes": "DOC-DEPENDENTE",
    "documentos_diversos": "DOC-PESSOA-OUTRO",
}

# Anexos que o pipe tem e que NÃO entram, com o motivo escrito — para o
# relatório poder dizer o que ficou para trás em vez de simplesmente omitir.
PAPEIS_DE_FORA: dict[str, str] = {
    "aso": ("o ASO vence e o card não diz até quando — sem a validade o "
            "sistema não teria como avisar antes"),
}

# ---------------------------------------------------------------------------
# DE-PARA DOS DADOS — campo do Pipefy → coluna do cadastro
#
# ⚑ SÓ O QUE É DIRETO. Ficaram de fora os conectores (cargo, obra, função,
# município, convenção, empresa), porque no Pipefy eles guardam o id de um
# registro de lá. E ficou de fora dinheiro (salário, diária, auxílios): o
# valor do card pode ser de dois anos atrás, e salário errado no cadastro vira
# folha errada — este é o campo onde "preencher o que está vazio" deixa de ser
# inofensivo.
# ---------------------------------------------------------------------------
TEXTOS: dict[str, str] = {
    "pis": "pis",
    "ctps": "ctps_numero",
    "s_rie_ctps": "ctps_serie",
    "n_identidade": "rg",
    "nome_da_m_e": "nome_mae",
    "nome_do_pai": "nome_pai",
    "matr_cula": "matricula",
    "endere_o_rua": "logradouro",
    "n_mero2": "numero_endereco",
    "bairro": "bairro",
    "cep": "cep",
    "phone_t_tulo": "telefone",
    # ⚑ DOIS QUE EU IA TRAZER E TIREI, porque pareciam casar e não casam:
    # "Convenção" do pipe é o NOME da convenção ("CE-Fortaleza"), e
    # `convencao_categoria` aqui guarda a CATEGORIA ("PROFISSIONAL"); e o nome
    # do cônjuge não tem coluna no cadastro — jogá-lo em "observações" deixa um
    # nome solto num campo que ninguém lê como estado civil.
}
DATAS: dict[str, str] = {
    "data_de_nascimento": "data_nascimento",
    "data_de_emiss_o_documento_de_identifica_o": "rg_emissao",
    "data_de_emiss_o_ctps": "ctps_emissao",
    "data_de_admiss_o": "admissao",
    "data_de_in_cio": "data_inicio",
}
# Campos de caixinha fechada: o valor só entra se for uma das opções que o
# cadastro aceita. Texto fora da lista é DESCARTADO e contado — o cadastro
# recusaria de qualquer jeito, e o erro pararia a pessoa inteira.
LISTAS: dict[str, str] = {
    "sexo": "sexo",
    "estado_civil": "estado_civil",
    "escolaridade": "escolaridade",
    "uf_endere_o": "uf",
    "uf_ctps": "ctps_uf",
    "uf_documento_de_identifica_o": "rg_uf",
    "select_t_tulo": "naturalidade_uf",
    "rg_o_emissor_documento_de_identifica_o": "rg_orgao",
    "utiliza_meio_de_transporte_para_obra": "meio_transporte",
    "categoria_aux_lio_alimenta_o": "categoria_alimentacao",
    "categoria_aux_lio_transporte": "categoria_transporte",
}


# ---------------------------------------------------------------------------
# CASAR CARD COM PESSOA PELO CPF — o conserto de 13/11/2026
#
# Eu tinha construído isto assumindo que o número do card já estava no cadastro
# (`colaboradores.ref_pipefy`). Não estava. E como não havia onde digitar, o
# botão só sabia dizer "nenhuma pessoa com número de card nesse filtro". Ele:
# *"onde é que eu coloco os números do card se não tem campo?"*
#
# ⚑ A RESPOSTA CERTA É QUE NÃO DEVE HAVER CAMPO. O card tem o CPF e o cadastro
# tem o CPF: quem casa os dois é o sistema. Ninguém digita 3.664 números, e um
# número digitado errado colaria o documento de uma pessoa na ficha de outra.
# ---------------------------------------------------------------------------
CAMPO_CPF_DO_CARD = "cpf"           # do export do pipe 301487297


def _so_digitos(valor: Any) -> str:
    return "".join(c for c in str(valor or "") if c.isdigit())


def casar_por_cpf(s: Session, cards: list[dict[str, Any]]) -> dict[str, Any]:
    """Liga cada card à pessoa do ERP pelo CPF. Devolve o que casou e o que não.

    Card sem CPF, CPF que não existe aqui e DOIS cards para o mesmo CPF são
    tratados como casos à parte — o último de propósito: escolher um dos dois
    seria adivinhar qual é o cadastro bom.
    """
    por_cpf: dict[str, list[dict]] = {}
    sem_cpf: list[dict] = []
    for card in cards:
        cpf = _so_digitos((card.get("campos") or {}).get(CAMPO_CPF_DO_CARD))
        if len(cpf) != 11:
            sem_cpf.append(card)
            continue
        por_cpf.setdefault(cpf, []).append(card)

    repetidos = {cpf: lista for cpf, lista in por_cpf.items() if len(lista) > 1}
    unicos = {cpf: lista[0] for cpf, lista in por_cpf.items() if len(lista) == 1}

    pessoas = {}
    if unicos:
        linhas = s.execute(text("""
            SELECT id, nome, cpf, situacao, btrim(COALESCE(ref_pipefy, '')) AS card
              FROM colaboradores
             WHERE regexp_replace(cpf, '\\D', '', 'g') = ANY(:cpfs)"""),
            {"cpfs": list(unicos)}).mappings().all()
        for l in linhas:
            pessoas[_so_digitos(l["cpf"])] = dict(l)

    casados, sem_pessoa = [], []
    for cpf, card in unicos.items():
        pessoa = pessoas.get(cpf)
        if pessoa is None:
            sem_pessoa.append({"card": card["id"], "titulo": card["titulo"],
                               "cpf": cpf})
            continue
        casados.append({"colaborador_id": pessoa["id"], "nome": pessoa["nome"],
                        "cpf": cpf, "situacao": pessoa["situacao"],
                        "card": card["id"], "ja_tinha_card": bool(pessoa["card"])})
    return {
        "casados": casados,
        "cards_sem_cpf": [{"card": c["id"], "titulo": c["titulo"]} for c in sem_cpf],
        "cards_sem_pessoa": sem_pessoa,
        "cpf_com_dois_cards": [
            {"cpf": cpf, "cards": [c["id"] for c in lista]}
            for cpf, lista in repetidos.items()],
    }


def gravar_o_card_na_pessoa(s: Session, casados: list[dict[str, Any]]) -> int:
    """Grava o número do card em quem ainda não tinha.

    Assim a segunda rodada não precisa varrer o pipe inteiro de novo — e a
    ficha da pessoa passa a dizer de qual card ela veio."""
    novos = [c for c in casados if not c.get("ja_tinha_card")]
    for c in novos:
        s.execute(text("UPDATE colaboradores SET ref_pipefy = :card"
                       " WHERE id = :i AND COALESCE(btrim(ref_pipefy),'') = ''"),
                  {"card": str(c["card"]), "i": c["colaborador_id"]})
    return len(novos)


# ---------------------------------------------------------------------------
# Quem entra
# ---------------------------------------------------------------------------
def quem_tem_card(s: Session, *, so_ativos: bool = True,
                  obra_id: Optional[int] = None,
                  teto: Optional[int] = None) -> list[dict[str, Any]]:
    """As pessoas do ERP que têm número de card — a fila da importação."""
    condicoes = ["COALESCE(btrim(c.ref_pipefy), '') <> ''"]
    parametros: dict[str, Any] = {}
    if so_ativos:
        condicoes.append("c.situacao = 'ATIVO'")
    if obra_id:
        condicoes.append("c.obra_id = :obra")
        parametros["obra"] = obra_id
    limite = ""
    if teto:
        limite = " LIMIT :teto"
        parametros["teto"] = int(teto)
    linhas = s.execute(text(f"""
        SELECT c.id, c.nome, c.cpf, btrim(c.ref_pipefy) AS card, c.situacao
          FROM colaboradores c
         WHERE {' AND '.join(condicoes)}
         ORDER BY c.nome{limite}"""), parametros).mappings().all()
    return [dict(l) for l in linhas]


def o_que_ja_veio(s: Session, cards: list[str]) -> set[tuple[str, str, int]]:
    """(card, campo, item) do que já foi importado — para não baixar de novo.

    ⚑ É ISTO que torna a segunda passada instantânea. Sem ela, "já tenho esse
    arquivo" só se descobre DEPOIS de baixá-lo, porque a comparação é pelo
    conteúdo."""
    if not cards:
        return set()
    linhas = s.execute(text("""
        SELECT card, campo, item FROM pipefy_importado
         WHERE card = ANY(:cards) AND resultado <> 'FALHOU'"""),
        {"cards": list(cards)}).all()
    return {(str(l[0]), str(l[1]), int(l[2])) for l in linhas}


# ---------------------------------------------------------------------------
# Ler o card e separar o que serve
# ---------------------------------------------------------------------------
def _limpo(valor: Any) -> str:
    texto = "" if valor is None else str(valor).strip()
    if texto.startswith("["):
        itens = cliente.lista_do_campo(texto)
        return itens[0] if itens else ""
    return texto


def dados_do_card(campos: dict[str, str]) -> dict[str, str]:
    """O que o card tem, já com o NOME DA COLUNA do cadastro do ERP."""
    from app.apps.erp.core.cadastros import pessoas as svc

    saida: dict[str, str] = {}
    for de, para in TEXTOS.items():
        valor = _limpo(campos.get(de))
        if valor:
            saida[para] = valor
    for de, para in DATAS.items():
        valor = _limpo(campos.get(de))
        if valor:
            saida[para] = valor[:10]           # "2024-03-01 00:00:00" → data
    for de, para in LISTAS.items():
        valor = _limpo(campos.get(de))
        if not valor:
            continue
        validos = svc.DA_LISTA.get(para) or ()
        achado = _da_lista(valor, validos, para)
        if achado:
            saida[para] = achado
    return saida


# ⚑ O DE-PARA DAS CAIXINHAS É ESCRITO, NÃO ADIVINHADO.
#
# A primeira versão disto comparava os textos "parecendo" (sem acento, sem
# caixa, aceitando começo igual) — e ela traduziu "Divorciado em 2020" como
# DIVORCIADO e "Masculino" como M por acaso, porque M é o começo de Masculino.
# Acertar por acaso é pior que errar: funciona no teste e erra no dado real.
#
# As opções dos dois lados são FECHADAS e conhecidas (as do Pipefy saíram do
# export do pipe 301487297), então a tradução é uma tabela. O que não estiver
# nela não entra — e é contado.
PALAVRAS: dict[str, dict[str, str]] = {
    "sexo": {"MASCULINO": "M", "FEMININO": "F"},
    "estado_civil": {
        "SOLTEIROA": "SOLTEIRO", "CASADOA": "CASADO",
        "DIVORCIADOA": "DIVORCIADO", "VIUVOA": "VIUVO",
        "UNIAOESTAVEL": "UNIAO_ESTAVEL"},
    "escolaridade": {
        "NAOALFABETIZADO": "NAO_ALFABETIZADO", "EDUCACAOINFANTIL": "INFANTIL",
        "ENSINOFUNDAMENTAL": "FUNDAMENTAL", "ENSINOMEDIO": "MEDIO",
        "ENSINOSUPERIOR": "SUPERIOR", "POSGRADUACAO": "POS",
        "MESTRADO": "MESTRADO", "DOUTORADO": "DOUTORADO"},
    "meio_transporte": {
        "NENHUM": "NENHUM", "TRANSPORTECOLETIVO": "COLETIVO",
        "MOTO": "MOTO", "BICICLETA": "BICICLETA"},
    "categoria_alimentacao": {
        "SEGUNDAAQUINTA": "SEG_QUI", "SEGUNDAASEXTA": "SEG_SEX",
        "MENSAL": "MENSAL", "MES": "MES"},
    "categoria_transporte": {
        "DIARIO": "DIARIO", "MENSAL": "MENSAL",
        "SEGUNDAESEXTA": "SEG_E_SEX", "VALETRANSPORTE": "VALE_TRANSPORTE",
        "DIARIOEVALETRANSPORTE": "DIARIO_E_VT", "VEICULO": "VEICULO",
        "MOTOCICLETA": "MOTOCICLETA", "BICICLETA": "BICICLETA",
        "CARTAO": "CARTAO"},
}


def _cru(texto: Any) -> str:
    """Sem acento, sem caixa, sem o que não for letra ou número."""
    import unicodedata
    sem = unicodedata.normalize("NFKD", str(texto or ""))
    sem = "".join(c for c in sem if not unicodedata.combining(c))
    return "".join(c for c in sem.upper() if c.isalnum())


def _da_lista(valor: str, validos, coluna: str = "") -> str:
    """O texto do Pipefy virando a opção do cadastro — ou NADA.

    Duas chances, as duas exatas: a tabela `PALAVRAS` da coluna, e a própria
    opção do cadastro escrita igual (é o caso das UFs e dos órgãos do RG, que
    são as mesmas letras dos dois lados). Fora disso, não entra."""
    alvo = _cru(valor)
    if not alvo:
        return ""
    traduzido = (PALAVRAS.get(coluna) or {}).get(alvo)
    if traduzido and traduzido in validos:
        return traduzido
    for opcao in validos:
        if _cru(opcao) == alvo:
            return opcao
    return ""


def anexos_do_card(campos: dict[str, str]) -> list[dict[str, Any]]:
    """Cada arquivo pendurado no card, com o tipo que ele vai ter aqui."""
    saida = []
    for campo, tipo in PAPEIS.items():
        for posicao, item in enumerate(cliente.lista_do_campo(campos.get(campo))):
            endereco = cliente.endereco_do_anexo(item)
            saida.append({"campo": campo, "item": posicao, "tipo": tipo,
                          "endereco": endereco,
                          "nome": cliente.nome_do_anexo(item),
                          "bruto": item})
    return saida


def _vazios(s: Session, colaborador_id: int,
            achados: dict[str, str]) -> dict[str, str]:
    """Dos campos que o card tem, só os que estão VAZIOS no cadastro.

    ⚑ A REGRA 2 MORA AQUI. O cadastro do ERP foi conferido por gente; o card
    pode ser de dois anos atrás. Quem já tem valor fica como está."""
    if not achados:
        return {}
    colunas = ", ".join(sorted(achados))
    linha = s.execute(
        text(f"SELECT {colunas} FROM colaboradores WHERE id = :i"),
        {"i": colaborador_id}).mappings().first()
    if linha is None:
        return {}
    return {campo: valor for campo, valor in achados.items()
            if not str(linha.get(campo) or "").strip()}


def mapear_cards(s: Session, *, simular: bool = True, varrer=None,
                 pipe_id: str = cliente.PIPE_CADASTRO) -> dict[str, Any]:
    """Varre o pipe, casa cada card com a pessoa pelo CPF e (se não for
    simulação) grava o número do card em quem ainda não tinha.

    `varrer` existe para o teste entregar cards de mentira sem rede."""
    busca = varrer or cliente.varrer_pipe
    cards = busca(pipe_id, campos=(CAMPO_CPF_DO_CARD,))
    r = casar_por_cpf(s, cards)
    gravados = 0
    if not simular:
        gravados = gravar_o_card_na_pessoa(s, r["casados"])
    novos = [c for c in r["casados"] if not c.get("ja_tinha_card")]
    return {
        "cards_no_pipe": len(cards),
        "casados": r["casados"],
        "cards_sem_cpf": r["cards_sem_cpf"],
        "cards_sem_pessoa": r["cards_sem_pessoa"],
        "cpf_com_dois_cards": r["cpf_com_dois_cards"],
        "gravados": gravados if not simular else len(novos),
        "detalhe": {
            "sem_pessoa": r["cards_sem_pessoa"][:50],
            "dois_cards": r["cpf_com_dois_cards"][:50]},
    }


# ---------------------------------------------------------------------------
# A importação
# ---------------------------------------------------------------------------
def importar(s: Session, usuario: Optional[Usuario] = None, *,
             simular: bool = True, so_ativos: bool = True,
             obra_id: Optional[int] = None,
             teto_de_pessoas: Optional[int] = POR_RODADA,
             ler_cards=None, casar: bool = True, varrer=None) -> dict[str, Any]:
    """Lê os cards das pessoas e traz documentos e campos vazios.

    Com `simular=True` (o padrão) NADA é gravado e nenhum arquivo é baixado:
    devolve o mesmo relatório, dizendo o que ENTRARIA. É a conferência antes,
    e ela é o padrão de propósito — importação é a operação em que o engano
    custa mais caro para desfazer.

    `ler_cards` existe para o teste poder entregar cards de mentira sem rede.
    """
    ler = ler_cards or cliente.ler_cards
    pessoas = quem_tem_card(s, so_ativos=so_ativos, obra_id=obra_id,
                            teto=teto_de_pessoas)
    # ⚑ NINGUÉM COM CARD? ENTÃO ACHE OS CARDS. Até 13/11/2026 isto parava aqui
    # com "nenhuma pessoa com número de card", e não havia onde digitar o
    # número — o botão não tinha como funcionar nunca. Agora o sistema varre o
    # pipe e casa pelo CPF; o número do card é RESULTADO da importação, não
    # requisito dela.
    mapeamento = None
    if not pessoas and casar:
        mapeamento = mapear_cards(s, simular=simular, varrer=varrer)
        if not simular and mapeamento["casados"]:
            s.flush()
        pessoas = quem_tem_card(s, so_ativos=so_ativos, obra_id=obra_id,
                                teto=teto_de_pessoas) if not simular else []
    relatorio: dict[str, Any] = {
        "simulado": bool(simular),
        "pessoas_na_fila": len(pessoas),
        "pessoas_lidas": 0,
        "documentos_guardados": 0,
        "documentos_ja_tinha": 0,
        "documentos_que_falharam": 0,
        "campos_preenchidos": 0,
        "cards_sem_resposta": [],
        "falhas": [],
        "por_pessoa": [],
        "de_fora": dict(PAPEIS_DE_FORA),
        "mapeamento": None,
    }
    if mapeamento is not None:
        relatorio["mapeamento"] = {
            k: (len(v) if isinstance(v, list) else v)
            for k, v in mapeamento.items() if k != "detalhe"}
        relatorio["mapeamento_detalhe"] = mapeamento.get("detalhe")
    relatorio["pessoas_na_fila"] = len(pessoas)
    if not pessoas:
        return relatorio

    cards = [p["card"] for p in pessoas]
    lidos, falhas_de_card = ler(cards)
    for card, motivo in (falhas_de_card or {}).items():
        relatorio["cards_sem_resposta"].append({"card": str(card),
                                                "motivo": str(motivo)})
    ja_veio = o_que_ja_veio(s, cards)

    for pessoa in pessoas:
        card = str(pessoa["card"])
        bruto = lidos.get(card)
        if not bruto:
            continue
        relatorio["pessoas_lidas"] += 1
        campos = bruto.get("campos") or {}
        linha = {"colaborador_id": pessoa["id"], "nome": pessoa["nome"],
                 "card": card, "documentos": [], "campos": {}}

        # 1. Os campos que estão vazios aqui.
        preencher = _vazios(s, pessoa["id"], dados_do_card(campos))
        if preencher:
            linha["campos"] = preencher
            relatorio["campos_preenchidos"] += len(preencher)
            if not simular:
                _gravar_campos(s, pessoa["id"], preencher, usuario)

        # 2. Os documentos.
        for anexo in anexos_do_card(campos):
            chave = (card, anexo["campo"], anexo["item"])
            if chave in ja_veio:
                relatorio["documentos_ja_tinha"] += 1
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "JÁ TINHA"})
                continue
            if not anexo["endereco"]:
                relatorio["documentos_que_falharam"] += 1
                motivo = "não deu para entender o endereço do arquivo"
                relatorio["falhas"].append(
                    {"nome": pessoa["nome"], "card": card,
                     "arquivo": anexo["nome"], "motivo": motivo})
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "SEM ENDEREÇO"})
                continue
            if simular:
                relatorio["documentos_guardados"] += 1
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "VAI ENTRAR"})
                continue
            ok, motivo = _trazer_um(s, pessoa, anexo, usuario)
            if ok:
                relatorio["documentos_guardados"] += 1
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "GUARDADO"})
            else:
                relatorio["documentos_que_falharam"] += 1
                relatorio["falhas"].append(
                    {"nome": pessoa["nome"], "card": card,
                     "arquivo": anexo["nome"], "motivo": motivo})
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "FALHOU"})
        relatorio["por_pessoa"].append(linha)

    logger.info(
        "ERP/pipefy: importação %s — %d pessoa(s), %d documento(s), %d campo(s)",
        "simulada" if simular else "de verdade", relatorio["pessoas_lidas"],
        relatorio["documentos_guardados"], relatorio["campos_preenchidos"])
    return relatorio


def _resumo(anexo: dict[str, Any]) -> dict[str, Any]:
    return {"tipo": anexo["tipo"], "arquivo": anexo["nome"],
            "campo": anexo["campo"]}


def _gravar_campos(s: Session, colaborador_id: int, campos: dict[str, str],
                   usuario: Optional[Usuario]) -> None:
    """Grava pelas MESMAS funções da tela, nunca com UPDATE solto.

    É a regra que a fila do DP já segue: se a conferência do cadastro recusa um
    PIS inválido, a importação tem de recusar também — senão a importação vira
    a porta dos fundos por onde entra o dado que a tela barraria."""
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Colaborador

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        return
    svc.salvar(s, {**campos, "cpf": c.cpf, "nome": c.nome}, usuario)


def _trazer_um(s: Session, pessoa: dict[str, Any], anexo: dict[str, Any],
               usuario: Optional[Usuario]) -> tuple[bool, str]:
    """Baixa UM arquivo e o arquiva no cadastro da pessoa.

    ⚑ CADA ARQUIVO NO SEU SAVEPOINT. Em Postgres, uma consulta que falha aborta
    a transação INTEIRA — mesmo pega por `except`. Sem isto, um arquivo
    problemático derrubaria as 49 pessoas seguintes da mesma rodada, com um
    erro que não tem nada a ver com elas. É a mesma lição do `_consulta_opcional`
    do diário.
    """
    from app.apps.erp.core.arquivo import service as svc_arquivo

    marca = s.begin_nested()
    try:
        conteudo = cliente.baixar(anexo["endereco"], TETO_POR_ARQUIVO)
        doc = svc_arquivo.arquivar(
            s, conteudo=conteudo, nome_arquivo=anexo["nome"],
            tipo_codigo=anexo["tipo"], colaborador_id=pessoa["id"],
            origem="PIPEFY", usuario=usuario)
        s.execute(text("""
            INSERT INTO pipefy_importado
                   (card, campo, item, colaborador_id, documento_id,
                    resultado, importado_por)
            VALUES (:card, :campo, :item, :pessoa, :doc, 'GUARDADO', :u)
            ON CONFLICT (card, campo, item) DO UPDATE
               SET documento_id = EXCLUDED.documento_id,
                   resultado = 'GUARDADO', detalhe = NULL,
                   importado_em = now()"""),
            {"card": str(pessoa["card"]), "campo": anexo["campo"],
             "item": anexo["item"], "pessoa": pessoa["id"], "doc": doc.id,
             "u": usuario.id if usuario else None})
        marca.commit()
        return True, ""
    except Exception as e:                  # noqa: BLE001 — um arquivo só cai
        marca.rollback()
        motivo = str(e)[:300]
        logger.warning("ERP/pipefy: %s de %s não veio (%s)",
                       anexo["nome"], pessoa["nome"], motivo)
        try:
            s.execute(text("""
                INSERT INTO pipefy_importado
                       (card, campo, item, colaborador_id, resultado,
                        detalhe, importado_por)
                VALUES (:card, :campo, :item, :pessoa, 'FALHOU', :d, :u)
                ON CONFLICT (card, campo, item) DO UPDATE
                   SET resultado = 'FALHOU', detalhe = EXCLUDED.detalhe,
                       importado_em = now()"""),
                {"card": str(pessoa["card"]), "campo": anexo["campo"],
                 "item": anexo["item"], "pessoa": pessoa["id"],
                 "d": motivo, "u": usuario.id if usuario else None})
        except Exception:                        # pragma: no cover - defensivo
            pass
        return False, motivo


# ---------------------------------------------------------------------------
# O que a tela mostra antes de qualquer coisa
# ---------------------------------------------------------------------------
def situacao(s: Session) -> dict[str, Any]:
    """Quantos têm card, quantos já vieram, e se o servidor tem o token.

    A tela pergunta isto ANTES de oferecer o botão: oferecer uma importação
    que vai falhar por falta de token é fazer a pessoa descobrir no erro o que
    dava para saber na porta."""
    linha = s.execute(text("""
        SELECT count(*) FILTER (WHERE COALESCE(btrim(ref_pipefy), '') <> '')
                 AS com_card,
               count(*) FILTER (WHERE COALESCE(btrim(ref_pipefy), '') <> ''
                                  AND situacao = 'ATIVO') AS ativos_com_card,
               count(*) FILTER (WHERE situacao = 'ATIVO') AS ativos
          FROM colaboradores""")).mappings().first() or {}
    ja = s.execute(text("""
        SELECT count(*) FILTER (WHERE resultado = 'GUARDADO') AS guardados,
               count(*) FILTER (WHERE resultado = 'FALHOU')   AS falharam,
               count(DISTINCT card)                           AS cards
          FROM pipefy_importado""")).mappings().first() or {}
    return {
        "tem_token": cliente.tem_token(),
        # ⚑ A TELA PRECISA SABER QUE ZERO CARDS NÃO É IMPEDIMENTO. Era por não
        # saber disso que ela oferecia um botão cujo único desfecho possível
        # era "nenhuma pessoa com número de card nesse filtro" (13/11/2026).
        "precisa_mapear": int(linha.get("ativos_com_card") or 0) == 0,
        "ativos": int(linha.get("ativos") or 0),
        "com_card": int(linha.get("com_card") or 0),
        "ativos_com_card": int(linha.get("ativos_com_card") or 0),
        "sem_card": int(linha.get("ativos") or 0) - int(linha.get("ativos_com_card") or 0),
        "ja_guardados": int(ja.get("guardados") or 0),
        "ja_falharam": int(ja.get("falharam") or 0),
        "cards_ja_lidos": int(ja.get("cards") or 0),
        "por_rodada": POR_RODADA,
    }
