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
import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroValidacao
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
    # ⚑ MATRÍCULA SAIU em 13/11/2026. Ele: *"essa informação de matrícula,
    # cara, isso aqui não precisa não. Esse campo é desnecessário, que a gente
    # não usa mais negócio de matrícula não. A gente vai usar o ID Fortes, o
    # CPF aqui dentro do nosso sistema e pronto."*
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
                          "forma": cliente.forma_do_anexo(item),
                          "nome": cliente.nome_do_anexo(item),
                          "bruto": item})
    return saida


def raio_x_dos_anexos(campos: dict[str, str]) -> list[dict[str, Any]]:
    """O que o Pipefy devolveu em CADA campo de anexo deste card.

    ⚑ ISTO EXISTE POR CAUSA DE 13/11/2026, quando ele importou e: *"a
    documentação que está anexada no pipe não veio para cá (…) não veio nenhum
    documento"*. Eu não tinha como saber por quê — aqui não há acesso ao
    Pipefy —, e o relatório não dizia nada: campo vazio e campo que eu não
    soube ler davam o mesmo silêncio.

    Agora a conferência MOSTRA, campo por campo, quantos itens vieram e de que
    forma. A próxima rodada dele responde a pergunta sozinha, em vez de
    virar mais uma ida e volta.
    """
    linhas = []
    for campo, tipo in PAPEIS.items():
        itens = cliente.lista_do_campo(campos.get(campo))
        formas = sorted({cliente.forma_do_anexo(x) for x in itens})
        linhas.append({
            "campo": campo, "tipo": tipo, "quantos": len(itens),
            "formas": formas,
            # Uma AMOSTRA curta do que veio, para eu poder consertar o que
            # ainda não entendi sem precisar de acesso ao Pipefy.
            "amostra": (str(itens[0])[:120] if itens else "")})
    return linhas


# ---------------------------------------------------------------------------
# A EMPRESA DE REGISTRO, pelo CNPJ que vem no próprio card
#
# Ele, em 13/11/2026: *"numa empresa para registro, o que o ERP grava é um
# pouquinho diferente do que está na planilha, mas na planilha dá para
# localizar o CNPJ. E assim, ele não importou, eu tenho que reimportar, não
# importou."*
#
# ⚑ O TEXTO DIFERE DOS DOIS LADOS, O CNPJ NÃO. No pipe a opção é
# "00.079.526/0001-09 - BWS"; no cadastro de Empresas a razão social é outra.
# Comparar texto nunca ia casar. Comparar os 14 dígitos casa sempre — e é o
# que ele apontou.
# ---------------------------------------------------------------------------
CAMPOS_DE_EMPRESA = ("cnpj_para_admiss_o", "empresa_para_registro")


def empresa_do_card(s: Session, campos: dict[str, str]) -> Optional[int]:
    """O id da empresa do ERP cujo CNPJ aparece no card — ou nada."""
    for campo in CAMPOS_DE_EMPRESA:
        digitos = _so_digitos(_limpo(campos.get(campo)))
        if len(digitos) != 14:
            continue
        achada = s.execute(text("""
            SELECT id FROM empresas
             WHERE regexp_replace(COALESCE(cnpj, ''), '\\D', '', 'g') = :c
               AND ativo IS NOT FALSE
             LIMIT 1"""), {"c": digitos}).scalar()
        if achada:
            return int(achada)
    return None


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


# ---------------------------------------------------------------------------
# A LISTA QUE ELE COLA
# ---------------------------------------------------------------------------
def ids_dos_cards(texto: Any) -> list[str]:
    """Os números dos cards, colados de qualquer jeito.

    Um por linha, separados por vírgula ou espaço, ou colados direto de uma
    COLUNA DA PLANILHA — é de lá que ele os tira. Endereço inteiro do card
    também serve. Repetido entra uma vez só: colar a mesma coluna duas vezes é
    acidente comum, e buscar o mesmo card duas vezes seria tempo jogado fora.

    A regra é a mesma do importador de SPs, que já roda há meses
    (`core/importadores/pipefy_cards.extrair_ids`): seis dígitos ou mais.
    """
    if isinstance(texto, (list, tuple)):
        texto = " ".join(str(x) for x in texto)
    return list(dict.fromkeys(re.findall(r"\b\d{6,}\b", str(texto or ""))))


def relatorio_vazio(motivo: str) -> dict[str, Any]:
    return {"simulado": True, "motivo": motivo, "pessoas_na_fila": 0,
            "pessoas_lidas": 0, "documentos_guardados": 0,
            "documentos_ja_tinha": 0, "documentos_que_falharam": 0,
            "campos_preenchidos": 0, "cards_sem_resposta": [], "falhas": [],
            "por_pessoa": [], "de_fora": dict(PAPEIS_DE_FORA),
            "cards_pedidos": 0, "cards_sem_pessoa": []}


def _pessoas_dos_cards(s: Session, lidos: dict[str, Any]) -> tuple[list, list]:
    """Casa cada card lido com a pessoa do ERP, pelo CPF do próprio card.

    ⚑ O CPF, e não o número do card guardado no cadastro: o cadastro não tem
    esse número (foi o que quebrou na primeira versão). O card traz o CPF, o
    ERP tem o CPF — e quem casa os dois é o sistema, não a digitação.
    """
    por_cpf: dict[str, dict] = {}
    sem_cpf = []
    for card, bruto in lidos.items():
        cpf = _so_digitos((bruto.get("campos") or {}).get(CAMPO_CPF_DO_CARD))
        if len(cpf) != 11:
            sem_cpf.append({"card": str(card), "titulo": bruto.get("titulo", ""),
                            "motivo": "o card não tem CPF preenchido"})
            continue
        por_cpf[cpf] = {"card": str(card), "titulo": bruto.get("titulo", "")}
    if not por_cpf:
        return [], sem_cpf
    linhas = s.execute(text("""
        SELECT id, nome, cpf, situacao
          FROM colaboradores
         WHERE regexp_replace(cpf, '\\D', '', 'g') = ANY(:cpfs)"""),
        {"cpfs": list(por_cpf)}).mappings().all()
    achadas = {_so_digitos(l["cpf"]): dict(l) for l in linhas}
    pessoas, sem_pessoa = [], list(sem_cpf)
    for cpf, dado in por_cpf.items():
        pessoa = achadas.get(cpf)
        if pessoa is None:
            sem_pessoa.append({"card": dado["card"], "titulo": dado["titulo"],
                               "motivo": "o CPF do card não está no ERP — a "
                                         "importação não cria pessoa"})
            continue
        pessoas.append({"id": pessoa["id"], "nome": pessoa["nome"],
                        "cpf": pessoa["cpf"], "card": dado["card"],
                        "situacao": pessoa["situacao"]})
    return pessoas, sem_pessoa


# ---------------------------------------------------------------------------
# O QUE O CARD NÃO DISSE, O DOCUMENTO PODE DIZER
#
# Ele, em 13/11/2026: *"teve um determinado cadastro aí que a gente tem o PIS
# gravado no pipe, mas ele não identificou o PIS, deve ter dado alguma
# divergência (…) eu quero assim: vai importar, se tem alguma coisa faltando,
# pô, lê os documentos, lê os anexos do pipe."*
#
# ⚑ SÓ O QUE FALTA, E SÓ COM O PAPEL QUE JÁ VEIO. A leitura custa IA, então
# ela não roda para quem está completo, e nunca baixa nada a mais: lê o
# arquivo que a importação ACABOU de trazer. O que ela achar entra pela mesma
# porta dos outros campos — nunca por cima do que já estava preenchido, a não
# ser que ele tenha pedido para regravar.
# ---------------------------------------------------------------------------
# Campo do pedido → coluna do cadastro. A leitura devolve os nomes do
# formulário do DP (`num_pis`), e aqui eles viram coluna.
DO_PAPEL_PARA_O_CADASTRO = {
    "num_pis": "pis", "num_ctps": "ctps_numero",
    "data_nascimento": "data_nascimento", "telefone": "telefone",
}
# Em que ordem tentar: a página de informações da CTPS é a que traz PIS e
# número; o documento de identificação vem depois.
PAPEIS_QUE_VALE_LER = ("CTPS-INFORMACOES", "DOC-IDENTIDADE", "CPF-DOC")


def _o_que_ainda_falta(s: Session, colaborador_id: int) -> set[str]:
    """As colunas que a leitura poderia preencher e que seguem vazias."""
    colunas = sorted(set(DO_PAPEL_PARA_O_CADASTRO.values()))
    linha = s.execute(
        text(f"SELECT {', '.join(colunas)} FROM colaboradores WHERE id = :i"),
        {"i": colaborador_id}).mappings().first()
    if linha is None:
        return set()
    return {c for c in colunas if not str(linha.get(c) or "").strip()}


def ler_o_que_veio(s: Session, colaborador_id: int,
                   trazidos: list[dict[str, Any]],
                   ler=None) -> tuple[dict[str, str], dict[str, str]]:
    """Lê os papéis recém-trazidos.

    Devolve DUAS coisas: o que preenche buraco (para gravar) e **tudo o que o
    papel disse** (para o confronto).

    ⚑ A SEGUNDA EXISTE POR UM DEFEITO QUE O TESTE PEGOU: eu devolvia só o que
    ia ser gravado, e o NOME não está entre os campos que a importação
    preenche sozinha. Resultado: o nome lido do documento nunca chegava ao
    confronto — justamente o campo que ele mais quer conferir (*"verificar se
    o nome cadastrado está batendo com o que está no pipe, com o que está no
    documento da pessoa"*).
    """
    faltando = _o_que_ainda_falta(s, colaborador_id)
    if not trazidos:
        return {}, {}
    funcao = ler
    if funcao is None:
        from app.apps.erp.core import documentos_de_pessoa
        funcao = documentos_de_pessoa.ler
    achados: dict[str, str] = {}
    tudo: dict[str, str] = {}
    for papel in PAPEIS_QUE_VALE_LER:
        for doc in trazidos:
            if doc.get("tipo") != papel or not doc.get("conteudo"):
                continue
            try:
                lido = funcao(doc["conteudo"], doc.get("nome") or "", papel)
            except Exception as e:                       # noqa: BLE001
                logger.warning("ERP/pipefy: leitura de %s falhou (%s)",
                               papel, e)
                continue
            for campo, valor in (lido.get("campos") or {}).items():
                if not str(valor or "").strip():
                    continue
                # TUDO fica, com o nome que a leitura usa — é o que o
                # confronto compara.
                tudo.setdefault(campo, str(valor).strip())
                coluna = DO_PAPEL_PARA_O_CADASTRO.get(campo)
                if coluna and coluna in faltando:
                    achados[coluna] = str(valor).strip()
                    faltando.discard(coluna)
            break           # um papel de cada tipo basta
    return achados, tudo


# ---------------------------------------------------------------------------
# O CONFRONTO: o cadastro, o card e o documento dizendo coisas diferentes
#
# Ele, em 13/11/2026: *"a gente tem a IA aqui para trabalhar com a gente.
# Seria a hora, na importação, da gente confrontar dados dos documentos.
# Verificar se o nome cadastrado está batendo com o que está no pipe, com o
# que está no documento da pessoa (…) porque o que tem mais força é o
# documento da pessoa (…) de repente a contabilidade está errada também.
# Errou a nomenclatura, o nome, a coisa. Eu preciso corrigir isso aí."*
#
# ⚑ O DOCUMENTO TEM MAIS FORÇA — MAS NÃO GRAVA SOZINHO. A ordem de confiança
# é documento > card > cadastro, e é por isso que o documento aparece como a
# sugestão. Só que quem lê o documento é uma IA, que erra em silêncio: trocar
# o nome de uma pessoa na folha por causa de uma leitura torta é estrago que
# ninguém percebe até o pagamento voltar.
#
# Então a divergência é MOSTRADA, com as três versões lado a lado e a de maior
# força já marcada — e alguém confirma. É a mesma regra do documento que
# preenche o cadastro: *"o que a leitura preenche fica editável e vai para
# conferência"* (CLAUDE.md, 17/09/2026).
# ---------------------------------------------------------------------------
# O que vale a pena confrontar: o que identifica a pessoa e o que vai para a
# folha. Não entra o que muda legitimamente (telefone, endereço): ali
# divergência é atualização, não erro.
A_CONFRONTAR = {
    "nome": "Nome",
    "data_nascimento": "Data de nascimento",
    "pis": "Nº PIS",
    "ctps_numero": "Nº CTPS",
}
# De onde o card traz cada um deles.
DO_CARD = {"nome": "nome_completo", "data_nascimento": "data_de_nascimento",
           "pis": "pis", "ctps_numero": "ctps"}
# E de onde a leitura do documento traz.
DO_DOCUMENTO = {"nome": "nome", "data_nascimento": "data_nascimento",
                "pis": "num_pis", "ctps_numero": "num_ctps"}


def _igual(a: Any, b: Any) -> bool:
    """Compara sem acento, sem caixa e sem pontuação.

    "José da Silva" e "JOSE DA SILVA" são a mesma pessoa; "123.45678.90-0" e
    "12345678900" são o mesmo PIS. Acusar isso como divergência encheria a
    tela de ruído e faria ninguém olhar o que importa."""
    return _cru(a) == _cru(b)


def confrontar(s: Session, colaborador_id: int, campos_do_card: dict[str, str],
               do_documento: dict[str, str]) -> list[dict[str, Any]]:
    """As três versões de cada dado, quando elas não batem.

    Devolve uma linha por campo divergente, com o que cada fonte diz e qual
    delas tem mais força. Campo que ninguém contradiz não vira linha.
    """
    colunas = sorted(A_CONFRONTAR)
    linha = s.execute(
        text(f"SELECT {', '.join(colunas)} FROM colaboradores WHERE id = :i"),
        {"i": colaborador_id}).mappings().first()
    if linha is None:
        return []
    saida = []
    for campo, rotulo in A_CONFRONTAR.items():
        aqui = str(linha.get(campo) or "").strip()
        no_card = _limpo(campos_do_card.get(DO_CARD.get(campo, ""), ""))
        no_papel = str(do_documento.get(DO_DOCUMENTO.get(campo, ""), "") or "").strip()
        tem = [x for x in (aqui, no_card, no_papel) if x]
        if len(tem) < 2:
            continue                     # uma fonte só não contradiz ninguém
        if all(_igual(tem[0], x) for x in tem[1:]):
            continue                     # todos de acordo
        # A de maior força entre as que existem: documento, depois card.
        vence = "documento" if no_papel else ("card" if no_card else "cadastro")
        saida.append({
            "campo": campo, "rotulo": rotulo,
            "no_cadastro": aqui, "no_card": no_card, "no_documento": no_papel,
            "mais_forte": vence,
            "sugerido": no_papel or no_card or aqui})
    return saida


def guardar_divergencias(s: Session, colaborador_id: int,
                        linhas: list[dict[str, Any]], card: str) -> int:
    """Põe cada divergência na fila de quem decide.

    ⚑ GUARDAR É O QUE TRANSFORMA UM AVISO EM TRABALHO. O relatório é de uma
    rodada: fechou a tela, perdeu-se. Com 575 pessoas, "anote num papel o que
    não bateu" não é plano. Ele, em 13/11/2026: *"quando houver divergência
    entre o que a IA lê e o que está cadastrado, aí você questiona. E o humano
    decide."*

    Reimportar a mesma gente é normal na carga inicial, então a pergunta
    ABERTA é uma por pessoa e campo — senão a lista cresceria sem ninguém ter
    decidido nada. Se o card mudou, a pergunta aberta é atualizada.
    """
    postas = 0
    for d in linhas:
        s.execute(text("""
            INSERT INTO cadastro_divergencias
                   (colaborador_id, campo, no_cadastro, no_card, no_documento,
                    mais_forte, origem, referencia)
            VALUES (:i, :campo, :aqui, :card_v, :papel, :forte, 'PIPEFY', :card)
            ON CONFLICT (colaborador_id, campo) WHERE situacao = 'ABERTA'
            DO UPDATE SET no_cadastro = EXCLUDED.no_cadastro,
                          no_card = EXCLUDED.no_card,
                          no_documento = EXCLUDED.no_documento,
                          mais_forte = EXCLUDED.mais_forte,
                          referencia = EXCLUDED.referencia"""),
            {"i": colaborador_id, "campo": d["campo"], "aqui": d["no_cadastro"],
             "card_v": d["no_card"], "papel": d["no_documento"],
             "forte": d["mais_forte"], "card": str(card)})
        postas += 1
    return postas


def perguntas_abertas(s: Session, teto: int = 300) -> list[dict[str, Any]]:
    """O que está esperando alguém decidir."""
    linhas = s.execute(text("""
        SELECT d.id, d.colaborador_id, c.nome, c.cpf, d.campo, d.no_cadastro,
               d.no_card, d.no_documento, d.mais_forte, d.referencia
          FROM cadastro_divergencias d
          JOIN colaboradores c ON c.id = d.colaborador_id
         WHERE d.situacao = 'ABERTA'
         ORDER BY c.nome, d.campo
         LIMIT :t"""), {"t": teto}).mappings().all()
    return [{**dict(l), "rotulo": A_CONFRONTAR.get(l["campo"], l["campo"])}
            for l in linhas]


def decidir(s: Session, divergencia_id: int, fonte: str,
            usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Alguém escolheu qual versão vale — e o cadastro passa a dizer isso.

    `fonte` é "cadastro" (fica como está), "card", "documento" ou "ignorar".
    A gravação passa pelas MESMAS funções da tela: se o cadastro recusa um PIS
    inválido, esta porta recusa também.
    """
    d = s.execute(text("""
        SELECT * FROM cadastro_divergencias WHERE id = :i"""),
        {"i": divergencia_id}).mappings().first()
    if d is None:
        raise ErroNaoEncontrado("Divergência não encontrada.")
    if d["situacao"] != "ABERTA":
        raise ErroValidacao("Esta divergência já foi decidida.")
    escolhas = {"cadastro": d["no_cadastro"], "card": d["no_card"],
                "documento": d["no_documento"]}
    if fonte == "ignorar":
        s.execute(text("""
            UPDATE cadastro_divergencias
               SET situacao = 'IGNORADA', decidido_por = :u, decidido_em = now()
             WHERE id = :i"""),
            {"i": divergencia_id, "u": usuario.id if usuario else None})
        return {"id": divergencia_id, "situacao": "IGNORADA"}
    if fonte not in escolhas:
        raise ErroValidacao(
            "Escolha de onde vem o valor: do cadastro, do card ou do documento.")
    valor = str(escolhas[fonte] or "").strip()
    if not valor:
        raise ErroValidacao("Essa fonte não tem valor para gravar.")
    if fonte != "cadastro":
        try:
            _gravar_campos(s, int(d["colaborador_id"]), {d["campo"]: valor},
                           usuario, permitir_nome=(d["campo"] == "nome"))
        except ErroValidacao as e:
            # ⚑ O RECADO PRECISA DIZER O QUE É. A gravação passa pelas
            # conferências do cadastro inteiro, então uma pessoa com CPF
            # inválido gravado dava "CPF inválido" ao decidir o NOME dela — e
            # quem leu isso ia procurar o erro no lugar errado. Visto na tela
            # em 13/11/2026.
            recado = str(e)
            if "CPF" in recado.upper() and d["campo"] != "cpf":
                raise ErroValidacao(
                    f"Não deu para gravar: o CPF desta pessoa está inválido no "
                    f"cadastro. Corrija a ficha dela primeiro, e depois decida "
                    f"isto aqui. (O cadastro recusou: {recado})") from e
            raise
    s.execute(text("""
        UPDATE cadastro_divergencias
           SET situacao = 'RESOLVIDA', escolhido = :v, decidido_por = :u,
               decidido_em = now()
         WHERE id = :i"""),
        {"i": divergencia_id, "v": valor,
         "u": usuario.id if usuario else None})
    logger.info("ERP/cadastro: divergência %s resolvida com o valor do %s",
                divergencia_id, fonte)
    return {"id": divergencia_id, "situacao": "RESOLVIDA", "valor": valor}


# ---------------------------------------------------------------------------
# A importação
# ---------------------------------------------------------------------------
def importar(s: Session, usuario: Optional[Usuario] = None, *,
             simular: bool = True, so_ativos: bool = True,
             obra_id: Optional[int] = None,
             teto_de_pessoas: Optional[int] = POR_RODADA,
             ler_cards=None, cards: Any = None,
             regravar: bool = False, ler_papel: Any = None) -> dict[str, Any]:
    """Lê os cards das pessoas e traz documentos e campos vazios.

    Com `simular=True` (o padrão) NADA é gravado e nenhum arquivo é baixado:
    devolve o mesmo relatório, dizendo o que ENTRARIA. É a conferência antes,
    e ela é o padrão de propósito — importação é a operação em que o engano
    custa mais caro para desfazer.

    `ler_cards` existe para o teste poder entregar cards de mentira sem rede.
    """
    ler = ler_cards or cliente.ler_cards
    # ⚑ QUEM DIZ QUAIS CARDS É ELE, e isso é a correção de 13/11/2026 (a
    # segunda). A primeira versão varria o pipe inteiro e casava pelo CPF —
    # trazia card CANCELADO e card REPETIDO, que não interessam. Ele:
    #
    #   *"eu prefiro listar os cards, porque você está trazendo card
    #   duplicado, só que aí tem card que está cancelado, isso aqui não me
    #   interessa. Eu vou listar os cards que eu quero que você busque. E você
    #   faz a pesquisa card a card e puxa as informações e grava."*
    #
    # Ele tem a planilha com os números; escolher quais entram é decisão dele,
    # não adivinhação minha. Uma porta só: a lista colada.
    pedidos = ids_dos_cards(cards)
    if not pedidos:
        return relatorio_vazio(
            "Cole os números dos cards que devem ser buscados.")
    relatorio: dict[str, Any] = {
        "simulado": bool(simular),
        "cards_pedidos": len(pedidos),
        "pessoas_na_fila": 0,
        "pessoas_lidas": 0,
        "documentos_guardados": 0,
        "documentos_ja_tinha": 0,
        "documentos_que_falharam": 0,
        "campos_preenchidos": 0,
        "cards_sem_resposta": [],
        "falhas": [],
        "por_pessoa": [],
        "de_fora": dict(PAPEIS_DE_FORA),
        "cards_sem_pessoa": [],
        "regravando": bool(regravar),
        "anexos_no_pipe": 0,
        "anexos_que_nao_entendi": 0,
        "campos_do_documento": 0,
        "divergencias": 0,
    }
    # CARD A CARD, como ele pediu — em levas de 20 por ida à API.
    lidos, falhas_de_card = ler(pedidos)
    for card, motivo in (falhas_de_card or {}).items():
        relatorio["cards_sem_resposta"].append({"card": str(card),
                                                "motivo": str(motivo)})
    pessoas, sem_pessoa = _pessoas_dos_cards(s, lidos)
    relatorio["cards_sem_pessoa"] = sem_pessoa
    relatorio["pessoas_na_fila"] = len(pessoas)
    if not pessoas:
        return relatorio

    # O número do card fica guardado no cadastro: é a procedência, e evita
    # buscar de novo quem já veio.
    if not simular:
        for p in pessoas:
            s.execute(text("UPDATE colaboradores SET ref_pipefy = :c"
                           " WHERE id = :i AND COALESCE(btrim(ref_pipefy),'') = ''"),
                      {"c": p["card"], "i": p["id"]})
    # ⚑ REGRAVAR É PARA A CARGA INICIAL, e ele pediu com todas as letras em
    # 13/11/2026: *"isso aí vai acontecer só nesse começo. Então, se eu
    # precisar reimportar, eu quero reimportar e regravar as coisas."*
    #
    # Desligado por padrão: no dia a dia, sobrescrever o que alguém conferiu à
    # mão é o pior desfecho possível de uma importação. Na carga inicial é o
    # contrário — o que está aqui veio pela metade, e o card é a fonte.
    ja_veio = set() if regravar else o_que_ja_veio(
        s, [p["card"] for p in pessoas])

    for pessoa in pessoas:
        card = str(pessoa["card"])
        bruto = lidos.get(card)
        if not bruto:
            continue
        relatorio["pessoas_lidas"] += 1
        campos = bruto.get("campos") or {}
        linha = {"colaborador_id": pessoa["id"], "nome": pessoa["nome"],
                 "card": card, "documentos": [], "campos": {},
                 "anexos_do_card": raio_x_dos_anexos(campos)}

        # 1. Os campos que estão vazios aqui.
        achados = dados_do_card(campos)
        empresa_id = empresa_do_card(s, campos)
        if empresa_id:
            # `empresa_id` não é texto: entra como ligação, e o servidor
            # deriva o CNPJ de admissão dela (ver `pessoas.salvar`).
            achados["empresa_id"] = empresa_id
        preencher = achados if regravar else _vazios(s, pessoa["id"], achados)
        if preencher:
            linha["campos"] = preencher
            relatorio["campos_preenchidos"] += len(preencher)
            if not simular:
                _gravar_campos(s, pessoa["id"], preencher, usuario)

        # 2. Os documentos.
        trazidos: list[dict[str, Any]] = []
        for item in linha["anexos_do_card"]:
            relatorio["anexos_no_pipe"] += item["quantos"]
            if "não reconhecido" in (item["formas"] or []):
                relatorio["anexos_que_nao_entendi"] += 1
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
            ok, motivo, conteudo = _trazer_um(s, pessoa, anexo, usuario)
            if ok:
                relatorio["documentos_guardados"] += 1
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "GUARDADO"})
                trazidos.append({"tipo": anexo["tipo"], "nome": anexo["nome"],
                                 "conteudo": conteudo})
            else:
                relatorio["documentos_que_falharam"] += 1
                relatorio["falhas"].append(
                    {"nome": pessoa["nome"], "card": card,
                     "arquivo": anexo["nome"], "motivo": motivo})
                linha["documentos"].append({**_resumo(anexo),
                                            "situacao": "FALHOU"})
        # 3. O QUE O CARD NÃO DISSE, O PAPEL PODE DIZER — e só agora, com os
        # arquivos na mão. Na simulação não roda: ela não baixa nada, e
        # leitura por IA custa dinheiro.
        disse_o_papel: dict[str, str] = {}
        if not simular and trazidos and ler_papel is not False:
            do_papel, disse_o_papel = ler_o_que_veio(
                s, pessoa["id"], trazidos, ler=(ler_papel or None))
            if do_papel:
                linha["do_documento"] = do_papel
                relatorio["campos_do_documento"] += len(do_papel)
                _gravar_campos(s, pessoa["id"], do_papel, usuario)
            # ⚑ O CONFRONTO roda DEPOIS de preencher: o que entrou agora já
            # não é divergência, é buraco tapado. O que sobrar é desacordo de
            # verdade entre o que está aqui, o que o card diz e o que o papel
            # mostra.
        # O confronto roda SEMPRE: card contra cadastro já vale, mesmo sem
        # papel lido — e vale na simulação também, que é onde ele quer ver.
        briga = confrontar(s, pessoa["id"], campos, disse_o_papel)
        if briga:
            linha["divergencias"] = briga
            relatorio["divergencias"] += len(briga)
            # Na simulação a dúvida só aparece; gravando, ela entra na fila
            # de quem decide — senão some junto com a tela.
            if not simular:
                guardar_divergencias(s, pessoa["id"], briga, card)
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
                   usuario: Optional[Usuario],
                   permitir_nome: bool = False) -> None:
    """Grava pelas MESMAS funções da tela, nunca com UPDATE solto.

    É a regra que a fila do DP já segue: se a conferência do cadastro recusa um
    PIS inválido, a importação tem de recusar também — senão a importação vira
    a porta dos fundos por onde entra o dado que a tela barraria."""
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Colaborador

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        return
    # ⚑ O NOME SÓ MUDA QUANDO ALGUÉM DECIDIU QUE MUDA — e o CPF, nunca.
    #
    # A importação em massa força `nome=c.nome` de propósito: ela preenche
    # buraco, não renomeia gente. Mas a DECISÃO de uma divergência é
    # exatamente o caso em que o nome deve mudar — e o teste pegou que, sem
    # esta porta, escolher "o do documento" no campo nome não fazia nada.
    #
    # O CPF fica de fora dos dois caminhos: ele é a identidade da pessoa no
    # sistema, e trocá-lo criaria um segundo cadastro em vez de corrigir o
    # primeiro.
    novo_nome = " ".join(str(campos.get("nome") or "").split())
    svc.salvar(s, {**campos, "cpf": c.cpf,
                   "nome": (novo_nome if (permitir_nome and novo_nome)
                            else c.nome)}, usuario)


def _trazer_um(s: Session, pessoa: dict[str, Any], anexo: dict[str, Any],
               usuario: Optional[Usuario]) -> tuple[bool, str, bytes]:
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
        return True, "", conteudo
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
        return False, motivo, b""


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
        # ⚑ A TELA NÃO DEPENDE MAIS DISTO para saber se dá para importar: a
        # fila é a LISTA QUE ELE COLA. Os números abaixo contam o que já veio,
        # que é outra pergunta — "de quem eu já trouxe?".
        "ativos": int(linha.get("ativos") or 0),
        "com_card": int(linha.get("com_card") or 0),
        "ativos_com_card": int(linha.get("ativos_com_card") or 0),
        "sem_card": int(linha.get("ativos") or 0) - int(linha.get("ativos_com_card") or 0),
        "ja_guardados": int(ja.get("guardados") or 0),
        "ja_falharam": int(ja.get("falharam") or 0),
        "cards_ja_lidos": int(ja.get("cards") or 0),
        "por_rodada": POR_RODADA,
    }
