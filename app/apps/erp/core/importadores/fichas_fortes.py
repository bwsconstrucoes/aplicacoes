# -*- coding: utf-8 -*-
"""
A FICHA DE REGISTRO DO FORTES, LIDA DIRETO DO PDF (16/11/2026).

Ele mandou a ficha ("Registro de Empregado") e decidiu: *"como eu já tenho as
fichas aqui, eu quero trabalhar com as fichas mesmo."*

⚑ E NÃO PRECISA DE IA. Eu havia recomendado exportar planilha do Fortes em vez
de ler 575 PDFs, porque leitura por IA custa dinheiro e erra justamente onde o
erro é caro — PIS e CTPS têm onze dígitos. Abrindo o arquivo, a recomendação
caiu: **a ficha é PDF GERADO pelo Fortes, com camada de texto**. O número está
escrito lá, exato. Ler é achar o rótulo e pegar o que vem depois — custo zero,
erro zero.

⚠️ FICHA DIGITALIZADA (escaneada) NÃO TEM ESSA CAMADA, e aí esta leitura
devolve a página como ilegível, com o número dela. Não tenta adivinhar: ficha
que não se lê é ficha que alguém abre na mão, e dizer isso é melhor que
entregar um PIS inventado.

QUEM MANDA EM QUAL CAMPO — decisão dele, 16/11/2026
---------------------------------------------------
**A FICHA MANDA** em PIS, CTPS, admissão, salário, demissão e no código do
Fortes. É o sistema que paga: se ele e o ERP discordam, quem está certo é quem
emitiu o holerite.

**O ERP MANDA** no nome da pessoa, do pai e da mãe — e a ficha é LIDA assim
mesmo. Ele: *"eu tinha dito que o ideal é que quem mande seja nós, a exportação
do cadastro Pipe, mas eu gostaria que fosse feita a leitura também, né? Para
saber, porque às vezes o pessoal pode errar."* Divergência vira PERGUNTA na
fila que já existe (`cadastro_divergencias`, migração 102), e gente decide.

**NÃO ENTRA NADA** de telefone nem de cargo:

  · telefone — ele: *"o telefone não é o da contabilidade, eu não sabia nem que
    eles tinham aí, então ignora isso aí."* E tinha razão de sobra: na ficha
    o DDD está num campo e o número noutro, e o celular tem OITO dígitos —
    falta o 9 que entrou em 2016. Importar isso encheria o cadastro de
    telefone que não completa, bem no campo que acabou de virar obrigatório.
  · cargo — ele: *"o cargo a gente vai utilizar a nossa nomenclatura. A dele
    está feia."* O CBO e o histórico de cargos da ficha ficam de fora.

**NÃO SE GUARDA** o que o ERP não tem campo para guardar — matrícula do
eSocial, etnia/raça, título de eleitor, CIPA, exame médico, jornada,
contribuição sindical. Ele: *"o que a ficha tem a mais que no ERP, deixa na
ficha. A gente não precisa."*

COMO A FICHA ACHA A OBRA
------------------------
Pelo **Setor**: "164.01 - OBRA ESCVARZEA CONST EEM COM 12 SALAS/SETOR". O
número é o centro de custo DA CONTABILIDADE, que passou a morar no cadastro da
obra (migração 104) vindo da planilha C. Diários. Casa pelo número, nunca pelo
nome escrito à mão.

⚠️ NÃO ACHOU A OBRA? A pessoa entra assim mesmo, SEM obra, e o relatório diz
qual centro de custo não tem dono. Recusar a ficha inteira porque falta uma
linha de planilha seria trocar um problema de cadastro por um de importação.
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Um arquivo pode trazer as 575 fichas de uma vez — uma por página.
TETO_DE_PAGINAS = 2000
# O mesmo teto de envio das outras telas. O OMM de julho de 2026 veio de
# download sem teto (`CONTEXTO.md` §9).
TETO_DE_BYTES = 40 * 1024 * 1024

# O que a ficha chama cada coisa → o campo do ERP. A leitura é por RÓTULO,
# nunca por posição: o Fortes muda a largura das colunas e a quebra de linha
# conforme o nome é mais comprido, e qualquer leitura "a partir da coluna 40"
# quebraria na primeira pessoa de nome grande.
ROTULOS = {
    "id_fortes": "Código:",
    "nome": "Nome:",
    "nome_pai": "Pai:",
    "nome_mae": "Mãe:",
    "data_nascimento": "Nascimento:",
    "cpf": "CPF:",
    "pis": "PIS/PASEP:",
    "ctps_numero": "CTPS Número:",
    "admissao": "Admissão:",
    "setor": "Setor:",
    "demissao": "Desligamento:",
}


# ---------------------------------------------------------------------------
# Ler o papel
# ---------------------------------------------------------------------------
def _limpo(texto: Any) -> str:
    """Sem espaço dobrado nas pontas — o resto fica como está escrito."""
    return re.sub(r"[ \t]+", " ", str(texto or "")).strip()


def _sem_acento(texto: Any) -> str:
    cru = unicodedata.normalize("NFD", str(texto or "").strip().lower())
    return "".join(c for c in cru if unicodedata.category(c) != "Mn")


def _digitos(texto: Any) -> str:
    return re.sub(r"\D", "", str(texto or ""))


def _depois_do_rotulo(linha: str, rotulo: str) -> Optional[str]:
    """O que vem depois de "Nome:" NA MESMA LINHA, até o próximo rótulo.

    ⚑ "ATÉ O PRÓXIMO RÓTULO" É O CUIDADO QUE IMPORTA: a ficha põe três coisas
    na mesma linha ("Nascimento: 06/10/1983  Naturalidade: Fortaleza  UF: CE").
    Pegar "tudo que vem depois" devolveria a data com a cidade grudada, e a
    data viraria lixo.
    """
    i = linha.find(rotulo)
    if i < 0:
        return None
    resto = linha[i + len(rotulo):]
    # O próximo rótulo é uma palavra seguida de dois-pontos. Duas formas, e
    # as duas são necessárias:
    #   · depois de espaço DUPLO, o rótulo pode ter espaço no meio
    #     ("Fortaleza    Órgão Emissor: SSP/CE");
    #   · depois de espaço SIMPLES, só rótulo de uma palavra — é o caso de
    #     "Série: 00001 DV:", onde o "DV:" cola no valor e, sem este segundo
    #     caminho, a série entrava no cadastro como "00001 DV:".
    corte = re.search(
        r"\s{2,}[A-Za-zÀ-ÿ][^:]{0,28}:|\s+[A-Za-zÀ-ÿ][^:\s]{0,20}:", resto)
    if corte:
        resto = resto[:corte.start()]
    return _limpo(resto)


def _acha(linhas: list[str], rotulo: str) -> Optional[str]:
    for linha in linhas:
        valor = _depois_do_rotulo(linha, rotulo)
        if valor:
            return valor
    return None


def _na_linha_de(linhas: list[str], ancora: str, rotulo: str) -> Optional[str]:
    """O valor de um rótulo NA LINHA onde outro rótulo está.

    Existe porque a ficha repete o mesmo rótulo em blocos diferentes: "UF:"
    aparece seis vezes (empregador, naturalidade, endereço, CTPS, RG, órgão de
    classe) e "Série:" duas (CTPS e certificado militar). Procurar "a
    primeira" devolveria o dado de outra pessoa jurídica ou de outro
    documento.
    """
    for linha in linhas:
        if ancora in linha:
            return _depois_do_rotulo(linha, rotulo) or None
    return None


def _data(texto: Any) -> Optional[str]:
    """"12/03/2014" vira "2014-03-12". O que não for data vira nada."""
    m = re.search(r"\b(\d{2})/(\d{2})/(\d{4})\b", str(texto or ""))
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else None


def _dinheiro(texto: Any) -> Optional[Decimal]:
    """"2.382,00" vira 2382.00 — formato brasileiro, que é o da ficha."""
    cru = _limpo(texto).replace("R$", "").strip()
    if not cru:
        return None
    try:
        return Decimal(cru.replace(".", "").replace(",", ".")).quantize(
            Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _ultimo_salario(texto: str) -> Optional[Decimal]:
    """O salário de HOJE é o último do histórico — e "último" é por DATA.

    ⚑ NÃO É O ÚLTIMO DA LISTA. A ficha escreve o histórico em quatro colunas
    por linha, e a ordem no papel é a de leitura (esquerda para a direita),
    não a cronológica. Pegar "o que está escrito por último" daria o salário
    de 06/2025 numa ficha cuja última mudança foi 06/2026, e a pessoa entraria
    no ERP ganhando menos do que ganha.
    """
    bloco = _secao(texto, "Histórico de Salários")
    if not bloco:
        return None
    achados: list[tuple[tuple[int, int], Decimal]] = []
    for m in re.finditer(
            r"M[êe]s/Ano:\s*(\d{2})/(\d{4})\s+Valor:\s*([\d.,]+)", bloco):
        valor = _dinheiro(m.group(3))
        if valor is not None:
            achados.append(((int(m.group(2)), int(m.group(1))), valor))
    return max(achados)[1] if achados else None


def _secao(texto: str, titulo: str) -> str:
    """O pedaço da ficha entre um título e o próximo.

    Os títulos são as faixas cinzas do papel ("Documentos", "Dados
    Contratuais", "Histórico de Salários"), e cada um começa a própria coluna
    na margem esquerda.
    """
    TITULOS = ("Empregador", "Dados Pessoais", "Documentos", "Imigração",
               "Dados Contratuais", "Jornada de Trabalho",
               "Outras Informações", "Histórico de Cargos e Funções",
               "Histórico de Salários", "Histórico de Contribuições Sindicais",
               "Assinatura do Empregador")
    linhas = texto.split("\n")
    dentro, saida = False, []
    for linha in linhas:
        comeco = _limpo(linha)
        if not dentro:
            if comeco.startswith(titulo):
                dentro = True
            continue
        if any(comeco.startswith(t) for t in TITULOS if t != titulo):
            break
        saida.append(linha)
    return "\n".join(saida)


# ---------------------------------------------------------------------------
# Uma ficha
# ---------------------------------------------------------------------------
def ler_ficha(texto: str, pagina: int = 1) -> dict[str, Any]:
    """Uma página do PDF → o que o ERP aproveita dela.

    Devolve sempre um dicionário, mesmo quando não deu: `legivel: False` com o
    motivo. Ficha que não se lê é ficha que alguém abre na mão — e dizer isso
    é melhor que entregar um PIS inventado.
    """
    linhas = [l for l in texto.split("\n")]
    cheio = _limpo(" ".join(linhas))
    if "REGISTRO DE EMPREGADO" not in cheio.upper():
        return {"pagina": pagina, "legivel": False,
                "motivo": "esta página não é uma ficha de registro do Fortes"}

    cpf = _digitos(_acha(linhas, "CPF:"))
    if len(cpf) != 11:
        return {"pagina": pagina, "legivel": False,
                "motivo": "não achei o CPF — sem ele não dá para saber de quem "
                          "é a ficha"}

    # ⚑ O SETOR VEM "164.01 - OBRA ESCVARZEA...": o número é o centro de custo
    # da contabilidade, e o resto é o nome deles para a obra. Guardo os dois:
    # o número é o que casa, o nome é o que explica quando não casar.
    setor = _acha(linhas, "Setor:") or ""
    m = re.match(r"\s*([\d.\-/]+)\s*[-–]\s*(.*)", setor)
    centro, setor_nome = (m.group(1), _limpo(m.group(2))) if m else (
        _limpo(setor), "")

    # ⚑ A DEMISSÃO MORA NUMA LINHA SOLTA à direita do bloco contratual, e às
    # vezes o rótulo e a data ficam em linhas diferentes. Por isso procuro no
    # BLOCO inteiro, e não na linha do rótulo.
    contratual = _secao(texto, "Dados Contratuais")
    demissao = _data(_acha(contratual.split("\n"), "Desligamento:") or "")

    return {
        "pagina": pagina,
        "legivel": True,
        # A IDENTIDADE
        "cpf": cpf,
        "id_fortes": _limpo(_acha(linhas, "Código:") or "") or None,
        # ⚑ O QUE A FICHA MANDA (decisão dele, 16/11/2026)
        "pis": _digitos(_acha(linhas, "PIS/PASEP:")) or None,
        "ctps_numero": _digitos(_acha(linhas, "CTPS Número:")) or None,
        # ⚑ SÉRIE E UF SAEM DA LINHA DA CTPS, não da primeira que tiver o
        # rótulo. "UF:" aparece SEIS vezes na ficha — a primeira é a do
        # EMPREGADOR, lá no cabeçalho. Lendo a primeira, todo mundo entraria
        # com a UF da BWS, e ninguém notaria enquanto as duas fossem CE.
        "ctps_serie": _na_linha_de(linhas, "CTPS Número:", "Série:"),
        "ctps_uf": (_na_linha_de(linhas, "CTPS Número:", "UF:") or "")[:2].upper()
                   or None,
        "admissao": _data(_acha(linhas, "Admissão:") or ""),
        "demissao": demissao,
        "salario": _ultimo_salario(texto),
        # ⚑ O QUE SÓ É LIDO PARA CONFERIR — o ERP manda nestes
        "nome": _limpo(_acha(linhas, "Nome:") or "") or None,
        "nome_pai": _limpo(_acha(linhas, "Pai:") or "") or None,
        "nome_mae": _limpo(_acha(linhas, "Mãe:") or "") or None,
        "data_nascimento": _data(_acha(linhas, "Nascimento:") or ""),
        # A OBRA, pelo número da contabilidade
        "centro_custo": centro or None,
        "setor_nome": setor_nome or None,
    }


def ler_arquivo(conteudo: bytes) -> dict[str, Any]:
    """O PDF inteiro — uma ficha por página.

    ⚑ PÁGINA SEM TEXTO É DITA, NÃO ADIVINHADA: ficha digitalizada não tem
    camada de texto, e aí a leitura devolve a página como ilegível com o
    número dela. Quem tiver três assim abre três na mão; quem tiver 575 sabe
    que o caminho é outro.
    """
    if len(conteudo) > TETO_DE_BYTES:
        from app.apps.erp.core.comum.auditoria import ErroValidacao
        raise ErroValidacao(
            f"O arquivo tem mais de {TETO_DE_BYTES // (1024 * 1024)} MB. "
            f"Mande as fichas em partes.")
    try:
        import fitz
    except ImportError:                              # pragma: no cover
        from app.apps.erp.core.comum.auditoria import ErroValidacao
        raise ErroValidacao("O leitor de PDF não está disponível no servidor.")

    fichas, ilegiveis = [], []
    doc = None
    try:
        doc = fitz.open(stream=conteudo, filetype="pdf")
        for n, pagina in enumerate(doc[:TETO_DE_PAGINAS], start=1):
            texto = pagina.get_text() or ""
            if not texto.strip():
                ilegiveis.append({
                    "pagina": n, "legivel": False,
                    "motivo": "página sem texto — ficha digitalizada, só a "
                              "imagem. Esta precisa ser aberta na mão."})
                continue
            lida = ler_ficha(texto, n)
            (fichas if lida["legivel"] else ilegiveis).append(lida)
    finally:
        if doc is not None:
            doc.close()
    return {"fichas": fichas, "ilegiveis": ilegiveis,
            "paginas": len(fichas) + len(ilegiveis)}


# ---------------------------------------------------------------------------
# Trazer para o ERP
# ---------------------------------------------------------------------------
# O que a ficha MANDA: chegou preenchido, vale sobre o que está no cadastro.
# Decisão dele, 16/11/2026: *"concordo em relação ao Fortes mandar em PIS,
# CTPS, admissão, salário, demissão."*
DA_FICHA = ("pis", "ctps_numero", "ctps_serie", "ctps_uf",
            "admissao", "demissao", "salario", "id_fortes")

# O que é LIDO SÓ PARA CONFERIR: o ERP manda, e a diferença vira pergunta.
# Ele: *"o ideal é que quem mande seja nós, a exportação do cadastro Pipe, mas
# eu gostaria que fosse feita a leitura também, para saber, porque às vezes o
# pessoal pode errar."*
A_CONFERIR = {"nome": "Nome", "nome_pai": "Nome do pai",
              "nome_mae": "Nome da mãe", "data_nascimento": "Data de nascimento"}

ROTULO_DO_CAMPO = {
    "pis": "PIS", "ctps_numero": "Nº da CTPS", "ctps_serie": "Série da CTPS",
    "ctps_uf": "UF da CTPS", "admissao": "Admissão", "demissao": "Demissão",
    "salario": "Salário", "id_fortes": "Código no Fortes",
}


def _texto_do_campo(valor: Any) -> str:
    """O valor como uma PESSOA lê, não como o banco guarda.

    ⚑ Isto é de 16/11/2026, pego exercitando a tela: o relatório mostrava
    "2014-03-12" e "2382.00" — data ao contrário e dinheiro em formato de
    outro país. Quem confere a importação está conferindo CONTRA O PAPEL, e
    o papel diz 12/03/2014 e 2.382,00. Comparar texto com texto continua
    valendo porque os dois lados passam por aqui.
    """
    if valor is None:
        return ""
    if isinstance(valor, dt.date):
        return valor.strftime("%d/%m/%Y")
    # ⚑ E A DATA QUE VEM COMO TEXTO TAMBÉM, o que conserta um defeito maior
    # que o feio: a ficha entrega "2014-03-12" e o cadastro guarda uma DATA.
    # Comparando um com o outro, nunca batiam — a admissão de todo mundo
    # entrava como "mudança" em toda importação, e o relatório dizia que 575
    # pessoas iam mudar quando nenhuma ia.
    if isinstance(valor, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", valor):
        return f"{valor[8:]}/{valor[5:7]}/{valor[:4]}"

    if isinstance(valor, Decimal):
        inteiro, _, centavos = f"{valor:.2f}".partition(".")
        milhar = f"{int(inteiro):,}".replace(",", ".")
        return f"{milhar},{centavos}"
    return str(valor)


def importar(s, conteudo: bytes, *, gravar: bool, criar_novos: bool = False,
             usuario: Any = None) -> dict[str, Any]:
    """As fichas do Fortes para dentro do ERP.

    `gravar=False` devolve o relatório inteiro sem tocar no banco — é o botão
    "conferir" da tela. Importação de cadastro roda uma vez e é vista por todo
    mundo depois; simular antes não é zelo, é o jeito de não refazer.

    ⚑ `criar_novos` NASCE DESLIGADO, e é escolha. A ficha tem o bastante para
    criar alguém (nome, CPF, admissão, PIS, CTPS), mas NÃO tem cargo — ele
    recusou a nomenclatura do Fortes (*"a dele está feia"*) — nem telefone nem
    chave Pix, que são obrigatórios no cadastro de hoje. Pessoa criada por aqui
    nasceria incompleta, e cadastro pela metade é dívida que ninguém paga.
    Quem diz QUEM existe continua sendo a exportação do Pipe; a ficha diz o que
    o Fortes sabe sobre quem já existe. O relatório conta quantas fichas são de
    gente que o ERP não tem, para a decisão ser com número na mão.
    """
    from sqlalchemy import select
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.core.comum.auditoria import ErroValidacao
    from app.apps.erp.db.models.cadastros import Colaborador, Obra

    lido = ler_arquivo(conteudo)
    obras = {}
    for o in s.scalars(select(Obra)).all():
        if o.centro_custo_contabil:
            obras.setdefault(_limpo(o.centro_custo_contabil), o)

    atualizadas, criadas, sem_cadastro = [], [], []
    divergencias, sem_obra, mudou_de_obra, recusadas = [], [], [], []

    for f in lido["fichas"]:
        pessoa = s.scalars(
            select(Colaborador).where(Colaborador.cpf == f["cpf"])).first()
        obra = obras.get(_limpo(f.get("centro_custo") or ""))
        if f.get("centro_custo") and obra is None:
            sem_obra.append({"centro_custo": f["centro_custo"],
                             "setor": f.get("setor_nome") or "",
                             "pagina": f["pagina"]})

        if pessoa is None:
            sem_cadastro.append({"pagina": f["pagina"], "cpf": f["cpf"],
                                 "nome": f.get("nome") or "",
                                 "admissao": f.get("admissao")})
            if not criar_novos:
                continue
            dados = {"cpf": f["cpf"], "nome": f.get("nome") or "",
                     "vinculo": "CTPS"}
            if obra is not None:
                dados["obra_id"] = obra.id
            for campo in DA_FICHA:
                if f.get(campo) is not None:
                    dados[campo] = f[campo]
            try:
                svc.salvar(s, dados, usuario)
            except ErroValidacao as e:
                recusadas.append({"pagina": f["pagina"], "cpf": f["cpf"],
                                  "nome": dados["nome"], "motivo": str(e)})
                continue
            criadas.append({"cpf": f["cpf"], "nome": dados["nome"],
                            "obra": obra.codigo if obra else None})
            continue

        # ---- o que a ficha manda, e o que mudou
        mudancas = []
        for campo in DA_FICHA:
            novo = f.get(campo)
            if novo is None or _texto_do_campo(novo) == "":
                continue
            atual = getattr(pessoa, campo, None)
            if _texto_do_campo(atual) == _texto_do_campo(novo):
                continue
            mudancas.append({"campo": campo,
                             "rotulo": ROTULO_DO_CAMPO.get(campo, campo),
                             "de": _texto_do_campo(atual),
                             "para": _texto_do_campo(novo)})
        # ---- o que só se confere: a diferença vira pergunta
        #
        # ⚑ CAMPO VAZIO NO ERP NÃO É DIVERGÊNCIA, É BURACO (16/11/2026, pego
        # exercitando a tela). Nome do pai, nome da mãe e nascimento são
        # campos que o cadastro quase nunca tem — e tratá-los como "o ERP
        # manda" abria uma pergunta para cada um, em cada ficha: 575 fichas
        # viravam mais de mil perguntas sobre dados que ninguém tinha. Não há
        # o que defender num campo vazio: a ficha PREENCHE. A decisão do dono
        # ("quem manda somos nós") vale onde há duas respostas diferentes —
        # e aí sim vira pergunta, sem mudar nada.
        for campo, rotulo in A_CONFERIR.items():
            na_ficha = f.get(campo)
            if not na_ficha:
                continue
            no_erp = getattr(pessoa, campo, None)
            if _mesma_coisa(campo, no_erp, na_ficha):
                continue
            if _texto_do_campo(no_erp) == "":
                mudancas.append({"campo": campo, "rotulo": rotulo,
                                 "de": "", "para": _texto_do_campo(na_ficha)})
                continue
            divergencias.append({
                "colaborador_id": pessoa.id, "cpf": pessoa.cpf,
                "nome": pessoa.nome, "campo": campo, "rotulo": rotulo,
                "no_cadastro": _texto_do_campo(no_erp),
                "na_ficha": _texto_do_campo(na_ficha)})

        # ⚑ UMA FICHA RUIM NÃO DERRUBA AS OUTRAS 574, e isto é conserto de
        # 16/11/2026 — o teste pegou. O cadastro CONFERE o que entra (PIS com
        # dígito errado é recusado, e ainda bem: é o campo que o INSS cobra).
        # Sem este `try`, a primeira ficha com número torto abortava a
        # importação inteira, e quem rodasse não saberia nem qual era.
        if mudancas and gravar:
            try:
                svc.salvar(s, {"cpf": pessoa.cpf, "nome": pessoa.nome,
                               **{m["campo"]: f[m["campo"]] for m in mudancas}},
                           usuario)
            except ErroValidacao as e:
                recusadas.append({"pagina": f["pagina"], "cpf": pessoa.cpf,
                                  "nome": pessoa.nome, "motivo": str(e)})
                continue
        if mudancas:
            atualizadas.append({"cpf": pessoa.cpf, "nome": pessoa.nome,
                                "mudancas": mudancas})

        # ---- a obra que a contabilidade diz, contra a que o ERP diz
        if obra is not None and pessoa.obra_id and pessoa.obra_id != obra.id:
            mudou_de_obra.append({
                "cpf": pessoa.cpf, "nome": pessoa.nome,
                "no_erp": s.get(Obra, pessoa.obra_id).codigo
                          if s.get(Obra, pessoa.obra_id) else "",
                "na_contabilidade": obra.codigo,
                "centro_custo": f.get("centro_custo")})

    if gravar and divergencias:
        _guardar_perguntas(s, divergencias)

    return {
        "paginas": lido["paginas"],
        "fichas_lidas": len(lido["fichas"]),
        "ilegiveis": lido["ilegiveis"],
        "atualizadas": atualizadas,
        "criadas": criadas,
        "sem_cadastro": sem_cadastro,
        "divergencias": divergencias,
        # Ficha que o cadastro recusou — com a página, para quem for conferir
        # achar o papel.
        "recusadas": recusadas,
        "sem_obra": sem_obra,
        "mudou_de_obra": mudou_de_obra,
        "gravou": bool(gravar),
    }


def _mesma_coisa(campo: str, no_erp: Any, na_ficha: Any) -> bool:
    """Compara sem brigar por acento, caixa ou espaço dobrado.

    ⚑ NOME SE COMPARA SEM ACENTO, e isso evita uma fila de perguntas falsas:
    o Fortes escreve tudo em maiúsculas e sem acento ("JOSE"), e o ERP guarda
    como a pessoa escreveu ("José"). São a mesma pessoa, e perguntar 575 vezes
    se "JOSE" é "José" faria a lista de divergências ser fechada sem ser lida —
    que é o pior destino de uma lista dessas.
    """
    a, b = _texto_do_campo(no_erp), _texto_do_campo(na_ficha)
    if campo == "data_nascimento":
        return a[:10] == b[:10]
    return re.sub(r"\s+", " ", _sem_acento(a)) == re.sub(
        r"\s+", " ", _sem_acento(b))


def _guardar_perguntas(s, divergencias: list[dict[str, Any]]) -> int:
    """A diferença vira pergunta na fila que já existe (migração 102).

    ⚑ REUSA A MESMA TABELA do confronto com o Pipefy, com `origem = 'FORTES'`.
    Uma segunda lista de pendências seria uma segunda lista para alguém
    esquecer de olhar.
    """
    from sqlalchemy import text as _sql
    postas = 0
    for d in divergencias:
        s.execute(_sql("""
            INSERT INTO cadastro_divergencias
                   (colaborador_id, campo, no_cadastro, no_card, no_documento,
                    mais_forte, origem, referencia)
            VALUES (:i, :campo, :aqui, NULL, :ficha, 'cadastro', 'FORTES', :ref)
            ON CONFLICT (colaborador_id, campo) WHERE situacao = 'ABERTA'
            DO UPDATE SET no_documento = EXCLUDED.no_documento,
                          origem = EXCLUDED.origem,
                          referencia = EXCLUDED.referencia"""),
            {"i": d["colaborador_id"], "campo": d["campo"],
             "aqui": d["no_cadastro"], "ficha": d["na_ficha"],
             "ref": "ficha de registro do Fortes"})
        postas += 1
    return postas
