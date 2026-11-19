# -*- coding: utf-8 -*-
"""
AFASTAMENTO E RETORNO — o tipo é que decide o que a volta exige.

Pergunta dele em 18/11/2026: *"Colaborador estava afastado e vai retornar ao
trabalho. O que deve ser exigido dele e o que o sistema deve pedir pra travar o
simples retorno?"*

**O QUE ACONTECIA ANTES, e é o que este arquivo conserta:** "Afastado" era uma
etapa do quadro e nada mais. Voltar era arrastar o cartão — a situação voltava
para ATIVO sozinha, a pessoa voltava a bater ponto, e nenhuma pergunta era
feita. Ficar 10 dias por atestado e 8 meses por acidente de trabalho eram
indistinguíveis para o sistema.

**POR QUE O TIPO VEM PRIMEIRO:** ele não é detalhe de cadastro, é a chave de
tudo o que vem depois — se a volta exige exame médico, se sobra estabilidade no
emprego, se o período de férias reinicia. Sem o tipo não há o que exigir, e era
por isso que o sistema não exigia nada.

**AS TRÊS COISAS QUE ESTE MÓDULO FAZ, e a ordem importa:**

  1. **Abrir** o afastamento com tipo e datas, e levar a etapa para "Afastado".
  2. **Dizer o que falta para voltar** (`o_que_falta_para_voltar`) — e dizer
     ANTES, para o DP marcar o exame antes de a pessoa aparecer na obra.
  3. **Encerrar** com a data real da volta, calculando a estabilidade.

⚑ A TRAVA MORA NO `mover_etapa`, não aqui, e isso é de propósito: o quadro
arrasta cartão direto para lá, e trava que vive na tela é trava que o arrasto
contorna. Quem decide é o servidor, num lugar só.

⚠️ **O QUE ESTE MÓDULO NÃO É.** Ele não é a fonte oficial da estabilidade nem
do benefício do INSS. Decisão dele: *"a contabilidade conta, mas é interessante
termos a informação também."* Então aqui se GUARDA e se MOSTRA — e a tela diz
que a conta oficial é da contabilidade. Prometer ser a fonte seria prometer o
que ninguém aqui confere.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Colaborador, Usuario

logger = logging.getLogger("erp.afastamentos")

# ---------------------------------------------------------------------------
# OS TIPOS, com a regra de cada um.
#
# ⚑ POR QUE EM CÓDIGO, e as licenças do ponto em tabela: o que está escrito
# aqui é LEI — não há o que a BWS configure. Quantos meses de estabilidade um
# acidente de trabalho gera não é preferência da empresa. As licenças do ponto
# são tabela porque a convenção coletiva pode dar mais dias que a lei.
#
# A lista é a DELE, respondida em 18/11/2026: *"atestado longo, INSS, acidente
# de trabalho, maternidade — sim, temos esses."*
#
# Os campos:
#   · `exame_a_partir_de_dias` — de quantos dias de afastamento em diante a
#     volta exige o ASO de retorno. `None` = nunca exige.
#   · `estabilidade_meses` / `estabilidade_de` — quanto tempo a pessoa fica
#     protegida e a partir de QUANDO conta.
#   · `ferias_reiniciam_apos_meses` — a partir de quanto tempo afastada o
#     período de férias dela começa de novo (é AVISO, não trava: quem calcula
#     férias é a contabilidade).
# ---------------------------------------------------------------------------
TIPOS: dict[str, dict[str, Any]] = {
    "ATESTADO": {
        "nome": "Atestado médico (até 15 dias — pago pela empresa)",
        "base_legal": "CLT art. 476; Lei 8.213/91 art. 60 §3º",
        "exame_a_partir_de_dias": 30,
        "estabilidade_meses": 0,
        "estabilidade_de": None,
        "ferias_reiniciam_apos_meses": 6,
        "pede_documento": "ATESTADO-MEDICO",
        "explicacao": ("Os primeiros 15 dias são da empresa. Passando disso, o "
                       "caso vira auxílio-doença do INSS e o tipo muda."),
    },
    "INSS": {
        "nome": "Auxílio-doença do INSS (acima de 15 dias)",
        "base_legal": "Lei 8.213/91 art. 59 e seguintes",
        "exame_a_partir_de_dias": 30,
        "estabilidade_meses": 0,
        "estabilidade_de": None,
        "ferias_reiniciam_apos_meses": 6,
        "pede_documento": None,
        "explicacao": ("O contrato fica suspenso e quem paga é o INSS. A volta "
                       "depende da alta — a data da cessação do benefício é a "
                       "data do retorno."),
    },
    "ACIDENTE": {
        "nome": "Acidente de trabalho ou doença ocupacional",
        "base_legal": "Lei 8.213/91 art. 118 (estabilidade); NR-7 (exame)",
        "exame_a_partir_de_dias": 30,
        # ⚑ O ITEM QUE CUSTA DINHEIRO. Quem voltou de acidente de trabalho não
        # pode ser demitido sem justa causa por 12 meses contados da ALTA. Ele:
        # *"a contabilidade conta, mas é interessante termos a informação."*
        "estabilidade_meses": 12,
        "estabilidade_de": "RETORNO",
        "ferias_reiniciam_apos_meses": 6,
        "pede_documento": "CAT",
        "explicacao": ("Gera 12 meses de estabilidade a contar da volta: neste "
                       "prazo não cabe demissão sem justa causa. O FGTS "
                       "continua sendo depositado durante o afastamento."),
    },
    "MATERNIDADE": {
        "nome": "Licença-maternidade",
        "base_legal": "CLT art. 392; CF art. 10, II, b (estabilidade)",
        # A NR-7 pede o exame de retorno para ausência de 30 dias ou mais por
        # doença, acidente OU PARTO. A licença tem 120 dias, então sempre cai.
        "exame_a_partir_de_dias": 30,
        "estabilidade_meses": 5,
        # ⚠️ Esta conta é a partir do PARTO, não da volta — e o parto é o
        # início do afastamento. Por isso a estabilidade dela quase sempre
        # termina pouco depois do retorno, e não 5 meses depois.
        "estabilidade_de": "INICIO",
        "ferias_reiniciam_apos_meses": None,
        "pede_documento": None,
        "explicacao": ("Estabilidade até 5 meses depois do parto. Na volta há "
                       "direito a intervalo para amamentação até os 6 meses do "
                       "bebê — combine o horário com a obra."),
    },
    # ⚑ ESTE TIPO EXISTE PARA O PASSADO, e é o único que não exige exame.
    #
    # Havia gente na etapa "Afastado" antes de o tipo ser pedido (migração
    # 111). Se a trava do retorno exigisse tipo conhecido, essas pessoas não
    # conseguiriam voltar de jeito nenhum — a trava nasceria impedindo o
    # trabalho em vez de organizá-lo. E cobrar o exame de um afastamento cuja
    # natureza ninguém registrou seria cobrar no escuro.
    #
    # Ele NÃO deve ser oferecido na tela para afastamento novo.
    "NAO_INFORMADO": {
        "nome": "Afastamento antigo, sem natureza registrada",
        "base_legal": "",
        "exame_a_partir_de_dias": None,
        "estabilidade_meses": 0,
        "estabilidade_de": None,
        "ferias_reiniciam_apos_meses": None,
        "pede_documento": None,
        "explicacao": ("Afastamento que já existia quando o sistema passou a "
                       "pedir o tipo. Corrija o tipo para o retorno ser "
                       "conferido como deve."),
        "so_para_o_passado": True,
    },
}
# Os que a tela oferece para abrir um afastamento NOVO.
TIPOS_NOVOS = tuple(k for k, v in TIPOS.items() if not v.get("so_para_o_passado"))
# O documento do exame de retorno, no catálogo do Arquivo.
DOC_EXAME_DE_RETORNO = "ASO"


def _tipo(codigo: str) -> dict[str, Any]:
    t = TIPOS.get((codigo or "").strip().upper())
    if t is None:
        raise ErroValidacao(
            "Escolha o tipo do afastamento na lista — é ele que decide o que "
            "a volta vai exigir.")
    return t


def opcoes() -> list[dict[str, Any]]:
    """Os tipos como a tela precisa: valor, rótulo e o que cada um implica."""
    return [{"valor": k, "rotulo": TIPOS[k]["nome"],
             "explicacao": TIPOS[k]["explicacao"],
             "base_legal": TIPOS[k]["base_legal"],
             "estabilidade_meses": TIPOS[k]["estabilidade_meses"],
             "pede_documento": TIPOS[k]["pede_documento"]}
            for k in TIPOS_NOVOS]


def _data(valor: Any, campo: str) -> dt.date:
    if isinstance(valor, dt.date):
        return valor
    bruto = str(valor or "").strip()[:10]
    if not bruto:
        raise ErroValidacao(f"Informe a {campo}.")
    try:
        return dt.date.fromisoformat(bruto)
    except ValueError:
        raise ErroValidacao(f"A {campo} está em formato inválido — use "
                            f"dia/mês/ano.")


def _mais_meses(base: dt.date, meses: int) -> dt.date:
    """A mesma data, tantos meses depois. Sem biblioteca nova (regra do
    CLAUDE.md), e sem estourar em 31 de janeiro + 1 mês."""
    mes = base.month - 1 + meses
    ano = base.year + mes // 12
    mes = mes % 12 + 1
    import calendar
    dia = min(base.day, calendar.monthrange(ano, mes)[1])
    return dt.date(ano, mes, dia)


# ---------------------------------------------------------------------------
# Ler
# ---------------------------------------------------------------------------
def aberto(s: Session, colaborador_id: int) -> Optional[dict[str, Any]]:
    """O afastamento em curso desta pessoa, ou None."""
    linha = s.execute(text("""
        SELECT id, tipo, data_inicio, previsao_retorno, motivo, observacao,
               criado_em
          FROM colaborador_afastamentos
         WHERE colaborador_id = :c AND data_retorno IS NULL"""),
        {"c": int(colaborador_id)}).mappings().first()
    if linha is None:
        return None
    t = TIPOS.get(linha["tipo"], {})
    hoje = dt.date.today()
    return {
        "id": int(linha["id"]), "tipo": linha["tipo"],
        "tipo_rotulo": t.get("nome", linha["tipo"]),
        "explicacao": t.get("explicacao", ""),
        "data_inicio": linha["data_inicio"].isoformat(),
        "previsao_retorno": (linha["previsao_retorno"].isoformat()
                             if linha["previsao_retorno"] else None),
        "motivo": linha["motivo"] or "", "observacao": linha["observacao"] or "",
        "dias": (hoje - linha["data_inicio"]).days,
        "tipo_desconhecido": bool(t.get("so_para_o_passado")),
    }


def historico(s: Session, colaborador_id: int) -> list[dict[str, Any]]:
    """Todos os afastamentos da pessoa, do mais recente para o mais antigo."""
    linhas = s.execute(text("""
        SELECT a.id, a.tipo, a.data_inicio, a.previsao_retorno, a.data_retorno,
               a.motivo, a.observacao, a.estabilidade_ate, a.estabilidade_motivo,
               u.nome AS abriu, v.nome AS encerrou
          FROM colaborador_afastamentos a
          LEFT JOIN usuarios u ON u.id = a.aberto_por
          LEFT JOIN usuarios v ON v.id = a.encerrado_por
         WHERE a.colaborador_id = :c
         ORDER BY a.data_inicio DESC, a.id DESC"""),
        {"c": int(colaborador_id)}).mappings().all()
    saida = []
    for l in linhas:
        t = TIPOS.get(l["tipo"], {})
        dias = ((l["data_retorno"] or dt.date.today()) - l["data_inicio"]).days
        saida.append({
            "id": int(l["id"]), "tipo": l["tipo"],
            "tipo_rotulo": t.get("nome", l["tipo"]),
            "data_inicio": l["data_inicio"].isoformat(),
            "previsao_retorno": (l["previsao_retorno"].isoformat()
                                 if l["previsao_retorno"] else None),
            "data_retorno": (l["data_retorno"].isoformat()
                             if l["data_retorno"] else None),
            "dias": dias, "em_curso": l["data_retorno"] is None,
            "motivo": l["motivo"] or "", "observacao": l["observacao"] or "",
            "estabilidade_ate": (l["estabilidade_ate"].isoformat()
                                 if l["estabilidade_ate"] else None),
            "estabilidade_motivo": l["estabilidade_motivo"] or "",
            "abriu": l["abriu"] or "", "encerrou": l["encerrou"] or ""})
    return saida


def estabilidade(s: Session, colaborador_id: int,
                 *, hoje: Optional[dt.date] = None) -> Optional[dict[str, Any]]:
    """A pessoa está no prazo de estabilidade? Para a tela do DESLIGAMENTO.

    ⚠️ **É AVISO, não trava.** Ele: *"a contabilidade conta, mas é interessante
    termos a informação também."* Quem decide desligar é gente, e há caso em
    que cabe (justa causa, pedido da pessoa, fim de contrato de experiência).
    O que não pode é ninguém SABER — era o estado anterior.
    """
    dia = hoje or dt.date.today()
    linha = s.execute(text("""
        SELECT tipo, data_inicio, data_retorno, estabilidade_ate,
               estabilidade_motivo
          FROM colaborador_afastamentos
         WHERE colaborador_id = :c AND estabilidade_ate IS NOT NULL
           AND estabilidade_ate >= :hoje
         ORDER BY estabilidade_ate DESC LIMIT 1"""),
        {"c": int(colaborador_id), "hoje": dia}).mappings().first()
    if linha is None:
        return None
    return {"ate": linha["estabilidade_ate"].isoformat(),
            "dias": (linha["estabilidade_ate"] - dia).days,
            "motivo": linha["estabilidade_motivo"] or "",
            "tipo": linha["tipo"],
            "tipo_rotulo": TIPOS.get(linha["tipo"], {}).get("nome", linha["tipo"]),
            # Dito na tela sem rodeio: a conta oficial não é desta casa.
            "aviso": ("Quem calcula o prazo oficial é a contabilidade. Esta é "
                      "a informação que o sistema guardou no retorno.")}


# ---------------------------------------------------------------------------
# Abrir
# ---------------------------------------------------------------------------
def abrir(s: Session, colaborador_id: int, dados: dict[str, Any],
          usuario: Optional[Usuario]) -> dict[str, Any]:
    """Registra o afastamento e leva a etapa para “Afastado”.

    A etapa é mexida AQUI, e não pela tela: afastar é um ato só, e separar os
    dois deixaria o cadastro "afastado" sem afastamento, ou o contrário.
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    if aberto(s, colaborador_id) is not None:
        raise ErroValidacao(
            f"{c.nome} já está com um afastamento em curso. Trate o retorno "
            "dele antes de abrir outro.")

    codigo = (dados.get("tipo") or "").strip().upper()
    if TIPOS.get(codigo, {}).get("so_para_o_passado"):
        raise ErroValidacao(
            "Este tipo não se escolhe: ele existe só para os afastamentos "
            "antigos, de quando o sistema ainda não pedia a natureza.")
    t = _tipo(codigo)
    inicio = _data(dados.get("data_inicio"), "data em que o afastamento começou")
    if inicio > dt.date.today() + dt.timedelta(days=1):
        raise ErroValidacao(
            "A data de início está no futuro. Afastamento se registra quando "
            "começa — se a pessoa ainda trabalha, não afaste ainda.")
    previsao = (_data(dados.get("previsao_retorno"), "previsão de retorno")
                if (dados.get("previsao_retorno") or "").strip() else None)
    if previsao and previsao < inicio:
        raise ErroValidacao("A previsão de retorno é antes do início.")

    linha = s.execute(text("""
        INSERT INTO colaborador_afastamentos
               (colaborador_id, tipo, data_inicio, previsao_retorno, motivo,
                observacao, aberto_por)
        VALUES (:c, :t, :i, :p, :m, :o, :u) RETURNING id"""),
        {"c": c.id, "t": codigo, "i": inicio, "p": previsao,
         "m": (dados.get("motivo") or "").strip()[:500] or None,
         "o": (dados.get("observacao") or "").strip()[:2000] or None,
         "u": usuario.id if usuario else None}).mappings().first()

    registrar_evento(s, "colaborador", c.id, "AFASTAMENTO_ABERTO",
                     {"tipo": codigo, "inicio": inicio.isoformat(),
                      "previsao_retorno": previsao.isoformat() if previsao else None},
                     usuario.id if usuario else None)
    # `automatico=True` porque a decisão foi tomada AQUI, com tipo e data — a
    # trava de etapa não tem o que conferir de novo.
    svc_pessoas.mover_etapa(
        s, c.id, "AFASTADO", usuario, automatico=True,
        motivo=f"{t['nome']}, desde {inicio:%d/%m/%Y}")
    s.flush()
    logger.info("ERP: %s afastado (%s) desde %s", c.nome, codigo, inicio)
    return {"id": int(linha["id"]), "tipo": codigo,
            "tipo_rotulo": t["nome"], "etapa": "AFASTADO"}


# ---------------------------------------------------------------------------
# O QUE FALTA PARA VOLTAR — a resposta que o DP precisa ANTES da volta
# ---------------------------------------------------------------------------
def o_que_falta_para_voltar(s: Session, colaborador_id: int,
                            *, data_retorno: Optional[dt.date] = None
                            ) -> dict[str, Any]:
    """O que impede, o que apenas avisa, e o que a volta vai acarretar.

    ⚑ TRÊS LISTAS SEPARADAS, de propósito. Misturar "falta o exame" com "o
    treinamento de altura venceu" faria o DP tratar os dois com o mesmo peso —
    e o segundo não impede a volta, impede a pessoa subir no andaime.
    """
    af = aberto(s, colaborador_id)
    if af is None:
        return {"tem_afastamento": False, "impede": [], "avisos": [],
                "acarreta": []}
    t = TIPOS.get(af["tipo"], {})
    volta = data_retorno or dt.date.today()
    inicio = dt.date.fromisoformat(af["data_inicio"])
    dias = (volta - inicio).days

    impede: list[str] = []
    avisos: list[str] = []
    acarreta: list[str] = []

    # 1. O TIPO. Afastamento antigo sem natureza registrada: o DP corrige
    #    agora, porque é o tipo que decide se o exame é devido.
    if af["tipo_desconhecido"]:
        impede.append(
            "Diga o tipo deste afastamento (ele foi registrado antes de o "
            "sistema pedir a natureza). É o tipo que decide se a volta exige "
            "exame médico.")

    # 2. O EXAME DE RETORNO (ASO). Decisão dele em 18/11/2026: *"acho que tem
    #    que travar."*
    #
    # ⚠️ DIFERENTE DO ASO DE ADMISSÃO, que ele decidiu NÃO travar em 10/11
    #    (*"não vamos colocar ele como obrigatório ainda"*). E não é
    #    contradição: na volta o exame é exigido por lei ANTES de a pessoa
    #    trabalhar, e ele pode voltar "apto com restrição" — em obra, essa é a
    #    diferença entre trabalho e acidente.
    #
    # ⚑ E TEM DE SER UM ASO NOVO. O da admissão está arquivado desde sempre;
    #    aceitá-lo faria a trava passar sem ninguém examinar ninguém. Vale o
    #    que foi emitido (ou arquivado) a partir do início do afastamento.
    minimo = t.get("exame_a_partir_de_dias")
    if minimo is not None and dias >= minimo:
        tem = s.execute(text("""
            SELECT 1 FROM documentos
             WHERE colaborador_id = :c AND tipo_codigo = :tipo
               AND COALESCE(emissao, criado_em::date) >= :inicio
             LIMIT 1"""),
            {"c": colaborador_id, "tipo": DOC_EXAME_DE_RETORNO,
             "inicio": inicio}).first()
        if not tem:
            impede.append(
                f"Falta o ASO de retorno ao trabalho. O afastamento chegou a "
                f"{dias} dias, e a partir de {minimo} o exame é obrigatório "
                "ANTES de a pessoa voltar a trabalhar. Jogue o ASO na aba "
                "Arquivos do cadastro — e confira se ele diz “apto” ou “apto "
                "com restrição”.")
    elif minimo is not None:
        avisos.append(
            f"O afastamento tem {dias} dias; o ASO de retorno passa a ser "
            f"obrigatório a partir de {minimo}. Não está sendo exigido aqui.")

    # 3. O DOCUMENTO QUE O TIPO PEDE (a CAT do acidente, o atestado) — AVISO,
    #    não trava: é papel de prontuário, e travar a volta por ele faria a
    #    pessoa ficar sem trabalhar por uma pasta.
    pede = t.get("pede_documento")
    if pede:
        tem = s.execute(text(
            "SELECT 1 FROM documentos WHERE colaborador_id = :c "
            "  AND tipo_codigo = :tipo LIMIT 1"),
            {"c": colaborador_id, "tipo": pede}).first()
        if not tem:
            avisos.append(f"Não há {pede} arquivada no cadastro desta pessoa.")

    # 4. TREINAMENTO VENCIDO. Quem ficou meses fora pode ter voltado com NR
    #    vencida — e ninguém olha, porque a volta não olhava nada.
    for l in s.execute(text("""
        SELECT nome_padronizado, validade FROM documentos
         WHERE colaborador_id = :c AND tipo_codigo = 'CERTIFICADO-NR'
           AND validade IS NOT NULL AND validade < :volta
         ORDER BY validade"""), {"c": colaborador_id, "volta": volta}
    ).mappings().all():
        avisos.append(
            f"Treinamento vencido em {l['validade']:%d/%m/%Y}: "
            f"{l['nome_padronizado']}. Quem volta não pode assumir a função "
            "com NR vencida.")

    # 5. O QUE A VOLTA ACARRETA — informação, não pendência.
    meses = t.get("estabilidade_meses") or 0
    if meses:
        base = volta if t.get("estabilidade_de") == "RETORNO" else inicio
        ate = _mais_meses(base, meses)
        de_onde = ("a contar da volta" if t.get("estabilidade_de") == "RETORNO"
                   else "a contar do início do afastamento")
        acarreta.append(
            f"Estabilidade no emprego até {ate:%d/%m/%Y} ({meses} meses "
            f"{de_onde}): neste prazo não cabe demissão sem justa causa. Quem "
            "calcula o prazo oficial é a contabilidade — o sistema guarda e "
            "avisa.")
    reinicia = t.get("ferias_reiniciam_apos_meses")
    if reinicia and dias >= reinicia * 30:
        acarreta.append(
            f"O afastamento passou de {reinicia} meses: o período de férias "
            "dela recomeça. Confira com a contabilidade antes de programar "
            "férias.")
    acarreta.append(
        "A contabilidade precisa saber do retorno — é evento com prazo no "
        "eSocial. Abra o pedido em Demandas depois de concluir a volta.")

    return {"tem_afastamento": True, "afastamento": af, "dias": dias,
            "impede": impede, "avisos": avisos, "acarreta": acarreta,
            "pode_voltar": not impede}


# ---------------------------------------------------------------------------
# Voltar
# ---------------------------------------------------------------------------
def retornar(s: Session, colaborador_id: int, dados: dict[str, Any],
             usuario: Optional[Usuario]) -> dict[str, Any]:
    """Encerra o afastamento, guarda a estabilidade e devolve a pessoa à ativa.

    Ele, sobre quem faz isto: *"o DP vai fazer isso depois que alimentar com as
    informações pertinentes para o retorno."*
    """
    from app.apps.erp.core.cadastros import pessoas as svc_pessoas

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroValidacao("Colaborador não encontrado.")
    af = aberto(s, colaborador_id)
    if af is None:
        raise ErroValidacao(
            f"{c.nome} não está com afastamento em curso — não há volta a "
            "registrar.")

    volta = _data(dados.get("data_retorno"), "data em que a pessoa voltou")

    # ⚑ O INÍCIO DO AFASTAMENTO ANTIGO PODE SER CORRIGIDO NA VOLTA (19/11/2026).
    #
    # A migração 111 deu aos afastamentos que já existiam uma data ESTIMADA: a
    # última vez que o cartão foi para "Afastado" ou, sem isso, a ADMISSÃO —
    # que é o caso de quem veio pela importação. Com a admissão no lugar do
    # início, a conta de dias explode e a trava cobra ASO de quem ficou uma
    # semana fora. Só o afastamento antigo aceita a correção: o novo teve a
    # data informada por alguém na hora, e mexer nela na volta seria reescrever
    # o que foi registrado.
    if (dados.get("data_inicio") or "").strip() and af["tipo_desconhecido"]:
        novo_inicio = _data(dados.get("data_inicio"), "início do afastamento")
        if novo_inicio > volta:
            raise ErroValidacao(
                f"O início ({novo_inicio:%d/%m/%Y}) é depois da volta "
                f"({volta:%d/%m/%Y}).")
        if novo_inicio.isoformat() != af["data_inicio"]:
            s.execute(text("UPDATE colaborador_afastamentos SET data_inicio = :d "
                           " WHERE id = :id"), {"d": novo_inicio, "id": af["id"]})
            registrar_evento(s, "colaborador", c.id,
                             "AFASTAMENTO_INICIO_CORRIGIDO",
                             {"de": af["data_inicio"],
                              "para": novo_inicio.isoformat()},
                             usuario.id if usuario else None)
            af = aberto(s, colaborador_id) or af

    inicio = dt.date.fromisoformat(af["data_inicio"])
    if volta < inicio:
        raise ErroValidacao(
            f"A volta ({volta:%d/%m/%Y}) é antes do início do afastamento "
            f"({inicio:%d/%m/%Y}).")
    if volta > dt.date.today() + dt.timedelta(days=30):
        raise ErroValidacao(
            "A data da volta está longe no futuro. Registre o retorno quando "
            "ele acontecer — a pessoa fica afastada até lá.")

    # ⚑ O TIPO PODE SER CORRIGIDO NA VOLTA, e é o caminho dos afastamentos
    # antigos (NAO_INFORMADO): o DP diz a natureza agora, e só então a trava
    # tem o que conferir.
    novo_tipo = (dados.get("tipo") or "").strip().upper()
    if novo_tipo and novo_tipo != af["tipo"]:
        t_novo = _tipo(novo_tipo)
        if t_novo.get("so_para_o_passado"):
            raise ErroValidacao("Este tipo não se escolhe.")
        s.execute(text("UPDATE colaborador_afastamentos SET tipo = :t "
                       " WHERE id = :id"), {"t": novo_tipo, "id": af["id"]})
        registrar_evento(s, "colaborador", c.id, "AFASTAMENTO_TIPO_CORRIGIDO",
                         {"de": af["tipo"], "para": novo_tipo},
                         usuario.id if usuario else None)
        af = aberto(s, colaborador_id) or af

    # A TRAVA. Conferida no servidor, com a data da volta que ele informou —
    # não com hoje: quem registra uma volta de ontem tem de ser conferido
    # pelos dias de ontem.
    situacao = o_que_falta_para_voltar(s, colaborador_id, data_retorno=volta)
    if situacao["impede"]:
        raise ErroValidacao(" ".join(situacao["impede"]))

    t = TIPOS.get(af["tipo"], {})
    meses = t.get("estabilidade_meses") or 0
    ate = motivo_est = None
    if meses:
        base = volta if t.get("estabilidade_de") == "RETORNO" else inicio
        ate = _mais_meses(base, meses)
        motivo_est = (f"{t['nome']}: {meses} meses "
                      + ("a contar da volta" if t.get("estabilidade_de") == "RETORNO"
                         else "a contar do início do afastamento")
                      + f" ({base:%d/%m/%Y}). {t['base_legal']}")

    s.execute(text("""
        UPDATE colaborador_afastamentos
           SET data_retorno = :v, estabilidade_ate = :ate,
               estabilidade_motivo = :mot, encerrado_por = :u,
               encerrado_em = now(),
               observacao = COALESCE(NULLIF(:obs, ''), observacao)
         WHERE id = :id"""),
        {"v": volta, "ate": ate, "mot": motivo_est,
         "u": usuario.id if usuario else None, "id": af["id"],
         "obs": (dados.get("observacao") or "").strip()[:2000]})

    registrar_evento(s, "colaborador", c.id, "AFASTAMENTO_ENCERRADO",
                     {"tipo": af["tipo"], "inicio": af["data_inicio"],
                      "retorno": volta.isoformat(), "dias": situacao["dias"],
                      "estabilidade_ate": ate.isoformat() if ate else None},
                     usuario.id if usuario else None)
    # ⚑ PARA ONDE A PESSOA VOLTA, e não é sempre "Concluído".
    #
    # Se houver pedido à contabilidade em aberto, ela volta para
    # "Solicitações" — porque é lá que o cadastro espera a contabilidade, e é
    # a mesma regra da trava de concluir (`pessoas._o_que_impede_concluir`).
    # Mandá-la para "Concluído" com pedido aberto faria o cadastro afirmar o
    # que não aconteceu, e por um caminho que a trava não vê (o retorno move a
    # etapa com `automatico=True`, de propósito: a conferência do retorno é
    # outra, e exigir o registro da CTPS de quem JÁ trabalhava seria cobrar
    # duas vezes o mesmo papel).
    tem_pedido = s.execute(text("""
        SELECT 1 FROM dp_pedidos
         WHERE colaborador_id = :c
           AND situacao IN ('RASCUNHO', 'ENVIADO', 'COBRADO', 'RESPONDIDO',
                            'DIVERGENTE')
         LIMIT 1"""), {"c": c.id}).first()
    destino = "NA_CONTABILIDADE" if tem_pedido else "CONCLUIDO"
    svc_pessoas.mover_etapa(
        s, c.id, destino, usuario, automatico=True,
        motivo=(f"voltou do afastamento ({t.get('nome', af['tipo'])}) em "
                f"{volta:%d/%m/%Y}, {situacao['dias']} dias afastada"
                + (" — segue em Contabilidade, há pedido à contabilidade em "
                   "aberto" if tem_pedido else "")))
    s.flush()
    logger.info("ERP: %s voltou do afastamento em %s (%s dias)",
                c.nome, volta, situacao["dias"])
    return {"ok": True, "data_retorno": volta.isoformat(),
            "etapa": destino,
            "dias": situacao["dias"],
            "estabilidade_ate": ate.isoformat() if ate else None,
            "estabilidade_motivo": motivo_est or "",
            "avisos": situacao["avisos"], "acarreta": situacao["acarreta"]}
