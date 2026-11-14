# -*- coding: utf-8 -*-
"""
O DIÁRIO DE OBRA.

Ditado pelo dono em 08/11/2026. **O PRINCÍPIO manda em tudo o que está aqui:**

> o diário pergunta SÓ o que ninguém sabe além de quem esteve lá.

Tudo o que o sistema já tem, o sistema preenche. Diário que obriga a redigitar
o que o ERP sabe é diário preenchido de qualquer jeito na sexta à tarde, com
todos os dias da semana iguais — e aí ele não serve nem para a obra nem para o
pleito de prazo.

**O QUE VEM DE GRAÇA, e de onde:**

| | De onde | Por quê |
|---|---|---|
| cabeçalho da obra e a conta de prazo | `obras` | é cadastro, e muda quando há aditivo |
| **o efetivo** | **o PONTO** | *"a gente não precisaria colocar quantidade de pessoas e função (…) já traria do ponto que é quem trabalhou"* |
| material recebido | Suprimentos | diário registra material recebido, e o dado já existe |
| o relato de ontem | o próprio diário | *"é muito comum o relato do dia anterior ser similar ao do dia seguinte"* |
| sábado, domingo e feriado | o calendário e o ponto | nascem "sem expediente" sem ninguém escrever |

**O QUE A PESSOA ESCREVE:** o tempo, o relato do dia, as ocorrências. E o
terceirizado, que é o único efetivo que o ponto não conhece.

---

**DUAS REGRAS QUE DÃO VALOR JURÍDICO AO DOCUMENTO, e não são detalhe de tela:**

1. **O bloco da fiscalização é DELA.** *"a gente poder conceder acesso a, por
   exemplo, a fiscalização, para a fiscalização fazer observações."* Quem lança
   o diário NÃO escreve nesse campo, e quem é fiscalização não reescreve o
   relato da obra. Um diário que a obra pode reescrever por inteiro não prova
   nada: é justamente a parte que ela não controla que dá valor ao resto.
2. **Depois de FECHADO, corrige-se com registro.** Não é proibido (erro de
   digitação acontece) nem silencioso (reescrever o passado sem rastro tira o
   valor do documento). Fica guardado o que estava, o que passou a estar, quem
   e por quê.

---

**O PONTO PODE NÃO ESTAR LÁ, e o diário não cai por isso.** O schema do ponto
nasce das migrações dele, que são de outra área: num banco onde o ponto nunca
rodou, a consulta do efetivo falha. Então ela é protegida e o diário abre com o
efetivo vazio e um recado — nunca com erro na cara de quem está na obra.
"""
from __future__ import annotations

import logging
import unicodedata
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.rotulos import rotulo_da_obra  # noqa: F401
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import Usuario

logger = logging.getLogger(__name__)

SITUACOES = ("ABERTO", "FECHADO")

# O tempo, por período. Lista aberta de propósito: o modelo que cada órgão
# exige tem a sua, e migração para acrescentar "garoa" seria absurdo.
TEMPOS = ("BOM", "NUBLADO", "CHUVOSO", "IMPRATICAVEL")
TEMPO_ROTULOS = {
    "BOM": "Bom", "NUBLADO": "Nublado", "CHUVOSO": "Chuvoso",
    "IMPRATICAVEL": "Impraticável",
}

# Os campos que a OBRA escreve. A observação da fiscalização NÃO está aqui, e
# essa ausência é a regra 1 lá de cima — ela não é esquecimento.
# Quantos dias uma repetição alcança de uma vez. Não é número mágico: é o que
# ainda dá para uma pessoa conferir na tela antes de confirmar. Repetir um mês
# inteiro numa tacada é o caminho para trinta diários iguais que ninguém leu.
TETO_DE_REPETICAO = 31

CAMPOS_DA_OBRA = (
    "tempo_manha", "tempo_tarde", "relato", "ocorrencias",
    "fiscalizacao_esteve", "fiscalizacao_quem", "sem_expediente",
    # Migração 093 — vieram do modelo que a Prefeitura de Abreu e Lima exige.
    # Decisão dele em 09/11/2026: *"como ele é mais amplo, a gente pode tentar
    # adaptar logo o nosso para esse aí (…) quanto mais amplo, melhor."*
    "horario_trabalho", "equipamentos", "historico",
)

# ---------------------------------------------------------------------------
# O HORÁRIO DE TRABALHO — e por que ele vem PREENCHIDO
#
# Ele: *"a gente pode definir um padrão aí de horário de trabalho. Que o
# normal é 7 às 17. Ou 7 às 16 na sexta-feira (…) como se fosse assim,
# pré-preenchido, a depender do dia da semana. E se necessário, a pessoa que
# estiver preenchendo o diário muda."*
#
# ⚑ ISTO É SUGESTÃO, NÃO VERDADE. O padrão entra no campo quando o dia ainda
# não foi escrito; a partir do momento em que alguém gravou, vale o que está
# gravado — mesmo que seja igual ao padrão, e mesmo que o padrão mude depois.
# Diário é documento: o que ele afirma é o que a pessoa afirmou, não o que a
# tabela de horários diria hoje.
#
# Por que texto e não hora-início/hora-fim: o papel imprime a FRASE, e obra
# com turno partido escreve "7:00 às 11:00 e 13:00 às 17:00".
# ---------------------------------------------------------------------------
HORARIO_PADRAO = {
    0: "7:00 às 17:00",     # segunda
    1: "7:00 às 17:00",
    2: "7:00 às 17:00",
    3: "7:00 às 17:00",
    4: "7:00 às 16:00",     # sexta — ele disse com todas as letras
    5: "",                  # sábado e domingo nascem sem expediente: deixar
    6: "",                  # horário aqui seria afirmar jornada que não houve
}


def horario_padrao(dia: date) -> str:
    """O horário que a tela oferece para ESTE dia da semana."""
    return HORARIO_PADRAO.get(dia.weekday(), "")


# ---------------------------------------------------------------------------
# OS MODELOS DE PAPEL — três, e a escolha é do CONTRATO
#
# Ele mandou dois modelos em 09/11/2026 e pediu: *"você ajusta aí, coloca já
# esses dois como opção. Na nossa opção, o que tiver a mais pode colocar
# também."*
#
# ⚑ POR QUE A ESCOLHA MORA NA OBRA, e não numa tela de impressão: *"tem um
# determinado contrato que a gente vai precisar utilizar esse modelo
# específico de diário de obra."* O modelo é cláusula de contrato. Se fosse
# escolha de quem imprime, um dia sairia o papel errado para a fiscalização —
# e um RDO fora do modelo exigido é motivo de glosa.
#
# O MESMO DADO alimenta os três. Nenhum modelo pergunta nada que os outros não
# perguntem: o que varia é o que o papel mostra e como. Foi por isso que valeu
# "adaptar o nosso para o mais amplo" — um diário por contrato seria três
# telas de preenchimento, e a obra preencheria mal as três.
# ---------------------------------------------------------------------------
MODELOS = {
    "completo": {
        "rotulo": "BWS ampliado (recomendado)",
        "descricao": "O nosso, com tudo o que os dois modelos pedem: efetivo "
                     "do ponto, equipamentos, histórico, material recebido e "
                     "o espaço da fiscalização.",
    },
    "bws": {
        "rotulo": "BWS (o de hoje)",
        "descricao": "O que a empresa já usa: anotações da contratada, "
                     "anotações da fiscalização e as duas assinaturas.",
    },
    "orgao": {
        "rotulo": "Do órgão (RDO em quadros)",
        "descricao": "O formato que a Prefeitura de Abreu e Lima exige em "
                     "contrato: tempo em grade, quadro técnico, equipamentos "
                     "e ocorrências em quadros separados.",
    },
}
MODELO_PADRAO = "completo"


def modelo_valido(nome: str) -> str:
    """Devolve o modelo pedido, ou o padrão. NUNCA levanta erro.

    Papel de diário não pode falhar por causa de um nome de modelo: se a obra
    tiver no cadastro um modelo que o sistema não conhece mais, sai o nosso —
    com dado certo — em vez de uma tela de erro na mão de quem está em obra.
    """
    nome = (nome or "").strip().lower()
    return nome if nome in MODELOS else MODELO_PADRAO


# ---------------------------------------------------------------------------
# A CONSULTA QUE PODE NÃO EXISTIR — e o savepoint que salva a transação
#
# ⚑ ISTO É A PARTE MAIS IMPORTANTE DO ARQUIVO, e custou um defeito para
# aparecer. Em Postgres, UMA consulta que falha ABORTA A TRANSAÇÃO INTEIRA:
# tudo o que vier depois responde "current transaction is aborted", mesmo
# tendo sido pego por `except`. Então `try/except` em volta de um SELECT NÃO
# BASTA — ele engole o erro e deixa a sessão quebrada para o resto do pedido.
#
# Foi exatamente o que aconteceu aqui: num banco sem o schema do ponto, a
# consulta do efetivo falhava, o `except` devolvia lista vazia "com
# elegância", e a busca do material logo abaixo morria com um erro que não
# tinha nada a ver. O diário inteiro caía por causa de uma consulta opcional.
#
# `begin_nested()` abre um SAVEPOINT: o que falhar dentro dele volta sozinho e
# a transação de quem chamou segue viva. É o mesmo conserto que o importador de
# pessoas precisou quando um PIS repetido derrubava todas as linhas seguintes.
# ---------------------------------------------------------------------------
def _consulta_opcional(s: Session, sql: str, **params):
    """Roda uma consulta que PODE não existir, sem levar a transação junto."""
    try:
        with s.begin_nested():
            return list(s.execute(text(sql), params).mappings().all())
    except Exception as e:                              # noqa: BLE001
        logger.warning("ERP/diário: consulta opcional falhou — %s", e)
        return None


# ---------------------------------------------------------------------------
# 1. O CABEÇALHO DA OBRA — e a conta de prazo
# ---------------------------------------------------------------------------


def cabecalho(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """O que vem do cadastro da obra. NADA DISSO É DIGITADO NO DIÁRIO.

    E nada disso é COPIADO para a tabela do diário, tampouco: obra que ganha
    aditivo de prazo passaria a ter diários antigos afirmando o prazo velho. A
    conta é feita na hora, sobre as datas que valem hoje.
    """
    o = s.execute(text("""
        SELECT o.id, o.codigo, o.nome, o.objeto, o.contrato,
               o.data_inicio, o.data_termino, o.ordem_servico,
               o.data_ordem_servico, o.prazo_execucao_dias,
               o.responsavel_tecnico, o.crea_obra, o.art_rrt,
               o.municipio, o.uf, o.endereco, o.numero_endereco, o.bairro,
               -- O CONTRATANTE, que é quem manda no modelo do papel
               o.cliente, o.cnpj_cliente, o.orgao_resumido,
               o.contratante_unidade, o.diario_modelo,
               (o.contratante_logo IS NOT NULL) AS tem_logo_contratante,
               -- A EMPRESA que executa: o nome e o CNPJ que assinam o diário
               e.id AS empresa_id, e.razao_social, e.nome_fantasia,
               e.cnpj AS cnpj_empresa,
               (e.logo IS NOT NULL) AS tem_logo_empresa
          FROM obras o
          LEFT JOIN empresas e ON e.id = o.empresa_id
         WHERE o.id = :o"""), {"o": obra_id}).mappings().first()
    if o is None:
        raise ErroNaoEncontrado("Obra não encontrada.")

    inicio = o["data_inicio"] or o["data_ordem_servico"]
    fim = o["data_termino"]
    prazo: dict[str, Any] = {"decorridos": None, "total": None, "faltam": None,
                             "pct": None}
    if inicio and dia >= inicio:
        prazo["decorridos"] = (dia - inicio).days + 1
    if inicio and fim and fim >= inicio:
        prazo["total"] = (fim - inicio).days + 1
    if fim:
        # Pode dar NEGATIVO, e tem de dar: obra que passou do prazo precisa
        # dizer isso em voz alta, não mostrar zero.
        prazo["faltam"] = (fim - dia).days
    if prazo["decorridos"] and prazo["total"]:
        prazo["pct"] = round(100 * prazo["decorridos"] / prazo["total"])

    return {
        "obra_id": o["id"], "codigo": o["codigo"], "nome": o["nome"],
        # O nome que as telas e o papel mostram — sem repetição.
        "rotulo": rotulo_da_obra(o["codigo"], o["nome"]),
        "objeto": o["objeto"] or "", "contrato": o["contrato"] or "",
        "data_inicio": str(inicio) if inicio else None,
        "data_termino": str(fim) if fim else None,
        "ordem_servico": o["ordem_servico"] or "",
        "data_ordem_servico": (str(o["data_ordem_servico"])
                               if o["data_ordem_servico"] else None),
        "responsavel_tecnico": o["responsavel_tecnico"] or "",
        "crea": o["crea_obra"] or "",
        "art_rrt": o["art_rrt"] or "",
        "municipio": o["municipio"] or "", "uf": o["uf"] or "",
        "endereco": _endereco(o),
        # ⚑ O CONTRATANTE. É ele que decide qual papel sai: o modelo é do
        # CONTRATO, não da empresa nem da pessoa que imprime.
        "contratante": o["cliente"] or "",
        "contratante_cnpj": o["cnpj_cliente"] or "",
        "orgao": o["orgao_resumido"] or "",
        "contratante_unidade": o["contratante_unidade"] or "",
        "tem_logo_contratante": bool(o["tem_logo_contratante"]),
        "modelo": (o["diario_modelo"] or "").strip().lower() or MODELO_PADRAO,
        # A empresa que executa, com o CNPJ que assina.
        "empresa_id": o["empresa_id"],
        "empresa": (o["nome_fantasia"] or o["razao_social"] or ""),
        "empresa_razao": o["razao_social"] or "",
        "empresa_cnpj": o["cnpj_empresa"] or "",
        "tem_logo_empresa": bool(o["tem_logo_empresa"]),
        "prazo": prazo,
    }


def _endereco(o) -> str:
    """O endereço numa linha, que é como o papel imprime."""
    partes = [p for p in (o["endereco"], o["numero_endereco"], o["bairro"])
              if (p or "").strip()]
    linha = ", ".join(partes)
    cidade = " - ".join([p for p in (o["municipio"], o["uf"]) if (p or "").strip()])
    return " — ".join([p for p in (linha, cidade) if p])


# ---------------------------------------------------------------------------
# 2. O EFETIVO — vem do PONTO, e é a melhor parte do desenho
# ---------------------------------------------------------------------------
# ⚑ A HORA DA BATIDA É UTC NO BANCO, E O DIÁRIO MOSTRAVA ASSIM (13/11/2026).
#
# Ele: *"aí tá aparecendo uns horários esquisitos também."* Esquisitos porque
# estavam TRÊS HORAS ADIANTADOS: `ponto.marcacoes.timestamp_servidor` é
# TIMESTAMPTZ gravado em UTC (o Render e o Postgres rodam em UTC), e a área do
# ponto converte para Fortaleza em toda saída (`ponto/horario.py:para_local`).
# O diário imprimia o valor cru — quem bateu 7:00 aparecia às 10:00.
#
# A conversão é feita NO BANCO, com `AT TIME ZONE`, e não em Python: assim o
# MIN/MAX continua sendo sobre o instante real, e o ERP não passa a depender
# do módulo de outra área, que é o que o `CLAUDE.md` manda evitar. O nome do
# fuso vive aqui, com a fonte apontada; Fortaleza não adota horário de verão,
# então não há armadilha de data.
FUSO_DA_EMPRESA = "America/Fortaleza"   # fonte: app/apps/ponto/horario.py

# ⚑ E O FILTRO DE BATIDA VÁLIDA ERA UMA PALAVRA QUE NÃO EXISTE NO PONTO.
#
# O diário pedia as batidas "não canceladas". Os status de `ponto.marcacoes`
# são VALIDA, EM_ANALISE, AJUSTADA e REJEITADA — cancelada não é um deles. Ou
# seja: o filtro não tirava NADA, e **batida REJEITADA contava como gente em
# obra**. O ponto inteiro usa "não rejeitada" (qualidade, folha, espelho,
# arquivos fiscais); o diário era o único fora do passo. AJUSTADA fica DENTRO
# de propósito: é a batida que a pessoa esqueceu e o supervisor incluiu, não
# uma correção que substitui outra.


def efetivo_do_ponto(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """Quem bateu ponto NESTA obra NESTE dia, com função e horário.

    Exigência dele, e é o que faz o diário deixar de ser trabalho: *"hoje a
    gente tem um ponto. Então a gente não precisaria colocar quantidade de
    pessoas e função. Na hora que fosse abrir o dia, já está ali listado."*

    **PROTEGIDA DE PROPÓSITO.** O schema `ponto` nasce das migrações da área do
    ponto. Num banco onde elas não rodaram — ou se o ponto mudar a tabela —
    isto falha, e o diário tem de abrir do mesmo jeito, com o efetivo vazio e
    um recado. Diário que não abre porque o ponto está fora é pior que diário
    sem efetivo automático.
    """
    vazio = {"pessoas": [], "total": 0, "por_funcao": [], "resumo": "",
             "indisponivel": None}
    # ⚑ A FUNÇÃO VEM DE TRÊS LUGARES, e descobrir isso custou uma reclamação
    # dele em 10/11/2026: *"não tem aqui a função da pessoa, está só o nome."*
    #
    # Eu procurava só nos `cargos` e nas `funcoes` do ERP. Acontece que, hoje,
    # a função da maioria mora no **Registro de Colaboradores**
    # (`analisesps.colaborador.cargo`) — a cópia da planilha do DP, que é de
    # onde o PRÓPRIO PONTO lê a função que mostra no tablet
    # (`ponto/core/registro.py`: `COALESCE(NULLIF(btrim(r.cargo),''), f.nome)`).
    # Os cargos do ERP só se enchem quando a planilha de cargos for importada,
    # o que ainda não aconteceu.
    #
    # A ORDEM: o cargo do ERP primeiro (é o que o DP mantém daqui para a
    # frente), depois a função antiga, e por último o Registro. Assim a
    # correção feita no ERP vence a planilha, e não o contrário.
    #
    # DUAS TENTATIVAS, cada uma protegida: num banco sem o schema
    # `analisesps`, a primeira falha INTEIRA e levaria o efetivo junto — e aí
    # voltaríamos ao "o ponto não respondeu" por causa de uma tabela que nem é
    # do ponto.
    # ⚑ O CARGO DO REGISTRO VEM POR SUBCONSULTA, E NÃO POR JOIN (13/11/2026).
    #
    # Com `LEFT JOIN analisesps.colaborador` + `GROUP BY …, r.cargo`, bastava
    # o mesmo CPF existir em DUAS grafias naquela tabela (lá o CPF é a chave
    # como TEXTO, e o casamento aqui é por dígitos) para a MESMA PESSOA virar
    # duas linhas — e duas pessoas na conta do efetivo. A subconsulta com
    # LIMIT 1 devolve no máximo um valor por pessoa, aconteça o que acontecer
    # do outro lado: contagem de gente em obra não pode depender de a tabela
    # vizinha estar limpa.
    de_onde = """
            SELECT c.id, c.nome,
                   COALESCE(NULLIF(btrim(cg.nome), ''),
                            NULLIF(btrim(f.nome), ''),
                            {registro}'') AS funcao,
                   MIN(m.timestamp_servidor AT TIME ZONE :fuso) AS entrada,
                   MAX(m.timestamp_servidor AT TIME ZONE :fuso) AS saida,
                   count(*) AS batidas
              FROM ponto.marcacoes m
              JOIN colaboradores c ON c.id = m.colaborador_id
              LEFT JOIN cargos cg ON cg.id = c.cargo_id
              LEFT JOIN funcoes f ON f.id = c.funcao_id
             WHERE m.obra_id = :o
               AND m.data_referencia = :d
               AND m.status <> 'REJEITADA'
             GROUP BY c.id, c.nome, c.cpf, cg.nome, f.nome
             ORDER BY c.nome"""
    do_registro = ("NULLIF(btrim(("
                   " SELECT r.cargo FROM analisesps.colaborador r"
                   " WHERE regexp_replace(r.cpf, '\\D', '', 'g')"
                   "     = regexp_replace(c.cpf, '\\D', '', 'g')"
                   " LIMIT 1)), ''), ")
    linhas = _consulta_opcional(s, de_onde.format(registro=do_registro),
                                o=obra_id, d=dia, fuso=FUSO_DA_EMPRESA)
    if linhas is None:
        linhas = _consulta_opcional(s, de_onde.format(registro=""),
                                    o=obra_id, d=dia, fuso=FUSO_DA_EMPRESA)
    if linhas is None:
        return {**vazio,
                "indisponivel": "O ponto não respondeu. O efetivo pode ser "
                                "acrescentado à mão."}

    pessoas = [{
        "colaborador_id": l["id"], "nome": l["nome"],
        "funcao": l["funcao"] or "—",
        "entrada": l["entrada"].strftime("%H:%M") if l["entrada"] else None,
        "saida": (l["saida"].strftime("%H:%M")
                  if l["saida"] and l["batidas"] > 1 else None),
    } for l in linhas]

    contagem: dict[str, int] = {}
    for p in pessoas:
        contagem[p["funcao"]] = contagem.get(p["funcao"], 0) + 1
    # Ordena pela QUANTIDADE e, empatando, pelo nome da função: sem o segundo
    # critério a lista dança de ordem a cada abertura, e quem confere o mesmo
    # dia duas vezes acha que mudou alguma coisa.
    por_funcao = [{"funcao": f, "quantidade": q}
                  for f, q in sorted(contagem.items(), key=lambda x: (-x[1], x[0]))]
    # O RESUMO EM UMA FRASE, pedido dele em 10/11/2026: *"nome, função,
    # inclusive um resumo, de quantas pessoas e tal — fica mais fácil entender
    # a coisa."* Montado aqui, e não na tela, porque o papel precisa da mesma
    # frase: duas versões divergiriam no primeiro ajuste.
    resumo = ""
    if pessoas:
        partes = [f"{x['quantidade']} {x['funcao'].lower()}"
                  for x in por_funcao if x["funcao"] != "—"]
        sem_funcao = contagem.get("—", 0)
        if sem_funcao:
            partes.append(f"{sem_funcao} sem função no cadastro")
        resumo = (f"{len(pessoas)} pessoa(s)"
                  + (": " + ", ".join(partes) if partes else ""))
    return {"pessoas": pessoas, "total": len(pessoas),
            "por_funcao": por_funcao, "resumo": resumo, "indisponivel": None}


def lotados_que_bateram_em_outra(s: Session, obra_id: int,
                                 dia: date) -> list[dict[str, Any]]:
    """Quem é desta obra e bateu ponto em OUTRA hoje.

    Não é acusação: é a transferência que ninguém avisou, e que faria o
    efetivo do diário não bater com a obra que pagou o dia. Hoje isso não é
    percebido por ninguém.
    """
    linhas = _consulta_opcional(s, """
            SELECT DISTINCT c.id, c.nome, o.codigo AS onde
              FROM ponto.marcacoes m
              JOIN colaboradores c ON c.id = m.colaborador_id
              JOIN obras o ON o.id = m.obra_id
             WHERE c.obra_id = :o AND m.obra_id <> :o
               AND m.data_referencia = :d
               AND m.status <> 'REJEITADA'
             ORDER BY c.nome LIMIT 50""", o=obra_id, d=dia) or []
    return [{"colaborador_id": l["id"], "nome": l["nome"], "onde": l["onde"]}
            for l in linhas]


# ---------------------------------------------------------------------------
# REPETIR UM DIA EM VÁRIOS DIAS (14/11/2026)
#
# Ele: *"digamos que eu queira replicar um determinado relato para vários dias
# e gostaria de poder fazer isso também. Ó, esse dia aqui foi igual aos 3
# próximos aqui. Ou então esse sábado foi igual a outros 4 sábados aqui."*
#
# ⚑ TRÊS TRAVAS, E NENHUMA É BUROCRACIA:
#
# 1. **DIA QUE JÁ TEM DIÁRIO NÃO É SOBRESCRITO.** É a mesma regra da
#    importação da planilha, e pelo mesmo motivo: diário é documento. Marcar
#    por engano um dia já escrito e perder o relato de quem esteve na obra é
#    estrago que não se desfaz. Os pulados voltam ditos, um a um.
# 2. **NÃO SE ESCREVE O FUTURO.** Diário é relato do que aconteceu. Dia à
#    frente de hoje é recusado — senão a repetição vira previsão, e previsão
#    assinada pela fiscalização é outra coisa.
# 3. **NÃO SE ESCREVE FORA DO PRAZO DA OBRA.** Antes do início ou depois do
#    término, o dia nem deveria existir no calendário.
#
# O QUE É COPIADO: o relato, as ocorrências, o histórico, o tempo e o horário
# de trabalho — o que de fato se repete quando dois dias são iguais. A
# observação da FISCALIZAÇÃO nunca é copiada (regra 1 do topo deste arquivo:
# ela é de quem fiscaliza, não da obra). Os equipamentos vão junto só se
# mandarem, porque máquina que saiu da obra não pode reaparecer sozinha em
# quatro dias.
# ---------------------------------------------------------------------------
CAMPOS_QUE_SE_REPETEM = ("tempo_manha", "tempo_tarde", "relato", "ocorrencias",
                         "historico", "horario_trabalho", "sem_expediente")


def repetir_dia(s: Session, obra_id: int, origem: date, destinos: list[date],
                usuario: Optional[Usuario] = None, *,
                com_equipamentos: bool = False) -> dict[str, Any]:
    """Copia um dia escrito para outros dias que ainda não têm diário."""
    cab = cabecalho(s, obra_id, origem)
    de = _linha(s, obra_id, origem)
    if de is None:
        raise ErroNaoEncontrado(
            "O dia que você quer repetir ainda não foi gravado. Grave-o "
            "primeiro — é ele que será copiado.")
    if not destinos:
        raise ErroValidacao("Escolha pelo menos um dia para receber a cópia.")
    if len(destinos) > TETO_DE_REPETICAO:
        raise ErroValidacao(
            f"São no máximo {TETO_DE_REPETICAO} dias de uma vez. Repetir mês "
            f"inteiro de uma tacada é o tipo de coisa que ninguém confere.")

    hoje = date.today()
    inicio = cab["data_inicio"]
    fim = cab["data_termino"]
    escritos = {l["data"] for l in (_consulta_opcional(s, """
            SELECT data FROM diarios_obra
             WHERE obra_id = :o AND data = ANY(:d)""",
            o=obra_id, d=list(destinos)) or [])}

    copiados, pulados = [], []
    equipamentos = _consulta_opcional(s, """
            SELECT descricao, quantidade, observacao, insumo_id
              FROM diario_equipamentos WHERE diario_id = :i ORDER BY id""",
            i=de["id"]) or [] if com_equipamentos else []

    for dia in sorted(set(destinos)):
        if dia == origem:
            continue
        if dia > hoje:
            pulados.append({"data": str(dia), "motivo": "ainda não aconteceu"})
            continue
        if inicio and str(dia) < inicio:
            pulados.append({"data": str(dia), "motivo": "antes do início da obra"})
            continue
        if fim and str(dia) > fim:
            pulados.append({"data": str(dia), "motivo": "depois do término da obra"})
            continue
        if dia in escritos:
            pulados.append({"data": str(dia),
                            "motivo": "este dia já tem diário escrito"})
            continue

        # ⚑ PASSA PELO `salvar`, e não por um INSERT próprio: é ele que valida
        # (dia com expediente precisa de relato), grava a auditoria e conhece
        # as regras do diário fechado. Um INSERT paralelo aqui seria uma
        # segunda verdade sobre como um diário nasce.
        salvar(s, obra_id, dia,
               {c: de[c] for c in CAMPOS_QUE_SE_REPETEM}, usuario)
        if equipamentos:
            novo = _linha(s, obra_id, dia)
            for item in equipamentos:
                s.execute(text("""
                    INSERT INTO diario_equipamentos
                        (diario_id, descricao, quantidade, observacao, insumo_id)
                    VALUES (:i, :d, :q, :obs, :ins)"""),
                    {"i": novo["id"], "d": item["descricao"],
                     "q": item["quantidade"], "obs": item["observacao"],
                     "ins": item["insumo_id"]})
        copiados.append(str(dia))

    s.flush()
    logger.info("ERP/diário: %s repetido em %d dia(s) na obra %s",
                origem, len(copiados), obra_id)
    return {"de": str(origem), "copiados": copiados, "pulados": pulados,
            "equipamentos_por_dia": len(equipamentos)}


def dias_para_repetir(s: Session, obra_id: int, origem: date,
                      modo: str, quantos: int) -> list[str]:
    """Os dias que os atalhos oferecem — já sem os que não podem receber.

    Dois atalhos, que são as duas frases dele: *"esse dia aqui foi igual aos 3
    próximos"* (`seguidos`) e *"esse sábado foi igual a outros 4 sábados"*
    (`mesmo_dia_da_semana`). O terceiro caminho é marcar no calendário à mão.
    """
    if modo not in ("seguidos", "mesmo_dia_da_semana"):
        raise ErroValidacao("Não entendi como repetir.")
    quantos = max(1, min(int(quantos or 1), TETO_DE_REPETICAO))
    cab = cabecalho(s, obra_id, origem)
    hoje = date.today()
    passo = 1 if modo == "seguidos" else 7
    achados: list[str] = []
    dia = origem
    # Procura para a frente até juntar `quantos` dias que de fato cabem. O
    # limite de voltas existe para a obra encerrada não varrer o calendário
    # inteiro atrás de dias que nunca vão aparecer.
    for _ in range(TETO_DE_REPETICAO * 7):
        dia = dia + timedelta(days=passo)
        if dia > hoje:
            break
        if cab["data_termino"] and str(dia) > cab["data_termino"]:
            break
        if _linha(s, obra_id, dia) is not None:
            continue
        achados.append(str(dia))
        if len(achados) >= quantos:
            break
    return achados


# ---------------------------------------------------------------------------
# 3. O MATERIAL QUE CHEGOU — vem de Suprimentos
# ---------------------------------------------------------------------------
def material_recebido(s: Session, obra_id: int, dia: date) -> list[dict[str, Any]]:
    """O que foi recebido nesta obra neste dia, das entregas já lançadas.

    Diário de obra registra material recebido, e esse dado já existe no ERP —
    digitá-lo de novo seria criar uma segunda verdade sobre a mesma entrega.
    Protegida pelo mesmo motivo do efetivo: Suprimentos pode não ter sido
    usado ainda neste banco.
    """
    linhas = _consulta_opcional(s, """
            SELECT COALESCE(i.nome, si.especificacao, '—') AS item,
                   ri.quantidade, si.unidade,
                   r.nota_numero, f.razao_social AS fornecedor
              FROM recebimentos r
              JOIN recebimento_itens ri ON ri.recebimento_id = r.id
              JOIN pedido_itens pi ON pi.id = ri.pedido_item_id
              LEFT JOIN suprimento_itens si ON si.id = pi.suprimento_item_id
              LEFT JOIN insumos i ON i.id = si.insumo_id
              LEFT JOIN pedidos_compra pc ON pc.id = r.pedido_id
              LEFT JOIN fornecedores f ON f.id = pc.fornecedor_id
             WHERE r.obra_id = :o AND r.data = :d
             ORDER BY item LIMIT 100""", o=obra_id, d=dia) or []
    return [{
        "item": l["item"],
        "quantidade": (f"{l['quantidade']:g}" if l["quantidade"] is not None else ""),
        "unidade": l["unidade"] or "",
        "nota": l["nota_numero"] or "",
        "fornecedor": l["fornecedor"] or "",
    } for l in linhas]


# ---------------------------------------------------------------------------
# 4. O DIA QUE NASCE SEM EXPEDIENTE
# ---------------------------------------------------------------------------
def _e_feriado(s: Session, obra_id: int, dia: date) -> Optional[str]:
    """O feriado é o do PONTO — ele já tem cadastro de feriado por obra.

    Não invento um segundo calendário: dois calendários de feriado divergem no
    primeiro município que decreta ponto facultativo, e aí o diário cobra um
    dia que o ponto considera folga.
    """
    linhas = _consulta_opcional(s, """
            SELECT nome FROM ponto.feriados
             WHERE data = :d
               AND (obra_id IS NULL OR obra_id = :o)
             ORDER BY (obra_id IS NULL) LIMIT 1""", d=dia, o=obra_id)
    return linhas[0]["nome"] if linhas else None


def nasce_sem_expediente(s: Session, obra_id: int, dia: date) -> tuple[bool, str]:
    """Sábado, domingo e feriado nascem "não houve expediente".

    Pedido dele: *"finais de semana, a princípio, se não houver relato, e
    possivelmente vai ter só a presença de vigias, aí já pode colocar que não
    houve expediente. Fim de semana e feriados, a menos que seja digitado
    alguma coisa."*

    **MAS SE ALGUÉM BATEU PONTO, NÃO NASCE ASSIM.** Obra que trabalhou no
    sábado tem diário de sábado — e deixar nascer "sem expediente" com doze
    pessoas no canteiro seria o sistema mentindo por comodidade. O vigia
    sozinho não conta como expediente; dois ou mais, conta.
    """
    feriado = _e_feriado(s, obra_id, dia)
    fim_de_semana = dia.weekday() >= 5
    if not (feriado or fim_de_semana):
        return False, ""
    quantos = efetivo_do_ponto(s, obra_id, dia)["total"]
    if quantos > 1:
        return False, ""
    motivo = f"Feriado: {feriado}" if feriado else (
        "Sábado" if dia.weekday() == 5 else "Domingo")
    if quantos == 1:
        motivo += " — só a vigilância"
    return True, motivo


# ---------------------------------------------------------------------------
# 5. ABRIR O DIA — é aqui que tudo se junta
# ---------------------------------------------------------------------------
def abrir_dia(s: Session, obra_id: int, dia: date,
              usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """O dia, já preenchido com tudo o que o sistema sabe. NÃO GRAVA NADA.

    Abrir não cria linha: quem cria é `salvar`. Assim dá para folhear os dias
    de uma obra sem encher a tabela de diários vazios — e o calendário de
    pendências continua dizendo a verdade sobre o que foi escrito.
    """
    cab = cabecalho(s, obra_id, dia)
    linha = _linha(s, obra_id, dia)
    efetivo = efetivo_do_ponto(s, obra_id, dia)
    sugere_sem, motivo_sem = (False, "")
    if linha is None:
        sugere_sem, motivo_sem = nasce_sem_expediente(s, obra_id, dia)

    # ⚑ `_para_tela` UMA VEZ SÓ: ela vai ao banco buscar extras, equipamentos e
    # correções. Chamá-la de novo para olhar o efetivo importado dobraria essas
    # consultas a cada abertura de dia.
    tela = _para_tela(s, linha) if linha is not None else {
            "situacao": "ABERTO",
            "tempo_manha": "", "tempo_tarde": "", "relato": "",
            "ocorrencias": "", "fiscalizacao_esteve": False,
            "fiscalizacao_quem": "",
            # O horário já vem escrito, conforme o dia da semana. Sábado,
            # domingo e dia sem expediente vêm vazios de propósito.
            "horario_trabalho": "" if sugere_sem else horario_padrao(dia),
            "equipamentos": "", "historico": "",
            "sem_expediente": sugere_sem,
            "obs_fiscalizacao": "", "obs_fiscalizacao_por": "",
            "extras": [], "efetivo_importado": [],
            "informado_por": "", "importado_de": "",
            "equipamentos_lista": [], "equipamentos_resumo": "",
            "equipamentos_disponivel": _tem_equipamentos_em_tabela(s),
        }

    return {
        "obra": cab,
        "data": str(dia),
        "existe": linha is not None,
        "diario": tela,
        "sugere_sem_expediente": sugere_sem,
        "motivo_sem_expediente": motivo_sem,
        "efetivo": efetivo,
        # ⚑ A LISTA DA PLANILHA ENTRA NA SOMA? Decidido aqui, onde se conhecem
        # as duas fontes, e não na tela — o papel usa a mesma regra, e duas
        # versões divergiriam no primeiro ajuste. Ver `importado_conta`.
        "efetivo_importado_conta": importado_conta(
            efetivo, tela.get("efetivo_importado")),
        "avisos_do_ponto": lotados_que_bateram_em_outra(s, obra_id, dia),
        "material": material_recebido(s, obra_id, dia),
        # ⚑ O RELATO DE ONTEM, para reaproveitar. Pedido dele: *"é muito comum
        # o relato do dia anterior ser similar ao do dia seguinte, porque às
        # vezes você começa uma coisa hoje e dá continuidade no dia seguinte.
        # Havia uma forma de facilitar esse resgate."* Vem junto, e a tela
        # oferece um botão — nunca preenche sozinho, senão o diário vira
        # cópia-cola e deixa de ser relato.
        "relato_anterior": _relato_anterior(s, obra_id, dia),
        # ⚑ O EQUIPAMENTO DE ONTEM, pelo mesmo motivo: retroescavadeira não vai
        # embora à noite. Vem oferecido com um botão — não entra sozinho, senão
        # o diário passa a declarar máquina que já saiu da obra.
        "equipamentos_anteriores": equipamentos_do_dia_anterior(s, obra_id, dia),
        # A lista de locáveis vai junto, para a caixinha não precisar de uma
        # segunda ida ao servidor na hora em que a pessoa já está esperando.
        "equipamentos_do_catalogo": equipamentos_do_catalogo(s),
        "tempos": [{"valor": t, "rotulo": TEMPO_ROTULOS[t]} for t in TEMPOS],
    }


def _linha(s: Session, obra_id: int, dia: date):
    return s.execute(text("SELECT * FROM diarios_obra WHERE obra_id = :o AND data = :d"),
                     {"o": obra_id, "d": dia}).mappings().first()


def _relato_anterior(s: Session, obra_id: int, dia: date) -> dict[str, Any]:
    """O último dia COM RELATO antes deste. Pula o fim de semana sozinho."""
    linha = s.execute(text("""
        SELECT data, relato FROM diarios_obra
         WHERE obra_id = :o AND data < :d
           AND relato IS NOT NULL AND btrim(relato) <> ''
         ORDER BY data DESC LIMIT 1"""),
        {"o": obra_id, "d": dia}).mappings().first()
    if linha is None:
        return {"data": None, "relato": ""}
    return {"data": str(linha["data"]), "relato": linha["relato"]}


def _para_tela(s: Session, l) -> dict[str, Any]:
    # ⚑ `origem` separa duas coisas que NÃO são a mesma: o terceirizado que
    # trabalhou e não bate ponto aqui (OBRA), e o efetivo próprio de um dia
    # importado da planilha antiga, quando o ponto eletrônico ainda não
    # existia naquela obra (IMPORTADO). Chamar o segundo de "terceirizado"
    # seria mentir no documento.
    extras = _consulta_opcional(s, """
        SELECT id, descricao, funcao, quantidade, empresa, origem
          FROM diario_efetivo_extra WHERE diario_id = :i ORDER BY id""",
        i=l["id"])
    if extras is None:                       # banco sem a migração 099
        extras = [dict(e, origem="OBRA") for e in s.execute(text("""
            SELECT id, descricao, funcao, quantidade, empresa
              FROM diario_efetivo_extra WHERE diario_id = :i ORDER BY id"""),
            {"i": l["id"]}).mappings().all()]
    # ⚑ `None` e `[]` querem dizer coisas DIFERENTES aqui: `None` é "a tabela
    # ainda não existe neste banco" (o código sobe para o Render antes de
    # alguém apertar o botão das migrações), `[]` é "existe e está vazia". A
    # tela precisa saber a diferença para não oferecer um botão que vai falhar.
    equipamentos = _consulta_opcional(s, """
        SELECT id, descricao, quantidade, observacao, insumo_id
          FROM diario_equipamentos WHERE diario_id = :i ORDER BY id""",
        i=l["id"])
    tem_tabela = equipamentos is not None
    equipamentos = equipamentos or []
    return {
        "id": l["id"], "situacao": l["situacao"],
        "tempo_manha": l["tempo_manha"] or "", "tempo_tarde": l["tempo_tarde"] or "",
        "relato": l["relato"] or "", "ocorrencias": l["ocorrencias"] or "",
        "horario_trabalho": l["horario_trabalho"] or "",
        "equipamentos": l["equipamentos"] or "",
        "historico": l["historico"] or "",
        "fiscalizacao_esteve": bool(l["fiscalizacao_esteve"]),
        "fiscalizacao_quem": l["fiscalizacao_quem"] or "",
        "sem_expediente": bool(l["sem_expediente"]),
        "obs_fiscalizacao": l["obs_fiscalizacao"] or "",
        "obs_fiscalizacao_por": l["obs_fiscalizacao_por"] or "",
        "obs_fiscalizacao_em": (str(l["obs_fiscalizacao_em"])
                                if l["obs_fiscalizacao_em"] else None),
        "fechado_em": str(l["fechado_em"]) if l["fechado_em"] else None,
        "assinado_em": str(l["assinado_em"]) if l["assinado_em"] else None,
        "assinado_nome": l["assinado_nome"] or "",
        "clima_medido": l["clima_medido"],
        "extras": [{"id": e["id"], "descricao": e["descricao"],
                    "funcao": e["funcao"] or "", "quantidade": e["quantidade"],
                    "empresa": e["empresa"] or ""}
                   for e in extras if e["origem"] != "IMPORTADO"],
        "efetivo_importado": [
            {"funcao": e["funcao"] or e["descricao"],
             "quantidade": e["quantidade"]}
            for e in extras if e["origem"] == "IMPORTADO"],
        "informado_por": (l["informado_por"] or "") if "informado_por" in l.keys() else "",
        "importado_de": (l["importado_de"] or "") if "importado_de" in l.keys() else "",
        "equipamentos_lista": [
            {"id": q["id"], "descricao": q["descricao"],
             "quantidade": q["quantidade"],
             "observacao": q["observacao"] or "",
             "insumo_id": q["insumo_id"],
             # A MARCA de quem entrou fora do cadastro. Não é erro — a obra
             # começa às 7 e não espera cadastro —, é trabalho para
             # Suprimentos arrumar depois.
             "fora_da_lista": q["insumo_id"] is None} for q in equipamentos],
        "equipamentos_resumo": resumo_dos_equipamentos(
            [dict(q) for q in equipamentos]),
        "equipamentos_disponivel": tem_tabela,
    }


# ---------------------------------------------------------------------------
# 6. GRAVAR — e a regra de quem escreve o quê
# ---------------------------------------------------------------------------
def salvar(s: Session, obra_id: int, dia: date, dados: dict[str, Any],
           usuario: Optional[Usuario], *, motivo: str = "") -> dict[str, Any]:
    """Grava o que a OBRA escreve. Nunca a observação da fiscalização.

    `CAMPOS_DA_OBRA` não inclui `obs_fiscalizacao` de propósito: mesmo que ela
    venha no corpo do pedido, não entra. É a regra 1 do topo deste arquivo, e
    ela precisa morar aqui — na tela, ela seria só um campo escondido, e campo
    escondido se mostra com o navegador.

    **DEPOIS DE FECHADO, exige motivo** e guarda o que mudou. Diário é
    documento: corrigir pode, apagar o rastro não.
    """
    cabecalho(s, obra_id, dia)                 # 404 se a obra não existe
    linha = _linha(s, obra_id, dia)
    fechado = linha is not None and linha["situacao"] == "FECHADO"
    if fechado and not (motivo or "").strip():
        raise ErroValidacao(
            "Este diário já foi fechado. Para corrigir, diga o motivo — ele "
            "fica registrado junto com o que mudou.")

    limpo: dict[str, Any] = {}
    for campo in CAMPOS_DA_OBRA:
        if campo not in dados:
            continue
        valor = dados[campo]
        if campo in ("fiscalizacao_esteve", "sem_expediente"):
            limpo[campo] = bool(valor)
        elif campo in ("tempo_manha", "tempo_tarde"):
            v = str(valor or "").strip().upper()
            if v and v not in TEMPOS:
                raise ErroValidacao(f"Tempo desconhecido: {valor}.")
            limpo[campo] = v or None
        else:
            limpo[campo] = str(valor or "").strip() or None

    # DIA COM EXPEDIENTE PRECISA DE RELATO. Sem isso o diário vira linha vazia
    # que conta como "preenchido" no calendário — e aí a pendência some sem o
    # trabalho ter sido feito.
    if not limpo.get("sem_expediente", linha["sem_expediente"] if linha else False):
        relato = limpo.get("relato", linha["relato"] if linha else None)
        if not (relato or "").strip():
            raise ErroValidacao(
                "Escreva o relato do dia. Se não houve expediente, marque "
                "“não houve expediente” — aí o relato não é exigido.")

    if linha is None:
        novo = s.execute(text("""
            INSERT INTO diarios_obra
                (obra_id, data, tempo_manha, tempo_tarde, relato, ocorrencias,
                 fiscalizacao_esteve, fiscalizacao_quem, sem_expediente,
                 horario_trabalho, equipamentos, historico,
                 criado_por)
            VALUES (:o, :d, :tm, :tt, :rel, :oc, :fe, :fq, :se,
                    :ht, :eq, :hi, :u)
            RETURNING id"""),
            {"o": obra_id, "d": dia,
             "tm": limpo.get("tempo_manha"), "tt": limpo.get("tempo_tarde"),
             "rel": limpo.get("relato"), "oc": limpo.get("ocorrencias"),
             "fe": limpo.get("fiscalizacao_esteve", False),
             "fq": limpo.get("fiscalizacao_quem"),
             "se": limpo.get("sem_expediente", False),
             # O horário NÃO é preenchido aqui com o padrão: quem grava um dia
             # sem horário está dizendo que não sabe o horário, e inventar a
             # jornada num documento é pior que deixar em branco.
             "ht": limpo.get("horario_trabalho"),
             "eq": limpo.get("equipamentos"),
             "hi": limpo.get("historico"),
             "u": usuario.id if usuario else None}).scalar()
        registrar_evento(s, "diario_obra", novo, "ABERTO",
                         {"obra_id": obra_id, "data": str(dia)},
                         usuario.id if usuario else None)
        s.flush()
        return {"id": novo, "criado": True}

    if fechado:
        for campo, valor in limpo.items():
            antes = linha[campo]
            if _mesmo(antes, valor):
                continue
            s.execute(text("""
                INSERT INTO diario_correcoes
                    (diario_id, campo, antes, depois, motivo, usuario_id)
                VALUES (:i, :c, :a, :d, :m, :u)"""),
                {"i": linha["id"], "c": campo,
                 "a": _texto(antes), "d": _texto(valor), "m": motivo.strip(),
                 "u": usuario.id if usuario else None})

    if limpo:
        sets = ", ".join(f"{c} = :{c}" for c in limpo)
        s.execute(text(f"UPDATE diarios_obra SET {sets}, atualizado_em = now() "
                       f"WHERE id = :id"), {**limpo, "id": linha["id"]})
    s.flush()
    return {"id": linha["id"], "criado": False, "corrigido": fechado}


def _mesmo(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    return (a or "") == (b or "")


def _texto(v: Any) -> str:
    if isinstance(v, bool):
        return "sim" if v else "não"
    return "" if v is None else str(v)


def fechar(s: Session, obra_id: int, dia: date,
           usuario: Optional[Usuario]) -> dict[str, Any]:
    """Fecha o dia. Daqui em diante, corrigir exige motivo."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Não há diário neste dia para fechar.")
    if linha["situacao"] == "FECHADO":
        raise ErroValidacao("Este diário já está fechado.")
    if not linha["sem_expediente"] and not (linha["relato"] or "").strip():
        raise ErroValidacao("Não dá para fechar um dia sem relato.")
    s.execute(text("""
        UPDATE diarios_obra
           SET situacao = 'FECHADO', fechado_em = now(), fechado_por = :u,
               atualizado_em = now()
         WHERE id = :i"""), {"i": linha["id"], "u": usuario.id if usuario else None})
    registrar_evento(s, "diario_obra", linha["id"], "FECHADO",
                     {"obra_id": obra_id, "data": str(dia)},
                     usuario.id if usuario else None)
    s.flush()
    return {"id": linha["id"], "situacao": "FECHADO"}


# ---------------------------------------------------------------------------
# 7. O BLOCO DA FISCALIZAÇÃO — e é só ele que a fiscalização escreve
# ---------------------------------------------------------------------------
def observar_como_fiscalizacao(s: Session, obra_id: int, dia: date, texto_obs: str,
                               usuario: Optional[Usuario]) -> dict[str, Any]:
    """A fiscalização escreve a observação dela. E NADA MAIS.

    Esta função é a outra metade da regra: `salvar` não deixa a obra escrever
    aqui, e esta não deixa a fiscalização escrever lá. Duas portas estreitas,
    cada uma com a sua chave — é isso que faz o registro valer alguma coisa.

    **O diário pode estar FECHADO e ela ainda escreve.** O fechamento é da
    obra sobre o relato da obra; a fiscalização costuma visitar depois, e
    travá-la pelo fechamento seria impedir justamente a parte independente.
    """
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Não há diário neste dia.")
    obs = (texto_obs or "").strip()
    if not obs:
        raise ErroValidacao("Escreva a observação.")
    quem = (usuario.nome if usuario else "") or "fiscalização"
    s.execute(text("""
        UPDATE diarios_obra
           SET obs_fiscalizacao = :t, obs_fiscalizacao_por = :q,
               obs_fiscalizacao_em = now(), atualizado_em = now()
         WHERE id = :i"""), {"i": linha["id"], "t": obs, "q": quem})
    registrar_evento(s, "diario_obra", linha["id"], "OBSERVADO_FISCALIZACAO",
                     {"obra_id": obra_id, "data": str(dia), "por": quem},
                     usuario.id if usuario else None)
    s.flush()
    return {"id": linha["id"], "por": quem}


# ---------------------------------------------------------------------------
# 8. O EFETIVO QUE O PONTO NÃO CONHECE
# ---------------------------------------------------------------------------
def acrescentar_extra(s: Session, obra_id: int, dia: date, dados: dict[str, Any],
                      usuario: Optional[Usuario]) -> dict[str, Any]:
    """O terceirizado que trabalhou e não bate ponto aqui."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado(
            "Abra e grave o diário do dia antes de acrescentar gente.")
    descricao = str(dados.get("descricao") or "").strip()
    if not descricao:
        raise ErroValidacao("Diga quem trabalhou (a pessoa ou a equipe).")
    # Mesma regra do equipamento: em branco é 1, "0" digitado é erro.
    bruto = dados.get("quantidade")
    if bruto is None or str(bruto).strip() == "":
        quantidade = 1
    else:
        try:
            quantidade = int(str(bruto).strip())
        except (TypeError, ValueError):
            raise ErroValidacao("Quantidade inválida.")
    if quantidade < 1:
        raise ErroValidacao("A quantidade tem de ser pelo menos 1.")
    novo = s.execute(text("""
        INSERT INTO diario_efetivo_extra
            (diario_id, descricao, funcao, quantidade, empresa)
        VALUES (:i, :d, :f, :q, :e) RETURNING id"""),
        {"i": linha["id"], "d": descricao,
         "f": str(dados.get("funcao") or "").strip() or None,
         "q": quantidade,
         "e": str(dados.get("empresa") or "").strip() or None}).scalar()
    s.flush()
    return {"id": novo}


def remover_extra(s: Session, obra_id: int, dia: date, extra_id: int) -> None:
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Diário não encontrado.")
    apagou = s.execute(text(
        "DELETE FROM diario_efetivo_extra WHERE id = :e AND diario_id = :i"),
        {"e": extra_id, "i": linha["id"]}).rowcount
    if not apagou:
        raise ErroNaoEncontrado("Registro não encontrado.")
    s.flush()


# ---------------------------------------------------------------------------
# 8-bis. OS EQUIPAMENTOS EM OBRA — lista, não frase
#
# Pedido dele em 10/11/2026, na mesma conversa em que pediu nome + função +
# resumo no efetivo: *"a parte dos equipamentos."*
#
# Ele estava descendo a tela e apontando o mesmo defeito em cada bloco: o
# diário mostrava TEXTO onde devia mostrar CONTA. Um campo "equipamentos" com
# "1 retroescavadeira, 2 betoneira e uma serra" serve para escrever e não
# serve para ler: de trinta dias assim ninguém tira quantos dias a
# retroescavadeira ficou na obra — e é essa conta que sustenta pleito de
# prazo e medição de mobilização.
#
# Então equipamento virou LISTA, igual ao efetivo terceirizado: descrição,
# quantidade e uma observação ("parada por manutenção" é informação que vale
# dinheiro num pleito). E, como retroescavadeira não vai embora à noite, o dia
# novo já abre com a lista do último dia que tinha equipamento — pelo mesmo
# princípio do relato de ontem: VEM OFERECIDO, nunca gravado sozinho.
#
# ⚑ O CAMPO DE TEXTO CONTINUA. Diário é documento: o que foi escrito e
# assinado antes de hoje continua escrito, e o papel imprime o texto quando não
# houver lista. Apagar a coluna seria reescrever o passado.
# ---------------------------------------------------------------------------
def equipamentos_do_catalogo(s: Session) -> list[dict[str, Any]]:
    """Os insumos marcados como LOCÁVEIS — a lista que a caixinha oferece.

    ⚑ É a mesma base de Suprimentos, e isso é o ponto: equipamento digitado à
    mão produz "retro", "retroescavadeira" e "Retro-escavadeira" para a MESMA
    máquina, e aí nenhuma conta de dias-máquina fecha.
    """
    linhas = _consulta_opcional(s, """
        SELECT id, codigo, descricao, unidade
          FROM insumos
         WHERE ativo IS TRUE AND locavel IS TRUE
         ORDER BY descricao""")
    return [{"id": l["id"], "codigo": l["codigo"], "descricao": l["descricao"],
             "unidade": l["unidade"] or ""} for l in (linhas or [])]


def _insumo_locavel(s: Session, insumo_id: int) -> dict[str, Any]:
    linha = s.execute(text("""
        SELECT id, descricao, unidade, ativo, locavel
          FROM insumos WHERE id = :i"""), {"i": insumo_id}).mappings().first()
    if linha is None:
        raise ErroNaoEncontrado("Equipamento não encontrado no cadastro.")
    if not linha["ativo"]:
        raise ErroValidacao(
            f"“{linha['descricao']}” está inativo no cadastro de insumos.")
    if not linha["locavel"]:
        raise ErroValidacao(
            f"“{linha['descricao']}” não está marcado como locável. Marque em "
            f"Suprimentos › Insumos para ele aparecer como equipamento.")
    return dict(linha)


def resumo_dos_equipamentos(linhas: list[dict[str, Any]]) -> str:
    """"3 equipamentos: 1 retroescavadeira, 2 betoneira" — montado aqui, na
    regra, porque a tela e o papel usam a MESMA frase. Duas versões
    divergiriam no primeiro ajuste."""
    if not linhas:
        return ""
    total = sum(int(l.get("quantidade") or 1) for l in linhas)
    partes = [f"{int(l.get('quantidade') or 1)} {str(l.get('descricao') or '').strip()}"
              for l in linhas if str(l.get("descricao") or "").strip()]
    if not partes:
        return ""
    return f"{total} equipamento(s): " + ", ".join(partes)


def equipamentos_do_dia_anterior(s: Session, obra_id: int,
                                 dia: date) -> dict[str, Any]:
    """O último dia COM EQUIPAMENTO antes deste. Pula o fim de semana sozinho,
    igual ao relato de ontem."""
    linhas = _consulta_opcional(s, """
        SELECT d.data, q.descricao, q.quantidade, q.observacao, q.insumo_id
          FROM diario_equipamentos q
          JOIN diarios_obra d ON d.id = q.diario_id
         WHERE d.obra_id = :o AND d.data < :dia
           AND d.data = (SELECT MAX(d2.data)
                           FROM diarios_obra d2
                           JOIN diario_equipamentos q2 ON q2.diario_id = d2.id
                          WHERE d2.obra_id = :o AND d2.data < :dia)
         ORDER BY q.id""", o=obra_id, dia=dia)
    if not linhas:
        return {"data": None, "itens": [], "resumo": ""}
    itens = [{"descricao": l["descricao"], "quantidade": l["quantidade"],
              "observacao": l["observacao"] or "",
              "insumo_id": l["insumo_id"]} for l in linhas]
    return {"data": str(linhas[0]["data"]), "itens": itens,
            "resumo": resumo_dos_equipamentos([dict(l) for l in linhas])}


def acrescentar_equipamento(s: Session, obra_id: int, dia: date,
                            dados: dict[str, Any]) -> dict[str, Any]:
    """Uma linha de equipamento no dia."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado(
            "Abra e grave o diário do dia antes de acrescentar equipamento.")
    # ⚑ O EQUIPAMENTO VEM DO CADASTRO quando dá. A descrição é gravada
    # assim mesmo: o insumo pode ser renomeado amanhã, e o diário tem de dizer
    # o que foi escrito NAQUELE dia. O `insumo_id` serve para contar; a
    # descrição, para provar.
    insumo_id = dados.get("insumo_id") or None
    if insumo_id:
        try:
            insumo_id = int(insumo_id)
        except (TypeError, ValueError):
            raise ErroValidacao("Equipamento inválido.")
        descricao = _insumo_locavel(s, insumo_id)["descricao"]
    else:
        descricao = str(dados.get("descricao") or "").strip()
    if not descricao:
        raise ErroValidacao("Diga qual é o equipamento.")
    # ⚑ CAMPO EM BRANCO É 1; "0" DIGITADO É ERRO. São coisas diferentes:
    # quem não digita quantidade quer dizer "um"; quem digita zero errou a
    # tecla, e "zero betoneira em obra" não é informação — é um diário que
    # declara o que não houve.
    bruto = dados.get("quantidade")
    if bruto is None or str(bruto).strip() == "":
        quantidade = 1
    else:
        try:
            quantidade = int(str(bruto).strip())
        except (TypeError, ValueError):
            raise ErroValidacao("Quantidade inválida.")
    if quantidade < 1:
        raise ErroValidacao("A quantidade tem de ser pelo menos 1.")
    novo = s.execute(text("""
        INSERT INTO diario_equipamentos
            (diario_id, descricao, quantidade, observacao, insumo_id)
        VALUES (:i, :d, :q, :obs, :ins) RETURNING id"""),
        {"i": linha["id"], "d": descricao, "q": quantidade,
         "obs": str(dados.get("observacao") or "").strip() or None,
         "ins": insumo_id}).scalar()
    s.flush()
    return {"id": novo}


def remover_equipamento(s: Session, obra_id: int, dia: date,
                        equipamento_id: int) -> None:
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado("Diário não encontrado.")
    apagou = s.execute(text(
        "DELETE FROM diario_equipamentos WHERE id = :e AND diario_id = :i"),
        {"e": equipamento_id, "i": linha["id"]}).rowcount
    if not apagou:
        raise ErroNaoEncontrado("Registro não encontrado.")
    s.flush()


def repetir_equipamentos_de_ontem(s: Session, obra_id: int,
                                  dia: date) -> dict[str, Any]:
    """Copia para hoje a lista do último dia que tinha equipamento.

    ⚑ Não acumula: se hoje já tem lista, o pedido é recusado em vez de dobrar
    o parque de máquinas. Quem quer trocar apaga o que está errado primeiro —
    equipamento duplicado num diário é erro que aparece em medição."""
    linha = _linha(s, obra_id, dia)
    if linha is None:
        raise ErroNaoEncontrado(
            "Abra e grave o diário do dia antes de repetir o equipamento.")
    ja = _consulta_opcional(
        s, "SELECT id FROM diario_equipamentos WHERE diario_id = :i",
        i=linha["id"])
    if ja:
        raise ErroValidacao(
            "Este dia já tem equipamento na lista. Apague o que não serve "
            "antes de repetir o dia anterior.")
    anterior = equipamentos_do_dia_anterior(s, obra_id, dia)
    if not anterior["itens"]:
        raise ErroValidacao("Nenhum dia anterior desta obra tem equipamento.")
    for item in anterior["itens"]:
        s.execute(text("""
            INSERT INTO diario_equipamentos
                (diario_id, descricao, quantidade, observacao, insumo_id)
            VALUES (:i, :d, :q, :obs, :ins)"""),
            {"i": linha["id"], "d": item["descricao"],
             "q": item["quantidade"], "obs": item["observacao"] or None,
             "ins": item.get("insumo_id")})
    s.flush()
    return {"copiados": len(anterior["itens"]), "de": anterior["data"]}


# ---------------------------------------------------------------------------
# 9. O CALENDÁRIO DE PENDÊNCIAS — "o que está faltando"
#
# Pedido dele em 08/11/2026: *"a gente precisa visualizar por obra facilmente o
# que é que tem diário, o que é que não tem, para entender o que é que está
# pendente. Até lembrando um pouco o ponto (…) uma espécie de calendário e a
# pessoa apontando o que é que tem e o que é que não tem."*
#
# ⚑ FIM DE SEMANA E FERIADO NÃO SÃO PENDÊNCIA. Se fossem, o calendário nasceria
# com 104 faltas por ano e ninguém olharia para ele de novo — um painel que
# grita sempre não avisa nunca. Mas um sábado em que GENTE BATEU PONTO vira
# pendência, porque ali houve obra.
# ---------------------------------------------------------------------------
def calendario(s: Session, obra_id: int, ano: int, mes: int) -> dict[str, Any]:
    """Dia a dia do mês: o que tem diário, o que falta, o que não era para ter."""
    import calendar as _cal

    cab = cabecalho(s, obra_id, date(ano, mes, 1))
    _, ultimo = _cal.monthrange(ano, mes)
    primeiro_dia, ultimo_dia = date(ano, mes, 1), date(ano, mes, ultimo)

    escritos = {l["data"]: l for l in s.execute(text("""
        SELECT data, situacao, sem_expediente, relato,
               (obs_fiscalizacao IS NOT NULL) AS tem_fiscal
          FROM diarios_obra
         WHERE obra_id = :o AND data BETWEEN :i AND :f"""),
        {"o": obra_id, "i": primeiro_dia, "f": ultimo_dia}).mappings().all()}

    # UMA consulta para o mês inteiro, não uma por dia: trinta idas ao banco
    # para desenhar um calendário é o tipo de coisa que deixa a tela lenta sem
    # ninguém entender por quê.
    com_ponto = {l["data_referencia"] for l in (_consulta_opcional(s, """
            SELECT DISTINCT data_referencia FROM ponto.marcacoes
             WHERE obra_id = :o AND data_referencia BETWEEN :i AND :f
               AND status <> 'REJEITADA'""",
            o=obra_id, i=primeiro_dia, f=ultimo_dia) or [])}
    feriados = {l["data"]: l["nome"] for l in (_consulta_opcional(s, """
            SELECT data, nome FROM ponto.feriados
             WHERE data BETWEEN :i AND :f AND (obra_id IS NULL OR obra_id = :o)""",
            o=obra_id, i=primeiro_dia, f=ultimo_dia) or [])}

    inicio_obra = cab["data_inicio"]
    fim_obra = cab["data_termino"]
    hoje = date.today()
    dias, faltam = [], 0
    for n in range(1, ultimo + 1):
        d = date(ano, mes, n)
        fora = bool(
            (inicio_obra and str(d) < inicio_obra)
            or (fim_obra and str(d) > fim_obra)
            or d > hoje)
        linha = escritos.get(d)
        feriado = feriados.get(d)
        descanso = (d.weekday() >= 5 or feriado is not None) and d not in com_ponto

        if linha is not None:
            estado = ("sem_expediente" if linha["sem_expediente"]
                      else ("fechado" if linha["situacao"] == "FECHADO" else "aberto"))
        elif fora:
            estado = "fora"
        elif descanso:
            estado = "descanso"
        else:
            estado = "falta"
            faltam += 1

        dias.append({
            "dia": n, "data": str(d), "semana": d.weekday(), "estado": estado,
            "feriado": feriado or "",
            "teve_ponto": d in com_ponto,
            "tem_fiscalizacao": bool(linha["tem_fiscal"]) if linha else False,
        })

    return {"obra": cab, "ano": ano, "mes": mes, "dias": dias,
            "faltam": faltam,
            "primeiro_dia_da_semana": primeiro_dia.weekday()}


def pendencias_por_obra(s: Session, ate: Optional[date] = None, *,
                        obra_ids: Optional[list[int]] = None,
                        dias: int = 30) -> list[dict[str, Any]]:
    """Quantos dias cada obra deve de diário nos últimos N dias.

    É a visão de quem olha TODAS as obras — o painel que diz onde correr. O
    calendário mostra uma obra por vez; este diz por onde começar.
    """
    ate = ate or date.today()
    de = ate - timedelta(days=dias)
    linhas = s.execute(text("""
        SELECT o.id, o.codigo, o.nome,
               max(d.data) AS ultimo
          FROM obras o
          LEFT JOIN diarios_obra d ON d.obra_id = o.id AND d.data BETWEEN :de AND :ate
         WHERE o.status <> 'ENCERRADA'
           AND (:todas OR o.id = ANY(:obras))
         GROUP BY o.id, o.codigo, o.nome
         ORDER BY o.codigo"""),
        {"de": de, "ate": ate,
         "obras": obra_ids or [], "todas": obra_ids is None}).mappings().all()

    saida = []
    for l in linhas:
        cal_faltas = 0
        for n in range((ate - de).days + 1):
            d = de + timedelta(days=n)
            if d.weekday() >= 5:
                continue
            cal_faltas += 1
        escritos = s.execute(text("""
            SELECT count(*) FROM diarios_obra
             WHERE obra_id = :o AND data BETWEEN :de AND :ate
               AND EXTRACT(ISODOW FROM data) < 6"""),
            {"o": l["id"], "de": de, "ate": ate}).scalar() or 0
        saida.append({
            "obra_id": l["id"], "codigo": l["codigo"], "nome": l["nome"],
            "esperados": cal_faltas, "escritos": int(escritos),
            "faltam": max(0, cal_faltas - int(escritos)),
            "ultimo": str(l["ultimo"]) if l["ultimo"] else None,
        })
    return sorted(saida, key=lambda x: -x["faltam"])


# ---------------------------------------------------------------------------
# 10. O RELATÓRIO — o papel que vai para a fiscalização
#
# Pedido dele em 09/11/2026: *"antes de eu lhe mandar os modelos de diário, eu
# queria que você gerasse um seu (…) gere aí um modelo para eu avaliar."*
#
# ⚑ A DIFERENÇA ENTRE A TELA E O PAPEL, e ela é dele: *"provavelmente a gente
# vai visualizar em tela o nome das pessoas, mas eu acredito que talvez a gente
# não vá relacionar nome a nome no diário, vai colocar só quantidade e
# função."* Por isso `efetivo_do_ponto` já devolve as DUAS coisas, e é aqui que
# a escolha acontece: o papel leva a CONTAGEM POR FUNÇÃO.
#
# Faz sentido além da preferência: o RDO é documento contratual, e listar nome
# e horário de cada pessoa num papel que circula fora da empresa é expor dado
# de gente sem necessidade — a fiscalização precisa saber QUANTOS pedreiros
# havia, não quem eram.
#
# O NÚMERO DO RDO é o dia de obra (dia 234 de 361). Não inventei sequência
# própria: ela divergiria do prazo no primeiro diário esquecido, e aí o papel
# passaria a contar uma história diferente do contrato.
# ---------------------------------------------------------------------------
def relatorio(s: Session, obra_id: int, de: date, ate: Optional[date] = None,
              *, so_escritos: bool = True,
              modelo: Optional[str] = None) -> dict[str, Any]:
    """Os dias de uma obra, prontos para imprimir. Um dia por página."""
    ate = ate or de
    if ate < de:
        raise ErroValidacao("A data final é anterior à inicial.")
    if (ate - de).days > 93:
        raise ErroValidacao(
            "O período é longo demais para um relatório só (máximo três "
            "meses). Gere por mês.")

    cab = cabecalho(s, obra_id, ate)
    escritos = {l["data"]: l for l in s.execute(text("""
        SELECT * FROM diarios_obra
         WHERE obra_id = :o AND data BETWEEN :de AND :ate
         ORDER BY data"""), {"o": obra_id, "de": de, "ate": ate}).mappings().all()}

    paginas = []
    n = 0
    while True:
        dia = de + timedelta(days=n)
        n += 1
        if dia > ate:
            break
        linha = escritos.get(dia)
        if linha is None and so_escritos:
            continue
        efetivo = efetivo_do_ponto(s, obra_id, dia)
        extras, importado = [], []
        if linha is not None:
            linhas_extra = _consulta_opcional(s, """
                SELECT descricao, funcao, quantidade, empresa, origem
                  FROM diario_efetivo_extra WHERE diario_id = :i ORDER BY id""",
                i=linha["id"])
            if linhas_extra is None:         # banco sem a migração 099
                linhas_extra = [dict(e, origem="OBRA") for e in s.execute(text("""
                    SELECT descricao, funcao, quantidade, empresa
                      FROM diario_efetivo_extra WHERE diario_id = :i
                     ORDER BY id"""), {"i": linha["id"]}).mappings().all()]
            extras = [{"descricao": e["descricao"], "funcao": e["funcao"] or "",
                       "quantidade": e["quantidade"], "empresa": e["empresa"] or ""}
                      for e in linhas_extra if e["origem"] != "IMPORTADO"]
            # ⚑ O EFETIVO IMPORTADO ENTRA NO QUADRO TÉCNICO, não no de
            # terceiros: num dia da planilha antiga ele É o efetivo próprio da
            # obra. Sem isto o papel de 1.380 dias sairia com "0 pessoas".
            importado = [{"funcao": e["funcao"] or e["descricao"],
                          "quantidade": e["quantidade"]}
                         for e in linhas_extra if e["origem"] == "IMPORTADO"]
        # A LISTA de equipamento do dia. O papel imprime a lista quando ela
        # existe e o texto antigo quando não — nenhum diário já assinado perde
        # o que tinha escrito.
        equipamentos = []
        if linha is not None:
            equipamentos = [dict(q) for q in (_consulta_opcional(s, """
                SELECT descricao, quantidade, observacao
                  FROM diario_equipamentos WHERE diario_id = :i ORDER BY id""",
                i=linha["id"]) or [])]
        paginas.append({
            # "FOLHA 1/1", que o modelo do órgão imprime. Contada aqui, e não
            # na folha de estilo: o número da folha dentro do laço do template
            # não atravessa para o pedaço incluído, e um papel que diz "1/1"
            # em todas as páginas de um relatório de três meses seria papel
            # errado — a fiscalização numera por folha.
            "folha": len(paginas) + 1,
            "data": str(dia),
            "dia_da_semana": _DIA_DA_SEMANA[dia.weekday()],
            # O cabeçalho é recalculado POR DIA: o "dia 234 de 361" do dia 10
            # não é o do dia 11, e um relatório com o mesmo número em todas as
            # páginas seria um relatório errado.
            "prazo": cabecalho(s, obra_id, dia)["prazo"],
            "existe": linha is not None,
            "sem_expediente": bool(linha["sem_expediente"]) if linha else False,
            "tempo_manha": TEMPO_ROTULOS.get(linha["tempo_manha"] or "", "—") if linha else "—",
            "tempo_tarde": TEMPO_ROTULOS.get(linha["tempo_tarde"] or "", "—") if linha else "—",
            "relato": (linha["relato"] or "") if linha else "",
            "ocorrencias": (linha["ocorrencias"] or "") if linha else "",
            "historico": (linha["historico"] or "") if linha else "",
            "equipamentos": (linha["equipamentos"] or "") if linha else "",
            "equipamentos_lista": equipamentos,
            "equipamentos_resumo": resumo_dos_equipamentos(equipamentos),
            # O horário SAI VAZIO num dia que ninguém escreveu. O padrão da
            # semana é sugestão de tela; no papel, afirmar jornada que nenhuma
            # pessoa confirmou seria inventar prova.
            "horario_trabalho": (linha["horario_trabalho"] or "") if linha else "",
            "fiscalizacao_esteve": bool(linha["fiscalizacao_esteve"]) if linha else False,
            "fiscalizacao_quem": (linha["fiscalizacao_quem"] or "") if linha else "",
            "obs_fiscalizacao": (linha["obs_fiscalizacao"] or "") if linha else "",
            "obs_fiscalizacao_por": (linha["obs_fiscalizacao_por"] or "") if linha else "",
            "situacao": linha["situacao"] if linha else "ABERTO",
            # ⚑ A CONTAGEM, NÃO OS NOMES. Decisão dele, e boa prática: o RDO
            # circula fora da empresa.
            "efetivo": _efetivo_para_o_papel(efetivo, extras, importado),
            "material": material_recebido(s, obra_id, dia),
            "correcoes": _correcoes(s, linha["id"]) if linha else [],
        })

    for pag in paginas:
        pag["folhas"] = len(paginas)

    return {"obra": cab, "de": str(de), "ate": str(ate), "paginas": paginas,
            "gerado_em": date.today().isoformat(),
            # O modelo do CONTRATO manda. O parâmetro existe para ele poder
            # VER os três antes de decidir qual cada contrato exige — e, por
            # isso mesmo, não grava nada: é espiada, não configuração.
            "modelo": modelo_valido(modelo or cab.get("modelo")),
            "modelo_da_obra": modelo_valido(cab.get("modelo")),
            "modelos": [{"valor": k, **v} for k, v in MODELOS.items()],
            "logos": logos(s, obra_id)}


# ===========================================================================
# PROCURAR UM DIÁRIO PELO QUE FOI ESCRITO (10/11/2026)
#
# Ele: *"será que dá para a gente pesquisar? Eu quero localizar um diário
# através de alguma informação que eu relatei."*
#
# Dá, e é a pergunta certa a fazer de um diário: ninguém lembra a DATA em que
# a laje do bloco B foi concretada — lembra que foi concretada. Sem busca, o
# diário vira um arquivo que só serve para quem já sabe onde olhar.
#
# ⚑ POR QUE `ILIKE` E NÃO BUSCA DE TEXTO DE VERDADE (`tsvector`): a busca
# completa do Postgres acha "concretagem" procurando "concretar", o que é
# melhor — mas exige índice próprio, migração, e ela IGNORA pedaço de palavra.
# Quem procura "P12" ou "bloco B" não quer raiz de palavra, quer o trecho. O
# `ILIKE` acha os dois, e com alguns milhares de diários ele é instantâneo. Se
# um dia a base crescer a ponto de pesar, aí vale o índice — e a troca é só
# aqui dentro.
#
# ⚑ ACENTO NÃO ATRAPALHA, e sem depender da extensão `unaccent` (que pode não
# estar instalada no banco): a comparação passa os dois lados por um
# `translate` que troca as vogais acentuadas pelas secas. Quem digita "ocorrencia"
# acha "ocorrência" — e é assim que se digita no celular, com pressa.
# ===========================================================================
CAMPOS_DA_BUSCA = (
    ("relato", "relato do dia"),
    ("ocorrencias", "ocorrências"),
    ("historico", "histórico dos serviços"),
    ("equipamentos", "equipamentos"),
    ("obs_fiscalizacao", "observação da fiscalização"),
    ("fiscalizacao_quem", "quem da fiscalização esteve"),
)
MINIMO_DA_BUSCA = 3
TETO_DA_BUSCA = 60
# O quanto do texto sai em volta da palavra achada.
VOLTA_DO_TRECHO = 90

_COM_ACENTO = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_SEM_ACENTO = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


# A LISTA DE EQUIPAMENTO TAMBÉM É PROCURÁVEL. Quando o equipamento virou
# lista (migração 097), procurar "retroescavadeira" deixaria de achar o dia em
# que ela esteve na obra — a informação saiu do campo de texto e foi para a
# tabela. Então a busca varre as duas coisas.
#
# ⚑ E varre SÓ SE A TABELA EXISTIR. O código sobe para o Render antes de
# alguém apertar "Aplicar atualizações do banco": uma busca que exige a tabela
# nova devolveria erro na cara de quem procura, por dias.
_CAMPO_EQUIPAMENTO_LISTA = "lista de equipamentos"


def _tem_equipamentos_em_tabela(s: Session) -> bool:
    return _consulta_opcional(
        s, "SELECT 1 FROM diario_equipamentos LIMIT 1") is not None


def _sem_acento(texto: Any) -> str:
    cru = unicodedata.normalize("NFD", str(texto or ""))
    return "".join(c for c in cru if unicodedata.category(c) != "Mn").lower()


def _trecho(texto: str, procurado: str) -> str:
    """O pedaço do texto em volta da palavra achada, com reticências.

    Devolver o relato inteiro faria a lista de resultados ter parágrafos; não
    devolver nada obrigaria a abrir cada dia para ver se era aquele. O trecho
    é o que responde "é este?" sem sair da lista.
    """
    inteiro = str(texto or "")
    onde = _sem_acento(inteiro).find(_sem_acento(procurado))
    if onde < 0:
        return inteiro[:VOLTA_DO_TRECHO * 2]
    comeco = max(0, onde - VOLTA_DO_TRECHO)
    fim = min(len(inteiro), onde + len(procurado) + VOLTA_DO_TRECHO)
    return (("…" if comeco else "") + inteiro[comeco:fim].strip()
            + ("…" if fim < len(inteiro) else ""))


def procurar(s: Session, texto: str, *, obra_id: Optional[int] = None,
             obra_ids: Optional[list[int]] = None,
             de: Optional[date] = None, ate: Optional[date] = None) -> dict[str, Any]:
    """Os diários que falam disto, do mais novo para o mais velho.

    `obra_ids` é o recorte de quem só alcança algumas obras — `None` quer
    dizer "todas". Lista vazia acha nada, que é o certo.
    """
    procurado = str(texto or "").strip()
    if len(procurado) < MINIMO_DA_BUSCA:
        return {"termo": procurado, "curto": True, "achados": []}

    traducao = (f"translate(lower({{coluna}}), '{_COM_ACENTO}', '{_SEM_ACENTO}')")
    # A lista de equipamento vira UMA coluna de texto ("1 Retroescavadeira
    # parada por manutenção · 2 Betoneira"), para ser procurada igual aos
    # campos escritos à mão.
    tem_lista = _tem_equipamentos_em_tabela(s)
    coluna_lista = ("""(SELECT string_agg(
                            q.descricao || ' ' || coalesce(q.observacao, ''),
                            ' · ' ORDER BY q.id)
                          FROM diario_equipamentos q
                         WHERE q.diario_id = d.id)""" if tem_lista else "''")
    alvos = [f"{traducao.format(coluna='d.' + campo)} LIKE :q"
             for campo, _rotulo in CAMPOS_DA_BUSCA]
    if tem_lista:
        coluna = f"coalesce({coluna_lista}, '')"
        alvos.append(f"{traducao.format(coluna=coluna)} LIKE :q")
    onde = " OR ".join(alvos)
    filtros = [f"({onde})"]
    params: dict[str, Any] = {"q": f"%{_sem_acento(procurado)}%"}
    if obra_id:
        filtros.append("d.obra_id = :obra")
        params["obra"] = obra_id
    elif obra_ids is not None:
        filtros.append("d.obra_id = ANY(:obras)")
        params["obras"] = list(obra_ids) or [-1]
    if de:
        filtros.append("d.data >= :de")
        params["de"] = de
    if ate:
        filtros.append("d.data <= :ate")
        params["ate"] = ate

    linhas = s.execute(text(f"""
        SELECT d.id, d.obra_id, d.data, d.situacao, d.sem_expediente,
               d.relato, d.ocorrencias, d.historico, d.equipamentos,
               d.obs_fiscalizacao, d.fiscalizacao_quem,
               {coluna_lista} AS equipamentos_lista_txt,
               o.codigo, o.nome
          FROM diarios_obra d
          JOIN obras o ON o.id = d.obra_id
         WHERE {' AND '.join(filtros)}
         ORDER BY d.data DESC
         LIMIT {TETO_DA_BUSCA + 1}"""), params).mappings().all()

    achados = []
    for l in linhas[:TETO_DA_BUSCA]:
        # ONDE ACHOU, e não só que achou: a mesma palavra no relato e na
        # observação da fiscalização querem dizer coisas diferentes.
        em = []
        for campo, rotulo in CAMPOS_DA_BUSCA:
            if _sem_acento(procurado) in _sem_acento(l[campo]):
                em.append({"campo": rotulo, "trecho": _trecho(l[campo], procurado)})
        lista_txt = l["equipamentos_lista_txt"] or ""
        if _sem_acento(procurado) in _sem_acento(lista_txt):
            em.append({"campo": _CAMPO_EQUIPAMENTO_LISTA,
                       "trecho": _trecho(lista_txt, procurado)})
        achados.append({
            "id": l["id"], "obra_id": l["obra_id"], "data": str(l["data"]),
            "dia_da_semana": _DIA_DA_SEMANA[l["data"].weekday()],
            "obra": rotulo_da_obra(l["codigo"], l["nome"]),
            "situacao": l["situacao"],
            "sem_expediente": bool(l["sem_expediente"]),
            "em": em,
        })
    return {"termo": procurado, "curto": False, "achados": achados,
            # "tem mais" é diferente de "são 60": a lista diz para apertar a
            # busca em vez de deixar a pessoa achando que viu tudo.
            "tem_mais": len(linhas) > TETO_DA_BUSCA}


_DIA_DA_SEMANA = ("segunda-feira", "terça-feira", "quarta-feira",
                  "quinta-feira", "sexta-feira", "sábado", "domingo")


# ---------------------------------------------------------------------------
# AS LOGOS — e por que elas vão DENTRO da folha, não por um endereço
#
# Ele: *"dentro do cadastro da empresa (…) colocar logo da empresa para ela
# ser colocada dentro do diário também. Tanto a logo da empresa, quanto outras
# logos (…) no caso, nós temos a logo da prefeitura."*
#
# ⚑ EMBUTIDAS COMO DADO, e isso resolve dois problemas de uma vez:
#
# 1. **Permissão.** A rota que serve a logo da empresa exige "configurar", que
#    o administrativo de obra não tem. Se o papel fosse buscar a imagem por
#    endereço, o cabeçalho sairia quebrado justamente para quem imprime o
#    diário todo dia. Aqui a folha já nasce com a imagem dentro, e a permissão
#    conferida é a do diário — a mesma do resto da página.
# 2. **O PDF.** Papel de diário é salvo em PDF e mandado por e-mail. Imagem
#    por endereço depende de sessão: o PDF abriria sem logo na mão de quem
#    recebeu, e a folha assinada ficaria diferente da arquivada.
#
# O TETO É O MESMO DO CADASTRO (`empresas.LIMITE_LOGO`), e isso importa: teto
# menor aqui daria logo aceita no cadastro e sumida na impressão, e quem
# descobriria é quem está com a folha na mão, na obra, na hora de entregar à
# fiscalização. Acima do teto sai sem logo — nunca uma página que não carrega.
# Há teste amarrando os dois números (tests/test_diario_de_obra.py); o número
# está repetido aqui, e não importado, para não atar a regra do diário ao
# cadastro de empresas por causa de uma constante.
#
# E a imagem entra UMA VEZ no documento, numa regra de estilo, não em cada
# <img>: relatório de três meses tem 90 folhas, e repetir a logo em cada uma
# faria um arquivo de dezenas de megabytes para mandar por e-mail.
# ---------------------------------------------------------------------------
TETO_DA_LOGO = 512 * 1024


def logos(s: Session, obra_id: int) -> dict[str, str]:
    """As duas logos do cabeçalho, já prontas para entrar na folha."""
    linha = s.execute(text("""
        SELECT o.contratante_logo, o.contratante_logo_mime,
               e.logo AS logo_empresa, e.logo_mime AS mime_empresa
          FROM obras o LEFT JOIN empresas e ON e.id = o.empresa_id
         WHERE o.id = :o"""), {"o": obra_id}).mappings().first()
    if linha is None:
        return {"empresa": "", "contratante": ""}
    return {
        "empresa": _embutir(linha["logo_empresa"], linha["mime_empresa"]),
        "contratante": _embutir(linha["contratante_logo"],
                                linha["contratante_logo_mime"]),
    }


def _embutir(dados: Optional[bytes], mime: Optional[str]) -> str:
    if not dados or len(dados) > TETO_DA_LOGO:
        if dados:
            logger.warning("ERP/diário: logo de %s bytes acima do teto — "
                           "a folha sai sem ela", len(dados))
        return ""
    import base64
    tipo = (mime or "image/png").strip() or "image/png"
    return f"data:{tipo};base64,{base64.b64encode(bytes(dados)).decode()}"


def importado_conta(efetivo: dict[str, Any],
                    importado: Optional[list[dict[str, Any]]]) -> bool:
    """O efetivo vindo da planilha entra na soma? Só se o ponto não tiver nada.

    ⚑ ELE ACHOU O DEFEITO OLHANDO O NÚMERO (13/11/2026): *"eu acho que ele
    está dobrando o efetivo (…) está dando um valor acima do que é o real. E
    não é nem dobrando, não."*

    Era isto. Os 1.427 diários da planilha antiga trazem o efetivo do dia em
    `diario_efetivo_extra` (origem IMPORTADO). Quando o MESMO dia também tem
    batida de ponto — e tem, porque a planilha vai até datas em que o ponto já
    rodava —, o diário somava **os dois registros da mesma gente**: 17 da
    planilha + 9 do ponto = 26 pessoas numa obra que teve 9. Não dava o dobro
    exato justamente porque são contagens diferentes do mesmo dia.

    A REGRA, e ela é de documento, não de tela: quando há batida, **o ponto
    manda** — é a fonte com nome, hora e foto. A lista da planilha continua
    aparecendo, dita como o que veio da planilha, mas FORA da soma. Sem
    batida (o caso dos anos anteriores), a planilha é o único registro que
    existe e conta inteira, como sempre contou.
    """
    return not bool(efetivo.get("por_funcao")) and bool(importado)


def _efetivo_para_o_papel(efetivo: dict[str, Any],
                          extras: list[dict[str, Any]],
                          importado: Optional[list[dict[str, Any]]] = None
                          ) -> dict[str, Any]:
    """Função e quantidade. O próprio e o terceirizado em linhas separadas —
    quem lê o RDO precisa distinguir mão de obra própria de contratada.

    ⚑ `importado` conta como PRÓPRIO **só quando o ponto não tem ninguém**.
    Num diário vindo da planilha antiga ele é o efetivo da obra naquele dia, e
    sem ele o papel de 1.380 dias sairia declarando zero pessoa em obra. Mas
    quando o dia TAMBÉM tem batida, os dois são registros da mesma gente, e
    somá-los inflava o efetivo — foi o que ele viu em 13/11/2026. Ver
    `importado_conta`.
    """
    proprio = [{"funcao": f["funcao"], "quantidade": f["quantidade"],
                "tipo": "próprio"} for f in efetivo.get("por_funcao", [])]
    soma_importado = importado_conta(efetivo, importado)
    if soma_importado:
        proprio += [{"funcao": f["funcao"], "quantidade": f["quantidade"],
                     "tipo": "próprio"} for f in (importado or [])]
    de_fora = [{"funcao": (e["funcao"] or e["descricao"]),
                "quantidade": e["quantidade"], "tipo": "terceirizado",
                "empresa": e.get("empresa") or e["descricao"]} for e in extras]
    return {
        "linhas": proprio + de_fora,
        "total_proprio": sum(f["quantidade"] for f in proprio),
        "total_terceirizado": sum(f["quantidade"] for f in de_fora),
        "total": (sum(f["quantidade"] for f in proprio)
                  + sum(f["quantidade"] for f in de_fora)),
        # A lista da planilha que FICOU DE FORA da soma, para o papel poder
        # dizer que ela existe. Calar seria esconder registro de obra.
        "da_planilha_fora_da_soma": ([] if soma_importado else (importado or [])),
        "indisponivel": efetivo.get("indisponivel"),
    }


def _correcoes(s: Session, diario_id: int) -> list[dict[str, Any]]:
    """As correções feitas depois do fechamento SAEM NO PAPEL.

    Esconder a correção seria o mesmo que não registrá-la: o valor do diário
    como documento está em o leitor poder ver que algo mudou, quando e por quê.
    """
    return [{"campo": l["campo"], "motivo": l["motivo"],
             "quando": str(l["criado_em"])[:16]}
            for l in s.execute(text("""
        SELECT campo, motivo, criado_em FROM diario_correcoes
         WHERE diario_id = :i ORDER BY criado_em"""),
        {"i": diario_id}).mappings().all()]
