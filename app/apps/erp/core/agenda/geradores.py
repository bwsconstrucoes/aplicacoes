# ============================================================================
# ERP — core/agenda/geradores.py
# Quem sabe o que tem data, e quando avisar.
#
# CADA GERADOR RESPONDE UMA PERGUNTA SÓ, e devolve eventos com CHAVE ESTÁVEL.
# A chave é o que permite rodar a sincronização todo dia sem empilhar avisos
# iguais — e o que permite APAGAR o aviso que deixou de valer.
#
# A REGRA QUE ATRAVESSA OS QUATRO: avisar com antecedência ÚTIL, não com
# antecedência bonita. Certidão avisada no dia do vencimento já é problema;
# reajuste avisado no dia do aniversário perdeu o mês de faturamento. Por isso
# cada gerador tem o seu prazo, escrito e justificado, em vez de um número
# único para tudo.
#
# E NENHUM GERADOR ESCREVE NO BANCO. Eles leem e descrevem; quem grava é o
# `service.sincronizar`. Assim dá para testar cada um sozinho, e dá para a tela
# mostrar "o que apareceria" sem sujar nada.
#
# OS GERADORES DESCREVEM O CALENDÁRIO INTEIRO, não só o que é urgente hoje.
# Cada evento carrega `avisar_em`, e é a LEITURA que decide o que aparece. A
# primeira versão fazia o filtro aqui — e o resultado é que a opção "mostrar o
# que ainda não é hora" não mostrava nada, porque o que ainda não era hora nem
# chegava a existir. Descrever tudo e filtrar na saída é o que transforma isto
# num calendário em vez de uma caixa de alarmes.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao

logger = logging.getLogger(__name__)

# Antecedências, em dias. Cada uma tem um motivo diferente:
AVISO_REAJUSTE = 45      # dá tempo de juntar índice, calcular e protocolar
AVISO_CONTRATO = 60      # aditivo de prazo não se pede na véspera
AVISO_CERTIDAO = 30      # é o padrão quando o tipo não disser outro
AVISO_LOCACAO = 0        # a conferência do mês é do mês; não se antecipa


def _mes(d: date) -> date:
    return d.replace(day=1)


def _somar_meses(d: date, meses: int) -> date:
    """Soma meses preservando o dia quando possível.

    Data-base em 31/01 com periodicidade de 12 meses cai em 31/01 — mas em
    31/03 + 1 mês não existe 31/04. Encurtar para o último dia do mês é o que
    todo contrato entende por "mesma data do mês seguinte".
    """
    total = d.month - 1 + meses
    ano, mes = d.year + total // 12, total % 12 + 1
    dia = d.day
    while dia > 1:
        try:
            return date(ano, mes, dia)
        except ValueError:
            dia -= 1
    return date(ano, mes, 1)


def _evento(*, chave: str, origem: str, titulo: str, quando: date,
            avisar_dias: int, detalhe: str = "", obra_id: Optional[int] = None,
            empresa_id: Optional[int] = None, link: str = "") -> dict[str, Any]:
    return {"chave": chave, "origem": origem, "titulo": titulo,
            "detalhe": detalhe, "quando": quando,
            "avisar_em": quando - timedelta(days=avisar_dias),
            "obra_id": obra_id, "empresa_id": empresa_id, "link": link}


# ---------------------------------------------------------------------------
# 1. O aniversário do reajuste
# ---------------------------------------------------------------------------
def reajustes(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """Quando cada contrato faz aniversário de reajuste — e o que já venceu.

    São DOIS avisos diferentes, e confundi-los custaria dinheiro:

      - o ANIVERSÁRIO que vem: hora de conferir se o índice está em dia e
        preparar o pedido;
      - o reajuste JÁ DEVIDO e ainda não pedido: aqui o dinheiro está na mesa
        e ninguém pegou.
    """
    from app.apps.erp.core.indices import reajuste as svc
    from app.apps.erp.db.models.cadastros import Contrato, Obra

    hoje = hoje or date.today()
    saida: list[dict[str, Any]] = []
    for c in s.scalars(select(Contrato).where(Contrato.status == "VIGENTE")).all():
        try:
            cfg = svc.configuracao(s, c)
        except Exception:                                  # pragma: no cover
            continue
        if not cfg["configurado"]:
            continue
        obra = s.get(Obra, c.obra_id) if c.obra_id else None
        onde = getattr(obra, "codigo", "") or c.objeto or f"contrato {c.id}"

        # ---- o próximo aniversário, contado da data-base
        proximo, n = cfg["data_base"], 0
        while proximo <= hoje:
            n += 1
            proximo = _somar_meses(cfg["data_base"], cfg["meses"] * n)
        # Contrato encerrado não faz aniversário útil: reajustar depois do fim
        # da vigência é discussão, não rotina.
        if c.vigencia_fim and proximo > c.vigencia_fim:
            continue
        de_onde = (f" ({cfg['data_base_origem'].lower()})"
                   if cfg["data_base_origem"] else "")
        saida.append(_evento(
            chave=f"REAJUSTE:contrato={c.id}:{proximo.isoformat()}",
            origem="REAJUSTE",
            titulo=f"Aniversário de reajuste — {onde}",
            detalhe=(f"{cfg['indice']}, data-base "
                     f"{cfg['data_base'].strftime('%d/%m/%Y')}{de_onde}. "
                     f"Confira se a tabela do índice está em dia antes de calcular."),
            quando=proximo, avisar_dias=AVISO_REAJUSTE,
            obra_id=c.obra_id, link=f"/erp/contratos#{c.id}"))

        # ---- o que já tem direito e ainda não virou título
        try:
            p = svc.previsao_do_contrato(s, c.id)
        except Exception:                                  # pragma: no cover
            continue
        if p["medicoes_a_gerar"]:
            saida.append(_evento(
                chave=f"REAJUSTE-DEVIDO:contrato={c.id}",
                origem="REAJUSTE",
                titulo=f"Reajuste a pedir — {onde}",
                detalhe=(f"As medições {', '.join(p['medicoes_a_gerar'])} já têm "
                         f"direito a reajuste e ainda não viraram título. "
                         f"Previsto: R$ {p['a_gerar']:,.2f}"
                         .replace(",", "@").replace(".", ",").replace("@", ".")),
                quando=hoje, avisar_dias=0,
                obra_id=c.obra_id, link=f"/erp/contratos#{c.id}"))
    return saida


# ---------------------------------------------------------------------------
# 2. Certidão vencendo
# ---------------------------------------------------------------------------
def certidoes(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """Certidão e documento com validade — o prazo vem do TIPO.

    O catálogo de tipos já traz `vence` e `avisar_dias` desde a migração 045.
    Usar o prazo do tipo em vez de um número fixo importa: certidão federal se
    tira no mesmo dia, alvará leva semanas, e avisar os dois com trinta dias
    trata como igual o que não é.

    Só a certidão MAIS NOVA de cada tipo/dono conta. A anterior vencida é
    histórico — avisar sobre ela seria avisar sobre um problema já resolvido.
    """
    from app.apps.erp.db.models.cadastros import Empresa, Obra
    from app.apps.erp.db.models.financeiro import Documento, DocumentoTipo

    hoje = hoje or date.today()
    tipos = {t.codigo: t for t in s.scalars(select(DocumentoTipo).where(
        DocumentoTipo.vence.is_(True))).all()}
    if not tipos:
        return []

    # a mais nova de cada (tipo, dono)
    melhor: dict[tuple, Documento] = {}
    for d in s.scalars(select(Documento).where(
            Documento.tipo_codigo.in_(list(tipos)),
            Documento.validade.is_not(None))).all():
        dono = (d.empresa_id, d.obra_id, d.colaborador_id, d.fornecedor_id)
        atual = melhor.get((d.tipo_codigo, dono))
        if atual is None or (d.validade or date.min) > (atual.validade or date.min):
            melhor[(d.tipo_codigo, dono)] = d

    saida = []
    for (codigo, _), doc in melhor.items():
        tipo = tipos[codigo]
        dias = tipo.avisar_dias or AVISO_CERTIDAO
        obra = s.get(Obra, doc.obra_id) if doc.obra_id else None
        empresa = s.get(Empresa, doc.empresa_id) if doc.empresa_id else None
        de_quem = (getattr(obra, "codigo", "")
                   or getattr(empresa, "nome_fantasia", "")
                   or getattr(empresa, "razao_social", "") or "a empresa")
        vencida = doc.validade < hoje
        saida.append(_evento(
            chave=f"CERTIDAO:documento={doc.id}",
            origem="CERTIDAO",
            titulo=(f"{'VENCIDA' if vencida else 'Vence'}: {tipo.nome} — {de_quem}"),
            detalhe=(f"Validade {doc.validade.strftime('%d/%m/%Y')}. "
                     + ("Documento vencido: renove e arquive o novo — o bloco "
                        "que o cliente pede na medição sai incompleto sem ele."
                        if vencida else
                        f"Avisado {dias} dias antes, como o tipo pede.")),
            quando=doc.validade, avisar_dias=dias,
            obra_id=doc.obra_id, empresa_id=doc.empresa_id,
            link="/erp/arquivo"))
    return saida


# ---------------------------------------------------------------------------
# 3. Conferência mensal dos equipamentos locados
# ---------------------------------------------------------------------------
def locacoes(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """A conferência do mês que a obra ainda não respondeu.

    Ela já existe desde a migração 039 e já tem dono com nome. O que faltava
    era aparecer fora da tela de locações — que é justamente a tela que quem
    precisa responder não abre.
    """
    from app.apps.erp.db.models.cadastros import Obra
    from app.apps.erp.db.models.financeiro import LocacaoConferencia

    hoje = hoje or date.today()
    saida = []
    for c in s.scalars(select(LocacaoConferencia).where(
            LocacaoConferencia.situacao == "ABERTA")).all():
        obra = s.get(Obra, c.obra_id) if c.obra_id else None
        # Vence no fim do mês da competência: a resposta é sobre aquele mês.
        fim = _somar_meses(_mes(c.competencia), 1) - timedelta(days=1)
        saida.append(_evento(
            chave=f"LOCACAO:conferencia={c.id}",
            origem="LOCACAO",
            titulo=(f"Conferir equipamentos locados — "
                    f"{getattr(obra, 'codigo', '') or 'obra não informada'}"),
            detalhe=(f"Competência {c.competencia.strftime('%m/%Y')}. Onde está "
                     f"cada equipamento, e o que já pode voltar. Equipamento "
                     f"esquecido na obra continua sendo cobrado todo mês."),
            quando=fim, avisar_dias=AVISO_LOCACAO,
            obra_id=c.obra_id, link="/erp/locacoes"))
    return saida


# ---------------------------------------------------------------------------
# 4. Fim da vigência do contrato
# ---------------------------------------------------------------------------
def contratos(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """Contrato chegando ao fim da vigência.

    Aditivo de prazo não se pede na véspera: quando o contrato vence sem
    aditivo, a obra para e a medição do mês não tem onde entrar.
    """
    from app.apps.erp.db.models.cadastros import Contrato, Obra

    hoje = hoje or date.today()
    saida = []
    for c in s.scalars(select(Contrato).where(
            Contrato.status == "VIGENTE",
            Contrato.vigencia_fim.is_not(None))).all():
        obra = s.get(Obra, c.obra_id) if c.obra_id else None
        saida.append(_evento(
            chave=f"CONTRATO:fim={c.id}:{c.vigencia_fim.isoformat()}",
            origem="CONTRATO",
            titulo=(f"{'VENCIDO' if c.vigencia_fim < hoje else 'Vigência termina'}: "
                    f"{getattr(obra, 'codigo', '') or c.objeto}"),
            detalhe=(f"Vigência até {c.vigencia_fim.strftime('%d/%m/%Y')}. "
                     f"Se a obra continua, o aditivo de prazo precisa estar "
                     f"assinado antes — depois do fim, a medição do mês não "
                     f"tem onde entrar."),
            quando=c.vigencia_fim, avisar_dias=AVISO_CONTRATO,
            obra_id=c.obra_id, link=f"/erp/contratos#{c.id}"))
    return saida


# ---------------------------------------------------------------------------
# 5. Certificado digital vencendo
# ---------------------------------------------------------------------------
def certificados(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """O A1 da empresa perto de vencer.

    Era o quarto aviso que a agenda prometia e não tinha de onde tirar: o
    certificado não morava no sistema. Agora mora, cifrado, e a validade vem
    lida de dentro do arquivo — então o aviso não depende de ninguém ter
    digitado a data certa.

    Sem certificado válido a nota de serviço não é assinada, e a obra para de
    faturar. Por isso o prazo é largo: certificado se renova com a contadora,
    e isso leva dias.
    """
    from app.apps.erp.core.cadastros import certificado as svc

    hoje = hoje or date.today()
    saida = []
    for c in svc.vencendo(s, dias=svc.AVISO_DIAS, hoje=hoje):
        vencido = c["dias"] < 0
        saida.append(_evento(
            chave=f"CERTIFICADO:empresa={c['empresa_id']}:{c['valido_ate'].isoformat()}",
            origem="CERTIFICADO",
            titulo=(f"{'VENCIDO' if vencido else 'Vence'}: certificado digital — "
                    f"{c['empresa']}"),
            detalhe=(f"Válido até {c['valido_ate'].strftime('%d/%m/%Y')}"
                     + (". Sem certificado válido a nota de serviço não é "
                        "assinada — a emissão para." if vencido else
                        ". Peça a renovação à contadora: leva dias, e sem ele "
                        "a emissão para.")),
            quando=c["valido_ate"], avisar_dias=svc.AVISO_DIAS,
            empresa_id=c["empresa_id"], link="/erp/empresas"))
    return saida


# ---------------------------------------------------------------------------
# 6. O bloco que vai sair incompleto (migração 057)
# ---------------------------------------------------------------------------
# Quantas competências para trás olhar. Duas: o mês que fechou e o anterior —
# a documentação fiscal de um mês costuma ficar pronta no seguinte, e cobrar
# no dia 1º só ensinaria a equipe a ignorar o aviso.
COMPETENCIAS_ATRAS = 2
# Quantos dias antes avisar. Bloco incompleto não tem "data de vencimento":
# a data é a próxima medição, que ninguém sabe. Trinta dias é o prazo em que
# ainda dá para tirar certidão e emitir guia.
AVISO_BLOCO = 30


def documentos(s: Session, hoje: Optional[date] = None) -> list[dict[str, Any]]:
    """O bloco obrigatório que sairia incompleto se pedissem hoje.

    O gerador das certidões avisa sobre documento que VAI VENCER. Este avisa
    sobre o outro lado, que é o que faz perder licitação e atrasar medição: o
    documento que NUNCA FOI ARQUIVADO.

    A diferença importa. Certidão vencida pelo menos existe. O documento que
    nunca entrou é silêncio — ninguém repara na ausência até o dia em que o
    cliente pede a pasta da medição e ela sai pela metade.

    Percorre obra por obra e empresa por empresa, o que não seria aceitável na
    frente de quem abre a tela. Desde a migração 055 o recálculo da agenda
    roda em segundo plano, e por isso este gerador pode existir.
    """
    from app.apps.erp.core.arquivo import blocos
    from app.apps.erp.db.models.cadastros import Empresa, Obra

    hoje = hoje or date.today()
    saida: list[dict[str, Any]] = []

    # ---- o que o cliente pede junto com a medição, por obra em execução
    obras = [o for o in s.scalars(select(Obra)).all()
             if (getattr(o, "fase", "") or "").upper() == "EM_EXECUCAO"]
    for obra in obras:
        for atras in range(1, COMPETENCIAS_ATRAS + 1):
            competencia = _somar_meses(_mes(hoje), -atras)
            try:
                # `ver_tudo`: quem confere aqui é o sistema, não uma pessoa.
                # Sem isso a pasta fiscal — quase toda restrita — pareceria
                # completa mesmo vazia.
                r = blocos.montar(s, "FISCAL", obra_id=obra.id,
                                  competencia=competencia, ver_tudo=True)
            except ErroValidacao:
                continue        # obra sem empresa, por exemplo — outro aviso cobra isso
            if r["completo"]:
                continue
            faltando = [f["nome"] for f in r["faltas"] if f["obrigatorio"]]
            saida.append(_evento(
                chave=f"DOCUMENTO:fiscal={obra.id}:{competencia.strftime('%Y-%m')}",
                origem="DOCUMENTO",
                titulo=(f"Documentação fiscal incompleta: {obra.codigo} — "
                        f"{competencia.strftime('%m/%Y')}"),
                detalhe=(f"{r['faltas_obrigatorias']} documento(s) obrigatório(s) "
                         f"faltando: {', '.join(faltando[:6])}"
                         + ("…" if len(faltando) > 6 else "")
                         + ". É o que o cliente pede junto com a medição — sem "
                           "isso a medição volta ou o pagamento atrasa."),
                # A data é o fim do mês seguinte à competência: é quando, na
                # prática, a medição daquele mês já foi protocolada.
                quando=_somar_meses(competencia, 2) - timedelta(days=1),
                avisar_dias=AVISO_BLOCO,
                obra_id=obra.id, empresa_id=obra.empresa_id,
                link="/erp/arquivo"))

    # ---- a habilitação da empresa, que é o que o edital exige
    for empresa in s.scalars(select(Empresa).where(Empresa.ativo.is_(True))).all():
        try:
            r = blocos.montar(s, "HABILITACAO", empresa_id=empresa.id, ver_tudo=True)
        except ErroValidacao:
            continue
        if r["completo"]:
            continue
        faltando = [f["nome"] for f in r["faltas"] if f["obrigatorio"]]
        nome = empresa.nome_fantasia or empresa.razao_social
        saida.append(_evento(
            chave=f"DOCUMENTO:habilitacao={empresa.id}",
            origem="DOCUMENTO",
            titulo=f"Habilitação incompleta: {nome}",
            detalhe=(f"{r['faltas_obrigatorias']} documento(s) obrigatório(s) "
                     f"faltando: {', '.join(faltando[:6])}"
                     + ("…" if len(faltando) > 6 else "")
                     + ". Sem eles a empresa não entrega envelope de licitação."),
            # Sem data própria: hoje, porque a próxima licitação pode ser amanhã.
            quando=hoje, avisar_dias=AVISO_BLOCO,
            empresa_id=empresa.id, link="/erp/arquivo"))

    return saida


TODOS = (reajustes, certidoes, locacoes, contratos, certificados, documentos)
