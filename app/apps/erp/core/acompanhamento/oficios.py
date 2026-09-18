# ============================================================================
# ERP — core/acompanhamento/oficios.py
# O ofício que o sistema escreve, e a pessoa confere antes de gerar.
#
# PEDIDO DO DONO, junto com o módulo (17/09/2026): *"gerar ofícios, dar
# entrada, lançar alguma coisa que a gente protocolou"*.
#
# É o item mais barato de construir e o que mais economiza tempo, porque o
# texto de um ofício de aditivo é quase sempre o mesmo: o que muda são os dados
# que o ERP já tem — a obra, o número do contrato, o objeto, o órgão, o número
# do processo. Datilografar isso de novo a cada pedido é trabalho puro.
#
# TRÊS DECISÕES QUE VALEM CONHECER:
#
# 1. O TEXTO É SEMPRE EDITÁVEL ANTES DE GERAR. O modelo é ponto de partida, não
#    formulário fechado. Órgão tem mania, obra tem particularidade, e um modelo
#    que não se pode ajustar faz a pessoa voltar para o Word — que é pior do
#    que não ter modelo nenhum.
#
# 2. O CORPO FICA GUARDADO COMO FOI ENVIADO. Regerar a partir do modelo meses
#    depois daria outro texto, e aí o papel que está no órgão e o que está no
#    sistema divergiriam sem ninguém perceber.
#
# 3. A NUMERAÇÃO É POR EMPRESA E POR ANO, com restrição única no banco. Duas
#    pessoas gerando ao mesmo tempo num sistema sem essa trava produzem dois
#    "OF 012/2026" — e número de ofício repetido é o tipo de coisa que só
#    aparece quando o órgão reclama.
# ============================================================================
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import (
    Contrato, Empresa, Obra, Processo, ProcessoOficio, Usuario,
)

logger = logging.getLogger(__name__)

MESES = ("janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro")

# O miolo de cada tipo. O cabeçalho, o fecho e a identificação da obra são
# iguais para todos e entram em `montar`; aqui fica só o PEDIDO, que é o que
# muda de assunto para assunto.
#
# As chaves entre chaves são preenchidas com o que o ERP já sabe. O que ele
# não sabe fica como ____________ de propósito: espaço em branco na tela é
# pedido de atenção; um valor inventado passa despercebido.
CORPOS: dict[str, str] = {
    "ADITIVO_PRAZO": (
        "Vimos, por meio deste, solicitar ADITIVO DE PRAZO ao contrato em "
        "epígrafe, pelo período de ____________ dias, prorrogando a vigência "
        "até ____________.\n\n"
        "A prorrogação decorre de ____________, fato alheio à vontade desta "
        "contratada e devidamente registrado no diário de obra, o que impacta "
        "diretamente o cronograma físico pactuado.\n\n"
        "Seguem anexos a justificativa técnica e o cronograma reprogramado."),
    "ADITIVO_VALOR": (
        "Vimos, por meio deste, solicitar ADITIVO DE VALOR ao contrato em "
        "epígrafe, no montante de R$ ____________, correspondente a "
        "____________% do valor contratado.\n\n"
        "O acréscimo decorre de ____________, conforme planilha de quantitativos "
        "e memorial anexos, respeitados os limites do art. 125 da Lei "
        "14.133/2021.\n\n"
        "Colocamo-nos à disposição para os esclarecimentos que se fizerem "
        "necessários."),
    "APOSTILAMENTO": (
        "Vimos, por meio deste, solicitar o APOSTILAMENTO do contrato em "
        "epígrafe, para fins de reajuste contratual, tendo por base o índice "
        "____________ e a data-base de ____________.\n\n"
        "Segue anexa a memória de cálculo, com os números-índice inicial e "
        "final utilizados e o percentual apurado."),
    "LICENCA": (
        "Vimos, por meio deste, solicitar a emissão/renovação da licença "
        "____________ referente à obra em epígrafe.\n\n"
        "Seguem anexos os documentos exigidos e o comprovante de recolhimento "
        "da taxa correspondente."),
    "CERTIDAO": (
        "Vimos, por meio deste, solicitar a emissão da certidão ____________ "
        "em nome desta empresa, para fins de ____________."),
    "PROTOCOLO": (
        "Vimos, por meio deste, protocolar a medição nº ____________, "
        "referente ao período de ____________ a ____________, no valor de "
        "R$ ____________.\n\n"
        "Seguem anexos o boletim de medição, as fotografias do período e a "
        "documentação fiscal exigida em contrato."),
}

CORPO_PADRAO = ("Vimos, por meio deste, solicitar ____________ referente ao "
                "contrato em epígrafe.\n\n"
                "Colocamo-nos à disposição para os esclarecimentos que se "
                "fizerem necessários.")


def _data_por_extenso(d: date) -> str:
    return f"{d.day} de {MESES[d.month - 1]} de {d.year}"


def _linha_da_empresa(e: Optional[Empresa]) -> str:
    if e is None:
        return ""
    partes = [e.razao_social]
    if e.cnpj:
        partes.append(f"CNPJ {e.cnpj}")
    endereco = ", ".join(p for p in (e.logradouro, e.numero, e.bairro) if p)
    if endereco:
        partes.append(endereco)
    cidade = " / ".join(p for p in (e.municipio, e.uf) if p)
    if cidade:
        partes.append(cidade)
    return " · ".join(partes)


def montar(s: Session, processo_id: int, *,
           hoje: Optional[date] = None) -> dict[str, Any]:
    """O rascunho do ofício, com o que o ERP já sabe preenchido.

    Não grava nada e não numera nada: numerar um rascunho que a pessoa vai
    descartar deixaria buracos na sequência, e buraco em sequência de ofício é
    pergunta que o órgão faz.
    """
    processo = s.get(Processo, processo_id)
    if processo is None:
        raise ErroValidacao("Processo não encontrado.")
    hoje = hoje or date.today()

    obra = s.get(Obra, processo.obra_id) if processo.obra_id else None
    empresa_id = processo.empresa_id or (obra.empresa_id if obra else None)
    empresa = s.get(Empresa, empresa_id) if empresa_id else None

    contrato = None
    if obra is not None:
        contratos = [c for c in s.scalars(select(Contrato)).all()
                     if getattr(c, "obra_id", None) == obra.id]
        contrato = contratos[0] if contratos else None

    epigrafe = []
    if obra is not None:
        if obra.contrato:
            epigrafe.append(f"Contrato nº {obra.contrato}")
        epigrafe.append(f"Obra: {obra.nome}")
        if obra.objeto and obra.objeto != obra.nome:
            epigrafe.append(f"Objeto: {obra.objeto}")
        if obra.municipio:
            epigrafe.append(f"Local: {obra.municipio}"
                            + (f"/{obra.uf}" if obra.uf else ""))
    if processo.protocolo:
        epigrafe.append(f"Processo administrativo nº {processo.protocolo}")

    corpo = "\n".join([
        *( [f"{linha}" for linha in epigrafe] if epigrafe else [] ),
        "" if epigrafe else "",
        CORPOS.get(processo.tipo, CORPO_PADRAO),
    ]).strip()

    return {
        "processo_id": processo.id,
        "numero_previsto": _proximo_numero(s, empresa_id, hoje.year),
        "empresa_id": empresa_id,
        "empresa": _linha_da_empresa(empresa),
        "destinatario": processo.orgao or "",
        "assunto": processo.assunto,
        "corpo": corpo,
        "local_e_data": (f"{(empresa.municipio if empresa else '') or 'Eusébio'}, "
                         f"{_data_por_extenso(hoje)}"),
        # Quem assina fica em branco de propósito: é escolha de quem envia, e
        # inventar o nome do sócio num documento oficial é pior que deixar
        # a linha para preencher à mão.
        "assinatura": "",
    }


def _proximo_numero(s: Session, empresa_id: Optional[int], ano: int) -> str:
    return f"OF {_proxima_sequencia(s, empresa_id, ano):03d}/{ano}"


def _proxima_sequencia(s: Session, empresa_id: Optional[int], ano: int) -> int:
    usadas = [o.sequencia for o in s.scalars(select(ProcessoOficio)).all()
              if o.ano == ano and (o.empresa_id or None) == (empresa_id or None)]
    return (max(usadas) + 1) if usadas else 1


def gerar(s: Session, processo_id: int, dados: dict[str, Any],
          usuario: Optional[Usuario],
          hoje: Optional[date] = None) -> ProcessoOficio:
    """Numera, monta o PDF, arquiva e pendura no processo — numa transação só.

    O andamento entra junto: um ofício que saiu e não aparece no histórico do
    processo é exatamente o tipo de coisa que faz alguém mandar o segundo.
    """
    from app.apps.erp.core.arquivo import service as arquivo

    processo = s.get(Processo, processo_id)
    if processo is None:
        raise ErroValidacao("Processo não encontrado.")
    corpo = (dados.get("corpo") or "").strip()
    if not corpo:
        raise ErroValidacao("O ofício está sem texto.")

    hoje = hoje or date.today()
    obra = s.get(Obra, processo.obra_id) if processo.obra_id else None
    empresa_id = (dados.get("empresa_id") or processo.empresa_id
                  or (obra.empresa_id if obra else None))
    empresa = s.get(Empresa, empresa_id) if empresa_id else None
    sequencia = _proxima_sequencia(s, empresa_id, hoje.year)
    numero = f"OF {sequencia:03d}/{hoje.year}"

    pdf = _pdf(numero=numero, empresa=_linha_da_empresa(empresa),
               destinatario=dados.get("destinatario") or processo.orgao or "",
               assunto=dados.get("assunto") or processo.assunto,
               corpo=corpo,
               local_e_data=dados.get("local_e_data") or "",
               assinatura=dados.get("assinatura") or "")

    oficio = ProcessoOficio(
        processo_id=processo.id, empresa_id=empresa_id, ano=hoje.year,
        sequencia=sequencia, numero=numero,
        destinatario=dados.get("destinatario") or processo.orgao,
        assunto=dados.get("assunto") or processo.assunto,
        corpo=corpo, criado_por=usuario.id if usuario else None)
    s.add(oficio)
    s.flush()

    # O PDF vai para o Arquivo de sempre, com o escopo e a busca de sempre.
    # Se não houver dono possível (processo sem obra e sem empresa), o ofício
    # existe assim mesmo: perder o número por causa do arquivamento seria o
    # rabo abanando o cachorro.
    nome = f"{numero.replace(' ', '-').replace('/', '-')}.pdf"
    falha_ao_arquivar = ""
    try:
        documento = arquivo.arquivar(
            s, pdf, nome, tipo_codigo="OFICIO",
            obra_id=obra.id if obra else None,
            empresa_id=None if obra else empresa_id,
            referencia=numero, emissao=hoje,
            resumo=f"{numero} — {oficio.assunto or ''}".strip(" —"),
            texto=corpo, origem="ERP", usuario=usuario)
        oficio.documento_id = documento.id
    except ErroValidacao as erro:
        # O ofício VALE mesmo sem arquivamento — perder o número por causa do
        # arquivo seria o rabo abanando o cachorro. Mas a falha NÃO pode ser
        # calada: um ofício que existe e não está no Arquivo é o que ninguém
        # acha no dia em que o órgão pergunta. Vai escrito no andamento, que é
        # onde quem toca o processo olha.
        falha_ao_arquivar = str(erro)
        logger.warning("ERP/acompanhamento: ofício %s não arquivado: %s",
                       numero, erro)

    from app.apps.erp.core.acompanhamento import processos as svc
    svc.lancar_andamento(s, processo.id, {
        "texto": f"Ofício {numero} gerado"
                 + (f" para {oficio.destinatario}." if oficio.destinatario else ".")
                 + (f" ATENÇÃO: não foi arquivado — {falha_ao_arquivar} "
                    f"Guarde o PDF à mão e avise quem cuida do sistema."
                    if falha_ao_arquivar else "")},
        usuario)

    registrar_evento(s, "processo", processo.id, "OFICIO_GERADO", {
        "numero": numero, "destinatario": oficio.destinatario,
        "documento_id": oficio.documento_id},
        usuario.id if usuario else None)
    return oficio


def _pdf(*, numero: str, empresa: str, destinatario: str, assunto: str,
         corpo: str, local_e_data: str, assinatura: str) -> bytes:
    """O papel. Sóbrio de propósito: ofício não é folheto."""
    from fpdf import FPDF

    from app.apps.erp.core.comum.exportar import _ascii_seguro

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.set_margins(25, 20, 25)
    pdf.add_page()
    largura = 210 - 50

    if empresa:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(11, 44, 92)
        pdf.multi_cell(largura, 4.5, _ascii_seguro(empresa),
                       new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(11, 44, 92)
        pdf.line(25, pdf.get_y() + 2, 185, pdf.get_y() + 2)
        pdf.ln(8)

    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "B", 12)
    pdf.multi_cell(largura, 6, _ascii_seguro(numero),
                   new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 11)
    if destinatario:
        pdf.multi_cell(largura, 5.5, _ascii_seguro(f"Ao(À) {destinatario}"),
                       new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)
    if assunto:
        pdf.set_font("Helvetica", "B", 11)
        pdf.multi_cell(largura, 5.5, _ascii_seguro(f"Assunto: {assunto}"),
                       new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    pdf.set_font("Helvetica", "", 11)
    for paragrafo in corpo.split("\n"):
        if paragrafo.strip():
            pdf.multi_cell(largura, 6, _ascii_seguro(paragrafo),
                           new_x="LMARGIN", new_y="NEXT")
        else:
            pdf.ln(3)
    pdf.ln(8)

    if local_e_data:
        pdf.multi_cell(largura, 6, _ascii_seguro(local_e_data),
                       new_x="LMARGIN", new_y="NEXT")
    pdf.ln(16)
    pdf.set_draw_color(120, 120, 120)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(90, 90, 90)
    pdf.multi_cell(largura, 4.5, _ascii_seguro(assinatura or "Assinatura"),
                   align="C", new_x="LMARGIN", new_y="NEXT")
    return bytes(pdf.output())


def listar_do_processo(s: Session, processo_id: int) -> list[dict[str, Any]]:
    """Os ofícios já expedidos, do mais novo para o mais antigo.

    Devolve `anexo_id` junto porque é por ele que a tela abre o PDF
    (`/erp/anexo/<id>`) — o `documento_id` é do catálogo, não do arquivo.
    """
    from app.apps.erp.db.models.financeiro import Documento

    oficios = sorted([o for o in s.scalars(select(ProcessoOficio)).all()
                      if o.processo_id == processo_id],
                     key=lambda o: (o.criado_em or date.today(), o.id or 0),
                     reverse=True)
    saida = []
    for o in oficios:
        anexo_id = None
        if o.documento_id:
            d = s.get(Documento, o.documento_id)
            anexo_id = getattr(d, "anexo_id", None) if d is not None else None
        saida.append({"id": o.id, "numero": o.numero,
                      "destinatario": o.destinatario, "assunto": o.assunto,
                      "documento_id": o.documento_id, "anexo_id": anexo_id,
                      "em": o.criado_em.strftime("%d/%m/%Y") if o.criado_em else ""})
    return saida
