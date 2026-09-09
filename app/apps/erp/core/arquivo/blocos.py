# ============================================================================
# ERP — core/arquivo/blocos.py
# O que sempre é pedido junto: baixar um conjunto de documentos de uma vez.
#
# A DÚVIDA DO DONO, em 09/09/2026: *"como é que esses blocos vão se associar a
# determinados documentos? Se isso é fácil de resolver."*
#
# É fácil, e a resposta é a decisão de desenho abaixo:
#
#     O BLOCO NÃO APONTA PARA DOCUMENTOS. ELE APONTA PARA TIPOS.
#
# Um bloco é uma lista de TIPOS mais um RECORTE ("desta obra, desta
# competência"). Na hora de baixar, o sistema procura, para cada tipo, o
# documento que casa com o recorte.
#
# Se o bloco apontasse para documentos, cada competência nova exigiria remontar
# o bloco à mão — que é exatamente o trabalho que a gestão de documentos veio
# eliminar. Apontando para tipos, o bloco fiscal de agosto e o de setembro são
# o MESMO bloco, com recortes diferentes.
#
# DUAS REGRAS QUE FAZEM O BLOCO SER ÚTIL EM VEZ DE ENGANOSO:
#
#   1. O ZIP TRAZ A LISTA DO QUE FALTA. Bloco que entrega oito de dez arquivos
#      calado é pior que bloco nenhum — quem monta o processo descobre a falta
#      na frente do cliente.
#   2. CERTIDÃO VENCIDA NÃO ENTRA. Ela vai para a lista de faltas, dizendo que
#      venceu e quando. Mandar certidão vencida é pior do que não mandar.
# ============================================================================
from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.arquivo import service as svc_arq
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.db.models.cadastros import Empresa, Obra, Usuario
from app.apps.erp.db.models.financeiro import (Documento, DocumentoBloco,
                                               DocumentoBlocoItem, DocumentoTipo)

logger = logging.getLogger(__name__)

RECORTES = ("EMPRESA", "EMPRESA_COMPETENCIA", "OBRA", "OBRA_COMPETENCIA")

# (código, nome, recorte, descrição, [(tipo, obrigatório), ...])
#
# O bloco FISCAL foi confirmado pelo dono em 09/09/2026 como "o que o cliente
# pede na medição". Repare que ele mistura documentos DA OBRA (folha, FGTS) com
# documentos DA EMPRESA (DCTFWeb, DARF) — e é justamente por isso que pedir o
# bloco de uma obra resolve os itens de empresa pela empresa DAQUELA obra.
PADRAO: tuple[tuple[Any, ...], ...] = (
    ("FISCAL", "Documentação fiscal da competência", "OBRA_COMPETENCIA",
     "O que o cliente pede junto com a medição.", (
         ("FOLHA", True), ("RELATORIO-FGTS", True), ("GUIA-FGTS", True),
         ("COMPROVANTE-FGTS", True), ("DCTFWEB-RECIBO", True),
         ("DCTFWEB-CREDITOS", True), ("DARF-INSS", True),
         ("COMPROVANTE-INSS", True), ("DARF-PIS-COFINS", False),
         ("COMPROVANTE-PIS-COFINS", False), ("ESOCIAL-RECIBO", False),
     )),
    ("HABILITACAO", "Habilitação para licitação", "EMPRESA",
     "O que o edital costuma exigir da empresa.", (
         ("CONTRATO-SOCIAL", True), ("CARTAO-CNPJ", True),
         ("CND-FEDERAL", True), ("CND-ESTADUAL", True), ("CND-MUNICIPAL", True),
         ("CRF-FGTS", True), ("CNDT", True), ("CND-FALENCIA", True),
         ("BALANCO", True), ("FATURAMENTO-12M", True),
         ("COMPROVANTE-ENDERECO", False), ("CERTIDAO-CREA", False),
         ("ATESTADO-CAPACIDADE", False), ("CAT", False),
     )),
    ("CADASTRO-FORNECEDOR", "Cadastro como fornecedor", "EMPRESA",
     "O que o cliente pede para cadastrar a BWS.", (
         ("CONTRATO-SOCIAL", True), ("CARTAO-CNPJ", True),
         ("FATURAMENTO-12M", True), ("CND-FEDERAL", False),
         ("CRF-FGTS", False), ("CNDT", False), ("COMPROVANTE-ENDERECO", False),
     )),
    ("OBRA", "Dossiê da obra", "OBRA",
     "Contrato, aditivos e documentação técnica.", (
         ("CONTRATO-OBRA", True), ("ADITIVO", False), ("OS", False),
         ("ART", True), ("MATRICULA-CEI-CNO", True), ("LICENCA", False),
         ("SEGURO", False),
     )),
    ("MEDICAO", "Medição e sua documentação", "OBRA_COMPETENCIA",
     "A medição, mais a documentação fiscal do mês.", (
         ("MEDICAO", True), ("FOLHA", True), ("RELATORIO-FGTS", True),
         ("GUIA-FGTS", True), ("COMPROVANTE-FGTS", True),
         ("DCTFWEB-RECIBO", True), ("DARF-INSS", True),
         ("COMPROVANTE-INSS", True),
     )),
)


def aplicar(s: Session) -> dict[str, int]:
    """Cria os blocos padrão. Não mexe em bloco que já existe.

    Diferente do catálogo de tipos, aqui NÃO se atualiza o que já está lá: o
    conteúdo de um bloco é decisão da BWS (o que o cliente dela pede), e
    sobrescrever isso apagaria ajuste feito por quem sabe.
    """
    existentes = {b.codigo for b in s.scalars(select(DocumentoBloco)).all()}
    tipos = {t.codigo for t in s.scalars(select(DocumentoTipo)).all()}
    criados = 0
    for ordem, (codigo, nome, recorte, descricao, itens) in enumerate(PADRAO, 1):
        if codigo in existentes:
            continue
        s.add(DocumentoBloco(codigo=codigo, nome=nome, recorte=recorte,
                             descricao=descricao, ordem=ordem))
        s.flush()
        for i, (tipo, obrigatorio) in enumerate(itens, 1):
            if tipo not in tipos:
                logger.warning("ERP/blocos: tipo %s não existe no catálogo", tipo)
                continue
            s.add(DocumentoBlocoItem(bloco_codigo=codigo, tipo_codigo=tipo,
                                     obrigatorio=obrigatorio, ordem=i))
        criados += 1
    s.flush()
    return {"criados": criados, "total": len(PADRAO)}


def listar(s: Session) -> list[dict[str, Any]]:
    saida = []
    for b in s.scalars(select(DocumentoBloco).where(DocumentoBloco.ativo.is_(True))
                       .order_by(DocumentoBloco.ordem)).all():
        saida.append({
            "codigo": b.codigo, "nome": b.nome, "descricao": b.descricao,
            "recorte": b.recorte,
            "precisa_obra": b.recorte.startswith("OBRA"),
            "precisa_empresa": b.recorte.startswith("EMPRESA"),
            "precisa_competencia": b.recorte.endswith("COMPETENCIA"),
            "itens": [{"tipo": i.tipo_codigo,
                       "nome": i.tipo.nome if i.tipo else i.tipo_codigo,
                       "obrigatorio": i.obrigatorio} for i in b.itens],
        })
    return saida


# ---------------------------------------------------------------------------
# Montar o bloco
# ---------------------------------------------------------------------------
def _achar(s: Session, tipo: DocumentoTipo, *, empresa_id: Optional[int],
           obra_id: Optional[int], competencia: Optional[date],
           hoje: date) -> tuple[list[Documento], Optional[str]]:
    """Os documentos deste tipo que casam com o recorte, e o motivo da falta.

    A escolha de qual coluna filtrar vem do DONO DO TIPO, não do recorte do
    bloco. É isso que faz o bloco fiscal de uma obra trazer também o recibo da
    DCTFWeb, que é da empresa: o tipo diz "sou de empresa", e o sistema usa a
    empresa DAQUELA obra.
    """
    stmt = select(Documento).where(Documento.tipo_codigo == tipo.codigo)
    if tipo.dono == "OBRA":
        if not obra_id:
            return [], "este bloco precisa de uma obra"
        stmt = stmt.where(Documento.obra_id == obra_id)
    elif tipo.dono == "EMPRESA":
        if not empresa_id:
            return [], "não sei de qual empresa é esta obra"
        stmt = stmt.where(Documento.empresa_id == empresa_id)
    else:
        return [], f"tipo de {tipo.dono.lower()} não entra em bloco por enquanto"

    if tipo.por_competencia:
        if competencia is None:
            return [], "este bloco precisa de uma competência"
        stmt = stmt.where(Documento.competencia == competencia)

    achados = list(s.scalars(stmt.order_by(Documento.id.desc())).all())
    if not achados:
        return [], "não está no arquivo"

    if tipo.vence:
        # O QUE VENCEU NÃO ENTRA. Mandar certidão vencida é pior do que não
        # mandar; e contrato fora de vigência no meio do processo confunde.
        validos = [d for d in achados if d.validade and d.validade >= hoje]
        if not validos:
            mais_nova = max((d.validade for d in achados if d.validade), default=None)
            quando = mais_nova.strftime("%d/%m/%Y") if mais_nova else "?"
            return [], f"VENCIDO em {quando} — precisa emitir de novo"
        validos.sort(key=lambda d: d.validade, reverse=True)
        # CERTIDÃO É UMA SÓ; ADITIVO SÃO TODOS.
        #
        # A diferença foi encontrada por um caso de teste, e ela é real: duas
        # certidões válidas do mesmo tipo acontecem (a nova é emitida antes de a
        # velha vencer) e mandar as duas confunde quem recebe — vai a de
        # validade mais longa. Já os aditivos de um contrato são vários DE
        # PROPÓSITO: mandar só o último esconderia o histórico do contrato.
        if tipo.grupo == "CERTIDAO":
            return [validos[0]], None
        return validos, None

    return achados, None


def montar(s: Session, codigo: str, *, empresa_id: Optional[int] = None,
           obra_id: Optional[int] = None, competencia: Optional[date] = None,
           usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """O que o bloco encontrou e o que faltou. Sem gerar arquivo ainda."""
    bloco = s.get(DocumentoBloco, (codigo or "").strip().upper())
    if bloco is None:
        raise ErroValidacao(f"Bloco desconhecido: {codigo}.")

    obra = s.get(Obra, obra_id) if obra_id else None
    if bloco.recorte.startswith("OBRA") and obra is None:
        raise ErroValidacao(f"O bloco {bloco.nome} é de uma obra — escolha qual.")
    if bloco.recorte.endswith("COMPETENCIA") and competencia is None:
        raise ErroValidacao(f"O bloco {bloco.nome} é de um mês — escolha a competência.")

    # A empresa vem da obra quando o bloco é de obra. É isto que faz os itens
    # de empresa (DCTFWeb, DARF) aparecerem no bloco fiscal de uma obra.
    if obra is not None and not empresa_id:
        empresa_id = obra.empresa_id
    empresa = s.get(Empresa, empresa_id) if empresa_id else None
    if bloco.recorte.startswith("EMPRESA") and empresa is None:
        raise ErroValidacao(f"O bloco {bloco.nome} é de uma empresa — escolha qual.")

    competencia = competencia.replace(day=1) if competencia else None
    hoje = date.today()
    faixas = svc_arq.sigilos_visiveis(usuario)

    encontrados, faltas = [], []
    for item in bloco.itens:
        tipo = item.tipo
        if tipo is None:
            continue
        if tipo.sigilo not in faixas:
            # Não é falta: é documento que esta pessoa não pode ver. Dizer
            # "está faltando" seria mentira, e dizer o nome do tipo já
            # entregaria informação que o sigilo existe para proteger.
            continue
        achados, motivo = _achar(s, tipo, empresa_id=empresa_id, obra_id=obra_id,
                                 competencia=competencia, hoje=hoje)
        if achados:
            encontrados.extend(achados)
        else:
            faltas.append({"tipo": tipo.codigo, "nome": tipo.nome,
                           "obrigatorio": item.obrigatorio,
                           "motivo": motivo or "não está no arquivo"})

    rotulo = _rotulo(bloco, empresa, obra, competencia)
    return {
        "bloco": bloco.codigo, "nome": bloco.nome, "rotulo": rotulo,
        "empresa": getattr(empresa, "nome_fantasia", None) or getattr(empresa, "razao_social", None),
        "obra": getattr(obra, "codigo", None),
        "competencia": competencia.strftime("%m/%Y") if competencia else None,
        "documentos": [svc_arq.ler(s, d, hoje) for d in encontrados],
        "faltas": faltas,
        "faltas_obrigatorias": sum(1 for f in faltas if f["obrigatorio"]),
        "completo": not any(f["obrigatorio"] for f in faltas),
    }


def _rotulo(bloco: DocumentoBloco, empresa, obra, competencia) -> str:
    """O nome do .zip — mesma disciplina do nome dos arquivos."""
    from app.apps.erp.core.arquivo import nomes
    partes = [bloco.codigo]
    if obra is not None:
        partes.append(nomes.apelido_da_obra(obra))
    elif empresa is not None:
        partes.append(nomes.apelido_da_empresa(empresa))
    if competencia is not None:
        partes.append(competencia.strftime("%Y-%m"))
    return "_".join(p for p in partes if p)


def _conferencia(resultado: dict[str, Any]) -> str:
    """O arquivo que vai DENTRO do zip dizendo o que veio e o que falta.

    É a parte mais importante do bloco. Sem ela, quem monta o processo descobre
    a falta na frente do cliente.
    """
    linhas = [
        f"CONFERÊNCIA — {resultado['nome']}",
        "=" * 60, "",
    ]
    if resultado.get("obra"):
        linhas.append(f"Obra:        {resultado['obra']}")
    if resultado.get("empresa"):
        linhas.append(f"Empresa:     {resultado['empresa']}")
    if resultado.get("competencia"):
        linhas.append(f"Competência: {resultado['competencia']}")
    linhas += [f"Gerado em:   {datetime.now():%d/%m/%Y %H:%M}", "",
               f"DOCUMENTOS NESTE ARQUIVO ({len(resultado['documentos'])})", "-" * 60]
    for d in resultado["documentos"]:
        linhas.append(f"  {d['nome']}")
        linhas.append(f"      {d['tipo_nome']}"
                      + (f" · vale até {d['validade'][8:10]}/{d['validade'][5:7]}/{d['validade'][:4]}"
                         if d.get("validade") else ""))
    if not resultado["documentos"]:
        linhas.append("  (nenhum)")

    linhas += ["", f"O QUE ESTÁ FALTANDO ({len(resultado['faltas'])})", "-" * 60]
    if not resultado["faltas"]:
        linhas.append("  Nada. O bloco está completo.")
    elif resultado["completo"]:
        # Distinguir isto importa: um bloco com só opcionais faltando ESTÁ
        # pronto para entregar, e quem lê precisa saber disso sem contar linha
        # por linha.
        linhas.append("  Nada OBRIGATÓRIO. O que segue é opcional:")
    for f in resultado["faltas"]:
        marca = "!! OBRIGATÓRIO" if f["obrigatorio"] else "   opcional   "
        linhas.append(f"  {marca}  {f['nome']}")
        linhas.append(f"                  ({f['motivo']})")

    if resultado["faltas_obrigatorias"]:
        linhas += ["", "=" * 60,
                   f"ATENÇÃO: {resultado['faltas_obrigatorias']} documento(s) "
                   f"obrigatório(s) não entraram neste arquivo.",
                   "Confira a lista acima antes de entregar.", "=" * 60]
    return "\n".join(linhas) + "\n"


def gerar_zip(s: Session, codigo: str, *, empresa_id: Optional[int] = None,
              obra_id: Optional[int] = None, competencia: Optional[date] = None,
              usuario: Optional[Usuario] = None) -> tuple[str, bytes, dict[str, Any]]:
    """Devolve (nome do arquivo, bytes do zip, o resultado da montagem)."""
    from app.apps.erp.core.documentos.armazenamento import conteudo_de
    from app.apps.erp.db.models.financeiro import Anexo

    resultado = montar(s, codigo, empresa_id=empresa_id, obra_id=obra_id,
                       competencia=competencia, usuario=usuario)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        # A conferência entra PRIMEIRO, para ser o primeiro nome que aparece
        # quando alguém abre o arquivo compactado.
        z.writestr("CONFERENCIA.txt", _conferencia(resultado).encode("utf-8"))
        usados: set[str] = set()
        for d in resultado["documentos"]:
            anexo = s.get(Anexo, d["anexo_id"])
            if anexo is None:
                continue
            try:
                dados = conteudo_de(s, anexo)
            except Exception as e:                      # noqa: BLE001
                logger.warning("ERP/blocos: %s não pôde ser lido — %s", d["nome"], e)
                continue
            nome = d["nome"]
            # Dois documentos do mesmo tipo (dois aditivos, por exemplo) podem
            # ter o mesmo nome; o zip não aceita repetido.
            if nome in usados:
                base, _, ext = nome.rpartition(".")
                n = 2
                while f"{base}-{n}.{ext}" in usados:
                    n += 1
                nome = f"{base}-{n}.{ext}"
            usados.add(nome)
            z.writestr(nome, dados)

    logger.info("ERP/blocos: %s com %d documento(s), %d falta(s)",
                resultado["rotulo"], len(resultado["documentos"]),
                len(resultado["faltas"]))
    return f"{resultado['rotulo']}.zip", buffer.getvalue(), resultado
