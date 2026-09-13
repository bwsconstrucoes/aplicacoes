# ============================================================================
# ERP — core/notas_emitidas/automatica.py
# A nota sai sozinha: o ERP declara à prefeitura e guarda o que ela devolve.
#
# Item 6 de `MEDICOES_E_NOTAS.md`. Estava preso a Petrolina; o dono tirou
# Petrolina da conta em 10/09/2026 e o que sobrou — a BWS no Eusébio — já tem
# tudo: inscrição municipal, token, controle de numeração e certificado A1.
#
# O QUE ACONTECE, EM ORDEM, E POR QUE NESTA ORDEM
#
#   1. CONFERE o cadastro inteiro ANTES de tocar em número de nota. Descobrir
#      no meio que falta o CNO obriga a queimar um número por um erro de
#      cadastro, e número de nota queimado não se apaga: se explica.
#   2. RESERVA o número da declaração. Ele é NOSSO — no padrão nacional quem
#      numera a DPS é quem emite; a prefeitura devolve o número da NOTA.
#   3. ASSINA e ENVIA. A assinatura usa o certificado A1 da empresa, guardado
#      cifrado (migração 053).
#   4. ESPERA a resposta e GRAVA: número da nota, chave de acesso e o XML,
#      que fica anexado ao título.
#   5. Se der errado, o número fica QUEIMADO com o motivo escrito, e não volta
#      para a fila. A prefeitura pode ter recebido a declaração e só a resposta
#      ter se perdido — reemitir com o mesmo número daria duplicidade do lado
#      dela, e aí o problema deixa de ser nosso e vira dela.
#
# POR QUE ISTO RODA NA FILA, E NÃO NO CLIQUE
#
# Entre enviar e a prefeitura responder passam-se de segundos a dois minutos.
# No clique, esse tempo seguraria uma das quatro linhas de atendimento do
# serviço — e o sistema inteiro ficaria pesado para todo mundo por causa de
# uma nota. Quem chama isto é o executor `emitir_nota` (migração 055).
#
# O QUE ESTE MÓDULO NÃO FAZ, e é decisão do dono (09/09/2026): não atualiza o
# Omie, não mexe em planilha, não move card no Pipefy. *"Isso tudo não vai ser
# necessário porque tudo vai ficar dentro do ERP."*
# ============================================================================
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import emissao as svc_emissao
from app.apps.erp.core.comum.auditoria import ErroValidacao
from app.apps.erp.core.notas_emitidas import numeracao
from app.apps.erp.core.titulos import tributacao
from app.apps.erp.db.models.cadastros import Empresa, Fornecedor, Obra, Usuario
from app.apps.erp.db.models.financeiro import NotaEmitida, Rateio, Titulo

logger = logging.getLogger(__name__)

# Códigos do padrão nacional para o que a BWS faz. Ficam aqui, nomeados, em vez
# de espalhados como números soltos no meio do XML.
COD_TRIB_NAC_EMPREITADA = "070202"      # 7.02.2 — empreitada e subempreitada
COD_NBS_EDIFICACOES = "101011100"       # 1.0101.11.00 — construção de edificações

# Quanto tempo esperar a prefeitura responder antes de desistir da espera. Ela
# NÃO desiste da emissão: a declaração já foi entregue, e o que sobra é
# consultar depois pelo identificador de processamento.
ESPERA_SEGUNDOS = 100


def _dec(v: Any) -> Decimal:
    if v in (None, ""):
        return Decimal("0.00")
    return Decimal(str(v)).quantize(Decimal("0.01"))


def _valor(v: Any) -> str:
    return f"{_dec(v):.2f}"


def _aliquota_iss(obra: Optional[Obra]) -> Any:
    """A alíquota de ISS da obra, da coluna que vale.

    A tabela carrega duas: `aliquota_iss_pct` — que a tributação, a tela de
    tributação e o cálculo da medição leem — e `aliquota_iss`, mais antiga.
    Esta emissão lia SÓ a antiga: obra cadastrada pela tela de tributação
    (onde todas são cadastradas hoje) era recusada por "sem alíquota de ISS",
    e obra com as duas preenchidas diferentes mandaria à prefeitura um
    percentual que a tela nunca mostrou. Vale a nova; a antiga só socorre obra
    que ainda não passou pela tela de tributação.
    """
    if obra is None:
        return None
    return (obra.aliquota_iss_pct
            if obra.aliquota_iss_pct not in (None, "") else obra.aliquota_iss)


def _obra_do_titulo(s: Session, titulo: Titulo) -> Optional[Obra]:
    obras = [r.obra_id for r in s.scalars(
        select(Rateio).where(Rateio.titulo_id == titulo.id)).all() if r.obra_id]
    return s.get(Obra, obras[0]) if obras else None


def _tomador(s: Session, titulo: Titulo, obra: Optional[Obra]) -> Optional[Fornecedor]:
    """Quem RECEBE o serviço. Num título a receber é o cliente da obra, que
    neste sistema mora no mesmo cadastro de fornecedores (marcado `e_cliente`).
    """
    if titulo.fornecedor_id:
        return s.get(Fornecedor, titulo.fornecedor_id)
    return None


# ---------------------------------------------------------------------------
# 1. A conferência — tudo que impede a nota de sair
# ---------------------------------------------------------------------------
def conferir(s: Session, titulo_id: int) -> dict[str, Any]:
    """O que falta para esta medição virar nota, em português.

    Roda ANTES de qualquer número ser tomado, e é o que a tela mostra: cadastro
    incompleto vira lista de pendências, não erro no meio da emissão.
    """
    faltas: list[str] = []
    titulo = s.get(Titulo, titulo_id)
    if titulo is None:
        raise ErroValidacao("Título não encontrado.")
    if not titulo.numero_medicao:
        raise ErroValidacao(
            "Este título não é uma medição — não tem número de medição. "
            "A nota de serviço sai da medição.")

    destino = svc_emissao.resolver(s, titulo_id=titulo.id)
    if not destino.get("empresa_id"):
        return {"pode": False, "faltas": [destino.get("motivo") or "Empresa não definida."],
                "destino": destino}

    empresa = s.get(Empresa, destino["empresa_id"])
    obra = _obra_do_titulo(s, titulo)
    tomador = _tomador(s, titulo, obra)

    if empresa.emissao_modo != "API":
        faltas.append(
            f"A empresa {empresa.nome_fantasia or empresa.razao_social} está "
            f"marcada para emissão MANUAL. Para a nota sair sozinha, mude o "
            f"modo em Administração › Empresas.")
    if (empresa.emissao_canal or "NACIONAL").upper() != "NACIONAL":
        faltas.append(
            f"O canal desta empresa está em {empresa.emissao_canal}. A emissão "
            f"automática do ERP fala o padrão NACIONAL, que é o obrigatório "
            f"desde a LC 214/2025.")
    pode_cfg, falta_cfg = svc_emissao.pode_emitir(empresa)
    faltas.extend(falta_cfg)

    # ---- certificado
    try:
        from app.apps.erp.core.cadastros.certificado import material_para_assinar
        material_para_assinar(s, empresa.id)
    except Exception as e:
        faltas.append(str(e))

    # ---- obra
    if obra is None:
        faltas.append("O título não está ligado a nenhuma obra.")
    else:
        if not obra.codigo_ibge:
            faltas.append(
                f"A obra {obra.codigo} está sem o código IBGE do município. É "
                f"ele que diz à prefeitura ONDE o serviço foi prestado — e o "
                f"ISS é devido no local da obra, não no da empresa.")
        if not obra.cno:
            faltas.append(f"A obra {obra.codigo} está sem CNO. A nota de obra "
                          f"exige a matrícula.")
        if _aliquota_iss(obra) in (None, ""):
            faltas.append(f"A obra {obra.codigo} está sem alíquota de ISS.")

    # ---- tomador
    if tomador is None:
        faltas.append("O título está sem cliente — sem tomador não há nota.")
    else:
        sem = [rotulo for campo, rotulo in (
            ("cnpj_cpf", "CNPJ/CPF"), ("razao_social", "razão social"),
            ("cep", "CEP"), ("logradouro", "logradouro"), ("numero", "número"),
            ("bairro", "bairro"), ("codigo_ibge", "código IBGE do município"))
            if not getattr(tomador, campo, None)]
        if sem:
            faltas.append(
                f"O cliente {tomador.razao_social} está sem {', '.join(sem)}. "
                f"A declaração exige o endereço de quem recebe o serviço — "
                f"use “Buscar na Receita” no cadastro dele.")

    # ---- já emitida?
    ja = s.scalars(select(NotaEmitida).where(
        NotaEmitida.titulo_id == titulo.id,
        NotaEmitida.situacao == "EMITIDA")).all()

    return {
        "pode": not faltas,
        "faltas": faltas,
        "destino": destino,
        "ambiente": empresa.emissao_ambiente,
        "serie": empresa.emissao_serie or "1",
        "proximo_numero": numeracao.proximo_numero(s, empresa),
        "ja_emitidas": [{"id": n.id, "numero_nota": n.numero_nota,
                         "valor": float(n.valor_bruto or 0)} for n in ja],
    }


# ---------------------------------------------------------------------------
# 2. O que vai na declaração
# ---------------------------------------------------------------------------
def _discriminacao(titulo: Titulo, obra: Optional[Obra], observacao: str) -> str:
    partes = [f"MEDICAO {titulo.numero_medicao}" if titulo.numero_medicao else "",
              (titulo.descricao or "").strip()]
    if obra is not None:
        if obra.objeto:
            partes.append(str(obra.objeto).strip())
        if obra.contrato:
            partes.append(f"CONTRATO {obra.contrato}")
        if obra.cno:
            partes.append(f"CNO {obra.cno}")
    if observacao.strip():
        partes.append(observacao.strip())
    return ". ".join(p for p in partes if p)[:2000].upper()


def montar_declaracao(s: Session, titulo_id: int, *, numero_dps: int,
                      valor: Any = None, observacao: str = "") -> Any:
    """Monta os dados da DPS a partir do cadastro. Não envia nada.

    Separado do envio de propósito: é o que a suíte consegue provar sem
    encostar na prefeitura, e é o que erra na prática — código de município
    trocado, alíquota errada, retenção fora do lugar.
    """
    from app.apps.emissaonf.el_nfse_nacional import DadosDPS

    titulo = s.get(Titulo, titulo_id)
    obra = _obra_do_titulo(s, titulo)
    tomador = _tomador(s, titulo, obra)
    destino = svc_emissao.resolver(s, titulo_id=titulo.id)
    empresa = s.get(Empresa, destino["empresa_id"])

    bruto = _dec(valor) if valor not in (None, "") else _dec(titulo.valor_liquido)
    if bruto <= 0:
        raise ErroValidacao("O valor da nota tem de ser maior que zero.")
    calculo = tributacao.calcular(obra, bruto)

    def _retido(tipo: str) -> str:
        return _valor(sum((r.valor for r in calculo.retencoes if r.tipo == tipo),
                          Decimal("0.00")))

    agora = datetime.now(timezone.utc).astimezone()
    competencia = titulo.competencia or date.today().replace(day=1)

    return DadosDPS(
        serie=int((empresa.emissao_serie or "1").strip() or 1),
        n_dps=numero_dps,
        dh_emi=agora.replace(microsecond=0).isoformat(),
        d_compet=competencia.replace(day=1).isoformat(),
        tp_amb=(1 if (empresa.emissao_ambiente or "").upper() == "PRODUCAO" else 2),
        # ---- prestador: a EMPRESA da obra, nunca uma constante
        prest_cnpj=empresa.cnpj or "",
        prest_im=empresa.inscricao_municipal or "",
        c_loc_emi=int(empresa.emissao_codigo_ibge or 0),
        # ---- tomador
        toma_doc=(tomador.cnpj_cpf if tomador else ""),
        toma_nome=(tomador.razao_social if tomador else ""),
        toma_cmun=int((tomador.codigo_ibge if tomador else "0") or 0),
        toma_cep=(tomador.cep if tomador else ""),
        toma_lgr=(tomador.logradouro if tomador else ""),
        toma_nro=(tomador.numero if tomador else ""),
        toma_bairro=(tomador.bairro if tomador else ""),
        # ---- serviço: o ISS é devido no local da OBRA, não no da empresa
        c_loc_prestacao=int((obra.codigo_ibge if obra else "0") or 0),
        c_trib_nac=COD_TRIB_NAC_EMPREITADA,
        c_nbs=COD_NBS_EDIFICACOES,
        c_int_contrib=(empresa.emissao_codigo_servico or ""),
        x_desc_serv=_discriminacao(titulo, obra, observacao),
        # ---- valores e retenções, do mesmo cálculo que a tela mostra
        v_serv=_valor(bruto),
        p_aliq=_valor(_aliquota_iss(obra) if obra else 0),
        tp_ret_issqn=(1 if (obra and obra.iss_retido) else 2),
        v_ret_inss=_retido("INSS"),
        v_ret_irrf=_retido("IRRF"),
        v_ret_csll=_retido("CSLL"),
    )


# ---------------------------------------------------------------------------
# 3. Emitir
# ---------------------------------------------------------------------------
def emitir(s: Session, titulo_id: int, *, valor: Any = None, observacao: str = "",
           usuario: Optional[Usuario] = None, andamento=None) -> dict[str, Any]:
    """Declara à prefeitura e guarda o que ela devolver.

    Roda na fila (migração 055) porque a resposta pode levar minutos.
    """
    from app.apps.emissaonf.el_nfse_nacional import ELNfseNacional
    from app.apps.erp.core.cadastros.certificado import chave_e_certificado_pem

    def _diz(passo: int, total: int, texto: str) -> None:
        if andamento is not None:
            andamento(passo, total, texto)

    # ---- 1. conferir ANTES de tocar em número
    _diz(1, 5, "Conferindo o cadastro…")
    check = conferir(s, titulo_id)
    if not check["pode"]:
        raise ErroValidacao("A nota não pode sair ainda: " + " ".join(check["faltas"]))

    titulo = s.get(Titulo, titulo_id)
    empresa = s.get(Empresa, check["destino"]["empresa_id"])
    obra = _obra_do_titulo(s, titulo)
    bruto = _dec(valor) if valor not in (None, "") else _dec(titulo.valor_liquido)
    calculo = tributacao.calcular(obra, bruto)

    # ---- 2. reservar o número (nosso), e só então emitir
    _diz(2, 5, "Reservando o número da declaração…")
    nota = numeracao.reservar(
        s, empresa, titulo_id=titulo.id, obra_id=(obra.id if obra else None),
        competencia=titulo.competencia, modo="API",
        valor_bruto=bruto, valor_liquido=calculo.valor_liquido,
        observacao=observacao, usuario=usuario)
    s.commit()          # o número está tomado, aconteça o que acontecer adiante

    try:
        _diz(3, 5, f"Assinando a declaração nº {nota.numero_dps}…")
        # O .pfx nunca vira arquivo em disco — só chave e certificado, em memória.
        chave_pem, cert_pem = chave_e_certificado_pem(s, empresa.id)
        dados = montar_declaracao(s, titulo_id, numero_dps=nota.numero_dps,
                                  valor=bruto, observacao=observacao)

        cliente = ELNfseNacional(
            token=svc_emissao.token_de(empresa) or "",
            chave_pem=chave_pem, cert_pem=cert_pem,
            ambiente=("producao" if (empresa.emissao_ambiente or "").upper() == "PRODUCAO"
                      else "homologacao"),
            urlbase=(empresa.emissao_url_base or ""))

        _diz(4, 5, "Enviando à prefeitura e aguardando a resposta…")
        retorno = cliente.emitir_e_aguardar(dados, timeout_s=ESPERA_SEGUNDOS)
    except Exception as e:
        # O número NÃO volta para a fila: a prefeitura pode ter recebido a
        # declaração e só a resposta ter se perdido.
        #
        # DUAS MENSAGENS, de propósito. O `motivo` gravado guarda o texto
        # técnico inteiro — é o que se manda para o suporte da prefeitura e o
        # que o fisco pergunta. A frase que vai para a TELA é em português:
        # despejar um erro de biblioteca na cara de quem está faturando não
        # ajuda ninguém a decidir o que fazer.
        motivo = str(e)[:1000] or e.__class__.__name__
        numeracao.falhar(s, nota.id, motivo=motivo, usuario=usuario)
        s.commit()
        logger.exception("ERP/emissao: DPS %s falhou", nota.numero_dps)
        raise ErroValidacao(
            f"{_em_portugues(motivo)} O número {nota.numero_dps} ficou "
            f"registrado como queimado, com o motivo guardado — número de nota "
            f"não se apaga, se explica. A próxima tentativa usa outro número.")

    # ---- 3. guardar o que voltou
    _diz(5, 5, "Guardando a nota…")
    chave = str(retorno.get("chaveAcesso") or "")
    xml = retorno.get("nfse_xml") or ""
    nota.id_dps = str(retorno.get("idDPS") or "") or None
    nota.retorno = {"chaveAcesso": chave, "idDPS": retorno.get("idDPS"),
                    "ambiente": empresa.emissao_ambiente}
    anexo_id = _guardar_xml(s, titulo, nota, xml, usuario)
    if anexo_id:
        nota.anexo_id = anexo_id

    from app.apps.erp.core.notas_emitidas.manual import retencoes_em_colunas
    numeracao.confirmar(
        s, nota.id,
        # A prefeitura devolve a chave de acesso; o número da nota está dentro
        # dela (posições 24 a 36 do padrão nacional). Quando não der para ler,
        # fica a chave, que é o que identifica a nota de verdade.
        numero_nota=_numero_da_chave(chave) or chave,
        chave_acesso=chave, data_emissao=date.today(),
        retencoes=retencoes_em_colunas(calculo), usuario=usuario)
    s.commit()
    logger.info("ERP/emissao: nota emitida — DPS %s, chave %s", nota.numero_dps, chave)
    return {"nota_id": nota.id, "numero_dps": nota.numero_dps,
            "numero_nota": nota.numero_nota, "chave_acesso": chave,
            "resumo": f"Nota {nota.numero_nota} emitida "
                      f"({empresa.emissao_ambiente.lower()})."}


def _em_portugues(motivo: str) -> str:
    """A primeira frase, escrita para quem está faturando.

    A recusa da própria prefeitura já vem em português e é útil — essa passa
    inteira. O que não pode chegar à tela é erro de biblioteca: "ProxyError",
    "Max retries exceeded", "SSLCertVerificationError" não dizem à pessoa o que
    ela deve fazer, e assustam.
    """
    texto = motivo or ""
    if "Rejeitada" in texto or "rejeit" in texto.lower():
        return f"A prefeitura recusou a declaração: {texto[:400]}"
    if "ainda em processamento" in texto or "TimeoutError" in texto or "processamento após" in texto:
        return ("A prefeitura recebeu a declaração mas ainda não respondeu. "
                "Confira no portal dela antes de emitir de novo — pode ser que "
                "a nota já exista.")
    rede = ("ConnectionPool", "Max retries", "ProxyError", "Timeout",
            "Connection", "SSL", "Name or service not known", "getaddrinfo")
    if any(p in texto for p in rede):
        return ("Não deu para falar com o serviço da prefeitura — o sistema não "
                "alcançou o endereço dela. Pode ser o serviço fora do ar, ou o "
                "endereço configurado errado em Administração › Empresas.")
    if "token" in texto.lower() or "401" in texto or "403" in texto:
        return ("A prefeitura não aceitou nossa credencial. Confira o token do "
                "canal em Administração › Empresas.")
    if "certificado" in texto.lower() or "pkcs12" in texto.lower():
        return ("Não deu para assinar a declaração com o certificado digital da "
                "empresa. Confira o certificado em Administração › Empresas.")
    return f"A prefeitura não confirmou a nota: {texto[:300]}"


def _numero_da_chave(chave: str) -> str:
    """O número sequencial da NFS-e dentro da chave de acesso do padrão nacional.

    A chave tem 50 dígitos e o número da nota ocupa uma faixa fixa dela. Ler
    daí evita depender de a prefeitura devolver o número num campo à parte —
    algumas devolvem, outras não.
    """
    digitos = "".join(c for c in (chave or "") if c.isdigit())
    if len(digitos) != 50:
        return ""
    return digitos[27:40].lstrip("0") or "0"


def _guardar_xml(s: Session, titulo: Titulo, nota: NotaEmitida, xml: str,
                 usuario: Optional[Usuario]) -> Optional[int]:
    """O XML da nota fica anexado ao título.

    É o documento que vale: o PDF é uma representação dele. Se guardar falhar,
    a emissão NÃO é desfeita — a nota existe do lado da prefeitura, e perder o
    anexo é problema menor do que fingir que a nota não saiu.
    """
    if not xml:
        return None
    try:
        from app.apps.erp.core.documentos.armazenamento import salvar
        nome = f"NFSE_{nota.numero_dps}_{nota.chave_acesso or nota.id}.xml"
        anexo = salvar(s, xml.encode("utf-8"), nome, entidade_tipo="titulo",
                       entidade_id=titulo.id, categoria="NOTA",
                       descricao="XML da NFS-e emitida", usuario=usuario)
        return anexo.id
    except Exception:
        logger.warning("ERP/emissao: nota emitida mas o XML não foi guardado",
                       exc_info=True)
        return None
