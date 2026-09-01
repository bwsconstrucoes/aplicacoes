# ============================================================================
# BWS ERP — db/models/financeiro.py
# Models do núcleo financeiro: documentos fiscais, pedidos, anexos, títulos,
# parcelas, rateios, retenções, análises, pagamentos, extratos, conciliações,
# sync_queue e eventos. Espelham fielmente o schema.sql.
# ============================================================================
from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, ForeignKey, Index, Integer, LargeBinary,
    Numeric, SmallInteger, Text, func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.apps.erp.db.database import Base
from app.apps.erp.db.models.cadastros import (
    Categoria, ContaBancaria, Contrato, FormaPagamento, Fornecedor,
    FornecedorConta, Obra, TipoTitulo, Usuario, pg_enum,
)


# ---------------------------------------------------------------------------
# Enums do financeiro
# ---------------------------------------------------------------------------
class StatusTitulo(str, enum.Enum):
    RASCUNHO = "RASCUNHO"
    EM_ANALISE = "EM_ANALISE"
    AGUARDANDO_AVAL = "AGUARDANDO_AVAL"
    DEVOLVIDO = "DEVOLVIDO"
    AGUARDANDO_APROVACAO = "AGUARDANDO_APROVACAO"
    APROVADO = "APROVADO"
    BLOQUEADO = "BLOQUEADO"
    PAGO_PARCIAL = "PAGO_PARCIAL"
    PAGO = "PAGO"
    CANCELADO = "CANCELADO"
    ESTORNADO = "ESTORNADO"


class EspecieTitulo(str, enum.Enum):
    PAGAR = "PAGAR"
    RECEBER = "RECEBER"


class StatusDedutibilidade(str, enum.Enum):
    PENDENTE = "PENDENTE"
    DEDUTIVEL = "DEDUTIVEL"
    INDEDUTIVEL = "INDEDUTIVEL"
    PARCIAL = "PARCIAL"


class StatusParcela(str, enum.Enum):
    ABERTA = "ABERTA"
    AGENDADA = "AGENDADA"
    PAGA = "PAGA"
    CANCELADA = "CANCELADA"


class TipoDocFiscal(str, enum.Enum):
    NFE = "NFE"
    NFSE = "NFSE"
    CTE = "CTE"
    NFCE = "NFCE"
    FATURA = "FATURA"
    RECIBO = "RECIBO"
    CONTRATO = "CONTRATO"
    OUTRO = "OUTRO"


class SituacaoNota(str, enum.Enum):
    AUTORIZADA = "AUTORIZADA"
    CANCELADA = "CANCELADA"
    DENEGADA = "DENEGADA"
    DESCONHECIDA = "DESCONHECIDA"


class TipoRetencao(str, enum.Enum):
    INSS = "INSS"
    ISS = "ISS"
    IRRF = "IRRF"
    PCC = "PCC"


class DestinoSync(str, enum.Enum):
    OMIE = "OMIE"
    SHEETS = "SHEETS"
    PIPEFY = "PIPEFY"


class StatusSync(str, enum.Enum):
    PENDENTE = "PENDENTE"
    PROCESSANDO = "PROCESSANDO"
    OK = "OK"
    ERRO = "ERRO"
    DESCARTADO = "DESCARTADO"


# ---------------------------------------------------------------------------
# Documentos
# ---------------------------------------------------------------------------
class DocumentoFiscal(Base):
    __tablename__ = "documentos_fiscais"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tipo: Mapped[TipoDocFiscal] = mapped_column(pg_enum(TipoDocFiscal, "tipo_doc_fiscal"), nullable=False)
    chave_acesso: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    numero: Mapped[Optional[str]] = mapped_column(Text)
    serie: Mapped[Optional[str]] = mapped_column(Text)
    codigo_verificacao: Mapped[Optional[str]] = mapped_column(Text)
    municipio_emissao: Mapped[Optional[str]] = mapped_column(Text)
    emitente_doc: Mapped[str] = mapped_column(Text, nullable=False)
    emitente_nome: Mapped[Optional[str]] = mapped_column(Text)
    destinatario_doc: Mapped[Optional[str]] = mapped_column(Text)
    valor_total: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    data_emissao: Mapped[Optional[date]] = mapped_column(Date)
    situacao: Mapped[SituacaoNota] = mapped_column(
        pg_enum(SituacaoNota, "situacao_nota"), nullable=False, default=SituacaoNota.DESCONHECIDA)
    situacao_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    manifestacao: Mapped[Optional[str]] = mapped_column(Text)
    xml_path: Mapped[Optional[str]] = mapped_column(Text)
    pdf_path: Mapped[Optional[str]] = mapped_column(Text)
    dados: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    origem: Mapped[str] = mapped_column(Text, nullable=False, default="UPLOAD")
    capturado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Pedido(Base):
    __tablename__ = "pedidos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    fornecedor_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("fornecedores.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    valor_total: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ABERTO")
    dados: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    ref_origem: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Anexo(Base):
    __tablename__ = "anexos"
    __table_args__ = (
        Index("idx_anexos_entidade", "entidade_tipo", "entidade_id"),
        Index("idx_anexos_hash", "hash_sha256"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    entidade_tipo: Mapped[str] = mapped_column(Text, nullable=False)
    entidade_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    nome_arquivo: Mapped[str] = mapped_column(Text, nullable=False)
    dropbox_path: Mapped[Optional[str]] = mapped_column(Text)   # legado; conteúdo vive no banco
    conteudo: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    mime_type: Mapped[Optional[str]] = mapped_column(Text)
    tamanho_original: Mapped[Optional[int]] = mapped_column(BigInteger)
    comprimido: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    categoria_anexo: Mapped[Optional[str]] = mapped_column(Text)
    hash_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    tamanho_bytes: Mapped[Optional[int]] = mapped_column(BigInteger)
    enviado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# Núcleo: títulos
# ---------------------------------------------------------------------------
class Titulo(Base):
    __tablename__ = "titulos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero_sp: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    tipo: Mapped[TipoTitulo] = mapped_column(pg_enum(TipoTitulo, "tipo_titulo"), nullable=False)
    fornecedor_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    valor_bruto: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    valor_retencoes: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    valor_liquido: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    competencia: Mapped[date] = mapped_column(Date, nullable=False)
    data_emissao_doc: Mapped[Optional[date]] = mapped_column(Date)
    categoria_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("categorias.id"), nullable=False)
    pedido_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("pedidos.id"))
    contrato_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("contratos.id"))
    documento_fiscal_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("documentos_fiscais.id"))
    forma_pagamento: Mapped[FormaPagamento] = mapped_column(
        pg_enum(FormaPagamento, "forma_pagamento"), nullable=False)
    fornecedor_conta_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("fornecedor_contas.id"))
    especie: Mapped[str] = mapped_column(
        pg_enum(EspecieTitulo, "especie_titulo"), nullable=False, default=EspecieTitulo.PAGAR)
    numero_medicao: Mapped[Optional[str]] = mapped_column(Text)
    periodo_inicio: Mapped[Optional[date]] = mapped_column(Date)
    periodo_fim: Mapped[Optional[date]] = mapped_column(Date)
    notas_fiscais: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    cliente_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("fornecedores.id"))
    dedutivel: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    dedutibilidade: Mapped[str] = mapped_column(
        pg_enum(StatusDedutibilidade, "status_dedutibilidade"),
        nullable=False, default=StatusDedutibilidade.PENDENTE)
    dedutibilidade_valor: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    dedutibilidade_motivo: Mapped[Optional[str]] = mapped_column(Text)
    dedutibilidade_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    dedutibilidade_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    dedutibilidade_origem: Mapped[Optional[str]] = mapped_column(Text)
    forma_liquidacao: Mapped[Optional[str]] = mapped_column(Text)
    modalidade: Mapped[str] = mapped_column(Text, nullable=False, default="NORMAL")
    fundo_fixo_tipo: Mapped[Optional[str]] = mapped_column(Text)
    adiantamento_titulo_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("titulos.id"))
    periodo_prestacao_inicio: Mapped[Optional[date]] = mapped_column(Date)
    periodo_prestacao_fim: Mapped[Optional[date]] = mapped_column(Date)
    alertas_confirmados: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    contrato_servico_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contratos_servico.id"))
    medicao_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contrato_medicoes.id"))
    adiantamento_contrato: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    locacao_parcela_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("locacao_parcelas.id"))
    chave_acesso_nfe: Mapped[Optional[str]] = mapped_column(Text)
    cno_documento: Mapped[Optional[str]] = mapped_column(Text)
    exige_aval: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    avalizado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    avalizado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    justificativa_excecao: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[StatusTitulo] = mapped_column(
        pg_enum(StatusTitulo, "status_titulo"), nullable=False, default=StatusTitulo.RASCUNHO)
    score_risco: Mapped[Optional[int]] = mapped_column(SmallInteger)
    solicitante_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    aprovador_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    aprovado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    estorna_titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    codigo_omie: Mapped[Optional[int]] = mapped_column(BigInteger, unique=True)
    ref_pipefy: Mapped[Optional[str]] = mapped_column(Text)
    origem: Mapped[str] = mapped_column(Text, nullable=False, default="SISTEMA")
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # duas FKs apontam para fornecedores (credor e cliente): explicitar qual é qual
    fornecedor: Mapped[Fornecedor] = relationship(foreign_keys=[fornecedor_id])
    cliente: Mapped[Optional[Fornecedor]] = relationship(foreign_keys=[cliente_id])
    categoria: Mapped[Categoria] = relationship()
    parcelas: Mapped[list["Parcela"]] = relationship(back_populates="titulo", order_by="Parcela.numero")
    rateios: Mapped[list["Rateio"]] = relationship(back_populates="titulo")
    retencoes: Mapped[list["Retencao"]] = relationship(
        back_populates="titulo", foreign_keys="Retencao.titulo_id")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Titulo {self.numero_sp} {self.tipo} R${self.valor_liquido} {self.status}>"


class Parcela(Base):
    __tablename__ = "parcelas"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    numero: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    vencimento: Mapped[date] = mapped_column(Date, nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    status: Mapped[StatusParcela] = mapped_column(
        pg_enum(StatusParcela, "status_parcela"), nullable=False, default=StatusParcela.ABERTA)
    linha_digitavel: Mapped[Optional[str]] = mapped_column(Text)
    codigo_barras: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    titulo: Mapped[Titulo] = relationship(back_populates="parcelas")
    pagamentos: Mapped[list["Pagamento"]] = relationship(
        back_populates="parcela", foreign_keys="Pagamento.parcela_id")


class Rateio(Base):
    __tablename__ = "rateios"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    percentual: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 4))
    # conta própria da linha: uma nota pode ter material e serviço juntos
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    titulo: Mapped[Titulo] = relationship(back_populates="rateios")
    obra: Mapped[Obra] = relationship()
    categoria: Mapped[Optional[Categoria]] = relationship()


class Retencao(Base):
    __tablename__ = "retencoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    tipo: Mapped[TipoRetencao] = mapped_column(pg_enum(TipoRetencao, "tipo_retencao"), nullable=False)
    base_calculo: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    aliquota: Mapped[Decimal] = mapped_column(Numeric(7, 4), nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    cno_obra: Mapped[Optional[str]] = mapped_column(Text)
    titulo_guia_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    memoria_calculo: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    titulo: Mapped[Titulo] = relationship(back_populates="retencoes", foreign_keys=[titulo_id])


class Analise(Base):
    __tablename__ = "analises"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    motor_versao: Mapped[str] = mapped_column(Text, nullable=False)
    resultado: Mapped[str] = mapped_column(Text, nullable=False)
    score: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    criticas: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    executada_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# Caixa
# ---------------------------------------------------------------------------
class Pagamento(Base):
    __tablename__ = "pagamentos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    parcela_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("parcelas.id"), nullable=False)
    conta_bancaria_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("contas_bancarias.id"), nullable=False)
    data_pagamento: Mapped[date] = mapped_column(Date, nullable=False)
    valor_pago: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    meio: Mapped[FormaPagamento] = mapped_column(pg_enum(FormaPagamento, "forma_pagamento"), nullable=False)
    comprovante_anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("anexos.id"))
    executado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    executado_por_robo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    estorna_pagamento_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("pagamentos.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    parcela: Mapped[Parcela] = relationship(back_populates="pagamentos", foreign_keys=[parcela_id])


class Extrato(Base):
    __tablename__ = "extratos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    conta_bancaria_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("contas_bancarias.id"), nullable=False)
    data_lancamento: Mapped[date] = mapped_column(Date, nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    historico: Mapped[Optional[str]] = mapped_column(Text)
    documento: Mapped[Optional[str]] = mapped_column(Text)
    nome_contraparte: Mapped[Optional[str]] = mapped_column(Text)
    hash_linha: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    importado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Conciliacao(Base):
    __tablename__ = "conciliacoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pagamento_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("pagamentos.id"), nullable=False, unique=True)
    extrato_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("extratos.id"), nullable=False, unique=True)
    metodo: Mapped[str] = mapped_column(Text, nullable=False, default="MANUAL")
    confianca: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3))
    conciliado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    conciliado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    desfeita_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


# ---------------------------------------------------------------------------
# Sincronização e auditoria
# ---------------------------------------------------------------------------
class SyncQueue(Base):
    __tablename__ = "sync_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    entidade_tipo: Mapped[str] = mapped_column(Text, nullable=False)
    entidade_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    destino: Mapped[DestinoSync] = mapped_column(pg_enum(DestinoSync, "destino_sync"), nullable=False)
    operacao: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    status: Mapped[StatusSync] = mapped_column(
        pg_enum(StatusSync, "status_sync"), nullable=False, default=StatusSync.PENDENTE)
    tentativas: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    ultimo_erro: Mapped[Optional[str]] = mapped_column(Text)
    proximo_retry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class Evento(Base):
    __tablename__ = "eventos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    entidade_tipo: Mapped[str] = mapped_column(Text, nullable=False)
    entidade_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    usuario_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    acao: Mapped[str] = mapped_column(Text, nullable=False)
    detalhe: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# Lotes de pagamento
# ---------------------------------------------------------------------------
class Lote(Base):
    __tablename__ = "lotes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    prioridade: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=3)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ABERTO")
    conta_bancaria_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contas_bancarias.id"))
    data_prevista: Mapped[Optional[date]] = mapped_column(Date)
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    fechado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    itens: Mapped[list["LoteItem"]] = relationship(
        back_populates="lote", order_by="LoteItem.ordem", cascade="all, delete-orphan")


class LoteItem(Base):
    __tablename__ = "lote_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    lote_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("lotes.id"), nullable=False)
    parcela_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("parcelas.id"), nullable=False)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    lote: Mapped[Lote] = relationship(back_populates="itens")
    parcela: Mapped[Parcela] = relationship()


class Movimentacao(Base):
    """Movimentação entre contas próprias — transferência, aplicação, resgate.
    Lançamento propositalmente simples: valor, data, contas e uma descrição."""
    __tablename__ = "movimentacoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tipo: Mapped[str] = mapped_column(Text, nullable=False, default="TRANSFERENCIA")
    conta_origem_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contas_bancarias.id"))
    conta_destino_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contas_bancarias.id"))
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    data_movimento: Mapped[date] = mapped_column(Date, nullable=False)
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    comprovante_anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("anexos.id"))
    extrato_saida_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("extratos.id"))
    extrato_entrada_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("extratos.id"))
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # movimentação neutra: par que se anula, ignorado em toda leitura gerencial
    neutra: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    par_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("movimentacoes.id"))
    motivo_neutra: Mapped[Optional[str]] = mapped_column(Text)
    contraparte: Mapped[Optional[str]] = mapped_column(Text)
    sentido: Mapped[Optional[str]] = mapped_column(Text)


class TituloAval(Base):
    """Assinatura da segunda pessoa. Guarda o resumo do título no momento do
    aval — se algo mudar depois, dá para provar o que foi assinado."""
    __tablename__ = "titulo_avais"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    usuario_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    papel: Mapped[str] = mapped_column(Text, nullable=False)
    decisao: Mapped[str] = mapped_column(Text, nullable=False)
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    assinatura: Mapped[str] = mapped_column(Text, nullable=False)
    resumo_assinado: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    ip: Mapped[Optional[str]] = mapped_column(Text)
    dispositivo: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TituloInteressado(Base):
    """Quem mais acompanha o título e recebe os avisos, além do solicitante."""
    __tablename__ = "titulo_interessados"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    usuario_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    adicionado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ObraInteressado(Base):
    """Interessado fixo de uma obra: entra automaticamente em todo título dela."""
    __tablename__ = "obra_interessados"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    usuario_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TituloItem(Base):
    """Linha da prestação de contas (fundo fixo) ou da fatura de cartão."""
    __tablename__ = "titulo_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    data_despesa: Mapped[Optional[date]] = mapped_column(Date)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    estabelecimento: Mapped[Optional[str]] = mapped_column(Text)
    documento: Mapped[Optional[str]] = mapped_column(Text)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("anexos.id"))
    origem_leitura: Mapped[Optional[str]] = mapped_column(Text)
    confianca: Mapped[Optional[str]] = mapped_column(Text)
    criticas: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    conferido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    conferido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ContratoServico(Base):
    """Empreita/subcontratação: o acordo com o prestador, com saldo próprio.

    Substitui a planilha da obra. O contrato guarda o combinado (quantidade,
    preço, valor total); cada medição consome o saldo. Medir mais que o
    contratado é impossível, e medir o mesmo período duas vezes é apontado —
    que é exatamente o erro que hoje passa despercebido.
    """
    __tablename__ = "contratos_servico"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    objeto: Mapped[str] = mapped_column(Text, nullable=False)
    modo: Mapped[str] = mapped_column(Text, nullable=False, default="MEDICAO")
    unidade: Mapped[Optional[str]] = mapped_column(Text)
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    preco_unitario: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    valor_total: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    valor_aditivos: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    parcelas_previstas: Mapped[Optional[int]] = mapped_column(SmallInteger)
    data_inicio: Mapped[Optional[date]] = mapped_column(Date)
    data_fim: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="RASCUNHO")
    exige_foto: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    aprovado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    aprovado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    obra: Mapped[Obra] = relationship()
    fornecedor: Mapped[Fornecedor] = relationship()
    medicoes: Mapped[list["ContratoMedicao"]] = relationship(
        back_populates="contrato", order_by="ContratoMedicao.numero",
        cascade="all, delete-orphan")


class ContratoMedicao(Base):
    """Uma medição do contrato: o que foi executado no período e vale pagar."""
    __tablename__ = "contrato_medicoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_servico.id"), nullable=False)
    numero: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    periodo_inicio: Mapped[Optional[date]] = mapped_column(Date)
    periodo_fim: Mapped[Optional[date]] = mapped_column(Date)
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    percentual: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 4))
    valor_medido: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    valor_adiantamento_abatido: Mapped[Decimal] = mapped_column(
        Numeric(14, 2), nullable=False, default=0)
    valor_liquido: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="MEDIDA")
    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    medido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    autorizado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    autorizado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    contrato: Mapped[ContratoServico] = relationship(back_populates="medicoes")


class PeriodoBloqueado(Base):
    """Trava do passado: até que data não se altera mais nada."""
    __tablename__ = "periodos_bloqueados"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ate_data: Mapped[date] = mapped_column(Date, nullable=False)
    liberado_ate: Mapped[Optional[date]] = mapped_column(Date)
    liberado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    liberado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    liberado_motivo: Mapped[Optional[str]] = mapped_column(Text)
    liberado_expira: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    atualizado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


class ContratoLocacao(Base):
    """Contrato de locação: o que está locado, onde, por quanto e até quando."""
    __tablename__ = "contratos_locacao"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    numero_externo: Mapped[Optional[str]] = mapped_column(Text)
    periodicidade: Mapped[str] = mapped_column(Text, nullable=False, default="MENSAL")
    dia_vencimento: Mapped[Optional[int]] = mapped_column(SmallInteger)
    data_inicio: Mapped[date] = mapped_column(Date, nullable=False)
    data_fim_prevista: Mapped[Optional[date]] = mapped_column(Date)
    data_encerramento: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ATIVO")
    responsavel_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    fornecedor: Mapped[Fornecedor] = relationship()
    obra: Mapped[Obra] = relationship()
    itens: Mapped[list["LocacaoItem"]] = relationship(
        back_populates="contrato", cascade="all, delete-orphan")


class LocacaoItem(Base):
    __tablename__ = "locacao_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_locacao.id"), nullable=False)
    insumo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("insumos.id"))
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    quantidade: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    quantidade_devolvida: Mapped[Decimal] = mapped_column(
        Numeric(14, 4), nullable=False, default=0)
    valor_unitario: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    contrato: Mapped[ContratoLocacao] = relationship(back_populates="itens")


class LocacaoMovimento(Base):
    """Devolução, remanejo entre obras ou acréscimo de equipamento."""
    __tablename__ = "locacao_movimentos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_locacao.id"), nullable=False)
    item_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("locacao_itens.id"))
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    obra_origem_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    obra_destino_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    data_movimento: Mapped[date] = mapped_column(Date, nullable=False)
    documento: Mapped[Optional[str]] = mapped_column(Text)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    usuario_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class LocacaoParcela(Base):
    """Previsão de cobrança: é o que faz o financeiro reconhecer o boleto."""
    __tablename__ = "locacao_parcelas"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_locacao.id"), nullable=False)
    competencia: Mapped[date] = mapped_column(Date, nullable=False)
    vencimento: Mapped[date] = mapped_column(Date, nullable=False)
    valor_previsto: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    status: Mapped[str] = mapped_column(Text, nullable=False, default="PREVISTA")
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ContratoServicoItem(Base):
    """Linha do orçamento da empreita: serviço, unidade, quantidade e preço."""
    __tablename__ = "contrato_servico_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_servico.id"), nullable=False)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    unidade: Mapped[Optional[str]] = mapped_column(Text)
    quantidade: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    preco_unitario: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    quantidade_aditivada: Mapped[Decimal] = mapped_column(
        Numeric(14, 4), nullable=False, default=0)
    insumo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("insumos.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MedicaoItem(Base):
    """Quanto de cada serviço foi executado nesta medição."""
    __tablename__ = "medicao_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    medicao_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contrato_medicoes.id"), nullable=False)
    contrato_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contrato_servico_itens.id"), nullable=False)
    quantidade: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    observacao: Mapped[Optional[str]] = mapped_column(Text)


class IaUso(Base):
    """Consumo de IA: uma linha por chamada, com tokens e custo."""
    __tablename__ = "ia_uso"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    modelo: Mapped[str] = mapped_column(Text, nullable=False)
    operacao: Mapped[str] = mapped_column(Text, nullable=False)
    tokens_entrada: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_saida: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    custo_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False, default=0)
    duracao_ms: Mapped[Optional[int]] = mapped_column(Integer)
    sucesso: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    erro: Mapped[Optional[str]] = mapped_column(Text)
    usuario_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    referencia: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DespesaColaborador(Base):
    """DC: lote de verbas de várias pessoas, aprovado em cadeia."""
    __tablename__ = "despesas_colaborador"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    competencia: Mapped[date] = mapped_column(Date, nullable=False)
    data_prevista: Mapped[Optional[date]] = mapped_column(Date)
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    meio_pagamento: Mapped[str] = mapped_column(Text, nullable=False, default="BEEVALE")
    status: Mapped[str] = mapped_column(Text, nullable=False, default="RASCUNHO")
    valor_total: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    aprovado_supervisor: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    aprovado_supervisor_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    aprovado_dp: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    aprovado_dp_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    aprovado_diretor: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    aprovado_diretor_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    motivo_devolucao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    itens: Mapped[list["DespesaColaboradorItem"]] = relationship(
        back_populates="despesa", cascade="all, delete-orphan")


class DespesaColaboradorItem(Base):
    __tablename__ = "despesa_colaborador_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    despesa_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("despesas_colaborador.id"), nullable=False)
    colaborador_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("colaboradores.id"), nullable=False)
    verba: Mapped[str] = mapped_column(Text, nullable=False)
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    valor_unitario: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criticas: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    conferido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    conferido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    despesa: Mapped[DespesaColaborador] = relationship(back_populates="itens")
