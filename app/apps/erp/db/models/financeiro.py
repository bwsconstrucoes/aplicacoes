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

    # ----- o cruzamento (migração 044) -----
    # Contra qual CNPJ NOSSO a nota foi emitida, de que pedido ela é, e — quando
    # ela entrou por ali — em que linha da prestação de fundo fixo ela está.
    # A ligação com o pedido mora aqui, do lado da NOTA, porque UM PEDIDO TEM
    # VÁRIAS NOTAS: dez carradas de brita viram dez notas e dez boletos.
    empresa_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("empresas.id"))
    pedido_compra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("pedidos_compra.id"))
    titulo_item_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulo_itens.id"))
    conferencia: Mapped[str] = mapped_column(Text, nullable=False, default="PENDENTE")
    conferencia_motivo: Mapped[Optional[str]] = mapped_column(Text)
    conferido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    conferido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


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
    # ONDE OS BYTES ESTÃO: 'BANCO' (padrão de sempre) ou 'DRIVE' (migração 043).
    # O banco garante que um dos dois esteja preenchido — anexo que não está em
    # lugar nenhum seria descoberto só no dia em que alguém precisasse dele.
    guardado_em: Mapped[str] = mapped_column(Text, nullable=False, default="BANCO")
    drive_file_id: Mapped[Optional[str]] = mapped_column(Text)
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
    # O TIPO da medição vem de catálogo editável, e o número é texto livre:
    # quem manda na nomenclatura é o ÓRGÃO, não o ERP (migração 049).
    medicao_tipo: Mapped[Optional[str]] = mapped_column(
        Text, ForeignKey("medicao_tipos.codigo"))
    # A medição de reajuste aponta para a que ela reajusta. Opcional de
    # propósito: há órgão que numera o reajuste em sequência, e mesmo assim a
    # ligação existe.
    medicao_de_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("titulos.id"))
    protocolo_numero: Mapped[Optional[str]] = mapped_column(Text)
    protocolo_em: Mapped[Optional[date]] = mapped_column(Date)
    # Como o reajuste foi calculado (migração 050). Um número solto não se
    # defende: guardando a data-base, o mês de referência e o fator, o sistema
    # mostra a conta inteira dois anos depois — que é quando a pergunta vem.
    reajuste_indice: Mapped[Optional[str]] = mapped_column(Text)
    reajuste_data_base: Mapped[Optional[date]] = mapped_column(Date)
    reajuste_ate: Mapped[Optional[date]] = mapped_column(Date)
    reajuste_fator: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 8))
    reajuste_previsto: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
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
    colaborador_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("colaboradores.id"))
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
    # QUANDO ESTE EQUIPAMENTO VOLTA. Por item, e não por contrato: a betoneira
    # fica os oito meses da obra, as escoras eram para três semanas na
    # concretagem da laje — e é a escora que se esquece. Perguntada na
    # contratação, que é quando a pessoa sabe a resposta.
    devolucao_prevista: Mapped[Optional[date]] = mapped_column(Date)
    # a PRIMEIRA data prometida. Prorrogar é normal; prorrogar em silêncio não.
    devolucao_prevista_original: Mapped[Optional[date]] = mapped_column(Date)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    contrato: Mapped[ContratoLocacao] = relationship(back_populates="itens")


class LocacaoConferencia(Base):
    """A prestação de contas mensal dos equipamentos de UM contrato.

    Existe porque o alerta sozinho não resolve: o ERP já sabe dizer "10 meses
    locado, o aluguel já pagou a compra" — o que faltava era ALGUÉM SER
    OBRIGADO A RESPONDER. Uma por contrato por mês, endereçada ao
    administrativo da obra, com nome.

    Ela NÃO bloqueia o pagamento do aluguel: bloquear trocaria equipamento
    esquecido por multa e briga com a locadora. Ela aparece como pendência e
    avisa quem vai lançar a parcela.
    """
    __tablename__ = "locacao_conferencias"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    contrato_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("contratos_locacao.id", ondelete="CASCADE"),
        nullable=False)
    competencia: Mapped[date] = mapped_column(Date, nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    responsavel_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    respondida_por: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    respondida_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    situacao: Mapped[str] = mapped_column(Text, nullable=False, default="ABERTA")
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class LocacaoConferenciaItem(Base):
    """O que a obra respondeu sobre UM equipamento, naquele mês.

    `onde_esta` é o campo que mais vale, e foi pedido assim: "na laje do bloco
    B, escorando até desforma" é uma resposta; "está aí" é outra, e quem lê
    percebe a diferença na hora.
    """
    __tablename__ = "locacao_conferencia_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    conferencia_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("locacao_conferencias.id", ondelete="CASCADE"),
        nullable=False)
    item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("locacao_itens.id", ondelete="CASCADE"), nullable=False)
    presente: Mapped[Optional[str]] = mapped_column(Text)
    em_uso: Mapped[Optional[bool]] = mapped_column(Boolean)
    onde_esta: Mapped[Optional[str]] = mapped_column(Text)
    devolucao_prevista: Mapped[Optional[date]] = mapped_column(Date)
    decisao: Mapped[Optional[str]] = mapped_column(Text)
    obra_destino_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("obras.id"))
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    quantidade_conferida: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


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
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 4))
    valor_unitario: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criticas: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    conferido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    conferido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    despesa: Mapped[DespesaColaborador] = relationship(back_populates="itens")


class TituloColaborador(Base):
    """Guia ou verba que se refere a vários colaboradores (FGTS do mês, por ex.).

    Existe para o histórico da pessoa não ter buraco: mesmo o pagamento feito
    em bloco aparece na ficha de cada um, com a parte que lhe cabe.
    """
    __tablename__ = "titulo_colaboradores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    titulo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("titulos.id"), nullable=False)
    colaborador_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("colaboradores.id"), nullable=False)
    valor: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AgenteMensagem(Base):
    """O que o agente falou, com quem, quando — e se saiu mesmo.

    Existe para responder à pergunta que o dono vai fazer quando a obra disser
    que não sabia: "o Ruan foi cobrado?". E para o agente não virar spam: a
    rotina roda todo dia, e a chave única (assunto, referência, pessoa, degrau)
    é o que garante UMA mensagem por degrau — a trava está no banco, não na
    esperança de o código lembrar.
    """
    __tablename__ = "agente_mensagens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    assunto: Mapped[str] = mapped_column(Text, nullable=False)
    referencia_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    destinatario_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    telefone: Mapped[Optional[str]] = mapped_column(Text)
    degrau: Mapped[str] = mapped_column(Text, nullable=False)
    texto: Mapped[str] = mapped_column(Text, nullable=False)
    canais: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    entregue: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    erro: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


class ComprovanteLido(Base):
    """Todo comprovante que o sistema já leu — e a trava contra baixar duas vezes.

    Duas restrições únicas no banco (migração 042) fazem o trabalho, e é de
    propósito que elas estejam LÁ e não aqui: restrição de banco não depende de
    o código lembrar de perguntar, e vale mesmo com duas execuções ao mesmo
    tempo. A trava antiga, no `baixabradesco`, vivia numa lista em memória e
    falhava LIBERANDO quando a leitura dela dava erro.

      1. `hash_conteudo` único — o mesmo ARQUIVO nunca entra duas vezes. O hash
         é do conteúdo e NÃO inclui o nome: "comprovante.pdf" e "comprovante
         (1).pdf" são o mesmo documento.
      2. (parcela, valor, data) único — o mesmo PAGAMENTO nunca é baixado duas
         vezes, mesmo vindo de um PDF regerado pelo banco, com outros bytes.

    Pagamento parcial continua possível: a mesma parcela aceita outra baixa em
    outro dia ou com outro valor. O que se barra é a repetição idêntica.
    """
    __tablename__ = "comprovantes_lidos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    hash_conteudo: Mapped[str] = mapped_column(Text, nullable=False)
    nome_arquivo: Mapped[Optional[str]] = mapped_column(Text)
    tamanho_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    origem: Mapped[str] = mapped_column(Text, nullable=False, default="TELA")
    situacao: Mapped[str] = mapped_column(Text, nullable=False)
    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    parcela_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("parcelas.id"))
    pagamento_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("pagamentos.id"))
    valor: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    data_pagamento: Mapped[Optional[date]] = mapped_column(Date)
    favorecido: Mapped[Optional[str]] = mapped_column(Text)
    documento: Mapped[Optional[str]] = mapped_column(Text)
    mensagem: Mapped[Optional[str]] = mapped_column(Text)
    usuario_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


# ---------------------------------------------------------------------------
# GESTÃO DE DOCUMENTOS DA EMPRESA (migração 045)
#
# Os BYTES continuam em `Anexo` — que desde a 043 sabe morar no banco ou no
# Google Drive. O que estas duas tabelas acrescentam é o que transforma
# "arquivo guardado" em "documento encontrável": o tipo, o dono, a validade, a
# competência e o texto de dentro.
#
# A especificação inteira está em `GESTAO_DOCUMENTOS.md`.
# ---------------------------------------------------------------------------
class DocumentoTipo(Base):
    """O catálogo de tipos de documento da empresa.

    Editável pela tela de propósito: quem sabe quais documentos a BWS usa toda
    semana é a BWS, não quem programa. O `codigo` vai literalmente para o nome
    do arquivo — por isso ele é maiúsculo, sem acento e sem espaço.
    """
    __tablename__ = "documento_tipos"

    codigo: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    grupo: Mapped[str] = mapped_column(Text, nullable=False)
    dono: Mapped[str] = mapped_column(Text, nullable=False)
    vence: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    avisar_dias: Mapped[Optional[int]] = mapped_column(Integer)
    por_competencia: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sigilo: Mapped[str] = mapped_column(Text, nullable=False, default="ABERTO")
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


class Documento(Base):
    """Um documento arquivado: os bytes (no anexo) mais o que se sabe dele."""
    __tablename__ = "documentos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tipo_codigo: Mapped[str] = mapped_column(
        Text, ForeignKey("documento_tipos.codigo"), nullable=False)
    anexo_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("anexos.id", ondelete="CASCADE"), nullable=False)

    nome_padronizado: Mapped[str] = mapped_column(Text, nullable=False)
    nome_original: Mapped[Optional[str]] = mapped_column(Text)

    # O dono — exatamente um, garantido por CHECK no banco.
    empresa_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("empresas.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    colaborador_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("colaboradores.id"))
    fornecedor_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"))
    lancamento_tipo: Mapped[Optional[str]] = mapped_column(Text)
    lancamento_id: Mapped[Optional[int]] = mapped_column(BigInteger)

    competencia: Mapped[Optional[date]] = mapped_column(Date)
    referencia: Mapped[Optional[str]] = mapped_column(Text)
    emissao: Mapped[Optional[date]] = mapped_column(Date)
    validade: Mapped[Optional[date]] = mapped_column(Date)

    texto: Mapped[Optional[str]] = mapped_column(Text)
    resumo: Mapped[Optional[str]] = mapped_column(Text)

    origem: Mapped[str] = mapped_column(Text, nullable=False, default="TELA")
    confirmado_por: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"))
    confirmado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    tipo: Mapped[DocumentoTipo] = relationship()
    anexo: Mapped[Anexo] = relationship()


class DocumentoBloco(Base):
    """Um conjunto de documentos que sempre é pedido junto (migração 046).

    A CHAVE DO DESENHO: o bloco aponta para TIPOS, não para documentos. Assim o
    bloco fiscal de agosto e o de setembro são o MESMO bloco, com recortes
    diferentes — e ninguém precisa manter lista nenhuma atualizada.
    """
    __tablename__ = "documento_blocos"

    codigo: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    descricao: Mapped[Optional[str]] = mapped_column(Text)
    recorte: Mapped[str] = mapped_column(Text, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    itens: Mapped[list["DocumentoBlocoItem"]] = relationship(
        back_populates="bloco", order_by="DocumentoBlocoItem.ordem",
        cascade="all, delete-orphan")


class DocumentoBlocoItem(Base):
    __tablename__ = "documento_bloco_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    bloco_codigo: Mapped[str] = mapped_column(
        Text, ForeignKey("documento_blocos.codigo", ondelete="CASCADE"), nullable=False)
    tipo_codigo: Mapped[str] = mapped_column(
        Text, ForeignKey("documento_tipos.codigo"), nullable=False)
    obrigatorio: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    observacao: Mapped[Optional[str]] = mapped_column(Text)

    bloco: Mapped[DocumentoBloco] = relationship(back_populates="itens")
    tipo: Mapped[DocumentoTipo] = relationship()


class NotaEmitida(Base):
    """Uma nota que a BWS emite contra o cliente (migração 048).

    DOIS NÚMEROS, e confundi-los é a origem da bagunça:

      numero_dps    a sequência da EMPRESA, por série. O ERP é dono dela —
                    no padrão nacional e no ABRASF, quem numera a declaração é
                    quem emite, não a prefeitura.
      numero_nota   o que a PREFEITURA devolveu. O ERP só registra.

    O número é RESERVADO antes de emitir, e a linha nasce RESERVADA. Se a
    emissão falha, o número não some: fica FALHADA, com motivo. Número de nota
    fiscal não se apaga — se explica.
    """
    __tablename__ = "notas_emitidas"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    empresa_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("empresas.id"), nullable=False)
    ambiente: Mapped[str] = mapped_column(Text, nullable=False, default="HOMOLOGACAO")
    serie: Mapped[str] = mapped_column(Text, nullable=False, default="1")

    numero_dps: Mapped[int] = mapped_column(Integer, nullable=False)
    numero_nota: Mapped[Optional[str]] = mapped_column(Text)
    codigo_verificacao: Mapped[Optional[str]] = mapped_column(Text)
    chave_acesso: Mapped[Optional[str]] = mapped_column(Text)

    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    competencia: Mapped[Optional[date]] = mapped_column(Date)

    modo: Mapped[str] = mapped_column(Text, nullable=False, default="MANUAL")
    situacao: Mapped[str] = mapped_column(Text, nullable=False, default="RESERVADA")
    valor_bruto: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    valor_liquido: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    retencoes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    data_emissao: Mapped[Optional[date]] = mapped_column(Date)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("anexos.id"))
    substituida_por: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("notas_emitidas.id"))

    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


class MedicaoTipo(Base):
    """O tipo da medição — catálogo EDITÁVEL, não lista no código (migração 049).

    O motivo, nas palavras do dono: há órgão que numera o reajuste em sequência
    (virou a medição 3), órgão que numera em paralelo (1 e 1R), e medições
    subsidiárias por fontes diferentes. **Quem manda na nomenclatura é o
    órgão** — impor uma lista fixa quebraria no primeiro contrato fora do
    padrão, e ele já viu isso acontecer.
    """
    __tablename__ = "medicao_tipos"

    codigo: Mapped[str] = mapped_column(Text, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    e_reajuste: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    criado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
