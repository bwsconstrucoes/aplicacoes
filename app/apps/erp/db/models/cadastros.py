# ============================================================================
# BWS ERP — db/models/cadastros.py
# Models do módulo de cadastros. Espelham fielmente o schema.sql.
# Models são estrutura; regra de negócio vive no core/.
# ============================================================================
from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime, Enum, ForeignKey, Index, Integer,
    Numeric, SmallInteger, Text, func,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.apps.erp.db.database import Base


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class TipoPessoa(str, enum.Enum):
    PF = "PF"
    PJ = "PJ"


class RegimeTributario(str, enum.Enum):
    SIMPLES = "SIMPLES"
    LUCRO_PRESUMIDO = "LUCRO_PRESUMIDO"
    LUCRO_REAL = "LUCRO_REAL"
    MEI = "MEI"
    NAO_INFORMADO = "NAO_INFORMADO"


class PerfilUsuario(str, enum.Enum):
    ADMIN = "ADMIN"
    FINANCEIRO = "FINANCEIRO"
    APROVADOR = "APROVADOR"
    LANCADOR = "LANCADOR"
    CONSULTA = "CONSULTA"


class FormaPagamento(str, enum.Enum):
    BOLETO = "BOLETO"
    PIX = "PIX"
    TED = "TED"
    DEBITO_AUTOMATICO = "DEBITO_AUTOMATICO"
    GUIA = "GUIA"
    DINHEIRO = "DINHEIRO"


class StatusConta(str, enum.Enum):
    PENDENTE = "PENDENTE"
    HOMOLOGADA = "HOMOLOGADA"
    BLOQUEADA = "BLOQUEADA"


class TipoTitulo(str, enum.Enum):
    T1_MATERIAL_NFE = "T1_MATERIAL_NFE"
    T2_SERVICO_NFSE = "T2_SERVICO_NFSE"
    T3_FRETE_CTE = "T3_FRETE_CTE"
    T4_LOCACAO = "T4_LOCACAO"
    T5_EMPREITEIRO = "T5_EMPREITEIRO"
    T6_SERVICO_PF_RPA = "T6_SERVICO_PF_RPA"
    T7_FOLHA_ENCARGOS = "T7_FOLHA_ENCARGOS"
    T8_TRIBUTO_GUIA = "T8_TRIBUTO_GUIA"
    T9_CONCESSIONARIA = "T9_CONCESSIONARIA"
    T10_FUNDO_FIXO = "T10_FUNDO_FIXO"
    T11_ADIANTAMENTO = "T11_ADIANTAMENTO"
    T12_REEMBOLSO = "T12_REEMBOLSO"
    T13_FINANCIAMENTO = "T13_FINANCIAMENTO"
    T14_EXCECAO_SEM_NOTA = "T14_EXCECAO_SEM_NOTA"


def pg_enum(py_enum: type[enum.Enum], nome: str) -> Enum:
    """ENUM do Postgres já criado pelo schema.sql."""
    return Enum(py_enum, name=nome, create_type=False,
                values_callable=lambda e: [m.value for m in e])


# ---------------------------------------------------------------------------
class Usuario(Base):
    __tablename__ = "usuarios"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    senha_hash: Mapped[str] = mapped_column(Text, nullable=False)
    perfil: Mapped[PerfilUsuario] = mapped_column(
        pg_enum(PerfilUsuario, "perfil_usuario"), nullable=False,
        default=PerfilUsuario.CONSULTA)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Usuario {self.id} {self.email} {self.perfil}>"


class Alcada(Base):
    __tablename__ = "alcadas"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    valor_max: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    perfil_minimo: Mapped[PerfilUsuario] = mapped_column(
        pg_enum(PerfilUsuario, "perfil_usuario"), nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Fornecedor(Base):
    __tablename__ = "fornecedores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tipo_pessoa: Mapped[TipoPessoa] = mapped_column(pg_enum(TipoPessoa, "tipo_pessoa"), nullable=False)
    cnpj_cpf: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    razao_social: Mapped[str] = mapped_column(Text, nullable=False)
    nome_fantasia: Mapped[Optional[str]] = mapped_column(Text)
    regime_tributario: Mapped[RegimeTributario] = mapped_column(
        pg_enum(RegimeTributario, "regime_tributario"), nullable=False,
        default=RegimeTributario.NAO_INFORMADO)
    cnae_principal: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    telefone: Mapped[Optional[str]] = mapped_column(Text)
    municipio: Mapped[Optional[str]] = mapped_column(Text)
    uf: Mapped[Optional[str]] = mapped_column(Text)
    situacao_rfb: Mapped[Optional[str]] = mapped_column(Text)
    situacao_rfb_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    data_abertura: Mapped[Optional[date]] = mapped_column(Date)
    codigo_omie: Mapped[Optional[int]] = mapped_column(BigInteger, unique=True)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    contas: Mapped[list["FornecedorConta"]] = relationship(
        back_populates="fornecedor", order_by="FornecedorConta.id")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Fornecedor {self.id} {self.cnpj_cpf} {self.razao_social!r}>"


class FornecedorConta(Base):
    __tablename__ = "fornecedor_contas"
    __table_args__ = (Index("idx_fornecedor_contas_forn", "fornecedor_id", "status"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fornecedor_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    forma: Mapped[FormaPagamento] = mapped_column(pg_enum(FormaPagamento, "forma_pagamento"), nullable=False)
    pix_tipo: Mapped[Optional[str]] = mapped_column(Text)
    pix_chave: Mapped[Optional[str]] = mapped_column(Text)
    banco_codigo: Mapped[Optional[str]] = mapped_column(Text)
    agencia: Mapped[Optional[str]] = mapped_column(Text)
    conta: Mapped[Optional[str]] = mapped_column(Text)
    conta_digito: Mapped[Optional[str]] = mapped_column(Text)
    titular_nome: Mapped[Optional[str]] = mapped_column(Text)
    titular_doc: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[StatusConta] = mapped_column(
        pg_enum(StatusConta, "status_conta"), nullable=False, default=StatusConta.PENDENTE)
    homologada_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    homologada_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    motivo_bloqueio: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    fornecedor: Mapped[Fornecedor] = relationship(back_populates="contas")


class Obra(Base):
    __tablename__ = "obras"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    codigo: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    cno: Mapped[Optional[str]] = mapped_column(Text)
    municipio: Mapped[Optional[str]] = mapped_column(Text)
    uf: Mapped[Optional[str]] = mapped_column(Text)
    endereco: Mapped[Optional[str]] = mapped_column(Text)
    codigo_omie_depto: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    ref_sheets: Mapped[Optional[str]] = mapped_column(Text)
    objeto: Mapped[Optional[str]] = mapped_column(Text)
    cliente: Mapped[Optional[str]] = mapped_column(Text)
    cnpj_cliente: Mapped[Optional[str]] = mapped_column(Text)
    contrato: Mapped[Optional[str]] = mapped_column(Text)
    valor_contrato: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    aliquota_iss: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    tributacao: Mapped[Optional[str]] = mapped_column(Text)
    data_inicio: Mapped[Optional[date]] = mapped_column(Date)
    data_termino: Mapped[Optional[date]] = mapped_column(Date)
    orgao_resumido: Mapped[Optional[str]] = mapped_column(Text)
    ref_pipefy: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ATIVA")
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Obra {self.codigo} {self.nome!r}>"


class Categoria(Base):
    __tablename__ = "categorias"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    codigo: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    categoria_pai_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    codigo_omie: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    tipos_permitidos: Mapped[list[str]] = mapped_column(
        ARRAY(pg_enum(TipoTitulo, "tipo_titulo")), nullable=False, default=list)
    natureza: Mapped[str] = mapped_column(Text, nullable=False, default="RESULTADO")
    grupo_codigo: Mapped[Optional[str]] = mapped_column(Text)
    grupo_nome: Mapped[Optional[str]] = mapped_column(Text)
    subgrupo_codigo: Mapped[Optional[str]] = mapped_column(Text)
    subgrupo_nome: Mapped[Optional[str]] = mapped_column(Text)
    descricao_uso: Mapped[Optional[str]] = mapped_column(Text)
    substituida_por_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dedutivel_padrao: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    credito_pis_cofins: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    conta_contabil: Mapped[Optional[str]] = mapped_column(Text)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Categoria {self.codigo} {self.descricao!r}>"


class Contrato(Base):
    __tablename__ = "contratos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fornecedor_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    objeto: Mapped[str] = mapped_column(Text, nullable=False)
    valor_total: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    valor_parcela: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    indice_reajuste: Mapped[Optional[str]] = mapped_column(Text)
    dia_vencimento: Mapped[Optional[int]] = mapped_column(SmallInteger)
    vigencia_inicio: Mapped[date] = mapped_column(Date, nullable=False)
    vigencia_fim: Mapped[Optional[date]] = mapped_column(Date)
    retencao_contratual_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), default=0)
    arquivo_anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="VIGENTE")
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ContaBancaria(Base):
    __tablename__ = "contas_bancarias"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    banco_codigo: Mapped[str] = mapped_column(Text, nullable=False)
    agencia: Mapped[str] = mapped_column(Text, nullable=False)
    conta: Mapped[str] = mapped_column(Text, nullable=False)
    codigo_omie: Mapped[Optional[int]] = mapped_column(BigInteger, unique=True)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
