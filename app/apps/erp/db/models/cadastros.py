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
    ADMIN = "ADMIN"                          # tudo, inclusive configurações
    DIRETOR_FINANCEIRO = "DIRETOR_FINANCEIRO"  # vê tudo e avaliza qualquer título
    FINANCEIRO = "FINANCEIRO"                # opera o sistema, não configura
    GESTOR_OBRA = "GESTOR_OBRA"              # lança e acompanha TODAS as obras
    SUPERVISOR_OBRA = "SUPERVISOR_OBRA"      # lança e acompanha as obras designadas
    ADMINISTRATIVO_OBRA = "ADMINISTRATIVO_OBRA"   # lança e acompanha o que ele lançou
    DEPARTAMENTO_PESSOAL = "DEPARTAMENTO_PESSOAL"  # revisa despesas com colaboradores
    APROVADOR = "APROVADOR"
    LANCADOR = "LANCADOR"
    CONSULTA = "CONSULTA"


class EscopoVisao(str, enum.Enum):
    """Alcance da visão do operador — configuração por PESSOA, não por cargo.

    Só tem efeito nos perfis que filtram por autoria (ADMINISTRATIVO_OBRA e
    LANCADOR). Quem já enxerga tudo continua enxergando; supervisor mantém a
    regra dele.
    """
    PROPRIOS = "PROPRIOS"                    # só o que a própria pessoa lançou
    OBRAS_DESIGNADAS = "OBRAS_DESIGNADAS"    # tudo das obras associadas a ela


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
    # Alcance da visão: ver EscopoVisao. Default no mais restritivo, para que
    # esquecer de configurar feche em vez de abrir.
    escopo_visao: Mapped[EscopoVisao] = mapped_column(
        pg_enum(EscopoVisao, "escopo_visao_usuario"), nullable=False,
        default=EscopoVisao.PROPRIOS, server_default="PROPRIOS")
    cpf: Mapped[Optional[str]] = mapped_column(Text)
    telefone: Mapped[Optional[str]] = mapped_column(Text)
    # fundo fixo: alçada de quem gasta, não do sistema
    ff_teto_item: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    ff_teto_prestacao: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    ff_saldo_adiantamento: Mapped[Decimal] = mapped_column(
        Numeric(14, 2), nullable=False, default=0)
    ff_autorizado: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Usuario {self.id} {self.email} {self.perfil}>"


class Parametro(Base):
    """Configuração do sistema, uma linha por chave (ex.: teto mensal de IA).

    Não é lugar de credencial — isso continua na Environment do Render. É
    para número e escolha que o ADMIN ajusta pela tela e que não justificam
    uma coluna própria em tabela nenhuma.
    """
    __tablename__ = "parametros"

    chave: Mapped[str] = mapped_column(Text, primary_key=True)
    valor: Mapped[str] = mapped_column(Text, nullable=False, default="")
    atualizado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
    atualizado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))


class UsuarioPermissao(Base):
    """Exceção de permissão marcada no cadastro de UMA pessoa.

    O cargo (`Usuario.perfil`) é a base e responde por tudo que já existe.
    Aqui ficam só as marcações feitas à mão: `concedida=True` acrescenta uma
    ação que o cargo não dá ("o Pedro também autoriza pedido"); `concedida=False`
    tira uma que o cargo daria ("a Ana não paga"). Sem linha, vale o cargo.

    É o que permite deixar outra pessoa autorizando enquanto o diretor está de
    férias, sem inventar um cargo novo para isso.
    """
    __tablename__ = "usuario_permissoes"

    usuario_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("usuarios.id", ondelete="CASCADE"), primary_key=True)
    acao: Mapped[str] = mapped_column(Text, primary_key=True)
    concedida: Mapped[bool] = mapped_column(Boolean, nullable=False)
    definida_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
    definida_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))


class UsuarioObra(Base):
    """Obras que o supervisor enxerga. Gestor vê todas; administrativo de obra
    vê o que ele mesmo lançou."""
    __tablename__ = "usuario_obras"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


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


class FornecedorPorte(str, enum.Enum):
    """Onde o fornecedor está na cadeia. Quanto mais perto da fábrica, melhor
    o preço — é o que o comprador usa para escolher quem cotar."""
    FABRICA = "FABRICA"
    REP_FABRICA = "REP_FABRICA"
    DISTRIBUIDOR = "DISTRIBUIDOR"
    LOCAL = "LOCAL"
    HOMECENTER = "HOMECENTER"


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
    e_cliente: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    e_fornecedor: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Suprimentos (migração 033). Região e canal são LISTAS porque na prática
    # já são: um fornecedor atende "CE, RMF" e aceita cotação por e-mail e por
    # WhatsApp ao mesmo tempo.
    porte: Mapped[Optional[FornecedorPorte]] = mapped_column(
        pg_enum(FornecedorPorte, "fornecedor_porte"))
    regioes_atuacao: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, default=list, server_default="{}")
    canais_cotacao: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, default=lambda: ["EMAIL"], server_default="{EMAIL}")

    contas: Mapped[list["FornecedorConta"]] = relationship(
        back_populates="fornecedor", order_by="FornecedorConta.id")
    contatos: Mapped[list["FornecedorContato"]] = relationship(
        order_by="FornecedorContato.id", cascade="all, delete-orphan")

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
    # endereço completo (serve de local de entrega nas compras)
    cep: Mapped[Optional[str]] = mapped_column(Text)
    bairro: Mapped[Optional[str]] = mapped_column(Text)
    numero_endereco: Mapped[Optional[str]] = mapped_column(Text)
    complemento: Mapped[Optional[str]] = mapped_column(Text)
    codigo_ibge: Mapped[Optional[str]] = mapped_column(Text)
    responsavel_tecnico: Mapped[Optional[str]] = mapped_column(Text)
    art_rrt: Mapped[Optional[str]] = mapped_column(Text)
    engenheiro_fiscal: Mapped[Optional[str]] = mapped_column(Text)
    # contrato
    vigencia_inicio: Mapped[Optional[date]] = mapped_column(Date)
    vigencia_fim: Mapped[Optional[date]] = mapped_column(Date)
    prazo_execucao_dias: Mapped[Optional[int]] = mapped_column(Integer)
    data_base_orcamento: Mapped[Optional[date]] = mapped_column(Date)
    indice_reajuste: Mapped[Optional[str]] = mapped_column(Text)
    conta_recebimento_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contas_bancarias.id"))
    ordem_servico: Mapped[Optional[str]] = mapped_column(Text)
    data_ordem_servico: Mapped[Optional[date]] = mapped_column(Date)
    # tributação da nota
    aliquota_iss_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4))
    iss_retido: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    aceita_deducao_material: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    pct_servico_iss: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    pct_servico_inss: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    inss_retido: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    federais_retidos: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    regime_obra: Mapped[Optional[str]] = mapped_column(Text)
    observacoes_fiscais: Mapped[Optional[str]] = mapped_column(Text)
    fase: Mapped[str] = mapped_column(Text, nullable=False, default="CRIACAO")
    fase_desde: Mapped[Optional[date]] = mapped_column(Date)
    seguro_garantia: Mapped[Optional[str]] = mapped_column(Text)
    seguro_vigencia_fim: Mapped[Optional[date]] = mapped_column(Date)
    caucao_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    retencao_contratual_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    crea_obra: Mapped[Optional[str]] = mapped_column(Text)
    cei_obra: Mapped[Optional[str]] = mapped_column(Text)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    conta_bancaria_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("contas_bancarias.id"))
    latitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    longitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    data_conclusao: Mapped[Optional[date]] = mapped_column(Date)
    data_recebimento_provisorio: Mapped[Optional[date]] = mapped_column(Date)
    data_recebimento_definitivo: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ATIVA")
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Obra {self.codigo} {self.nome!r}>"


class ObraFase(Base):
    """Histórico de fases da obra — o acompanhamento que hoje vive no pipe."""
    __tablename__ = "obra_fases"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    fase: Mapped[str] = mapped_column(Text, nullable=False)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    usuario_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


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
    personalizada: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
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


class ObraAditivo(Base):
    """Aditivo de valor e/ou prazo. O valor do contrato vigente é o original
    mais a soma dos aditivos — o histórico de cada alteração fica preservado."""
    __tablename__ = "obra_aditivos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    numero: Mapped[str] = mapped_column(Text, nullable=False)
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    dias: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    nova_vigencia_fim: Mapped[Optional[date]] = mapped_column(Date)
    data_assinatura: Mapped[Optional[date]] = mapped_column(Date)
    objeto: Mapped[Optional[str]] = mapped_column(Text)
    anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    criado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class UsuarioCategoria(Base):
    """Contas do plano liberadas para o operador. Lista vazia = todas."""
    __tablename__ = "usuario_categorias"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    categoria_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("categorias.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsumoCategoria(Base):
    """Categoria de suprimentos — distinta da conta do plano financeiro."""
    __tablename__ = "insumo_categorias"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    codigo: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Insumo(Base):
    """Cadastro único de insumos, base para suprimentos e locação.

    O mesmo item pode ser comprado ou locado: a marca `locavel` é o que faz
    ele aparecer na lista de locação, sem precisar de uma segunda base.
    Carrega as duas categorias: a de suprimento e a conta do plano financeiro.
    """
    __tablename__ = "insumos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    codigo: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    categoria_insumo_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("insumo_categorias.id"))
    categoria_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("categorias.id"))
    unidade: Mapped[Optional[str]] = mapped_column(Text)
    locavel: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    valor_referencia_compra: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    valor_referencia_locacao: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    categoria_insumo: Mapped[Optional[InsumoCategoria]] = relationship()


# ---------------------------------------------------------------------------
# Suprimentos — cadastros (migração 033). A especificação está em
# app/apps/erp/SUPRIMENTOS.md.
# ---------------------------------------------------------------------------
class StatusSolicitacaoInsumo(str, enum.Enum):
    PENDENTE = "PENDENTE"
    CADASTRADO = "CADASTRADO"
    RECUSADO = "RECUSADO"


class UnidadeCompra(Base):
    """Unidade em que se compra: unidade, metro, saco, vara, carrada…"""
    __tablename__ = "unidades_compra"

    codigo: Mapped[str] = mapped_column(Text, primary_key=True)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CondicaoPagamento(Base):
    """Como se paga, guardado como REGRA e não como texto.

    Duas informações dão conta dos 121 arranjos que a planilha tinha: quanto
    entra na hora (`entrada_percentual`) e em quantos dias vencem as demais
    parcelas (`dias`). "30% + 28/56" é entrada 30 e dias [28, 56]; "6x
    parcelas" é entrada 0 e dias [30, 60, …, 180]. Quem gera as parcelas a
    partir disso é `core/suprimentos/pagamento.py`.
    """
    __tablename__ = "condicoes_pagamento"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    entrada_percentual: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=0)
    dias: Mapped[list[int]] = mapped_column(ARRAY(Integer), nullable=False, default=list)
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FornecedorCategoria(Base):
    """O que este fornecedor vende. Sem isso, cotar cimento manda e-mail para
    quem vende cabo elétrico — e o fornecedor para de responder."""
    __tablename__ = "fornecedor_categorias"

    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id", ondelete="CASCADE"), primary_key=True)
    categoria_insumo_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("insumo_categorias.id", ondelete="CASCADE"), primary_key=True)


class FornecedorContato(Base):
    """O cotador: a pessoa que responde pelo fornecedor.

    São vários por fornecedor, com função diferente, e o mapa de cotação
    precisa registrar qual deles mandou cada proposta.
    """
    __tablename__ = "fornecedor_contatos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id", ondelete="CASCADE"), nullable=False)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    funcao: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    telefone: Mapped[Optional[str]] = mapped_column(Text)
    recebe_cotacao: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class InsumoSolicitacao(Base):
    """Pedido para cadastrar um insumo que ainda não existe.

    Cadastro aberto a todos produz duplicidade e nomenclatura inconsistente —
    e aí os relatórios param de significar coisa alguma. Quem precisa pede;
    quem responde por suprimentos decide o nome, a categoria e a conta.
    """
    __tablename__ = "insumo_solicitacoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    descricao: Mapped[str] = mapped_column(Text, nullable=False)
    justificativa: Mapped[Optional[str]] = mapped_column(Text)
    unidade: Mapped[Optional[str]] = mapped_column(Text, ForeignKey("unidades_compra.codigo"))
    solicitante_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"), nullable=False)
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    status: Mapped[StatusSolicitacaoInsumo] = mapped_column(
        pg_enum(StatusSolicitacaoInsumo, "status_solicitacao_insumo"),
        nullable=False, default=StatusSolicitacaoInsumo.PENDENTE)
    insumo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("insumos.id"))
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    decidido_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    decidido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    avisado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PrioridadeSolicitacao(str, enum.Enum):
    """A empresa nem sempre consegue comprar tudo. A prioridade é o que permite
    ao comprador focar no que, se atrasar, para a obra."""
    ALTA = "ALTA"
    MEDIA = "MEDIA"
    NORMAL = "NORMAL"


class StatusItemSuprimento(str, enum.Enum):
    """As 15 situações que a planilha usa hoje. O acompanhamento é POR ITEM:
    os itens de uma mesma solicitação seguem caminhos diferentes."""
    SOLICITACAO = "SOLICITACAO"
    SALA_TECNICA = "SALA_TECNICA"
    COTACAO = "COTACAO"
    ANALISE_PROPOSTAS = "ANALISE_PROPOSTAS"
    AUTORIZACAO = "AUTORIZACAO"
    PEDIDO_EMITIDO = "PEDIDO_EMITIDO"
    ALMOXARIFADO = "ALMOXARIFADO"
    AGUARDANDO_COLETA = "AGUARDANDO_COLETA"
    AGUARDANDO_ENTREGA = "AGUARDANDO_ENTREGA"
    EM_TRANSITO = "EM_TRANSITO"
    ENTREGUE = "ENTREGUE"
    RECEBIDO = "RECEBIDO"
    PENDENCIA = "PENDENCIA"
    CANCELADO = "CANCELADO"
    SUSPENSO = "SUSPENSO"


class SuprimentoSolicitacao(Base):
    """O pedido de material feito pela obra ou pela sala técnica.

    O `titulo` é texto livre ("armadura da fundação") e é o que torna a
    solicitação localizável depois — é por ele que se procura.
    """
    __tablename__ = "suprimento_solicitacoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    previsao_entrega: Mapped[Optional[date]] = mapped_column(Date)
    prioridade: Mapped[PrioridadeSolicitacao] = mapped_column(
        pg_enum(PrioridadeSolicitacao, "prioridade_solicitacao"),
        nullable=False, default=PrioridadeSolicitacao.NORMAL)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    solicitante_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("usuarios.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    itens: Mapped[list["SuprimentoItem"]] = relationship(
        back_populates="solicitacao", order_by="SuprimentoItem.numero",
        cascade="all, delete-orphan")


class SuprimentoItem(Base):
    """Uma linha da solicitação: insumo, especificação, quantidade e OBRA.

    A obra é do item, não da solicitação — uma mesma solicitação pode pedir
    material para obras diferentes, e o relatório ao fornecedor separa por
    endereço de entrega.

    `especificacao` é texto livre e não é enfeite: o catálogo guarda "Tarucel
    p/ Junta de Dilatação" e a especificação guarda "6mm". Sem ela, o catálogo
    precisaria de uma entrada para cada variação.
    """
    __tablename__ = "suprimento_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    solicitacao_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("suprimento_solicitacoes.id", ondelete="CASCADE"),
        nullable=False)
    numero: Mapped[int] = mapped_column(Integer, nullable=False)
    insumo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("insumos.id"), nullable=False)
    especificacao: Mapped[Optional[str]] = mapped_column(Text)
    quantidade: Mapped[Decimal] = mapped_column(Numeric(14, 3), nullable=False)
    quantidade_recebida: Mapped[Decimal] = mapped_column(
        Numeric(14, 3), nullable=False, default=0)
    unidade: Mapped[str] = mapped_column(
        Text, ForeignKey("unidades_compra.codigo"), nullable=False)
    obra_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("obras.id"), nullable=False)
    status: Mapped[StatusItemSuprimento] = mapped_column(
        pg_enum(StatusItemSuprimento, "status_item_suprimento"),
        nullable=False, default=StatusItemSuprimento.SOLICITACAO)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    solicitacao: Mapped[SuprimentoSolicitacao] = relationship(back_populates="itens")

    @property
    def saldo(self) -> Decimal:
        """O que falta chegar. É isto que a pendência é: saldo do próprio item,
        e não um registro novo em outra tabela."""
        return (self.quantidade or Decimal(0)) - (self.quantidade_recebida or Decimal(0))


class StatusCotacao(str, enum.Enum):
    ABERTA = "ABERTA"
    FECHADA = "FECHADA"
    CANCELADA = "CANCELADA"


class ModoEntrega(str, enum.Enum):
    """Quem leva o material: o fornecedor entrega ou a obra coleta. Muda a
    logística e às vezes é o que decide a compra."""
    ENTREGA = "ENTREGA"
    COLETA = "COLETA"


class OrigemPreco(str, enum.Enum):
    """Como o preço entrou no mapa. HERDADO precisa aparecer na tela: comprar
    com base num preço de três meses atrás sem perceber é o risco."""
    DIGITADO = "DIGITADO"
    IA = "IA"
    HERDADO = "HERDADO"


class TipoPreco(str, enum.Enum):
    """COTADO é o que o fornecedor ofereceu; COMPRADO é o que a empresa aceitou
    pagar. O comprado vale mais ao julgar se um preço novo está bom."""
    COTADO = "COTADO"
    COMPRADO = "COMPRADO"


class Cotacao(Base):
    """A rodada de cotação. Dela nasce o mapa."""
    __tablename__ = "cotacoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[StatusCotacao] = mapped_column(
        pg_enum(StatusCotacao, "status_cotacao"), nullable=False,
        default=StatusCotacao.ABERTA)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    criado_por: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class CotacaoItem(Base):
    """A linha do mapa: o item da solicitação que está sendo cotado."""
    __tablename__ = "cotacao_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cotacao_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cotacoes.id", ondelete="CASCADE"), nullable=False)
    suprimento_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("suprimento_itens.id"), nullable=False)
    numero: Mapped[int] = mapped_column(Integer, nullable=False)


class CotacaoFornecedor(Base):
    """A coluna do mapa: o fornecedor e as condições que ele ofereceu.

    Forma de pagamento, frete, desconto e modo de entrega vivem aqui porque
    todos mudam o preço final — comparar só o preço unitário engana.
    """
    __tablename__ = "cotacao_fornecedores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cotacao_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cotacoes.id", ondelete="CASCADE"), nullable=False)
    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    contato_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("fornecedor_contatos.id"))
    condicao_pagamento_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("condicoes_pagamento.id"))
    entrega: Mapped[Optional[ModoEntrega]] = mapped_column(
        pg_enum(ModoEntrega, "modo_entrega"))
    frete: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    desconto: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    acrescimo_percentual: Mapped[Decimal] = mapped_column(
        Numeric(6, 3), nullable=False, default=0)
    respondido_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    respondido_por: Mapped[Optional[str]] = mapped_column(Text)
    anexo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("anexos.id"))
    ordem: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class CotacaoPreco(Base):
    """O preço de um item para um fornecedor — uma célula do mapa."""
    __tablename__ = "cotacao_precos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    cotacao_fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cotacao_fornecedores.id", ondelete="CASCADE"),
        nullable=False)
    cotacao_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cotacao_itens.id", ondelete="CASCADE"), nullable=False)
    preco_unitario: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    observacao: Mapped[Optional[str]] = mapped_column(Text)
    origem: Mapped[OrigemPreco] = mapped_column(
        pg_enum(OrigemPreco, "origem_preco"), nullable=False, default=OrigemPreco.DIGITADO)
    herdado_de_cotacao_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("cotacoes.id"))
    registrado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())


class PrecoHistorico(Base):
    """O banco de preços: tudo que já foi cotado e comprado.

    É o que responde "este preço está bom?" na hora de fechar, sem depender da
    memória de ninguém — e é de onde sai o preço herdado de uma cotação
    anterior.
    """
    __tablename__ = "precos_historico"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    insumo_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("insumos.id"), nullable=False)
    especificacao: Mapped[Optional[str]] = mapped_column(Text)
    unidade: Mapped[Optional[str]] = mapped_column(Text, ForeignKey("unidades_compra.codigo"))
    preco_unitario: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)
    quantidade: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 3))
    fornecedor_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    condicao_pagamento_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("condicoes_pagamento.id"))
    tipo: Mapped[TipoPreco] = mapped_column(pg_enum(TipoPreco, "tipo_preco"), nullable=False)
    cotacao_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("cotacoes.id"))
    data: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class StatusPedidoCompra(str, enum.Enum):
    AGUARDANDO_AUTORIZACAO = "AGUARDANDO_AUTORIZACAO"
    AUTORIZADO = "AUTORIZADO"
    RECUSADO = "RECUSADO"
    CANCELADO = "CANCELADO"


class PedidoCompra(Base):
    """O pedido fechado com um fornecedor.

    Nasce de duas formas — do mapa (e aí quem autoriza vê as alternativas que o
    comprador tinha) ou direto, sem mapa — e as duas vão para a mesma fila de
    autorização. Nasce SEM autorização: o comprador não compra sozinho.
    """
    __tablename__ = "pedidos_compra"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    numero: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    cotacao_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("cotacoes.id"))
    fornecedor_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fornecedores.id"), nullable=False)
    contato_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("fornecedor_contatos.id"))
    condicao_pagamento_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("condicoes_pagamento.id"))
    entrega: Mapped[Optional[ModoEntrega]] = mapped_column(pg_enum(ModoEntrega, "modo_entrega"))
    frete: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    desconto: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    previsao_entrega: Mapped[Optional[date]] = mapped_column(Date)
    antecipado: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    codigo_barras: Mapped[Optional[str]] = mapped_column(Text)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[StatusPedidoCompra] = mapped_column(
        pg_enum(StatusPedidoCompra, "status_pedido_compra"), nullable=False,
        default=StatusPedidoCompra.AGUARDANDO_AUTORIZACAO)
    motivo: Mapped[Optional[str]] = mapped_column(Text)
    criado_por: Mapped[int] = mapped_column(BigInteger, ForeignKey("usuarios.id"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    autorizado_por: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("usuarios.id"))
    autorizado_em: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class PedidoItem(Base):
    """A linha do pedido, com o preço que foi fechado."""
    __tablename__ = "pedido_itens"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pedido_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pedidos_compra.id", ondelete="CASCADE"), nullable=False)
    suprimento_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("suprimento_itens.id"), nullable=False)
    numero: Mapped[int] = mapped_column(Integer, nullable=False)
    quantidade: Mapped[Decimal] = mapped_column(Numeric(14, 3), nullable=False)
    preco_unitario: Mapped[Decimal] = mapped_column(Numeric(14, 4), nullable=False)


class PedidoItemReserva(Base):
    """A garantia, no banco, de que um item não entra em dois pedidos vivos.

    A chave primária é a regra: o item entra aqui quando o pedido nasce e sai
    quando o pedido é recusado ou cancelado.
    """
    __tablename__ = "pedido_item_reserva"

    suprimento_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("suprimento_itens.id", ondelete="CASCADE"),
        primary_key=True)
    pedido_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pedidos_compra.id", ondelete="CASCADE"), nullable=False)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PrevisaoPagamento(Base):
    """A obrigação que nasce com a autorização do pedido.

    Não é título: vira título quando a nota fiscal chegar. Guardar separado
    deixa claro o que já é dívida documentada e o que ainda é compromisso.
    """
    __tablename__ = "previsoes_pagamento"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    pedido_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pedidos_compra.id", ondelete="CASCADE"), nullable=False)
    numero: Mapped[int] = mapped_column(Integer, nullable=False)
    vencimento: Mapped[date] = mapped_column(Date, nullable=False)
    valor: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    entrada: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    titulo_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("titulos.id"))
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Funcao(Base):
    """Função na obra, com a diária de referência."""
    __tablename__ = "funcoes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    valor_diaria: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Colaborador(Base):
    """Cadastro enxuto: o suficiente para pagar e para criticar o que se paga."""
    __tablename__ = "colaboradores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    cpf: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    matricula: Mapped[Optional[str]] = mapped_column(Text)
    funcao_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("funcoes.id"))
    obra_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("obras.id"))
    admissao: Mapped[Optional[date]] = mapped_column(Date)
    demissao: Mapped[Optional[date]] = mapped_column(Date)
    regime: Mapped[str] = mapped_column(Text, nullable=False, default="CLT")
    valor_diaria: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    aux_alimentacao: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    aux_transporte: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    pix_chave: Mapped[Optional[str]] = mapped_column(Text)
    pix_tipo: Mapped[Optional[str]] = mapped_column(Text)
    banco: Mapped[Optional[str]] = mapped_column(Text)
    agencia: Mapped[Optional[str]] = mapped_column(Text)
    conta: Mapped[Optional[str]] = mapped_column(Text)
    telefone: Mapped[Optional[str]] = mapped_column(Text)
    situacao: Mapped[str] = mapped_column(Text, nullable=False, default="ATIVO")
    ref_pipefy: Mapped[Optional[str]] = mapped_column(Text)
    observacoes: Mapped[Optional[str]] = mapped_column(Text)
    criado_em: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    atualizado_em: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())

    funcao: Mapped[Optional[Funcao]] = relationship()
    obra: Mapped[Optional[Obra]] = relationship()
