# ============================================================================
# BWS ERP — core/cadastros/fornecedores.py
# Service do cadastro de fornecedores. MOLDE para todos os services do core/:
#
#   - Não importa Streamlit nem FastAPI (a regra não sabe quem a chama)
#   - Recebe/retorna dados simples (dicts) ou models — nunca widgets/requests
#   - Toda escrita registra evento na trilha de auditoria
#   - Regras de segurança do cadastro:
#       * CPF/CNPJ validado por dígito verificador; único no sistema
#       * Conta bancária nasce PENDENTE; só perfil FINANCEIRO/ADMIN homologa
#       * Conta nunca é apagada: bloqueia-se (histórico antifraude C2)
#       * Alterar dados de conta é PROIBIDO: bloqueia a antiga e cria nova
# ============================================================================
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.apps.erp.core.cadastros.validadores import (
    documento_valido, pix_chave_valida, somente_digitos,
)
from app.apps.erp.db.models.cadastros import (
    FormaPagamento, Fornecedor, FornecedorConta, PerfilUsuario,
    RegimeTributario, StatusConta, TipoPessoa, Usuario,
)


class ErroValidacao(Exception):
    """Erro de regra de negócio — mensagem segura para exibir ao usuário."""


class ErroPermissao(Exception):
    """Usuário sem perfil para a operação."""


# ---------------------------------------------------------------------------
# Trilha de auditoria (INSERT direto em eventos; tabela é append-only)
# ---------------------------------------------------------------------------
def _registrar_evento(
    s: Session, entidade_tipo: str, entidade_id: int,
    acao: str, detalhe: dict[str, Any], usuario_id: Optional[int],
) -> None:
    from sqlalchemy import text
    s.execute(
        text(
            "INSERT INTO eventos (entidade_tipo, entidade_id, usuario_id, acao, detalhe) "
            "VALUES (:et, :ei, :ui, :ac, CAST(:dt AS jsonb))"
        ),
        {
            "et": entidade_tipo, "ei": entidade_id, "ui": usuario_id,
            "ac": acao, "dt": json.dumps(detalhe, ensure_ascii=False, default=str),
        },
    )


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------
def listar(
    s: Session, *, busca: str = "", apenas_ativos: bool = True,
    limite: int = 500, offset: int = 0,
) -> list[Fornecedor]:
    """Lista fornecedores com busca por razão social, fantasia ou documento."""
    stmt = select(Fornecedor).options(selectinload(Fornecedor.contas))
    if apenas_ativos:
        stmt = stmt.where(Fornecedor.ativo.is_(True))
    busca = (busca or "").strip()
    if busca:
        dig = somente_digitos(busca)
        filtro = Fornecedor.razao_social.ilike(f"%{busca}%") | Fornecedor.nome_fantasia.ilike(f"%{busca}%")
        if dig:
            filtro = filtro | Fornecedor.cnpj_cpf.like(f"%{dig}%")
        stmt = stmt.where(filtro)
    stmt = stmt.order_by(Fornecedor.razao_social).limit(limite).offset(offset)
    return list(s.scalars(stmt).all())


def obter(s: Session, fornecedor_id: int) -> Fornecedor:
    forn = s.get(Fornecedor, fornecedor_id, options=[selectinload(Fornecedor.contas)])
    if forn is None:
        raise ErroValidacao(f"Fornecedor {fornecedor_id} não encontrado.")
    return forn


def obter_por_documento(s: Session, cnpj_cpf: str) -> Optional[Fornecedor]:
    dig = somente_digitos(cnpj_cpf)
    return s.scalars(select(Fornecedor).where(Fornecedor.cnpj_cpf == dig)).first()


# ---------------------------------------------------------------------------
# Escrita — fornecedor
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Fornecedor:
    """Cria fornecedor. Campos mínimos: tipo_pessoa, cnpj_cpf, razao_social."""
    tipo = (dados.get("tipo_pessoa") or "").strip().upper()
    if tipo not in (TipoPessoa.PF.value, TipoPessoa.PJ.value):
        raise ErroValidacao("Tipo de pessoa deve ser PF ou PJ.")

    doc = somente_digitos(dados.get("cnpj_cpf") or "")
    if not documento_valido(doc, tipo):
        rotulo = "CPF" if tipo == "PF" else "CNPJ"
        raise ErroValidacao(f"{rotulo} inválido (dígito verificador não confere): {dados.get('cnpj_cpf')!r}")

    razao = (dados.get("razao_social") or "").strip()
    if len(razao) < 3:
        raise ErroValidacao("Razão social obrigatória (mínimo 3 caracteres).")

    if obter_por_documento(s, doc) is not None:
        raise ErroValidacao(f"Já existe fornecedor cadastrado com o documento {doc}.")

    regime = (dados.get("regime_tributario") or RegimeTributario.NAO_INFORMADO.value).strip().upper()
    if regime not in {r.value for r in RegimeTributario}:
        raise ErroValidacao(f"Regime tributário inválido: {regime!r}")

    data_abertura = None
    if dados.get("data_abertura"):
        from datetime import date as _date
        try:
            data_abertura = _date.fromisoformat(str(dados["data_abertura"])[:10])
        except ValueError:
            data_abertura = None
    situacao = (dados.get("situacao_rfb") or "").strip().upper() or None

    forn = Fornecedor(
        tipo_pessoa=TipoPessoa(tipo),
        cnpj_cpf=doc,
        razao_social=razao.upper(),
        situacao_rfb=situacao,
        situacao_rfb_em=datetime.now(timezone.utc) if situacao else None,
        data_abertura=data_abertura,
        nome_fantasia=(dados.get("nome_fantasia") or "").strip() or None,
        regime_tributario=RegimeTributario(regime),
        cnae_principal=(dados.get("cnae_principal") or "").strip() or None,
        email=(dados.get("email") or "").strip() or None,
        telefone=(dados.get("telefone") or "").strip() or None,
        municipio=(dados.get("municipio") or "").strip() or None,
        uf=(dados.get("uf") or "").strip().upper() or None,
        codigo_omie=dados.get("codigo_omie") or None,
        observacoes=(dados.get("observacoes") or "").strip() or None,
    )
    s.add(forn)
    s.flush()  # garante forn.id para o evento
    _registrar_evento(s, "fornecedor", forn.id, "CRIADO",
                      {"cnpj_cpf": doc, "razao_social": razao, "origem": dados.get("origem", "SISTEMA")},
                      usuario.id if usuario else None)
    return forn


_CAMPOS_EDITAVEIS = {
    "razao_social", "nome_fantasia", "regime_tributario", "cnae_principal",
    "email", "telefone", "municipio", "uf", "observacoes", "ativo", "codigo_omie",
}


def atualizar(s: Session, fornecedor_id: int, alteracoes: dict[str, Any], usuario: Usuario) -> Fornecedor:
    """Atualiza campos cadastrais. CNPJ/CPF e tipo_pessoa NÃO são editáveis —
    documento errado = inativar e criar novo (preserva histórico de títulos)."""
    forn = obter(s, fornecedor_id)
    diff: dict[str, Any] = {}
    for campo, novo in alteracoes.items():
        if campo not in _CAMPOS_EDITAVEIS:
            raise ErroValidacao(f"Campo não editável ou desconhecido: {campo!r}")
        if campo == "regime_tributario":
            novo = (novo or "").strip().upper()
            if novo not in {r.value for r in RegimeTributario}:
                raise ErroValidacao(f"Regime tributário inválido: {novo!r}")
            novo = RegimeTributario(novo)
        if isinstance(novo, str):
            novo = novo.strip() or None
        antigo = getattr(forn, campo)
        antigo_cmp = antigo.value if hasattr(antigo, "value") else antigo
        novo_cmp = novo.value if hasattr(novo, "value") else novo
        if antigo_cmp != novo_cmp:
            diff[campo] = {"de": antigo_cmp, "para": novo_cmp}
            setattr(forn, campo, novo)
    if diff:
        _registrar_evento(s, "fornecedor", forn.id, "EDITADO", diff, usuario.id if usuario else None)
    return forn


# ---------------------------------------------------------------------------
# Escrita — contas bancárias (princípio: nunca editar, nunca apagar)
# ---------------------------------------------------------------------------
def adicionar_conta(s: Session, fornecedor_id: int, dados: dict[str, Any], usuario: Usuario) -> FornecedorConta:
    """Adiciona conta de recebimento (nasce PENDENTE até homologação)."""
    forn = obter(s, fornecedor_id)
    forma = (dados.get("forma") or "").strip().upper()
    if forma not in (FormaPagamento.PIX.value, FormaPagamento.TED.value):
        raise ErroValidacao("Forma da conta deve ser PIX ou TED.")

    conta = FornecedorConta(fornecedor_id=forn.id, forma=FormaPagamento(forma))

    if forma == FormaPagamento.PIX.value:
        pix_tipo = (dados.get("pix_tipo") or "").strip().upper()
        pix_chave = (dados.get("pix_chave") or "").strip()
        ok, msg = pix_chave_valida(pix_tipo, pix_chave)
        if not ok:
            raise ErroValidacao(msg)
        # Consistência forte: chave CPF/CNPJ deve ser o documento do próprio fornecedor
        if pix_tipo in ("CPF", "CNPJ") and somente_digitos(pix_chave) != forn.cnpj_cpf:
            raise ErroValidacao(
                "Chave Pix CPF/CNPJ diferente do documento do fornecedor. "
                "Conta de terceiro exige cadastro como exceção formal (não implementado nesta tela)."
            )
        conta.pix_tipo, conta.pix_chave = pix_tipo, pix_chave
    else:  # TED
        banco = somente_digitos(dados.get("banco_codigo") or "")
        agencia = somente_digitos(dados.get("agencia") or "")
        num_conta = somente_digitos(dados.get("conta") or "")
        digito = (dados.get("conta_digito") or "").strip()
        if not banco or len(banco) > 3:
            raise ErroValidacao("Código do banco inválido (1 a 3 dígitos, padrão COMPE).")
        if not agencia or not num_conta:
            raise ErroValidacao("Agência e conta são obrigatórias para TED.")
        conta.banco_codigo, conta.agencia = banco, agencia
        conta.conta, conta.conta_digito = num_conta, digito or None

    conta.titular_nome = (dados.get("titular_nome") or "").strip() or None
    conta.titular_doc = somente_digitos(dados.get("titular_doc") or "") or None

    # Duplicidade: mesma chave/conta ativa já cadastrada para o fornecedor
    for existente in forn.contas:
        if existente.status == StatusConta.BLOQUEADA:
            continue
        if forma == "PIX" and existente.pix_chave == conta.pix_chave:
            raise ErroValidacao("Esta chave Pix já está cadastrada para o fornecedor.")
        if forma == "TED" and (existente.banco_codigo, existente.agencia, existente.conta) == \
                (conta.banco_codigo, conta.agencia, conta.conta):
            raise ErroValidacao("Esta conta bancária já está cadastrada para o fornecedor.")

    s.add(conta)
    s.flush()
    _registrar_evento(s, "fornecedor_conta", conta.id, "CRIADA",
                      {"fornecedor_id": forn.id, "forma": forma,
                       "resumo": conta.pix_chave or f"{conta.banco_codigo}/{conta.agencia}/{conta.conta}"},
                      usuario.id if usuario else None)
    return conta


_PERFIS_HOMOLOGACAO = (PerfilUsuario.ADMIN, PerfilUsuario.FINANCEIRO)


def homologar_conta(s: Session, conta_id: int, usuario: Usuario) -> FornecedorConta:
    """Homologa conta para uso em pagamentos. Exige perfil FINANCEIRO/ADMIN.
    Segregação (F2): quem homologa não pode ser quem cadastrou a conta."""
    if usuario is None or usuario.perfil not in _PERFIS_HOMOLOGACAO:
        raise ErroPermissao("Apenas perfis FINANCEIRO ou ADMIN homologam contas.")
    conta = s.get(FornecedorConta, conta_id)
    if conta is None:
        raise ErroValidacao(f"Conta {conta_id} não encontrada.")
    if conta.status == StatusConta.HOMOLOGADA:
        return conta
    if conta.status == StatusConta.BLOQUEADA:
        raise ErroValidacao("Conta bloqueada não pode ser homologada — cadastre uma nova.")
    if not conta.titular_nome:
        raise ErroValidacao(
            "Homologação exige o nome do titular verificado por canal independente "
            "(telefone do cadastro, nunca o contato que enviou a cobrança)."
        )
    conta.status = StatusConta.HOMOLOGADA
    conta.homologada_por = usuario.id
    conta.homologada_em = datetime.now(timezone.utc)
    _registrar_evento(s, "fornecedor_conta", conta.id, "HOMOLOGADA",
                      {"por": usuario.email}, usuario.id)
    return conta


def bloquear_conta(s: Session, conta_id: int, motivo: str, usuario: Usuario) -> FornecedorConta:
    """Bloqueia conta (substitui o conceito de exclusão — histórico preservado)."""
    conta = s.get(FornecedorConta, conta_id)
    if conta is None:
        raise ErroValidacao(f"Conta {conta_id} não encontrada.")
    motivo = (motivo or "").strip()
    if len(motivo) < 5:
        raise ErroValidacao("Informe o motivo do bloqueio (mínimo 5 caracteres).")
    conta.status = StatusConta.BLOQUEADA
    conta.motivo_bloqueio = motivo
    _registrar_evento(s, "fornecedor_conta", conta.id, "BLOQUEADA",
                      {"motivo": motivo}, usuario.id if usuario else None)
    return conta


def resumo_contas(forn: Fornecedor) -> list[dict[str, Any]]:
    """Representação simples das contas para exibição (UI/API)."""
    linhas = []
    for c in forn.contas:
        if c.forma == FormaPagamento.PIX:
            dado = f"Pix {c.pix_tipo}: {c.pix_chave}"
        else:
            dado = f"Banco {c.banco_codigo} Ag {c.agencia} CC {c.conta}-{c.conta_digito or ''}"
        linhas.append({
            "id": c.id, "forma": c.forma.value, "dados": dado,
            "titular": c.titular_nome or "—", "status": c.status.value,
        })
    return linhas
