# ============================================================================
# ERP — core/suprimentos/prontidao.py
# O que falta nos CADASTROS para o suprimento rodar sem parar no meio.
#
# Nasceu da pergunta do dono em 24/11/2026 — "o suprimentos já está pronto para
# liberar pra produção? Sofri muito com o DP (...) o maior problema foi o fluxo
# incompleto" — e da caminhada de ponta a ponta do mesmo dia, que mostrou que o
# fluxo fecha, mas para em cinco cadastros: sem conta de e-mail a cotação e o
# pedido não saem; sem conta do plano no insumo a conta a pagar não nasce; sem
# conta bancária conferida do fornecedor o Pix nasce bloqueado; sem obra ligada
# a pessoa não pede; sem alguém para liberar e autorizar, o pedido fica parado.
#
# Descobrir cada um desses no meio de uma compra de verdade é exatamente a
# semana que ele perdeu no DP. Esta tela mostra todos de uma vez, ANTES.
#
# Só lê. Uma consulta por conferência — onze, com qualquer volume.
# ============================================================================
from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.apps.erp.core.auth.permissoes import pode, ve_todas_as_obras
from app.apps.erp.db.models.cadastros import (
    CondicaoPagamento, Empresa, Fornecedor, FornecedorConta, Insumo, Obra,
    PrecoHistorico, StatusConta, Usuario, UsuarioObra,
)

logger = logging.getLogger(__name__)

OK, FALTA, ATENCAO = "OK", "FALTA", "ATENCAO"


def _ativos(lista: list) -> list:
    return [x for x in lista if getattr(x, "ativo", True) is not False]


def conferir(s: Session) -> dict[str, Any]:
    itens: list[dict[str, Any]] = []

    def item(titulo: str, situacao: str, resumo: str, por_que: str, onde: str,
             exemplos: list[str] | None = None) -> None:
        itens.append({"titulo": titulo, "situacao": situacao, "resumo": resumo,
                      "por_que": por_que, "onde": onde, "exemplos": (exemplos or [])[:12],
                      "mais": max(len(exemplos or []) - 12, 0)})

    # ---- 1. conta de e-mail da empresa
    from app.apps.erp.core.comum import email as correio
    empresas = _ativos(list(s.scalars(select(Empresa)).all()))
    sem_email = []
    for e in empresas:
        try:
            pronta, _ = correio.conta_configurada(e, s)
        except Exception:
            pronta = False
        if not pronta:
            sem_email.append(e.nome_fantasia or e.razao_social)
    item("Conta de e-mail da empresa",
         FALTA if sem_email else OK,
         f"{len(sem_email)} de {len(empresas)} empresa(s) sem conta de e-mail completa"
         if sem_email else "todas as empresas enviam e-mail",
         "Sem ela a cotação e o pedido de compra não saem para o fornecedor.",
         "Administração › Empresas", sem_email)

    # ---- 2. insumo sem conta do plano
    insumos = _ativos(list(s.scalars(select(Insumo)).all()))
    sem_conta = [i.descricao for i in insumos if not i.categoria_id]
    item("Insumos com a conta do plano financeiro",
         (FALTA if sem_conta else OK) if insumos else FALTA,
         "nenhum insumo cadastrado" if not insumos else
         (f"{len(sem_conta)} de {len(insumos)} insumo(s) sem conta do plano"
          if sem_conta else f"os {len(insumos)} insumos têm conta do plano"),
         "Sem a conta, a obra recebe o material mas a conta a pagar não nasce.",
         "Suprimentos › Cadastros › Insumos", sem_conta)

    # ---- 3 e 4. fornecedores
    fornecedores = _ativos(list(s.scalars(select(Fornecedor)).all()))
    sem_contato = [f.razao_social for f in fornecedores if not (f.email or "").strip()]
    item("Fornecedores com e-mail",
         (ATENCAO if sem_contato else OK) if fornecedores else FALTA,
         "nenhum fornecedor cadastrado" if not fornecedores else
         (f"{len(sem_contato)} de {len(fornecedores)} sem e-mail no cadastro"
          if sem_contato else "todos têm e-mail"),
         "Fornecedor sem e-mail não recebe a cotação nem o pedido pelo sistema "
         "(o e-mail também pode estar num contato dele — esta conta olha só o do cadastro).",
         "Suprimentos › Cadastros › Fornecedores", sem_contato)
    com_banco = set(s.scalars(select(FornecedorConta.fornecedor_id).where(
        FornecedorConta.status == StatusConta.HOMOLOGADA)).all())
    sem_banco = [f.razao_social for f in fornecedores if f.id not in com_banco]
    item("Fornecedores com conta bancária ou Pix conferido",
         ATENCAO if sem_banco else OK,
         f"{len(sem_banco)} de {len(fornecedores)} sem conta conferida"
         if sem_banco else "todos têm conta conferida",
         "Pagamento por Pix ou transferência para fornecedor sem conta conferida "
         "nasce bloqueado no financeiro. Boleto não precisa.",
         "Cadastros › Fornecedores › contas", sem_banco)

    # ---- 5. obras
    obras = [o for o in s.scalars(select(Obra)).all()
             if str(getattr(o, "status", "") or "").upper() != "ENCERRADA"]
    sem_endereco = [o.codigo for o in obras if not (o.endereco or "").strip()
                    or not (o.municipio or "").strip()]
    item("Obras com endereço de entrega",
         FALTA if sem_endereco else OK,
         f"{len(sem_endereco)} de {len(obras)} obra(s) sem endereço completo"
         if sem_endereco else "todas têm endereço",
         "O endereço vai impresso na cotação e no pedido: é onde o material desce.",
         "Obras › cadastro da obra", sem_endereco)
    sem_cno = [o.codigo for o in obras if not (getattr(o, "cno", "") or "").strip()]
    item("Obras com CNO",
         ATENCAO if sem_cno else OK,
         f"{len(sem_cno)} de {len(obras)} obra(s) sem CNO" if sem_cno else "todas têm CNO",
         "O CNO sai no pedido para o fornecedor pôr na nota. Sem ele, o pedido sai sem.",
         "Obras › cadastro da obra", sem_cno)
    sem_empresa = [o.codigo for o in obras if not o.empresa_id]
    item("Obras ligadas a uma empresa",
         FALTA if sem_empresa else OK,
         f"{len(sem_empresa)} obra(s) sem empresa" if sem_empresa else "todas ligadas",
         "É a empresa da obra que diz por qual CNPJ a compra é faturada e de qual e-mail sai.",
         "Administração › Empresas", sem_empresa)

    # ---- 6. pessoas
    usuarios = _ativos(list(s.scalars(select(Usuario)).all()))
    ligacoes: dict[int, set[int]] = {}
    for l in s.scalars(select(UsuarioObra)).all():
        ligacoes.setdefault(l.usuario_id, set()).add(l.obra_id)
    sem_obra = [u.nome for u in usuarios
                if pode(u, "solicitar_suprimento") and not ve_todas_as_obras(u)
                and not ligacoes.get(u.id)]
    item("Quem pede material tem obra ligada",
         ATENCAO if sem_obra else OK,
         f"{len(sem_obra)} pessoa(s) que pedem material sem obra ligada"
         if sem_obra else "todos têm obra",
         "Só se pede material para as próprias obras. Quem não tem obra no cadastro "
         "só consegue pedir para a obra onde bate ponto — ou para nenhuma.",
         "Configurações › Operadores", sem_obra)

    def quem_alcanca(acao: str, obra_id: int) -> list[Usuario]:
        return [u for u in usuarios if pode(u, acao)
                and (ve_todas_as_obras(u) or obra_id in ligacoes.get(u.id, set()))]
    sem_liberador = [o.codigo for o in obras if not quem_alcanca("liberar_solicitacao", o.id)]
    item("Toda obra tem quem libere o pedido (engenharia)",
         FALTA if sem_liberador else OK,
         f"{len(sem_liberador)} obra(s) sem ninguém para dar o ok" if sem_liberador
         else "todas têm quem libere",
         "O pedido do administrativo espera o ok de um gestor ou supervisor da obra. "
         "Sem ninguém, só o administrador consegue liberar.",
         "Configurações › Operadores (ligar o gestor ou supervisor à obra)", sem_liberador)
    autorizam = [u.nome for u in usuarios if pode(u, "autorizar_pedido")]
    compram = [u.nome for u in usuarios if pode(u, "comprar")]
    item("Quem compra e quem autoriza",
         FALTA if not compram or not autorizam else (ATENCAO if len(set(autorizam)) < 2 else OK),
         f"compram: {len(compram)} · autorizam: {len(autorizam)}",
         "Quem fecha o pedido não autoriza o próprio (só o administrador pode). "
         "Com uma pessoa só, tudo depende dela.",
         "Configurações › Operadores", sorted(set(compram + autorizam)))

    # ---- 7. o resto
    condicoes = _ativos(list(s.scalars(select(CondicaoPagamento)).all()))
    item("Condições de pagamento", OK if condicoes else FALTA,
         f"{len(condicoes)} cadastrada(s)",
         "É a condição que gera a previsão de pagamento do pedido.",
         "Suprimentos › Cadastros › Condições")
    precos = s.execute(select(func.count(PrecoHistorico.id))).scalar() or 0
    item("Histórico de preços", ATENCAO if precos == 0 else OK,
         f"{precos} preço(s) registrados",
         "Sem histórico, a previsão do que ainda não foi cotado e o relatório de "
         "preço contra a compra anterior começam vazios.",
         "Suprimentos › Cadastros › importar o banco de preços")
    tem_ia = bool((os.environ.get("OPENAI_API_KEY") or "").strip())
    item("Leitura automática (IA)", OK if tem_ia else ATENCAO,
         "configurada" if tem_ia else "sem chave configurada neste servidor",
         "É ela que lê a nota, o boleto, a proposta do fornecedor e a lista colada. "
         "Sem ela, tudo funciona digitando.", "variável OPENAI_API_KEY no servidor")
    from app.apps.erp.core.suprimentos import fora_do_fluxo
    try:
        with s.begin_nested():
            modo = fora_do_fluxo.estado(s)[0]
    except Exception:
        modo = None
    item("Compra feita por fora do suprimento",
         OK if modo == "TRAVAR" else ATENCAO,
         {"TRAVAR": "travando até a ciência", "AVISAR": "só avisando",
          None: "não está sendo acompanhada"}[modo],
         "O combinado é travar: a conta de material sem pedido não segue para "
         "pagamento antes de alguém dar a ciência.",
         "Suprimentos › Painel (só o administrador liga)")

    return {"itens": itens,
            "faltam": len([i for i in itens if i["situacao"] == FALTA]),
            "atencao": len([i for i in itens if i["situacao"] == ATENCAO]),
            "prontos": len([i for i in itens if i["situacao"] == OK])}
