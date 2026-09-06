# ============================================================================
# ERP — core/auth/permissoes.py
# Quem pode o quê, e principalmente QUEM VÊ O QUÊ.
#
# ADMIN                 tudo, inclusive configurações e plano de contas
# FINANCEIRO            opera o sistema inteiro (lança, aprova, paga, concilia),
#                       mas não mexe em configuração
# GESTOR_OBRA           lança e acompanha TODAS as obras; não paga nem configura
# SUPERVISOR_OBRA       lança e acompanha as obras designadas a ele
# ADMINISTRATIVO_OBRA   lança e acompanha o que ELE MESMO lançou — ou tudo das
#                       obras designadas, se assim estiver configurado no
#                       cadastro dele (campo escopo_visao, por PESSOA)
# APROVADOR / LANCADOR / CONSULTA   perfis herdados, mantidos
#
# O escopo não é enfeite de tela: ele entra na consulta, então o que está fora
# do alcance do usuário nem chega ao navegador.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroPermissao
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, PerfilUsuario, Usuario, UsuarioObra,
)
from app.apps.erp.db.models.financeiro import Rateio, Titulo

P = PerfilUsuario

# ação → perfis autorizados
PERMISSOES: dict[str, set[PerfilUsuario]] = {
    # Leitura geral das telas do ERP. Não é "qualquer um": é a declaração
    # consciente de que a rota é aberta a todo operador, e o que ela devolve
    # é limitado por ESCOPO DE OBJETO, não por alçada. Toda rota tem de
    # declarar alguma ação — esta existe para que "aberto a todos" seja uma
    # escolha escrita, e não o silêncio de quem esqueceu.
    "ver_erp":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL,
                        P.APROVADOR, P.LANCADOR, P.CONSULTA},
    "lancar":          {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.LANCADOR,
                        P.DEPARTAMENTO_PESSOAL},
    "avalizar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.GESTOR_OBRA, P.SUPERVISOR_OBRA},
    "aprovar":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.APROVADOR},
    "pagar":           {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "conciliar":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "receber":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "reclassificar":   {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "desfazer":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "importar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    "ver_dados_pagamento": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO,
                            P.GESTOR_OBRA, P.SUPERVISOR_OBRA, P.APROVADOR},
    "configurar":      {P.ADMIN},
    "gerir_usuarios":  {P.ADMIN},
    "ver_relatorios":  {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA},
    # Pessoal: o DP revisa a despesa com colaborador depois do supervisor,
    # porque só ele conhece o cadastro e sabe se a verba é devida
    "ver_pessoal":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL},
    "lancar_dc":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL},
    "editar_colaboradores": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.DEPARTAMENTO_PESSOAL},
    # Suprimentos. Não existe cargo de "comprador" no ERP, e não vai existir:
    # pela decisão de 04/09/2026, quem compra e quem autoriza pedido ganham a
    # ação MARCADA no cadastro, uma pessoa de cada vez. O padrão abaixo é
    # deliberadamente estreito — pedir material é de todo mundo da obra,
    # comprar e autorizar não são de ninguém por herança de cargo.
    "ver_suprimentos":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                            P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.CONSULTA},
    "solicitar_suprimento": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.GESTOR_OBRA,
                             P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA},
    "comprar":             {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "autorizar_pedido":    {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "administrar_insumos": {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "administrar_fornecedores": {P.ADMIN, P.DIRETOR_FINANCEIRO},
    # A fila de pedidos serve a DOIS papéis: quem compra acompanha o que fechou,
    # quem autoriza libera. Ver a seção de ações implicadas abaixo.
    "ver_pedidos_compra":  {P.ADMIN, P.DIRETOR_FINANCEIRO},
}

# Ações que uma pessoa ganha de graça por já ter outra.
#
# Existe por um motivo prático: marcar alguém como comprador e ele não
# conseguir abrir a própria fila de pedidos seria uma armadilha — e a saída
# fácil (a rota declarar uma ação e conferir outra por dentro) é justamente o
# que o teste estrutural proíbe, porque aí a declaração deixa de dizer a
# verdade sobre quem entra.
ACOES_IMPLICADAS: dict[str, tuple[str, ...]] = {
    "ver_pedidos_compra": ("comprar", "autorizar_pedido"),
}

# Nome de cada ação em português, para a tela de cadastro do operador. Quem
# marca a caixinha não é programador: "pagar" precisa dizer o que libera.
ACAO_ROTULOS = {
    "ver_erp":              "Entrar no ERP e ver as telas",
    "lancar":               "Lançar título",
    "avalizar":             "Avalizar (1º aval)",
    "aprovar":              "Aprovar título",
    "pagar":                "Dar baixa em pagamento",
    "conciliar":            "Conciliar extrato",
    "receber":              "Lançar recebimento",
    "reclassificar":        "Reclassificar lançamento",
    "desfazer":             "Desfazer operação",
    "importar":             "Importar extrato e planilha",
    "ver_dados_pagamento":  "Ver dados bancários e chave Pix",
    "configurar":           "Abrir Configurações",
    "gerir_usuarios":       "Cadastrar e editar operadores",
    "ver_relatorios":       "Ver relatórios",
    "ver_pessoal":          "Ver despesas de colaborador",
    "lancar_dc":            "Lançar despesa de colaborador",
    "editar_colaboradores": "Cadastrar e editar colaboradores",
    "ver_suprimentos":      "Ver as telas de Suprimentos",
    "solicitar_suprimento": "Pedir material para a obra",
    "comprar":              "Cotar e fechar pedido de compra",
    "autorizar_pedido":     "Autorizar pedido de compra",
    "administrar_insumos":  "Cadastrar e corrigir insumos",
    "administrar_fornecedores": "Cadastrar e corrigir fornecedores",
    "ver_pedidos_compra":   "Ver a fila de pedidos de compra",
}

ROTULOS = {
    P.ADMIN: "Administrador",
    P.DIRETOR_FINANCEIRO: "Diretor financeiro",
    P.FINANCEIRO: "Administrativo financeiro",
    P.GESTOR_OBRA: "Gestor de obras (todas)",
    P.SUPERVISOR_OBRA: "Supervisor de obras (designadas)",
    P.ADMINISTRATIVO_OBRA: "Administrativo de obra",
    P.DEPARTAMENTO_PESSOAL: "Departamento pessoal",
    P.APROVADOR: "Aprovador",
    P.LANCADOR: "Lançador",
    P.CONSULTA: "Consulta",
}


# Ações que o ADMIN nunca perde, por mais que alguém desmarque no cadastro.
# Sem isso, um clique errado tira do único administrador a tela que conserta o
# erro — e não sobra ninguém para desfazer.
PROTEGIDAS_DO_ADMIN = ("configurar", "gerir_usuarios", "ver_erp")


def excecoes_do_usuario(usuario: Usuario) -> dict[str, bool]:
    """As marcações feitas no cadastro DESTA pessoa (ação → concedida).

    Vem preenchida por quem carregou o usuário (`_usuario_logado`). Objeto sem
    o atributo — construído em teste, ou carregado por um caminho antigo — vale
    como "nenhuma exceção", isto é, exatamente o cargo.
    """
    valor = getattr(usuario, "permissoes_extras", None)
    return valor if isinstance(valor, dict) else {}


def pode(usuario: Usuario, acao: str) -> bool:
    """Pode esta ação? O cargo decide; a marcação no cadastro corrige.

    Ordem: o cargo dá a base, a exceção da pessoa vence, e o ADMIN não pode ser
    trancado para fora das telas que consertam o sistema.
    """
    if usuario is None:
        return False
    excecoes = excecoes_do_usuario(usuario)
    return decidir(usuario.perfil, acao, excecoes)


def decidir(perfil: PerfilUsuario, acao: str, excecoes: dict[str, bool]) -> bool:
    """A mesma decisão de `pode`, a partir de valores soltos.

    Existe porque a guarda que roda antes de toda rota lê perfil e exceções por
    SQL direto, sem carregar o objeto Usuario — ver a explicação em
    `routes._guarda_permissao`. Regra num lugar só: se mudar aqui, muda nos dois.
    """
    excecoes = excecoes or {}
    base = perfil in PERMISSOES.get(acao, set())
    marcada = excecoes.get(acao)
    efetiva = base if marcada is None else bool(marcada)
    if not efetiva and acao in ACOES_IMPLICADAS and marcada is not True:
        # Desmarcar explicitamente continua valendo (marcada is False fecha),
        # mas quem NÃO tem marcação nenhuma ganha pela ação que já possui.
        if marcada is None:
            efetiva = any(decidir(perfil, outra, excecoes)
                          for outra in ACOES_IMPLICADAS[acao])
    if not efetiva and perfil is P.ADMIN and acao in PROTEGIDAS_DO_ADMIN:
        return True
    return efetiva


def exigir(usuario: Usuario, acao: str) -> None:
    if not pode(usuario, acao):
        raise ErroPermissao(
            f"Seu perfil ({ROTULOS.get(usuario.perfil, usuario.perfil.value)}) "
            f"não tem permissão para esta operação.")


# Perfis que enxergam a base inteira: nem escopo de obra, nem de autoria.
VE_TUDO = (P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
           P.APROVADOR, P.CONSULTA)

# Perfis cujo alcance é configurável por pessoa (campo escopo_visao). O padrão
# de todos eles é PROPRIOS — ampliar é escolha feita no cadastro do operador.
ESCOPO_CONFIGURAVEL = (P.ADMINISTRATIVO_OBRA, P.LANCADOR)


def escopo_visao(usuario: Usuario) -> EscopoVisao:
    """Alcance configurado para esta pessoa; na dúvida, o mais restritivo.

    Cadastro antigo, objeto recém-instanciado ou valor estranho no banco caem
    todos em PROPRIOS: a ausência de configuração tem de fechar, nunca abrir.
    """
    valor = getattr(usuario, "escopo_visao", None)
    try:
        return EscopoVisao(valor)
    except ValueError:
        return EscopoVisao.PROPRIOS


def _obras_designadas(s: Session, usuario: Usuario) -> list[int]:
    """Obras associadas a esta pessoa em usuario_obras."""
    return [o.obra_id for o in s.scalars(
        select(UsuarioObra).where(UsuarioObra.usuario_id == usuario.id)).all()]


def _ve_por_obra(usuario: Usuario) -> bool:
    """Esta pessoa enxerga por OBRA (e não apenas o que ela mesma lançou)?"""
    if usuario.perfil == P.SUPERVISOR_OBRA:
        return True
    return (usuario.perfil in ESCOPO_CONFIGURAVEL
            and escopo_visao(usuario) is EscopoVisao.OBRAS_DESIGNADAS)


def obras_do_usuario(s: Session, usuario: Usuario) -> Optional[list[int]]:
    """IDs das obras que o usuário enxerga. None = todas."""
    if usuario.perfil in VE_TUDO:
        return None
    if _ve_por_obra(usuario):
        return _obras_designadas(s, usuario)
    return None          # filtra por autoria, não por obra


def _escopo_por_obras(stmt: Select, usuario: Usuario, obras: list[int]) -> Select:
    """O que a pessoa lançou MAIS o que estiver rateado nas obras dela.

    Sem obra designada sobra só a autoria — quem não foi associado a obra
    nenhuma não passa a ver a base inteira por causa de uma lista vazia.
    """
    if not obras:
        return stmt.where(Titulo.solicitante_id == usuario.id)
    return stmt.where(or_(
        Titulo.solicitante_id == usuario.id,
        Titulo.id.in_(select(Rateio.titulo_id).where(Rateio.obra_id.in_(obras)))))


def aplicar_escopo(stmt: Select, s: Session, usuario: Usuario) -> Select:
    """Restringe a consulta de títulos ao que o usuário pode ver.

    Um só caminho para listagem e detalhe: `pode_ver_titulo` passa por aqui,
    então mudar a regra muda os dois de uma vez.
    """
    if usuario.perfil in VE_TUDO:
        return stmt
    if _ve_por_obra(usuario):
        return _escopo_por_obras(stmt, usuario, _obras_designadas(s, usuario))
    return stmt.where(Titulo.solicitante_id == usuario.id)


# ---------------------------------------------------------------------------
# Escopo de OBJETO
#
# Alçada responde "este perfil pode executar esta ação?". Não responde "pode
# executá-la NESTE registro?". Um supervisor tem a ação de lançar; isso não o
# autoriza a abrir o título da obra de outro. As funções abaixo respondem a
# segunda pergunta, e o fazem passando pelo MESMO `aplicar_escopo` que a
# listagem usa — de modo que detalhe e lista não têm como divergir sem que
# alguém altere os dois.
# ---------------------------------------------------------------------------
def pode_ver_titulo(s: Session, usuario: Usuario, titulo_id: int) -> bool:
    """O título existe E está dentro do escopo deste usuário?"""
    stmt = aplicar_escopo(select(Titulo.id).where(Titulo.id == titulo_id), s, usuario)
    return s.scalar(stmt) is not None


def exigir_titulo_no_escopo(s: Session, usuario: Usuario, titulo_id: int) -> None:
    """Fora do escopo responde igual a inexistente — ver ErroNaoEncontrado."""
    if not pode_ver_titulo(s, usuario, titulo_id):
        raise ErroNaoEncontrado("Título não encontrado.")


def pode_ver_obra(s: Session, usuario: Usuario, obra_id: int) -> bool:
    obras = obras_do_usuario(s, usuario)
    if obras is None:
        return True                      # perfil que enxerga todas as obras
    return int(obra_id) in set(obras)


def exigir_obra_no_escopo(s: Session, usuario: Usuario, obra_id: int) -> None:
    if not pode_ver_obra(s, usuario, obra_id):
        raise ErroNaoEncontrado("Obra não encontrada.")


def exigir_parcela_no_escopo(s: Session, usuario: Usuario, parcela_id: int) -> None:
    """A parcela herda o escopo do título dela."""
    from app.apps.erp.db.models.financeiro import Parcela

    p = s.get(Parcela, parcela_id)
    if p is None:
        raise ErroNaoEncontrado("Parcela não encontrada.")
    if not pode_ver_titulo(s, usuario, p.titulo_id):
        raise ErroNaoEncontrado("Parcela não encontrada.")


def exigir_entidade_no_escopo(s: Session, usuario: Usuario,
                              entidade_tipo: str, entidade_id: int) -> None:
    """Escopo de qualquer coisa que possa receber anexo.

    Cada tipo é levado até o dono que já tem escopo definido: título ou obra.
    Fornecedor e movimentação não pertencem a uma obra — são do cadastro
    central —, então ficam com quem já enxerga todas as obras. É a leitura
    estrita de "cada perfil vê o que compete à sua função": na dúvida, fecha.
    """
    from app.apps.erp.db.models.financeiro import ContratoMedicao, ContratoServico

    tipo = (entidade_tipo or "").strip()
    if tipo == "titulo":
        exigir_titulo_no_escopo(s, usuario, entidade_id)
        return
    if tipo == "obra":
        exigir_obra_no_escopo(s, usuario, entidade_id)
        return
    if tipo == "contrato_servico":
        c = s.get(ContratoServico, entidade_id)
        if c is None:
            raise ErroNaoEncontrado("Contrato não encontrado.")
        exigir_obra_no_escopo(s, usuario, c.obra_id)
        return
    if tipo == "medicao":
        m = s.get(ContratoMedicao, entidade_id)
        if m is None:
            raise ErroNaoEncontrado("Medição não encontrada.")
        c = s.get(ContratoServico, m.contrato_id)
        if c is None:
            raise ErroNaoEncontrado("Medição não encontrada.")
        exigir_obra_no_escopo(s, usuario, c.obra_id)
        return
    if obras_do_usuario(s, usuario) is not None:
        # perfil preso a obras não alcança cadastro central
        raise ErroNaoEncontrado("Registro não encontrado.")


def exigir_despesa_no_escopo(s: Session, usuario: Usuario, despesa_id: int) -> None:
    """Despesa com colaborador pertence a uma obra e segue o escopo dela."""
    from app.apps.erp.db.models.financeiro import DespesaColaborador

    d = s.get(DespesaColaborador, despesa_id)
    if d is None:
        raise ErroNaoEncontrado("Despesa não encontrada.")
    try:
        exigir_obra_no_escopo(s, usuario, d.obra_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Despesa não encontrada.")


def exigir_colaborador_no_escopo(s: Session, usuario: Usuario,
                                 colaborador_id: int) -> None:
    """A ficha do colaborador é histórico de pagamento de uma pessoa física:
    fica com quem responde pela obra dela."""
    from app.apps.erp.db.models.cadastros import Colaborador

    c = s.get(Colaborador, colaborador_id)
    if c is None:
        raise ErroNaoEncontrado("Colaborador não encontrado.")
    try:
        exigir_obra_no_escopo(s, usuario, c.obra_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Colaborador não encontrado.")


def exigir_empreita_no_escopo(s: Session, usuario: Usuario, contrato_id: int) -> None:
    """Contrato de empreita pertence a uma obra."""
    from app.apps.erp.db.models.financeiro import ContratoServico

    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroNaoEncontrado("Contrato não encontrado.")
    try:
        exigir_obra_no_escopo(s, usuario, c.obra_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Contrato não encontrado.")


def exigir_locacao_no_escopo(s: Session, usuario: Usuario, contrato_id: int) -> None:
    from app.apps.erp.db.models.financeiro import ContratoLocacao

    c = s.get(ContratoLocacao, contrato_id)
    if c is None:
        raise ErroNaoEncontrado("Contrato de locação não encontrado.")
    try:
        exigir_obra_no_escopo(s, usuario, c.obra_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Contrato de locação não encontrado.")


def exigir_parcela_locacao_no_escopo(s: Session, usuario: Usuario,
                                     parcela_id: int) -> None:
    """A parcela da locação herda o escopo do contrato dela."""
    from app.apps.erp.db.models.financeiro import LocacaoParcela

    p = s.get(LocacaoParcela, parcela_id)
    if p is None:
        raise ErroNaoEncontrado("Parcela não encontrada.")
    try:
        exigir_locacao_no_escopo(s, usuario, p.contrato_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Parcela não encontrada.")


def exigir_anexo_no_escopo(s: Session, usuario: Usuario, anexo_id: int):
    """Anexo herda o escopo da entidade a que está preso.

    Sem isto, os ids são sequenciais e um laço baixa o acervo inteiro.
    """
    from app.apps.erp.db.models.financeiro import Anexo

    a = s.get(Anexo, anexo_id)
    if a is None:
        raise ErroNaoEncontrado("Anexo não encontrado.")
    try:
        exigir_entidade_no_escopo(s, usuario, a.entidade_tipo, a.entidade_id)
    except ErroNaoEncontrado:
        raise ErroNaoEncontrado("Anexo não encontrado.")
    return a


def contexto_permissoes(s: Session, usuario: Usuario) -> dict[str, Any]:
    """O que a tela precisa saber para esconder o que o usuário não pode."""
    obras = obras_do_usuario(s, usuario)
    return {
        "perfil": usuario.perfil.value,
        "perfil_rotulo": ROTULOS.get(usuario.perfil, usuario.perfil.value),
        "pode": {acao: pode(usuario, acao) for acao in PERMISSOES},
        "excecoes": dict(excecoes_do_usuario(usuario)),
        "escopo_obras": obras,
        "escopo_descricao": (
            "todas as obras" if obras is None and usuario.perfil != P.ADMINISTRATIVO_OBRA
            else f"{len(obras)} obra(s) designada(s)" if obras
            else "apenas os lançamentos que você fez"),
    }
