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
# DEPARTAMENTO_PESSOAL  revisa a despesa com colaborador; enxerga TUDO que é
#                       do mundo dele (despesa com colaborador, RPA, folha,
#                       reembolso), em TODAS as obras e lançado por qualquer
#                       pessoa — e nada além disso. Não aprova, não paga e não
#                       vê dado bancário.
# APROVADOR / LANCADOR / CONSULTA   perfis herdados, mantidos
#
# O escopo não é enfeite de tela: ele entra na consulta, então o que está fora
# do alcance do usuário nem chega ao navegador.
# ============================================================================
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import false as sql_false, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from app.apps.erp.core.comum.auditoria import ErroNaoEncontrado, ErroPermissao
from app.apps.erp.db.models.cadastros import (
    EscopoVisao, PerfilUsuario, TipoTitulo, Usuario, UsuarioObra,
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
                        P.APROVADOR, P.LANCADOR, P.CONSULTA, P.PARCEIRO},
    "lancar":          {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.LANCADOR,
                        P.DEPARTAMENTO_PESSOAL},
    "avalizar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.GESTOR_OBRA, P.SUPERVISOR_OBRA},
    "aprovar":         {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.APROVADOR},
    # CANCELAR TÍTULO é ação PRÓPRIA, e não um pedaço de "aprovar", desde
    # 12/09/2026. Decisão do dono: *"liberado o lançamento que não está baixado
    # ou conciliado"* — quem lançou desfaz o próprio engano, sem pedir ao
    # financeiro. Quem tem "lancar" ganha esta por implicação (ACOES_IMPLICADAS
    # logo abaixo); o que ele NÃO ganha é cancelar o lançamento dos outros, e
    # isso quem decide é o serviço, olhando de quem é o título.
    "cancelar_titulo": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.APROVADOR},
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
    # Uso do sistema por pessoa. DELIBERADAMENTE estreita: ver o que os OUTROS
    # fizeram é informação de gestão de gente, não de operação. Ver a PRÓPRIA
    # semana não passa por aqui — é `ver_erp`, e a rota nem aceita o número de
    # outra pessoa, então não há como uma virar a outra.
    "ver_uso_da_equipe": {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "ver_relatorios":  {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.PARCEIRO},
    # Pessoal: o DP revisa a despesa com colaborador depois do supervisor,
    # porque só ele conhece o cadastro e sabe se a verba é devida
    "ver_pessoal":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL, P.PARCEIRO},
    "lancar_dc":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.DEPARTAMENTO_PESSOAL},
    "editar_colaboradores": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.DEPARTAMENTO_PESSOAL},
    # Suprimentos. Não existe cargo de "comprador" no ERP, e não vai existir:
    # pela decisão de 04/09/2026, quem compra e quem autoriza pedido ganham a
    # ação MARCADA no cadastro, uma pessoa de cada vez. O padrão abaixo é
    # deliberadamente estreito — pedir material é de todo mundo da obra,
    # comprar e autorizar não são de ninguém por herança de cargo.
    "ver_suprimentos":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                            P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA, P.CONSULTA, P.PARCEIRO},
    "solicitar_suprimento": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.GESTOR_OBRA,
                             P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA},
    "comprar":             {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "autorizar_pedido":    {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "administrar_insumos": {P.ADMIN, P.DIRETOR_FINANCEIRO},
    "administrar_fornecedores": {P.ADMIN, P.DIRETOR_FINANCEIRO},
    # A fila de pedidos serve a DOIS papéis: quem compra acompanha o que fechou,
    # quem autoriza libera. Ver a seção de ações implicadas abaixo.
    "ver_pedidos_compra":  {P.ADMIN, P.DIRETOR_FINANCEIRO},
    # Notas fiscais emitidas contra os CNPJs da empresa. VER é largo de
    # propósito — nota emitida contra a empresa sem ninguém saber é problema
    # fiscal, e mais olhos ajudam. CRUZAR é decisão que muda a contabilidade:
    # fica com o financeiro por cargo, e chega a quem compra pela implicação
    # abaixo, porque é o comprador que sabe de que pedido a nota é.
    "ver_notas":       {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.CONSULTA},
    "cruzar_notas":    {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    # Arquivo da empresa. VER é largo — certidão e contrato social são o tipo de
    # documento que todo mundo precisa e ninguém acha. O que separa quem vê o
    # quê NÃO é esta ação, é o SIGILO do tipo (aberto, restrito, pessoal) e o
    # escopo por obra: folha de pagamento e documento de sócio não aparecem
    # para quem não é do financeiro ou do DP, mesmo com esta ação marcada.
    "ver_arquivo":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA,
                        P.DEPARTAMENTO_PESSOAL, P.APROVADOR, P.CONSULTA, P.PARCEIRO},
    "arquivar":        {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO,
                        P.DEPARTAMENTO_PESSOAL, P.GESTOR_OBRA},
    # Quadro financeiro do contrato: medições, faturamento e recebimento.
    #
    # A lista é DELIBERADAMENTE só de perfis que já enxergam a base inteira
    # (VE_TUDO). O quadro mostra o contrato de ponta a ponta — todas as
    # medições, de todas as obras do contrato — e não há como recortá-lo por
    # obra designada sem mentir no total. Quem é preso a obra ou a autoria
    # fica de fora até existir um recorte que faça sentido; abrir depois é
    # uma linha, fechar depois é conversa constrangedora.
    "ver_contratos":   {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.CONSULTA},
    # Notas que a BWS EMITE contra o cliente. VER é largo dentro do escritório
    # — é dessa tela que sai o relatório da contabilidade. EMITIR (registrar a
    # nota, cancelar) é estreito: número de nota fiscal não se apaga, se
    # explica, e cada linha aqui é documento perante o fisco.
    "ver_notas_emitidas": {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO,
                           P.GESTOR_OBRA, P.CONSULTA},
    "emitir_nota":     {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO},
    # A agenda é LARGA de propósito: quase todo perfil tem alguma obrigação com
    # data (o administrativo da obra responde a conferência de equipamento, o
    # DP tem documento vencendo, o financeiro tem o reajuste). Uma agenda que
    # só o administrador enxerga não avisa ninguém — e o que cada um VÊ dentro
    # dela continua limitado pelo escopo de obra, não por esta ação.
    "ver_agenda":      {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA,
                        P.DEPARTAMENTO_PESSOAL, P.APROVADOR, P.CONSULTA, P.PARCEIRO},
    # Marcar como resolvido é afirmação com nome e data. Fica fora de CONSULTA
    # — quem só olha não resolve.
    "tratar_agenda":   {P.ADMIN, P.DIRETOR_FINANCEIRO, P.FINANCEIRO, P.GESTOR_OBRA,
                        P.SUPERVISOR_OBRA, P.ADMINISTRATIVO_OBRA,
                        P.DEPARTAMENTO_PESSOAL},
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
    # Quem compra enxerga as notas e cruza: é ele que sabe de que pedido cada
    # nota é, e o dono deixou em aberto quem confere — a resposta prática é
    # "os dois, na mesma tela", porque a nota tem uma ponta em cada mundo.
    "ver_notas":   ("comprar", "autorizar_pedido", "cruzar_notas"),
    "cruzar_notas": ("comprar",),
    # Quem lança recebimento precisa do quadro do contrato: é lá que ele
    # confere o que já foi medido, faturado e recebido antes de baixar.
    "ver_contratos": ("receber",),
    # Quem emite enxerga a própria tela — do contrário marcar alguém como
    # emissor e ele não conseguir abrir a lista seria uma armadilha.
    "ver_notas_emitidas": ("emitir_nota", "ver_contratos"),
    "ver_agenda": ("tratar_agenda",),
    # Quem lança cancela — o PRÓPRIO lançamento, e só enquanto ninguém baixou
    # nem conciliou. Sem esta linha, corrigir o próprio engano dependeria de
    # interromper o financeiro, e o erro ficaria no ar até alguém ter tempo.
    "cancelar_titulo": ("aprovar", "lancar"),
}

# Nome de cada ação em português, para a tela de cadastro do operador. Quem
# marca a caixinha não é programador: "pagar" precisa dizer o que libera.
ACAO_ROTULOS = {
    "ver_erp":              "Entrar no ERP e ver as telas",
    "lancar":               "Lançar título",
    "avalizar":             "Avalizar (1º aval)",
    "aprovar":              "Aprovar título",
    "cancelar_titulo":      "Cancelar título (o próprio, se ninguém baixou)",
    "pagar":                "Dar baixa em pagamento",
    "conciliar":            "Conciliar extrato",
    "receber":              "Lançar recebimento",
    "reclassificar":        "Reclassificar lançamento",
    "desfazer":             "Desfazer operação",
    "importar":             "Importar extrato e planilha",
    "ver_dados_pagamento":  "Ver dados bancários e chave Pix",
    "configurar":           "Abrir Configurações",
    "gerir_usuarios":       "Cadastrar e editar operadores",
    "ver_uso_da_equipe":    "Ver o trabalho da equipe no sistema",
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
    "ver_notas":            "Ver as notas emitidas contra a empresa",
    "cruzar_notas":         "Cruzar nota com pedido, título e fundo fixo",
    "ver_arquivo":          "Ver o arquivo de documentos da empresa",
    "arquivar":             "Guardar e organizar documento no arquivo",
    "ver_contratos":        "Ver o quadro financeiro dos contratos",
    "ver_notas_emitidas":   "Ver as notas emitidas contra o cliente",
    "emitir_nota":          "Registrar e cancelar nota emitida",
    "ver_agenda":           "Ver a agenda de obrigações",
    "tratar_agenda":        "Resolver, dispensar e anotar na agenda",
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
    P.PARCEIRO: "Parceiro da obra (só olha)",
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


def pode_com_banco(s: Session, usuario: Usuario, acao: str) -> bool:
    """A mesma decisão de `pode`, mas sem depender de quem carregou o usuário.

    `pode` lê as marcações do cadastro de um atributo que `_usuario_logado`
    preenche. Funciona nas rotas, e falha calado em qualquer outro caminho: a
    pessoa perde a ação que foi MARCADA para ela e passa a ver menos do que
    devia, sem nada acusar. Como regra de recorte roda também fora de rota
    (relatório agendado, robô, teste), esta versão vai buscar as marcações no
    banco quando elas não vieram junto.

    A decisão em si continua sendo uma só — `decidir` — para não haver duas
    respostas possíveis à mesma pergunta.
    """
    if usuario is None:
        return False
    excecoes = excecoes_do_usuario(usuario)
    if not excecoes:
        excecoes = _excecoes_no_banco(s, usuario.id)
    return decidir(usuario.perfil, acao, excecoes)


def _excecoes_no_banco(s: Session, usuario_id: int) -> dict[str, bool]:
    """As marcações desta pessoa, por SQL direto.

    SQL direto e não ORM pelo mesmo motivo do resto da guarda: enquanto a
    migração 032 não tiver rodado a tabela não existe, e a resposta certa é
    "nenhuma marcação" — o que faz valer o cargo — e não derrubar a tela.
    """
    from sqlalchemy import text as _text

    try:
        linhas = s.execute(
            _text("SELECT acao, concedida FROM usuario_permissoes "
                  "WHERE usuario_id = :i"), {"i": usuario_id}).all()
    except Exception:
        return {}
    return {acao: bool(concedida) for acao, concedida in linhas}


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

# ---------------------------------------------------------------------------
# O DEPARTAMENTO PESSOAL enxerga por ASSUNTO, não por obra nem por autoria.
#
# Decisão do dono, 07/09/2026, em duas partes. Primeiro: "a trava de
# visualização é semelhante ao do financeiro" — ou seja, ele NÃO pode ficar
# preso ao que ele mesmo lançou, porque a despesa que ele revisa foi lançada
# PELA OBRA, nunca por ele. Depois, refinando: ele alcança no financeiro "as
# coisas relacionadas ao departamento pessoal".
#
# São duas coisas diferentes, e a segunda é mais estreita. Vale a segunda:
# quando a instrução comporta duas leituras, a que mostra MENOS é a que se
# implementa — abrir depois é uma linha, e fechar depois é uma conversa
# constrangedora sobre quem viu o que não devia.
#
# O mundo do DP é, então: todo título que nasceu de uma DESPESA COM
# COLABORADOR (o lote que ele mesmo aprova em cadeia), mais os títulos cuja
# natureza é pessoa — RPA, folha e encargos, reembolso a colaborador. Fica de
# fora T11 (adiantamento a FORNECEDOR), que apesar do nome parecido é compra.
TIPOS_DO_PESSOAL = (
    TipoTitulo.T6_SERVICO_PF_RPA,       # serviço de pessoa física (RPA)
    TipoTitulo.T7_FOLHA_ENCARGOS,       # folha de pagamento e encargos
    TipoTitulo.T12_REEMBOLSO,           # reembolso a colaborador
)

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
    if usuario.perfil in (P.SUPERVISOR_OBRA, P.PARCEIRO):
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


def obras_de_registro_sem_autor(s: Session, usuario: Usuario) -> Optional[list[int]]:
    """Obras que a pessoa alcança num registro que NÃO TEM AUTOR.

    Título tem `solicitante_id`, então quem enxerga "só o que eu lancei" tem
    por onde ser filtrado. **Contrato de locação não tem autor nenhum** — e aí
    `obras_do_usuario` devolvia None, que significa "sem filtro de obra", e
    quem enxergava por autoria passava a ver TODOS os contratos da empresa.
    Achado por um teste em 11/09/2026.

    A regra que fica: para registro sem autor, o único recorte possível é a
    OBRA. Quem não enxerga a base inteira vê apenas as obras designadas a ele —
    e **sem obra designada não vê nenhum**, que é o padrão NEGAR do ERP e não
    um efeito colateral de lista vazia.

    Devolve None só para quem enxerga tudo.
    """
    if usuario.perfil in VE_TUDO:
        return None
    return _obras_designadas(s, usuario)


def _escopo_por_obras(stmt: Select, usuario: Usuario, obras: list[int]) -> Select:
    """O que a pessoa lançou MAIS o que estiver rateado nas obras dela.

    Sem obra designada sobra só a autoria — quem não foi associado a obra
    nenhuma não passa a ver a base inteira por causa de uma lista vazia.

    O PARCEIRO é a exceção e é de propósito: ele é de fora da BWS e não lança
    nada, então "o que eu lancei" seria uma porta que só existe por descuido.
    Sem obra designada, ele não vê NADA — o padrão NEGAR, dito em voz alta.
    """
    if usuario.perfil == P.PARCEIRO and not obras:
        return stmt.where(sql_false())
    if not obras:
        return stmt.where(Titulo.solicitante_id == usuario.id)
    return stmt.where(or_(
        Titulo.solicitante_id == usuario.id,
        Titulo.id.in_(select(Rateio.titulo_id).where(Rateio.obra_id.in_(obras)))))


def _escopo_do_pessoal(stmt: Select) -> Select:
    """Tudo que é do mundo do Departamento Pessoal, em qualquer obra.

    Duas portas para o mesmo assunto, e as duas precisam estar abertas: o
    título que NASCEU de uma despesa com colaborador (o lote que o DP aprova
    em cadeia) e o título cuja natureza já é pessoa (RPA, folha, reembolso).
    Só a primeira deixaria de fora a folha lançada direto; só a segunda
    deixaria de fora o lote, que pode sair com outro tipo.
    """
    from app.apps.erp.db.models.financeiro import DespesaColaborador
    return stmt.where(or_(
        Titulo.tipo.in_(TIPOS_DO_PESSOAL),
        Titulo.id.in_(select(DespesaColaborador.titulo_id)
                      .where(DespesaColaborador.titulo_id.is_not(None)))))


def condicao_escopo_sql(s: Session, usuario: Usuario,
                        t: str = "t") -> tuple[str, dict[str, Any]]:
    """O MESMO recorte de `aplicar_escopo`, escrito como pedaço de WHERE.

    Existe porque os relatórios somam no banco com SQL escrito à mão (agregar
    milhares de títulos em memória não cabe nos 2 GB da instância) e, por isso,
    não tinham como passar pelo `aplicar_escopo`, que só monta consulta do
    SQLAlchemy. O resultado era grave e silencioso: quem enxerga uma obra via
    o resultado da empresa inteira na tela de Relatórios. Achado em 11/09/2026.

    As duas formas do recorte precisam concordar SEMPRE, e é isso que o teste
    `test_escopo_sql_igual_ao_orm` (em `tests/test_auditoria_financeira_banco.py`)
    prova, perfil a perfil, com banco de verdade. Mudou a regra aqui, muda lá
    em cima — e o teste acusa se só um dos dois mudou.

    `t` é o apelido da tabela de títulos na consulta que vai receber o pedaço.
    """
    if usuario.perfil in VE_TUDO:
        return "TRUE", {}

    if usuario.perfil == P.DEPARTAMENTO_PESSOAL:
        tipos = ", ".join(f"'{x.value}'" for x in TIPOS_DO_PESSOAL)
        return (f"({t}.tipo::text IN ({tipos}) OR {t}.id IN "
                f"(SELECT titulo_id FROM despesas_colaborador "
                f"WHERE titulo_id IS NOT NULL))"), {}

    if _ve_por_obra(usuario):
        obras = _obras_designadas(s, usuario)
        if not obras:
            if usuario.perfil == P.PARCEIRO:
                return "FALSE", {}          # parceiro sem obra não vê nada
            return f"{t}.solicitante_id = :escopo_usuario", {"escopo_usuario": usuario.id}
        return (f"({t}.solicitante_id = :escopo_usuario OR {t}.id IN "
                f"(SELECT titulo_id FROM rateios WHERE obra_id = ANY(:escopo_obras)))"
                ), {"escopo_usuario": usuario.id, "escopo_obras": list(obras)}

    return f"{t}.solicitante_id = :escopo_usuario", {"escopo_usuario": usuario.id}


def aplicar_escopo(stmt: Select, s: Session, usuario: Usuario) -> Select:
    """Restringe a consulta de títulos ao que o usuário pode ver.

    Um só caminho para listagem e detalhe: `pode_ver_titulo` passa por aqui,
    então mudar a regra muda os dois de uma vez.
    """
    if usuario.perfil in VE_TUDO:
        return stmt
    if usuario.perfil == P.DEPARTAMENTO_PESSOAL:
        return _escopo_do_pessoal(stmt)
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


def exigir_agendada_no_escopo(s: Session, usuario: Usuario,
                              agendada_id: int) -> None:
    """O relatório automático é DE QUEM O RECEBE, e de mais ninguém.

    Ele roda com a permissão do destinatário, então mexer no de outra pessoa
    seria mexer num recorte que não é seu. Fora do escopo responde "não
    encontrado": dizer "sem permissão" para um número que existe confirma que
    ele existe, e varrer os números mapearia quem recebe o quê.
    """
    from app.apps.erp.db.models.financeiro import PerguntaAgendada

    a = s.get(PerguntaAgendada, agendada_id)
    if a is None or a.usuario_id != usuario.id:
        raise ErroNaoEncontrado("Relatório automático não encontrado.")


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


def exigir_tarefa_no_escopo(s: Session, usuario: Usuario, tarefa_id: int):
    """Trabalho em segundo plano é de quem pediu (migração 055).

    Regra curta de propósito: quem enfileirou vê e mexe no que enfileirou; quem
    já enxerga o sistema inteiro (o mesmo `VE_TUDO` das outras listagens) vê a
    fila inteira, porque é a essas pessoas que se pergunta quando o sistema
    está lento. O que aparece aqui é rótulo e andamento — "importar 37 cards",
    "emitir a nota 412" —, não o conteúdo dos registros; o escopo por obra
    continua valendo lá dentro, quando o trabalho toca em título ou obra.
    """
    from app.apps.erp.db.models.financeiro import Tarefa

    t = s.get(Tarefa, tarefa_id)
    if t is None:
        raise ErroNaoEncontrado("Trabalho não encontrado.")
    if usuario.perfil in VE_TUDO or t.usuario_id == usuario.id:
        return t
    raise ErroNaoEncontrado("Trabalho não encontrado.")


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
