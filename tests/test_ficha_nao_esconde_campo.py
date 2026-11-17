"""A FICHA DA PESSOA TEM CAMPO PARA TUDO O QUE O CADASTRO GUARDA.

⚑ POR QUE ESTE ARQUIVO EXISTE (17/11/2026).

Ele abriu a ficha e deu falta de três datas: *"no cadastro, não vi campo
referente à Data de Aviso Prévio, Saída e Último dia Trabalhado. Faça uma
varredura e veja se ainda tem campo omisso."*

A varredura achou **onze** colunas sem campo na ficha. As piores não eram as
três que ele citou:

  · **data_inicio** — a folha compara início × admissão para decidir se o dia
    é DIÁRIA ou CTPS (`analisesps/folha_vinculo.py`). O intervalo em que a
    pessoa trabalhou antes de ser registrada era ilegível E ineditável.
  · **banco, agência, conta** — há quem receba por depósito, e a ficha não
    respondia "por onde esta pessoa recebe?".
  · **aviso_previo, ultimo_dia, demissao** — entravam só pelo pedido de
    desligamento; corrigir uma data errada exigia abrir outro desligamento.

⚠️ O QUE ESTE TESTE IMPEDE é a reincidência: coluna nova no cadastro que
ninguém expõe na tela é dado que o sistema guarda e a pessoa não vê nem
corrige — e, quando ele alimenta a folha, é dinheiro decidido por um campo
invisível.

Coluna que NÃO deve ter campo entra na lista de exceções, COM O MOTIVO. A
lista é curta de propósito: cada linha dela é uma decisão, não um esquecimento.
"""
from __future__ import annotations

import re
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
TELA = RAIZ / "app" / "apps" / "erp" / "templates" / "erp_colaboradores.html"

# ---------------------------------------------------------------------------
# O que NÃO tem campo, de propósito — e por quê.
# ---------------------------------------------------------------------------
SEM_CAMPO_DE_PROPOSITO = {
    # Técnicas: o banco cuida.
    "id": "a chave da linha",
    "criado_em": "carimbo de quando a linha nasceu, posto pelo banco",
    "atualizado_em": "carimbo da última gravação, posto pelo banco",
    # Decisões registradas.
    "usuario_id": "a ligação com o login é feita em “Liberar acesso”, não "
                  "digitada (migração 096)",
    "cnpj_admissao": "vem da EMPRESA escolhida; campo próprio faria os dois "
                     "divergir (decisão dele, 15/11/2026)",
    "etapa": "muda movendo a pessoa de etapa — no quadro ou no botão da ficha",
    "situacao": "vem da ETAPA desde 17/11/2026 (SITUACAO_DA_ETAPA); era o "
                "campo que ele mandou tirar",
    "ref_pipefy": "número do card de origem; o Pipefy vai ser desligado",
    "requerente": "quem pediu o cadastro, gravado pelo pedido",
    "responsavel_id": "definido pela obra, não digitado na ficha",
    # ⚠️ ESTAS DUAS SÃO DÚVIDA ABERTA, não decisão fechada — ver o HISTORICO
    # de 17/11/2026. Ficam aqui para o teste não travar, com o aviso de que
    # alguém precisa decidir se somem ou ganham campo.
    "regime": "parece ser o VÍNCULO com outro nome (nasce 'CLT'); acrescentar "
              "campo seria dar duas maneiras de dizer a mesma coisa — "
              "PENDENTE de decisão do dono",
    "convencao_categoria": "a categoria vem da convenção do cargo "
                           "(`cargo_convencao.categoria`); aqui parece cópia "
                           "da importação — PENDENTE de decisão do dono",
}


def _coletadas() -> set[str]:
    """As colunas que a ficha manda ao servidor quando se grava."""
    tela = TELA.read_text(encoding="utf-8")
    # o mapa id-do-campo → coluna
    pares = dict(re.findall(r'"(c-[\w-]+)":\s*"(\w+)"', tela))
    colunas = set(pares.values())
    # nome e CPF são montados à parte (o CPF não se edita depois de criado)
    colunas |= set(re.findall(r'\{(nome): valorDe', tela))
    colunas |= {"nome", "cpf"}
    return colunas


def test_toda_coluna_do_cadastro_tem_campo_na_ficha():
    from app.apps.erp.db.models.cadastros import Colaborador

    coletadas = _coletadas()
    faltam = [c.name for c in Colaborador.__table__.columns
              if c.name not in coletadas and c.name not in SEM_CAMPO_DE_PROPOSITO]

    assert faltam == [], (
        "estas colunas do cadastro NÃO têm campo na ficha: "
        + ", ".join(sorted(faltam))
        + ".\n  Dado que o sistema guarda e a pessoa não vê é dado que ninguém "
        "corrige — e se ele alimenta a folha, é dinheiro decidido por um campo "
        "invisível.\n  Se a coluna não deve MESMO ter campo, acrescente-a a "
        "SEM_CAMPO_DE_PROPOSITO com o motivo escrito."
    )


def test_a_lista_de_excecoes_nao_tem_coluna_que_deixou_de_existir():
    """Exceção para coluna que já não existe é lixo que esconde a próxima."""
    from app.apps.erp.db.models.cadastros import Colaborador

    colunas = {c.name for c in Colaborador.__table__.columns}
    sobrando = [c for c in SEM_CAMPO_DE_PROPOSITO if c not in colunas]

    assert sobrando == [], f"exceções de colunas que não existem mais: {sobrando}"


def test_toda_excecao_tem_o_motivo_escrito():
    for coluna, motivo in SEM_CAMPO_DE_PROPOSITO.items():
        assert motivo and len(motivo) > 15, (
            f"{coluna}: exceção sem motivo escrito. A lista é curta de "
            f"propósito — cada linha é uma decisão, não um esquecimento")


def test_as_datas_que_ele_deu_falta_estao_na_ficha():
    """As três que ele citou, e a que a varredura achou e é a mais perigosa."""
    coletadas = _coletadas()

    for coluna in ("aviso_previo", "ultimo_dia", "demissao", "data_inicio"):
        assert coluna in coletadas, f"{coluna} continua sem campo"


def test_os_dados_bancarios_estao_na_ficha():
    """Há quem receba por depósito, e o Pix não responde por essa gente."""
    coletadas = _coletadas()

    for coluna in ("banco", "agencia", "conta"):
        assert coluna in coletadas, f"{coluna} continua sem campo"


# ---------------------------------------------------------------------------
# ⚑ GRAVAR DE QUALQUER ABA (17/11/2026)
#
# As abas do formulário se desenham UMA POR VEZ, e o que vai para o servidor é
# "só o que está na tela" — regra certa, que existe para não apagar o que a aba
# fechada guardava. Mas `pessoas.salvar` é "cria ou atualiza PELO CPF", então
# exige nome e CPF SEMPRE, em toda gravação.
#
# As duas regras juntas quebravam três abas: gravar em Endereço, em Vínculo e
# pagamento ou em Benefícios respondia "Informe o nome completo" e não gravava
# nada. Só se descobriu dirigindo a tela no navegador.
#
# Este teste prende o conserto: o que o servidor exige sempre tem de ter de
# onde vir quando o campo não está na aba aberta.
# ---------------------------------------------------------------------------
SEMPRE_EXIGIDOS = {
    # campo mandado  → id do campo na tela
    "nome": "c-nome",
    "cpf": "c-cpf",
}


def test_o_que_o_servidor_exige_sempre_tem_volta_por_cima():
    tela = TELA.read_text(encoding="utf-8")
    for campo, ident in SEMPRE_EXIGIDOS.items():
        achado = re.search(rf'{campo}: valorDe\("{ident}"\)\s*(\?\?|\|\|)', tela)
        assert achado, (
            f'a tela manda `{campo}` sem volta por cima: se o campo "{ident}" '
            f"não estiver na aba aberta, a gravação vai sem ele e o servidor "
            f"recusa a ficha inteira. Use `valorDe(...) ?? FICHA.{campo}`")


def test_o_servidor_continua_exigindo_nome_e_cpf():
    """Se um dia deixar de exigir, o teste de cima perde o motivo — e quem
    mexer precisa saber disso em vez de descobrir pelo caminho."""
    fonte = (RAIZ / "app" / "apps" / "erp" / "core" / "cadastros"
             / "pessoas.py").read_text(encoding="utf-8")
    assert "CPF inválido." in fonte
    assert "Informe o nome completo." in fonte
