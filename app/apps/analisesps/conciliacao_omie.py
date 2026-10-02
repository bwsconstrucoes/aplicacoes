# -*- coding: utf-8 -*-
"""
LANÇAR NO OMIE a partir do extrato da Conciliação.

Pedido do dono em 24/09/2026: *"tarifas bancárias, rentabilidade de
investimento (…) tem muita tarifa de PIX. Esse aí a gente poder lançar direto
no OMIE: selecionar e gravar essa movimentação financeira."*

São lançamentos que aparecem no extrato e **não nascem de uma SP** — ninguém
pede autorização para pagar tarifa de PIX. Hoje ele digita um por um no OMIE.

⚠️ ISTO ESCREVE NO OMIE, e é a coisa mais perigosa que este módulo faz depois
dos aportes. As proteções, todas deliberadas:

1. **Nada é lançado sem TIPO reconhecido.** Uma linha cujo histórico não casa
   com nenhum tipo configurado é recusada, não chutada. Chutar a categoria
   poria tarifa bancária dentro de "material de obra" — e no OMIE, depois,
   isso vira relatório errado que ninguém desconfia.
2. **Nada é lançado duas vezes.** O código de integração sai do número da
   linha do extrato: mandar de novo faz o OMIE recusar sozinho. A recusa dele
   vale mais que qualquer conferência feita deste lado.
3. **A conta corrente do OMIE vem da CONTA BANCÁRIA**, nunca do tipo. Lançar
   uma tarifa do Bradesco dentro da conta do Santander é o erro mais caro
   possível aqui, e o único jeito de não cometê-lo é não ter onde errar.
4. **Ensaiar antes.** A tela mostra o que SERIA mandado, linha a linha, e só
   manda depois do "pode". É o mesmo desenho dos aportes.

⚠️ O SENTIDO NÃO É CONFIGURADO, É DEDUZIDO DO SINAL. Valor negativo vira Conta
a Pagar; positivo, Conta a Receber. Um estorno de tarifa entra sozinho do lado
certo — e não há um campo a mais para alguém marcar errado.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from decimal import Decimal

logger = logging.getLogger("analisesps.conciliacao")

# Quantas linhas se pode mandar de uma vez. Cada uma são DUAS chamadas ao OMIE
# (incluir e baixar), e a tela espera pela resposta.
MAX_POR_VEZ = 40


class ErroDoLancamento(RuntimeError):
    """Recusa com mensagem pronta para a tela."""


def _sem_acento(texto: str) -> str:
    cru = unicodedata.normalize("NFKD", str(texto or ""))
    return "".join(c for c in cru if not unicodedata.combining(c)).upper()


def _pronto() -> bool:
    from .db import consultar_um
    try:
        consultar_um("SELECT 1 FROM analisesps.conciliacao_tipo LIMIT 1")
        return True
    except Exception:  # noqa: BLE001 — antes da migração é estado normal
        return False


# ---------------------------------------------------------------------------
# OS TIPOS — o que o dono digitaria no OMIE, guardado uma vez
# ---------------------------------------------------------------------------
def tipos(so_ativos: bool = True) -> list[dict]:
    if not _pronto():
        return []
    from .db import consultar, tem_coluna
    onde = " WHERE ativo" if so_ativos else ""
    # ⚠️ A NATUREZA É DA MIGRAÇÃO 022, e o código sobe antes do botão.
    natureza = ("natureza" if tem_coluna("conciliacao_tipo", "natureza")
                else "'normal' AS natureza")
    linhas = consultar(
        "SELECT id, nome, palavras, codigo_categoria, codigo_cliente, "
        f"       cod_departamento, ativo, ordem, {natureza} "
        f"  FROM analisesps.conciliacao_tipo{onde} ORDER BY ordem, lower(nome)")
    nomes = ["id", "nome", "palavras", "codigo_categoria", "codigo_cliente",
             "cod_departamento", "ativo", "ordem", "natureza"]
    return [dict(zip(nomes, linha)) for linha in linhas]


FALTA_MIGRAR = (
    "A parte do OMIE ainda não foi ligada no banco. Aperte "
    "\"Aplicar atualizações do banco\" em Configurações do Análise de SPs — "
    "é a atualização que cria os tipos de movimento. Nada do que você digitou "
    "se perdeu: é só apertar e gravar de novo.")


def gravar_tipo(dados: dict, quem: str = "") -> int:
    # ⚠️ ESTA GUARDA FALTAVA, e o dono pagou por isso em 24/09/2026: ele
    # recebeu na tela a frase crua do Postgres, *"relation
    # analisesps.conciliacao_tipo does not exist"*.
    #
    # A tela inteira se dava por pronta porque a Conciliação olhava UMA tabela
    # (a das contas, da migração 019) para decidir isso — e a parte do OMIE
    # veio depois, na 021. Entre uma e outra, a tela abria, o formulário
    # aparecia, e só o Gravar quebrava. **Cada pedaço tem de conferir a SUA
    # tabela**, e dizer em português o que falta.
    if not _pronto():
        raise ErroDoLancamento(FALTA_MIGRAR)

    nome = str(dados.get("nome") or "").strip()
    if not nome:
        raise ErroDoLancamento("O tipo precisa de um nome.")
    from .db import conexao

    natureza = ("transferencia"
                if str(dados.get("natureza") or "").strip() == "transferencia"
                else "normal")
    campos = (
        nome,
        str(dados.get("palavras") or "").strip(),
        str(dados.get("codigo_categoria") or "").strip(),
        int(dados["codigo_cliente"]) if str(
            dados.get("codigo_cliente") or "").strip().isdigit() else None,
        str(dados.get("cod_departamento") or "").strip(),
        bool(dados.get("ativo", True)),
        int(dados.get("ordem") or 0),
        natureza,
    )
    tipo_id = dados.get("id")
    with conexao() as con:
        if tipo_id:
            con.execute(
                "UPDATE analisesps.conciliacao_tipo SET nome=?, palavras=?, "
                "       codigo_categoria=?, codigo_cliente=?, "
                "       cod_departamento=?, ativo=?, ordem=?, natureza=? "
                " WHERE id=?",
                campos + (int(tipo_id),))
            con.commit()
            return int(tipo_id)
        cur = con.execute(
            "INSERT INTO analisesps.conciliacao_tipo "
            "  (nome, palavras, codigo_categoria, codigo_cliente, "
            "   cod_departamento, ativo, ordem, natureza, criado_por) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id", campos + (quem,))
        novo = int(cur.fetchone()[0])
        con.commit()
    logger.info("Conciliação: %s criou o tipo %s.", quem or "?", nome)
    return novo


def reconhecer(descricao: str, lista: list = None) -> dict | None:
    """Qual tipo é este lançamento, pelo histórico do banco.

    ⚠️ CASA POR PEDAÇO, SEM ACENTO E SEM LIGAR PARA MAIÚSCULA — o extrato
    escreve "TARIFA BANCARIA", "Tarifa Pacote de Serviços" e "TAR PIX" para a
    mesma coisa. E devolve `None` quando não reconhece: **chutar a categoria
    é pior do que não lançar**, porque no OMIE vira relatório errado que
    ninguém desconfia.

    Quando mais de um tipo casa, vale o de palavra MAIS LONGA: "TARIFA PIX" é
    mais específico que "TARIFA", e quem escreveu os dois quis o específico.
    """
    alvo = _sem_acento(descricao)
    if not alvo:
        return None
    melhor = None
    melhor_tamanho = 0
    for tipo in (lista if lista is not None else tipos()):
        for palavra in str(tipo.get("palavras") or "").split(";"):
            palavra = _sem_acento(palavra).strip()
            if palavra and palavra in alvo and len(palavra) > melhor_tamanho:
                melhor, melhor_tamanho = tipo, len(palavra)
    return melhor


# ---------------------------------------------------------------------------
# O QUE SERIA MANDADO — conferido ANTES de falar com o OMIE
# ---------------------------------------------------------------------------
# O teto que o OMIE impôs na prática, em 21/09/2026: oito tentativas repetidas
# até descobrir que `numero_documento` acima de 20 caracteres é recusado.
#
# ⚠️ O QUE EU NÃO SEI, E DIGO EM VEZ DE CHUTAR: qual é o teto documentado do
# `codigo_lancamento_integracao`. O dono perguntou em 25/09/2026 e a
# documentação do OMIE não é alcançável do ambiente onde isto foi escrito (a
# rede bloqueia o domínio). Então este código trata os DOIS campos pelo teto
# que se conhece — o de 20 —, que é o lado seguro: se o teto de verdade for
# maior, nada se perde; se for 20, já está respeitado.
#
# Na prática sobra muito espaço: "CONC" + o número da linha + "D" dá 12 ou 13
# caracteres com os números de hoje, e o teste trava isso.
MAX_CODIGO = 20


def _codigo_de_integracao(linha_id: int) -> str:
    """A identidade do lançamento no OMIE, tirada da linha do extrato.

    ⚠️ É O QUE IMPEDE LANÇAR DUAS VEZES, e a trava não é este código: é o OMIE
    recusando o segundo envio com "código de integração já cadastrado". Uma
    conferência feita só deste lado não sobrevive a duas pessoas clicando ao
    mesmo tempo; a recusa dele sobrevive.

    ⚠️ O "D" DA SEGUNDA PONTA CABE DENTRO DO TETO, e é por isso que a conta é
    feita com um caractere a menos: a transferência manda dois títulos, e o da
    entrada é este código mais um "D". Se o código estourasse só na segunda
    ponta, a transferência ficaria pela metade — o pior estado possível aqui.
    """
    return f"CONC{int(linha_id)}"


def _fornecedor_de(conta: dict, tipo: dict = None):
    """Quem é o fornecedor/cliente deste lançamento no OMIE: o da CONTA.

    ⚠️ SÓ A CONTA, desde 02/10/2026. Quem cobra a tarifa é o banco da conta — a
    do BD 50024 é do Bradesco, a da Sicredi é da Sicredi. O tipo tinha um campo
    de "reserva", e o dono o tirou: *"O Código do fornecedor/cliente não pode
    ser cadastrado em Tipos do OMIE, visto que a associação deve ser a partir da
    conta."* O valor antigo do tipo fica no banco e não é mais lido — usá-lo
    lançaria a tarifa de uma conta com o banco de outra.
    """
    da_conta = (conta or {}).get("omie_fornecedor")
    return int(da_conta) if da_conta else None


def _obra_de(conta: dict) -> str:
    """A obra (departamento do OMIE) de TODO movimento de conta corrente desta
    conta — tarifa, rentabilidade e os demais tipos.

    ⚠️ DA CONTA, NUNCA DO TIPO (dono, 02/10/2026): *"como temos várias contas e
    o TIPO OMIE é meio genérico vai dar erro de apropriação. (…) Definir lá pra
    qual obra vão as tarifas."* Conta sem obra lança sem departamento."""
    return str((conta or {}).get("omie_departamento") or "").strip()


def planejar(linhas: list, conta: dict, lista_tipos: list = None,
             destinos: dict = None) -> dict:
    """O que aconteceria com cada linha marcada. NÃO fala com o OMIE.

    Separa em `vai` (dá para lançar) e `nao_vai` (e o motivo de cada uma). A
    lista dos que não vão é tão importante quanto a outra: é ela que diz ao
    dono o que falta configurar, em vez de o lote inteiro falhar sem explicar.

    `destinos` é `{linha_id: conta}` e só vale para tipo de TRANSFERÊNCIA —
    é a conta para onde o dinheiro foi.
    """
    lista_tipos = lista_tipos if lista_tipos is not None else tipos()
    vai, nao_vai = [], []

    # ⚠️ SEM A MIGRAÇÃO, A LISTA DE TIPOS VEM VAZIA — e sem esta frase o
    # ensaio diria "não reconheci o tipo" para TODAS as linhas, mandando o
    # dono cadastrar tipos num lugar que não grava.
    if not lista_tipos and not _pronto():
        return {"vai": [], "total": 0,
                "nao_vai": [{"id": l.get("id"), "motivo": FALTA_MIGRAR,
                             "descricao": (l.get("descricao") or "")[:90],
                             "data": l.get("data"), "valor": l.get("valor")}
                            for l in linhas]}

    for linha in linhas:
        motivo = None
        tipo = reconhecer(linha.get("descricao") or "", lista_tipos)

        if linha.get("omie_codigo"):
            motivo = f"já foi lançada no OMIE (título {linha['omie_codigo']})"
        elif not conta.get("omie_conta_corrente"):
            motivo = ("a conta bancária não tem a conta corrente do OMIE "
                      "apontada — sem ela o lançamento nasceria na conta errada")
        elif tipo is None:
            motivo = ("não reconheci o tipo deste lançamento pelo histórico. "
                      "Cadastre um tipo com uma palavra que apareça nele")
        elif not str(tipo.get("codigo_categoria") or "").strip():
            motivo = f"o tipo \"{tipo['nome']}\" está sem categoria do OMIE"
        elif not _fornecedor_de(conta):
            # ⚠️ O FORNECEDOR VEM DA CONTA — e só dela, desde 02/10/2026.
            #
            # Pedido do dono em 25/09/2026: *"o código fornecedor tem que estar
            # atrelado à conta bancária"*. E ele tem razão — quem cobra a
            # tarifa é o banco DESTA conta. No tipo, seria preciso um "Tarifa
            # Bradesco", um "Tarifa Sicredi" e um "Tarifa BB", todos repetindo
            # as mesmas palavras do histórico e brigando entre si na hora de
            # reconhecer o lançamento.
            #
            # A frase diz onde resolver, e não só o que falta: quem lê isto
            # está com a tela aberta e precisa saber para onde ir.
            motivo = (f"a conta \"{conta.get('nome', '?')}\" está sem o "
                      "fornecedor do OMIE — é quem cobra a tarifa. Abra "
                      "\"Contas\", procure o banco no campo "
                      "\"Fornecedor no OMIE\" e grave")
        elif not linha.get("valor"):
            motivo = "o valor é zero"
        elif len(_codigo_de_integracao(linha["id"])) + 1 > MAX_CODIGO:
            # Guarda que nunca deve disparar: o número da linha teria de passar
            # de quinze dígitos. Existe para que, se um dia disparar, ela vire
            # uma linha recusada com motivo em vez de um erro do OMIE no meio
            # do lote — e, na transferência, em vez de meia transferência.
            motivo = ("o número interno desta linha ficou grande demais para o "
                      "código de integração do OMIE. Me avise, é defeito meu")

        if motivo:
            nao_vai.append({"id": linha.get("id"), "motivo": motivo,
                            "descricao": (linha.get("descricao") or "")[:90],
                            "data": linha.get("data"),
                            "valor": linha.get("valor")})
            continue

        valor = Decimal(str(linha["valor"]))
        # ⚠️ A TRANSFERÊNCIA PRECISA DA CONTA DE DESTINO, e ela não é chutada:
        # sem destino escolhido, a linha é recusada com o motivo. Adivinhar o
        # destino poria o dinheiro numa conta que ninguém pediu.
        e_transferencia = (tipo.get("natureza") == "transferencia")
        destino = destinos.get(linha["id"]) if destinos else None
        if e_transferencia and not destino:
            nao_vai.append({"id": linha.get("id"),
                            "motivo": "é transferência — escolha a conta de "
                                      "destino",
                            "descricao": (linha.get("descricao") or "")[:90],
                            "data": linha.get("data"),
                            "valor": linha.get("valor"),
                            "pede_destino": True})
            continue
        if e_transferencia and not (destino or {}).get("omie_conta_corrente"):
            nao_vai.append({"id": linha.get("id"),
                            "motivo": f"a conta de destino "
                                      f"\"{(destino or {}).get('nome', '?')}\" "
                                      "não tem a conta corrente do OMIE",
                            "descricao": (linha.get("descricao") or "")[:90],
                            "data": linha.get("data"),
                            "valor": linha.get("valor")})
            continue

        vai.append({
            "linha_id": linha["id"],
            "tipo_id": tipo["id"],
            "tipo": tipo["nome"],
            "transferencia": e_transferencia,
            "destino_id": (destino or {}).get("id"),
            "destino_nome": (destino or {}).get("nome", ""),
            "destino_conta_corrente": (destino or {}).get("omie_conta_corrente"),
            # A ponta que RECEBE é do banco de destino — se ele tiver
            # fornecedor próprio, é o dele que vale naquele título.
            "destino_fornecedor": (destino or {}).get("omie_fornecedor"),
            "destino_departamento": _obra_de(destino),
            # ⚠️ O SENTIDO VEM DO SINAL, não de configuração: negativo é conta
            # a pagar, positivo é conta a receber. Um estorno de tarifa entra
            # sozinho do lado certo, e não há campo a mais para errar.
            "sentido": "pagar" if valor < 0 else "receber",
            "valor": abs(valor),
            "data": linha["data"],
            "descricao": (linha.get("descricao") or "").strip(),
            "documento": (linha.get("documento") or "").strip(),
            "codigo_categoria": str(tipo["codigo_categoria"]).strip(),
            "codigo_cliente": int(_fornecedor_de(conta)),
            "cod_departamento": _obra_de(conta),
            "id_conta_corrente": int(conta["omie_conta_corrente"]),
            "codigo_integracao": _codigo_de_integracao(linha["id"]),
        })

    return {"vai": vai, "nao_vai": nao_vai,
            "total": sum(x["valor"] if x["sentido"] == "receber"
                         else -x["valor"] for x in vai)}


# ---------------------------------------------------------------------------
# O LANÇAMENTO DE CONTA CORRENTE — e por que ele substituiu o título
#
# ⚠️ ISTO É CORREÇÃO DE 29/09/2026, e o dono achou o erro olhando o resultado:
#
#     *"Acho que você criou uma conta a pagar para a tarifa, e não um lançamento
#     de conta corrente."*
#
# Ele está certo, e o erro era de conceito. Uma linha do EXTRATO é dinheiro que
# JÁ se moveu na conta — não é um compromisso a vencer. Criando conta a pagar, o
# sistema inventava um título em aberto e precisava dar baixa nele em seguida
# para "consumir" o que nunca devia existir. Dois passos onde há um, e o primeiro
# deles sujando o contas a pagar do OMIE.
#
# E o segundo passo nem funcionava: a baixa respondia **404** em
# `financas/contapagarbaixa/`. Ele desconfiou disso também — *"ainda assim, acho
# que isso não existe"* — e o resultado era o pior dos mundos: título criado,
# baixa falhando, e um recado mandando ele dar baixa na mão.
#
# O CAMINHO CERTO ele já usava no Make, e mandou o blueprint: `IncluirLancCC`,
# em `financas/contacorrentelancamentos/`. Um lançamento, direto na conta
# corrente, sem título e sem baixa.
#
# ⚠️ UMA DIVERGÊNCIA DELIBERADA EM RELAÇÃO AO BLUEPRINT DELE, e está aqui para ele
# poder discordar: lá o `cTipo` mapeia Receita → "TRA", junto com Transferência.
# Aqui receita vira "CRE". A razão é que naquele fluxo as receitas eram todas
# transferência entre contas da empresa; aqui o sentido vem do SINAL da linha do
# extrato, e uma entrada que não é transferência marcada como TRA apareceria no
# OMIE como movimento entre contas — sem a outra ponta. Se ele preferir o mapa do
# Make, é uma linha.
# ---------------------------------------------------------------------------
URL_LANC_CC = "https://app.omie.com.br/api/v1/financas/contacorrentelancamentos/"
ACAO_LANC_CC = "IncluirLancCC"

# O tipo do lançamento, no vocabulário do OMIE.
TIPO_DEBITO = "DEB"
TIPO_CREDITO = "CRE"
TIPO_TRANSFERENCIA = "TRA"


def tipo_do_lancamento(item: dict) -> str:
    """`DEB`, `CRE` ou `TRA`. Ver o aviso acima sobre a divergência do Make."""
    if item.get("transferencia"):
        return TIPO_TRANSFERENCIA
    return TIPO_DEBITO if item.get("sentido") == "pagar" else TIPO_CREDITO


def montar_lancamento_cc(item: dict) -> dict:
    """O `param` do `IncluirLancCC`. A forma é a do blueprint do Make dele.

    ⚠️ O VALOR VAI SEMPRE POSITIVO, e quem diz a direção é o `cTipo` — é assim no
    Make dele (o blueprint tira o sinal com um `replace`) e é assim no OMIE. Mandar
    negativo com `cTipo` DEB debitaria duas vezes o sinal.
    """
    valor = float(item["valor"])
    param = {
        "cCodIntLanc": item["codigo_integracao"],
        "cabecalho": {
            "nCodCC": int(item["id_conta_corrente"]),
            "dDtLanc": item["data"].strftime("%d/%m/%Y"),
            "nValorLanc": valor,
        },
        "detalhes": {
            "cCodCateg": item["codigo_categoria"],
            "cTipo": tipo_do_lancamento(item),
            "nCodCliente": int(item["codigo_cliente"]),
            # O histórico do banco vira a observação, numa linha só: a descrição
            # do Bradesco vem com quebra de linha dentro.
            "cObs": re.sub(r"\s+", " ", item["descricao"])[:200],
        },
    }
    if item.get("cod_departamento"):
        # ⚠️ AQUI O DEPARTAMENTO VAI EM VALOR, não em percentual — é a diferença
        # para o título (`distribuicao`/`nPerDep`), e está no blueprint dele.
        param["departamentos"] = [{"cCodDep": item["cod_departamento"],
                                   "nValDep": valor}]
    return param


def montar_inclusao(item: dict) -> dict:
    """O `param` da inclusão no OMIE como TÍTULO (conta a pagar / a receber).

    ⚠️ NÃO É MAIS O CAMINHO DA CONCILIAÇÃO — ver o aviso acima. Fica porque os
    APORTES continuam sendo título de verdade (um compromisso a vencer), e porque
    apagar uma função que o aporte usa para "limpar" seria trocar um problema por
    outro.

    ⚠️ CAMPO SOBRANDO FAZ A CHAMADA INTEIRA FALHAR, e a mensagem do OMIE não
    diz qual foi o culpado. Vai o mínimo que resolve, e nada de enfeite — a
    mesma regra escrita no `aportes_omie.py`.
    """
    data_br = item["data"].strftime("%d/%m/%Y")
    # O histórico do banco é a observação, cortado: o OMIE tem limite e a
    # descrição do Bradesco vem com quebra de linha dentro.
    observacao = re.sub(r"\s+", " ", item["descricao"])[:200]
    param = {
        "codigo_lancamento_integracao": item["codigo_integracao"],
        "codigo_cliente_fornecedor": int(item["codigo_cliente"]),
        "data_vencimento": data_br,
        "data_previsao": data_br,
        "data_emissao": data_br,
        "valor_documento": float(item["valor"]),
        "codigo_categoria": item["codigo_categoria"],
        "id_conta_corrente": int(item["id_conta_corrente"]),
        # ⚠️ 20 CARACTERES é o teto do OMIE para este campo, e passar disso
        # custou oito tentativas repetidas em 21/09/2026.
        "numero_documento": (item.get("documento")
                             or item["codigo_integracao"])[:20],
        "observacao": observacao,
    }
    if item.get("cod_departamento"):
        param["distribuicao"] = [{"cCodDep": item["cod_departamento"],
                                  "nPerDep": 100}]
    return param


def montar_baixa(item: dict, codigo_lancamento: int) -> dict:
    """A baixa de um TÍTULO.

    ⚠️ A CONCILIAÇÃO NÃO USA MAIS ISTO, e o motivo está no aviso de
    `montar_lancamento_cc`: a linha do extrato virou lançamento de conta corrente,
    que já é o dinheiro movimentado — não há título em aberto para consumir, e era
    justamente esta chamada que respondia 404.

    Fica porque o formato continua descrito em teste e porque o dia em que alguém
    precisar baixar um título por aqui, o molde está pronto e com a URL num lugar
    só. Não deve ser usada sem antes conferir a rota no OMIE.
    """
    return {
        "codigo_lancamento": int(codigo_lancamento),
        "codigo_conta_corrente": int(item["id_conta_corrente"]),
        "valor": float(item["valor"]),
        "data": item["data"].strftime("%d/%m/%Y"),
        "observacao": re.sub(r"\s+", " ", item["descricao"])[:200],
    }


# ---------------------------------------------------------------------------
# LANÇAR DE VERDADE
# ---------------------------------------------------------------------------
SEGUNDOS_POR_TENTATIVA = 30
TENTATIVAS = 3


def _cliente():
    """O cliente do OMIE ajustado PARA TELA, e não para a carga da madrugada.

    Os padrões do `OmieClient` esperam até 17 minutos por chamada — bom para
    um processo que roda sozinho de noite, péssimo para quem está olhando. O
    teto de tela existe por causa disso, e a lição é de 21/09/2026: *"tá
    demorando muito, com certeza o OMIE já teria respondido"*.
    """
    from app.apps.painel.sync.omie_client import (TETO_DE_ESPERA_NA_TELA,
                                                  OmieClient)
    return OmieClient.de_ambiente(
        timeout=SEGUNDOS_POR_TENTATIVA, max_tentativas=TENTATIVAS,
        backoff_base=1.4, teto_de_espera=TETO_DE_ESPERA_NA_TELA)


def _numero_do_titulo(resposta: dict):
    """O número que o OMIE devolveu. `nCodLanc` é o do lançamento de conta
    corrente; os outros são dos títulos, e continuam valendo porque o aporte
    ainda os usa."""
    for chave in ("nCodLanc", "codigo_lancamento_omie", "codigo_lancamento",
                  "nCodTitulo"):
        valor = (resposta or {}).get(chave)
        if valor:
            try:
                return int(valor)
            except (TypeError, ValueError):
                continue
    return None


def lancar(itens: list, quem: str = "", cliente=None) -> dict:
    """Lança na conta corrente do OMIE, um por um. Devolve o que deu e o que não.

    ⚠️ AQUI CADA LINHA É INDEPENDENTE, e é o CONTRÁRIO do aporte. No aporte, um
    título sem o outro é meio aporte — um lado do dinheiro sem o outro —, e por
    isso lá a falha de um desfaz todos. Aqui cada linha é uma tarifa isolada:
    desfazer as que já entraram porque a décima falhou faria o dono perder
    trabalho bom por causa de um problema que não é dele.

    Então: o que entrar, fica; o que falhar, fica marcado com o erro e pode ser
    tentado de novo. Reenviar é seguro — o OMIE recusa o código de integração
    repetido.
    """
    if not _pronto():
        raise ErroDoLancamento(FALTA_MIGRAR)
    if not itens:
        return {"gravados": 0, "falhas": [], "feitos": []}
    if len(itens) > MAX_POR_VEZ:
        raise ErroDoLancamento(
            f"São {len(itens)} linhas de uma vez, acima do teto de "
            f"{MAX_POR_VEZ}. Cada uma são duas conversas com o OMIE, e a tela "
            "ficaria esperando tempo demais. Mande em levas menores.")

    cli = cliente or _cliente()
    feitos, falhas = [], []

    for item in itens:
        # ⚠️ REGISTRA "ENVIANDO" ANTES DE ENVIAR. Se a resposta se perder no
        # caminho, o lançamento pode ter entrado no OMIE — e a linha precisa
        # apontar isso, em vez de parecer que nada aconteceu. Foi assim que o
        # aporte ficou seguro.
        _registrar(item["linha_id"], item["tipo_id"],
                   item["codigo_integracao"], "enviando", quem)
        try:
            # ⚠️ LANÇAMENTO DE CONTA CORRENTE, não título — ver o aviso longo em
            # `montar_lancamento_cc`. Uma linha do extrato é dinheiro que JÁ se
            # moveu; título é compromisso a vencer.
            resposta = cli._call(URL_LANC_CC, ACAO_LANC_CC,
                                 montar_lancamento_cc(item))
            codigo = _numero_do_titulo(resposta)
            if not codigo:
                raise ErroDoLancamento(
                    "o OMIE aceitou mas não devolveu o número do lançamento")
        except Exception as e:  # noqa: BLE001 — a tela precisa da frase
            logger.exception("Conciliação: falhou lançar a linha %s",
                             item["linha_id"])
            _registrar(item["linha_id"], item["tipo_id"],
                       item["codigo_integracao"], "falhou", quem,
                       erro=str(e)[:400])
            falhas.append({"linha_id": item["linha_id"],
                           "descricao": item["descricao"][:90],
                           "erro": str(e)[:300]})
            continue

        # ⚠️ NÃO HÁ MAIS BAIXA, e a ausência dela é o conserto. O lançamento de
        # conta corrente JÁ É o dinheiro movimentado: não existe título em aberto
        # para consumir. Era a baixa que respondia 404 e mandava o dono terminar o
        # serviço na mão.

        # ⚠️ A TRANSFERÊNCIA TEM DUAS PONTAS, e a segunda é feita AQUI, depois
        # da primeira ter entrado. No OMIE a transferência é um PAR DE TÍTULOS
        # com categoria marcada como transferência — foi o espelho do painel
        # que respondeu isso (`painel/sync/fato.py`: transferencia=S vai para o
        # balde TRF e não entra no resultado). Não há rota especial a inventar.
        #
        # ⚠️ E SE A SEGUNDA PONTA FALHAR, A LINHA DIZ ISSO ALTO. Meia
        # transferência é dinheiro que saiu de uma conta e não entrou em
        # nenhuma — o saldo das duas fica errado, e é o pior estado possível.
        codigo_par = None
        if item.get("transferencia"):
            codigo_par, erro_par = _outra_ponta(cli, item, quem)
            if erro_par:
                _registrar(item["linha_id"], item["tipo_id"],
                           item["codigo_integracao"], "meia_transferencia",
                           quem, codigo=codigo, erro=erro_par,
                           conta_par=item.get("destino_id"))
                falhas.append({
                    "linha_id": item["linha_id"],
                    "descricao": item["descricao"][:90],
                    "erro": (f"⚠️ METADE DA TRANSFERÊNCIA ENTROU: o título "
                             f"{codigo} saiu da conta de origem, mas a entrada "
                             f"em \"{item.get('destino_nome')}\" falhou. O "
                             f"saldo das duas contas está errado no OMIE até "
                             f"alguém lançar a outra ponta. ({erro_par[:150]})")})
                feitos.append({"linha_id": item["linha_id"], "codigo": codigo,
                               "descricao": item["descricao"][:90]})
                continue

        _registrar(item["linha_id"], item["tipo_id"],
                   item["codigo_integracao"], "gravado", quem,
                   codigo=codigo, codigo_par=codigo_par,
                   conta_par=item.get("destino_id"))
        feitos.append({"linha_id": item["linha_id"], "codigo": codigo,
                       "codigo_par": codigo_par,
                       "descricao": item["descricao"][:90]})

    logger.info("Conciliação: %s lançou %s de %s no OMIE.", quem or "?",
                len(feitos), len(itens))
    return {"gravados": len(feitos), "feitos": feitos, "falhas": falhas}


def _outra_ponta(cli, item: dict, quem: str):
    """A ENTRADA na conta de destino. Devolve `(codigo, erro)`.

    ⚠️ O CÓDIGO DE INTEGRAÇÃO DELA É OUTRO ("CONC5D"), senão o OMIE recusaria
    a segunda ponta como repetição da primeira — e a transferência ficaria
    pela metade toda vez, sem ninguém entender por quê.
    """
    entrada = dict(item,
                   sentido="receber",
                   id_conta_corrente=int(item["destino_conta_corrente"]),
                   codigo_integracao=item["codigo_integracao"] + "D",
                   descricao=f"{item['descricao']} (entrada da transferência)")
    # O lançamento que ENTRA é do banco de destino. Quando ele tem fornecedor
    # próprio, é o dele — senão fica o da origem, que é melhor que nenhum.
    if item.get("destino_fornecedor"):
        entrada["codigo_cliente"] = int(item["destino_fornecedor"])
    # E a obra da ponta que entra é a da conta de destino (02/10/2026).
    entrada["cod_departamento"] = item.get("destino_departamento") or ""
    try:
        # ⚠️ AS DUAS PONTAS SÃO LANÇAMENTOS DE CONTA CORRENTE, e as duas com
        # `cTipo` = TRA: no OMIE a transferência é isso — uma saída numa conta e
        # uma entrada em outra, marcadas como transferência para não entrarem no
        # resultado. E não há baixa em nenhuma das duas.
        resposta = cli._call(URL_LANC_CC, ACAO_LANC_CC,
                             montar_lancamento_cc(entrada))
        codigo = _numero_do_titulo(resposta)
        if not codigo:
            return None, "o OMIE aceitou mas não devolveu o número do lançamento"
        return codigo, ""
    except Exception as e:  # noqa: BLE001 — quem lê a frase é o dono
        logger.exception("Conciliação: falhou a outra ponta da transferência")
        return None, str(e)[:400]


def _registrar(linha_id: int, tipo_id: int, integracao: str, situacao: str,
               quem: str, codigo=None, erro: str = "", codigo_par=None,
               conta_par=None) -> None:
    """Grava na linha do extrato o que aconteceu com ela no OMIE."""
    from .db import conexao, tem_coluna
    # ⚠️ AS COLUNAS DO PAR SÃO DA MIGRAÇÃO 022, e o código sobe antes do botão.
    com_par = tem_coluna("conciliacao_extrato", "omie_codigo_par")
    extra = (", omie_codigo_par = coalesce(?, omie_codigo_par), "
             "conta_par_id = coalesce(?, conta_par_id)") if com_par else ""
    valores_par = ((int(codigo_par) if codigo_par else None,
                    int(conta_par) if conta_par else None) if com_par else ())
    with conexao() as con:
        con.execute(
            "UPDATE analisesps.conciliacao_extrato "
            "   SET tipo_id = ?, omie_integracao = ?, omie_situacao = ?, "
            "       omie_codigo = coalesce(?, omie_codigo), omie_erro = ?, "
            f"       omie_em = now(), omie_por = ?{extra} "
            " WHERE id = ?",
            (int(tipo_id) if tipo_id else None, integracao, situacao,
             int(codigo) if codigo else None, erro, quem) + valores_par
            + (int(linha_id),))
        con.commit()


def pendencias() -> list[dict]:
    """O que ficou pelo caminho: enviado sem resposta, ou título sem baixa.

    ⚠️ ESTA LISTA É O QUE IMPEDE O TRABALHO MANUAL ESQUECIDO. Um título criado
    no OMIE cuja baixa falhou fica lá, em aberto, dizendo que há algo a pagar
    que já foi pago — e ninguém descobre isso olhando o extrato daqui.
    """
    if not _pronto():
        return []
    from .db import consultar
    linhas = consultar(
        "SELECT e.id, e.data, e.descricao, e.valor, e.omie_codigo, "
        "       e.omie_situacao, e.omie_erro, c.nome "
        "  FROM analisesps.conciliacao_extrato e "
        "  JOIN analisesps.conciliacao_conta c ON c.id = e.conta_id "
        " WHERE e.omie_situacao IN ('enviando', 'sem_baixa', 'falhou', "
        "                          'meia_transferencia') "
        " ORDER BY e.omie_em DESC LIMIT 100")
    nomes = ["id", "data", "descricao", "valor", "omie_codigo",
             "omie_situacao", "omie_erro", "conta"]
    return [dict(zip(nomes, linha)) for linha in linhas]


# ---------------------------------------------------------------------------
# AS LISTAS DO OMIE — para ESCOLHER em vez de digitar código
#
# Pedido do dono em 24/09/2026: *"eu acho que você pode utilizar a própria API
# dele para atualizar aqui, criar uma basezinha de informações com elas. Tem a
# questão do código dos departamentos também (…) o ideal é que a gente já
# extraia direto do OMIE."*
#
# ⚠️ E A RESPOSTA É QUE ISSO JÁ EXISTE — não se chama a API do OMIE aqui.
# A carga do painel traz TODA NOITE as contas correntes, o plano financeiro,
# os cadastros e o rateio, e guarda no espelho (`painel.*`). Chamar a API de
# novo daqui seria: mais uma credencial para manter, mais uma chance de bater
# no limite do OMIE, e duas cópias dos mesmos dados que um dia divergiriam.
#
# O preço, dito claro: **estas listas têm a idade da última carga do painel**.
# Uma conta corrente criada hoje de manhã no OMIE só aparece aqui depois que a
# carga rodar. Para o que esta tela faz — apontar categoria de tarifa —, isso
# não incomoda; se um dia incomodar, o conserto é rodar a carga, não duplicar
# a integração.
# ---------------------------------------------------------------------------
def listas_do_omie() -> dict:
    """Contas correntes, plano financeiro e obras, do espelho do painel.

    Nunca levanta: uma tela de configuração que não abre porque o espelho está
    vazio é pior do que uma que abre dizendo que a lista está vazia.
    """
    from . import aportes_de_para as dp

    saida = {"contas": [], "categorias": [], "obras": [], "fornecedores": [],
             "erro": ""}
    try:
        saida["contas"] = dp.contas_do_omie()
    except Exception as e:  # noqa: BLE001 — a tela diz, e segue
        saida["erro"] = str(e)
    try:
        # Os BANCOS, para o campo de fornecedor da conta. A lista inteira de
        # fornecedores tem milhares de linhas e não cabe num `select`; quem se
        # cadastra como cobrador de tarifa é banco, então é por banco que se
        # procura. Quem não achar digita o código, como antes.
        saida["fornecedores"] = bancos_do_omie()
    except Exception:  # noqa: BLE001 — a tela abre sem a lista
        pass
    try:
        saida["obras"] = dp.obras()
    except Exception:  # noqa: BLE001
        pass
    try:
        saida["categorias"] = categorias_do_omie()
    except Exception as e:  # noqa: BLE001
        saida["erro"] = saida["erro"] or str(e)
    return saida


def categorias_do_omie(busca: str = "", limite: int = 400) -> list:
    """O plano financeiro do OMIE, do espelho do painel.

    ⚠️ AS INATIVAS VÊM MARCADAS, NÃO ESCONDIDAS. Uma categoria desativada
    ontem ainda é a certa para um lançamento de mês passado — sumir com ela
    faria o dono procurar o que existe e não achar.
    """
    from .aportes_de_para import _consultar

    termo = str(busca or "").strip().lower()
    linhas = _consultar(
        "SELECT codigo, COALESCE(descricao, ''), COALESCE(conta_inativa, ''), "
        "       COALESCE(transferencia, '') "
        "  FROM painel.cat "
        " WHERE ? = '' OR LOWER(COALESCE(descricao, '')) LIKE ? "
        "    OR LOWER(codigo) LIKE ? "
        " ORDER BY codigo LIMIT ?",
        (termo, f"%{termo}%", f"%{termo}%", int(limite)))
    return [{"codigo": l[0], "descricao": l[1],
             "inativa": str(l[2]).upper().startswith("S"),
             "transferencia": str(l[3]).upper().startswith("S")}
            for l in linhas]


def nomes_de_fornecedores(codigos) -> dict:
    """`{codigo: "RAZÃO SOCIAL · documento"}` — para a tela dizer QUEM está
    gravado na conta, e não só o número. Nunca levanta."""
    from .aportes_de_para import _consultar

    limpos = sorted({int(c) for c in codigos or [] if str(c or "").strip().isdigit()})
    if not limpos:
        return {}
    try:
        marcas = ",".join("?" * len(limpos))
        linhas = _consultar(
            "SELECT codigo, COALESCE(razao_social, ''), COALESCE(cnpj_cpf, '') "
            f"  FROM painel.clientes WHERE codigo IN ({marcas})", tuple(limpos))
    except Exception:  # noqa: BLE001 — sem o espelho, a tela mostra o código
        logger.exception("Conciliação: não consegui ler os nomes dos fornecedores")
        return {}
    return {int(l[0]): " · ".join(x for x in (l[1], l[2]) if x) for l in linhas}


def bancos_do_omie(busca: str = "", limite: int = 300) -> list:
    """Cadastros do OMIE que parecem ser bancos, para o campo da conta.

    ⚠️ POR QUE FILTRAR, E NÃO TRAZER TODOS. O espelho tem milhares de
    fornecedores — a lista inteira não cabe num campo de escolha, e rolar
    milhares de linhas para achar "BRADESCO" é pior do que digitar o código.
    Quem cobra tarifa bancária é banco, e é por banco que se procura aqui.

    A busca livre continua valendo: digitando qualquer coisa, procura em tudo.
    Assim um cobrador que não seja banco ainda é alcançável.
    """
    from .aportes_de_para import _consultar

    termo = str(busca or "").strip().lower()
    if termo:
        # O CNPJ é procurado SÓ PELOS DÍGITOS: no espelho ele vem com ponto e
        # barra, e quem digita "60746948" tem de achar "60.746.948/0001-12".
        digitos = re.sub(r"\D", "", termo)
        linhas = _consultar(
            "SELECT codigo, COALESCE(razao_social, ''), COALESCE(cnpj_cpf, '') "
            "  FROM painel.clientes "
            " WHERE LOWER(COALESCE(razao_social, '')) LIKE ? "
            "    OR LOWER(COALESCE(nome_fantasia, '')) LIKE ? "
            "    OR (? <> '' AND regexp_replace(COALESCE(cnpj_cpf, ''), '[^0-9]', '', 'g') LIKE ?) "
            " ORDER BY razao_social LIMIT ?",
            (f"%{termo}%", f"%{termo}%", digitos, f"%{digitos}%", int(limite)))
    else:
        # As palavras que aparecem na razão social de banco. Não é lista de
        # bancos — é o que basta para o campo nascer útil.
        curinga = ("%banco%", "%bradesco%", "%itau%", "%itaú%", "%santander%",
                   "%sicredi%", "%caixa%", "%brasil%", "%safra%", "%inter%",
                   "%sicoob%", "%btg%", "%nubank%", "%c6%", "%daycoval%")
        onde = " OR ".join(["LOWER(COALESCE(razao_social, '')) LIKE ?"] * len(curinga))
        linhas = _consultar(
            "SELECT codigo, COALESCE(razao_social, ''), COALESCE(cnpj_cpf, '') "
            f"  FROM painel.clientes WHERE {onde} "
            " ORDER BY razao_social LIMIT ?",
            curinga + (int(limite),))
    return [{"codigo": l[0], "nome": l[1], "documento": l[2]} for l in linhas]
