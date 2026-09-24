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
def _codigo_de_integracao(linha_id: int) -> str:
    """A identidade do lançamento no OMIE, tirada da linha do extrato.

    ⚠️ É O QUE IMPEDE LANÇAR DUAS VEZES, e a trava não é este código: é o OMIE
    recusando o segundo envio com "código de integração já cadastrado". Uma
    conferência feita só deste lado não sobrevive a duas pessoas clicando ao
    mesmo tempo; a recusa dele sobrevive.

    ⚠️ MÁXIMO 20 CARACTERES no `numero_documento` — o OMIE recusa acima disso,
    e essa lição custou oito tentativas repetidas em 21/09/2026.
    """
    return f"CONC{int(linha_id)}"


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
        elif not tipo.get("codigo_cliente"):
            motivo = (f"o tipo \"{tipo['nome']}\" está sem o fornecedor/cliente "
                      "do OMIE")
        elif not linha.get("valor"):
            motivo = "o valor é zero"

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
            # ⚠️ O SENTIDO VEM DO SINAL, não de configuração: negativo é conta
            # a pagar, positivo é conta a receber. Um estorno de tarifa entra
            # sozinho do lado certo, e não há campo a mais para errar.
            "sentido": "pagar" if valor < 0 else "receber",
            "valor": abs(valor),
            "data": linha["data"],
            "descricao": (linha.get("descricao") or "").strip(),
            "documento": (linha.get("documento") or "").strip(),
            "codigo_categoria": str(tipo["codigo_categoria"]).strip(),
            "codigo_cliente": int(tipo["codigo_cliente"]),
            "cod_departamento": str(tipo.get("cod_departamento") or "").strip(),
            "id_conta_corrente": int(conta["omie_conta_corrente"]),
            "codigo_integracao": _codigo_de_integracao(linha["id"]),
        })

    return {"vai": vai, "nao_vai": nao_vai,
            "total": sum(x["valor"] if x["sentido"] == "receber"
                         else -x["valor"] for x in vai)}


def montar_inclusao(item: dict) -> dict:
    """O `param` da inclusão no OMIE. Os campos são os do aporte, que roda.

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
    """A baixa — o que faz o dinheiro constar como movimentado.

    ⚠️ AQUI A BAIXA NÃO É OPCIONAL, e é diferente do aporte. A linha veio do
    EXTRATO DO BANCO: o dinheiro já saiu ou já entrou, isso é fato consumado.
    Criar o título em aberto deixaria um saldo falso no OMIE dizendo que ainda
    há algo a pagar que já foi pago.
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
    for chave in ("codigo_lancamento_omie", "codigo_lancamento", "nCodTitulo"):
        valor = (resposta or {}).get(chave)
        if valor:
            try:
                return int(valor)
            except (TypeError, ValueError):
                continue
    return None


def lancar(itens: list, quem: str = "", cliente=None) -> dict:
    """Cria e baixa no OMIE, um por um. Devolve o que deu e o que não deu.

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
        # ⚠️ AS PORTAS SÃO AS MESMAS DO APORTE, importadas de lá em vez de
        # reescritas. Elas já estão certas e em produção; uma segunda lista de
        # URLs neste repositório seria uma cópia esperando divergir.
        from .aportes_omie import PORTAS
        url, acao, _excluir, url_baixa, acao_baixa = PORTAS[
            "P" if item["sentido"] == "pagar" else "R"]
        # ⚠️ REGISTRA "ENVIANDO" ANTES DE ENVIAR. Se a resposta se perder no
        # caminho, o título pode ter entrado no OMIE — e a linha precisa
        # apontar isso, em vez de parecer que nada aconteceu. Foi assim que o
        # aporte ficou seguro.
        _registrar(item["linha_id"], item["tipo_id"],
                   item["codigo_integracao"], "enviando", quem)
        try:
            resposta = cli._call(url, acao, montar_inclusao(item))
            codigo = _numero_do_titulo(resposta)
            if not codigo:
                raise ErroDoLancamento(
                    "o OMIE aceitou mas não devolveu o número do título")
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

        # A baixa. Se ela falhar, o título JÁ EXISTE — e a linha tem de dizer
        # isso, senão alguém lança de novo e duplica no OMIE.
        baixa_ok = True
        erro_baixa = ""
        try:
            cli._call(url_baixa, acao_baixa, montar_baixa(item, codigo))
        except Exception as e:  # noqa: BLE001
            logger.exception("Conciliação: título %s criado, baixa falhou",
                             codigo)
            baixa_ok = False
            erro_baixa = str(e)[:400]

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
        if item.get("transferencia") and baixa_ok:
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
                               "baixado": baixa_ok,
                               "descricao": item["descricao"][:90]})
                continue

        _registrar(item["linha_id"], item["tipo_id"],
                   item["codigo_integracao"],
                   "gravado" if baixa_ok else "sem_baixa", quem,
                   codigo=codigo, erro=erro_baixa, codigo_par=codigo_par,
                   conta_par=item.get("destino_id"))
        feitos.append({"linha_id": item["linha_id"], "codigo": codigo,
                       "codigo_par": codigo_par, "baixado": baixa_ok,
                       "descricao": item["descricao"][:90]})
        if not baixa_ok:
            falhas.append({
                "linha_id": item["linha_id"],
                "descricao": item["descricao"][:90],
                "erro": (f"o título {codigo} FOI CRIADO no OMIE, mas a baixa "
                         f"falhou — dê a baixa por lá. ({erro_baixa[:180]})")})

    logger.info("Conciliação: %s lançou %s de %s no OMIE.", quem or "?",
                len(feitos), len(itens))
    return {"gravados": len(feitos), "feitos": feitos, "falhas": falhas}


def _outra_ponta(cli, item: dict, quem: str):
    """A ENTRADA na conta de destino. Devolve `(codigo, erro)`.

    ⚠️ O CÓDIGO DE INTEGRAÇÃO DELA É OUTRO ("CONC5D"), senão o OMIE recusaria
    a segunda ponta como repetição da primeira — e a transferência ficaria
    pela metade toda vez, sem ninguém entender por quê.
    """
    from .aportes_omie import PORTAS
    url, acao, _excluir, url_baixa, acao_baixa = PORTAS["R"]
    entrada = dict(item,
                   sentido="receber",
                   id_conta_corrente=int(item["destino_conta_corrente"]),
                   codigo_integracao=item["codigo_integracao"] + "D",
                   descricao=f"{item['descricao']} (entrada da transferência)")
    try:
        resposta = cli._call(url, acao, montar_inclusao(entrada))
        codigo = _numero_do_titulo(resposta)
        if not codigo:
            return None, "o OMIE aceitou mas não devolveu o número do título"
        cli._call(url_baixa, acao_baixa, montar_baixa(entrada, codigo))
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

    saida = {"contas": [], "categorias": [], "obras": [], "erro": ""}
    try:
        saida["contas"] = dp.contas_do_omie()
    except Exception as e:  # noqa: BLE001 — a tela diz, e segue
        saida["erro"] = str(e)
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
