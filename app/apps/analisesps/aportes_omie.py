# -*- coding: utf-8 -*-
"""
A escrita dos aportes NO OMIE.

⚠️ ESTE É O SEGUNDO LUGAR DO REPOSITÓRIO QUE ESCREVE NUM SISTEMA DE FORA, e o
primeiro que CRIA alguma coisa lá. O outro (`painel/sync/omie_escrita.py`) só
altera categoria e departamento de título que já existe. A diferença importa:
um erro ali deixa um título mal classificado; um erro aqui inventa dinheiro no
cadastro da empresa.

Três cuidados, todos exigidos pelo dono no briefing:

  1. **Nunca escrever sem ensaio antes.** `ensaiar()` monta exatamente o que
     seria enviado e não envia nada. A sequência combinada é: ensaio → UM
     lançamento conferido por ele dentro do OMIE, com os olhos → uso normal.
  2. **Os dois títulos nascem amarrados.** Se o segundo falhar depois de o
     primeiro ter entrado, o primeiro é DESFEITO. Não dando para desfazer, a
     tela entrega o número do título órfão para ele resolver à mão — nunca um
     "deu erro" genérico que esconde meio aporte lá dentro.
  3. **Tudo que for gravado fica registrado**: o que, quando, por quem e o
     número que o OMIE devolveu.

⚠️ O QUE NÃO FOI POSSÍVEL CONFERIR AQUI, e está dito sem rodeio porque muda o
que se deve esperar da primeira tentativa: **a documentação do OMIE não é
alcançável deste ambiente** (a rede bloqueia o domínio). Os campos da
INCLUSÃO vieram de código que já roda em produção neste mesmo repositório
(`app/apps/atualizaspbotao/omie.py`, que inclui conta a pagar há meses) — esses
são firmes. Os campos da BAIXA e da EXCLUSÃO vieram da documentação pública
citada de segunda mão, e **não foram exercitados contra a API**. É exatamente
para isso que serve o ensaio e o primeiro lançamento conferido a olho: se o
OMIE recusar, a mensagem dele aparece inteira na tela, sem tradução minha.
"""
from __future__ import annotations

import hmac
import logging
import os

logger = logging.getLogger("analisesps.aportes_omie")

URL_CONTAPAGAR = "https://app.omie.com.br/api/v1/financas/contapagar/"
URL_CONTARECEBER = "https://app.omie.com.br/api/v1/financas/contareceber/"
URL_BAIXA_PAGAR = "https://app.omie.com.br/api/v1/financas/contapagarbaixa/"
URL_BAIXA_RECEBER = "https://app.omie.com.br/api/v1/financas/contareceberbaixa/"

# Por natureza do título: (url, incluir, excluir, url da baixa, chamada da baixa)
PORTAS = {
    "P": (URL_CONTAPAGAR, "IncluirContaPagar", "ExcluirContaPagar",
          URL_BAIXA_PAGAR, "LancarPagamento"),
    "R": (URL_CONTARECEBER, "IncluirContaReceber", "ExcluirContaReceber",
          URL_BAIXA_RECEBER, "LancarRecebimento"),
}

VARIAVEL_SENHA = "PAINEL_SENHA_ESCRITA"


class SemAutorizacao(Exception):
    """Senha de escrita ausente ou errada."""


class FalhaNoOmie(Exception):
    """O OMIE recusou. A mensagem dele vai junto, inteira."""


# ---------------------------------------------------------------------------
# A SENHA DE ESCRITA
# ---------------------------------------------------------------------------
def senha_configurada() -> bool:
    return bool(os.environ.get(VARIAVEL_SENHA, "").strip())


def conferir_senha(digitada: str) -> None:
    """Pedida no momento da gravação, NUNCA no ensaio.

    É o mesmo padrão do painel, e a razão é a mesma: quem está só conferindo
    não deve precisar da senha, e quem vai escrever no OMIE deve parar um
    segundo para digitá-la. `compare_digest` porque comparar segredo com `==`
    vaza o tamanho pelo tempo — custa nada fazer certo.
    """
    esperada = os.environ.get(VARIAVEL_SENHA, "").strip()
    if not esperada:
        raise SemAutorizacao(
            f"A senha de escrita no OMIE não está configurada no Render "
            f"(variável {VARIAVEL_SENHA}). Sem ela, nada é gravado.")
    if not hmac.compare_digest(str(digitada or "").strip(), esperada):
        raise SemAutorizacao("Senha de escrita incorreta.")


# ---------------------------------------------------------------------------
# OS PACOTES QUE VÃO PARA O OMIE
# ---------------------------------------------------------------------------
def montar_inclusao(titulo: dict) -> dict:
    """O `param` da inclusão de UM título.

    Os nomes dos campos vieram do `atualizaspbotao/omie.py`, que inclui conta a
    pagar em produção há meses. Campo sobrando faz a chamada inteira falhar, e
    a mensagem de erro do OMIE não diz qual foi o culpado — por isso aqui vai o
    mínimo que resolve, e nada de enfeite.
    """
    param = {
        "codigo_lancamento_integracao": titulo["codigo_integracao"],
        "codigo_cliente_fornecedor": int(titulo["codigo_cliente_fornecedor"]),
        "data_vencimento": titulo["data_br"],
        "data_previsao": titulo["data_br"],
        "data_emissao": titulo["data_br"],
        "valor_documento": float(titulo["valor"]),
        "codigo_categoria": str(titulo["codigo_categoria"]),
        "id_conta_corrente": int(titulo["id_conta_corrente"]),
        "numero_documento": titulo["numero_documento"],
        "observacao": titulo["observacao"],
    }
    # A obra. `nPerDep` 100 porque o aporte é inteiro de uma obra só — este
    # caminho não rateia, e não deve: rateio de aporte seria outra conversa,
    # com outra tela.
    if titulo.get("cod_departamento"):
        param["distribuicao"] = [{
            "cCodDep": str(titulo["cod_departamento"]),
            "nPerDep": 100,
        }]
    return param


def montar_baixa(titulo: dict, codigo_lancamento: int) -> dict:
    """O `param` da baixa — o que faz o dinheiro constar como movimentado.

    Decisão do dono em 20/09/2026: o aporte nasce JÁ BAIXADO, com a marcação
    desmarcável na tela. O motivo dele: aporte quase sempre é registro do que
    já aconteceu, e título em aberto esquecido vira saldo falso.
    """
    return {
        "codigo_lancamento": int(codigo_lancamento),
        "codigo_conta_corrente": int(titulo["id_conta_corrente"]),
        "valor": float(titulo["valor"]),
        "data": titulo["data_br"],
        "observacao": titulo["observacao"][:200],
    }


def ensaiar(plano: dict) -> list:
    """O que SERIA enviado, título a título. Não fala com o OMIE.

    É a peça que torna o "nunca escrever sem ensaio" possível de cumprir: a
    tela mostra o pacote exato, e o dono confere antes de qualquer coisa sair
    daqui.
    """
    ensaio = []
    for t in plano.get("titulos", []):
        url, incluir, _excluir, url_baixa, chamada_baixa = PORTAS[t["natureza"]]
        item = {
            "titulo": t,
            "url": url,
            "chamada": incluir,
            "param": montar_inclusao(t),
            "baixa": None,
        }
        if t.get("baixar"):
            item["baixa"] = {
                "url": url_baixa,
                "chamada": chamada_baixa,
                # O número do título ainda não existe no ensaio, e dizer isso é
                # mais honesto do que inventar um número de mentira.
                "param": montar_baixa(t, 0) | {
                    "codigo_lancamento": "(o número que o OMIE devolver)"},
            }
        ensaio.append(item)
    return ensaio


# ---------------------------------------------------------------------------
# A GRAVAÇÃO
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# ⚠️ O CLIENTE DAQUI É OUTRO — 21/09/2026
#
# O dono tentou gravar e a tela ficou pendurada: *"tá demorando muito. Com
# certeza o OMIE já terá respondido."*
#
# Ele estava certo, e a causa não era o OMIE. O `OmieClient` nasceu para a
# CARGA NOTURNA DO PAINEL, que roda sozinha e pode esperar o quanto for: 120
# segundos de espera por tentativa, OITO tentativas, com pausa crescente entre
# elas. Pior caso de UMA chamada: 17 minutos. Um lançamento são quatro
# chamadas (dois títulos e duas baixas): quase 70 minutos.
#
# ⚠️ E O ESTRAGO NÃO PARA NA TELA DELE. O serviço roda com 1 processo e 4
# linhas de atendimento (`--workers 1 --threads 4`). Cada gravação pendurada
# ocupa uma delas por todo esse tempo — quatro e o monorepo inteiro, os 18
# blueprints, para de responder. O `--timeout 3600` do gunicorn não socorre:
# ele deixa passar.
#
# Aqui tem gente esperando, então os números são outros:
#
#   · 30 s de espera por tentativa — o OMIE responde em segundos; passou
#     disso, não é lentidão, é alguma coisa errada;
#   · 3 tentativas, não 8;
#   · pausa menor entre elas.
#
# Pior caso: pouco mais de um minuto e meio por chamada, contra dezessete.
#
# ⚠️ REPETIR UMA INCLUSÃO NÃO DUPLICA TÍTULO: o `codigo_lancamento_integracao`
# vai em toda inclusão, e o OMIE recusa a segunda com "código de integração já
# cadastrado". É o que torna a retentativa segura mesmo escrevendo.
# ---------------------------------------------------------------------------
SEGUNDOS_POR_TENTATIVA = 30
TENTATIVAS = 3


def _cliente():
    from app.apps.painel.sync.omie_client import OmieClient
    return OmieClient.de_ambiente(
        timeout=SEGUNDOS_POR_TENTATIVA,
        max_tentativas=TENTATIVAS,
        backoff_base=1.4,
    )


def _numero_do_titulo(resposta: dict) -> int | None:
    """O OMIE devolve o número com nomes diferentes conforme a rota."""
    for chave in ("codigo_lancamento_omie", "codigo_lancamento", "nCodTitulo"):
        valor = (resposta or {}).get(chave)
        if valor:
            try:
                return int(valor)
            except (TypeError, ValueError):
                continue
    return None


def gravar(plano: dict, quem: str = "", cliente=None) -> dict:
    """Cria os títulos no OMIE e, se pedido, dá a baixa deles.

    A ORDEM É DELIBERADA, e é o que protege contra o meio-aporte:

      1. cria TODOS os títulos;
      2. se algum falhar, DESFAZ os que já entraram e para;
      3. só com todos de pé, dá as baixas.

    Por que a baixa fica por último: baixa que falha deixa um título correto e
    em aberto — chato, visível e fácil de resolver à mão. Título que falha
    deixa metade de uma operação, que é o que não pode existir. E por que não
    se desfaz nada depois de uma baixa falhar: apagar título que talvez já
    tenha baixa é trocar um problema pequeno por um grande.
    """
    cli = cliente or _cliente()
    criados, resultado = [], {"ok": False, "titulos": [], "avisos": [],
                              "orfaos": [], "grupo": plano.get("grupo", "")}

    for t in plano.get("titulos", []):
        url, incluir = PORTAS[t["natureza"]][:2]
        linha = {"titulo": t, "codigo": None, "erro": "", "baixado": False}
        # ⚠️ REGISTRA ANTES DE CHAMAR, e isto é o que salva quando tudo dá
        # errado ao mesmo tempo. Se o processo morrer no meio da chamada — o
        # Render reiniciando, a rede caindo —, sem esta linha não sobraria
        # nada dizendo que uma inclusão chegou a ser tentada, e ninguém
        # saberia se há título solto no OMIE. Com ela, a tela mostra
        # "enviando" e quem for conferir sabe onde procurar.
        _registrar(plano, t, None, False, "enviando", "", quem)
        try:
            resposta = cli._call(url, incluir, montar_inclusao(t))
            codigo = _numero_do_titulo(resposta)
            if not codigo:
                raise FalhaNoOmie(
                    "O OMIE aceitou mas não devolveu o número do título. Sem "
                    "ele não dá para desfazer nem para conferir — confira no "
                    "OMIE antes de lançar de novo.")
            linha["codigo"] = codigo
            criados.append((t, codigo))
        except Exception as e:  # noqa: BLE001 — a mensagem do OMIE vai inteira
            logger.exception("Aportes: falhou incluir título no OMIE")
            linha["erro"] = str(e)
            resultado["titulos"].append(linha)
            _registrar(plano, t, None, False, "falhou", str(e), quem)
            resultado.update(_desfazer(cli, plano, criados, quem))
            resultado["erro"] = (
                f"Não consegui criar o título da "
                f"{t['papel_rotulo']}: {e}")
            return resultado
        resultado["titulos"].append(linha)

    # Todos de pé. Agora as baixas.
    for linha in resultado["titulos"]:
        t = linha["titulo"]
        if not t.get("baixar") or not linha["codigo"]:
            continue
        url_baixa, chamada = PORTAS[t["natureza"]][3], PORTAS[t["natureza"]][4]
        try:
            cli._call(url_baixa, chamada, montar_baixa(t, linha["codigo"]))
            linha["baixado"] = True
        except Exception as e:  # noqa: BLE001
            logger.exception("Aportes: falhou dar baixa no título %s",
                             linha["codigo"])
            linha["erro"] = str(e)
            resultado["avisos"].append(
                f"O título {linha['codigo']} ({t['papel_rotulo']}, "
                f"{t['sentido_rotulo'].lower()}) FOI CRIADO, mas a baixa não "
                f"passou: {e}. Ele está no OMIE em aberto — ou você dá a baixa "
                f"lá, ou o saldo da conta fica sem este movimento.")

    for linha in resultado["titulos"]:
        _registrar(plano, linha["titulo"], linha["codigo"], linha["baixado"],
                   "gravado", linha["erro"], quem)

    resultado["ok"] = True
    return resultado


def gravar_varios(planos: list, quem: str = "", cliente=None) -> list:
    """Grava um lote. Um lançamento que falha NÃO para os outros.

    ⚠️ ESTA É A DIFERENÇA ENTRE O LOTE E O LANÇAMENTO ÚNICO, e ela é
    deliberada. Dentro de UM lançamento os dois títulos são amarrados: ou os
    dois entram, ou o que entrou é desfeito — meia operação não pode existir.
    ENTRE lançamentos é o contrário: eles são independentes, e parar na
    terceira linha deixaria as outras quarenta e sete por fazer sem motivo
    nenhum. Cada linha tem o seu próprio grupo e o seu próprio número.

    O cliente é UM só para o lote inteiro: ele carrega a sessão HTTP e o
    controle de excesso de chamadas do OMIE, e criar um por linha jogaria isso
    fora justamente quando mais importa.
    """
    cli = cliente or _cliente()
    resultados = []
    for plano in planos:
        try:
            resultados.append(gravar(plano, quem, cliente=cli))
        except Exception as e:  # noqa: BLE001 — a linha ruim não leva o lote
            logger.exception("Aportes: falhou gravar o lançamento %s do lote",
                             plano.get("grupo"))
            resultados.append({
                "ok": False, "grupo": plano.get("grupo", ""),
                "erro": f"Não consegui falar com o OMIE: {e}",
                "titulos": [], "avisos": [], "orfaos": []})
    return resultados


def _desfazer(cli, plano: dict, criados: list, quem: str) -> dict:
    """Apaga no OMIE os títulos que já tinham entrado. Órfão nunca fica calado."""
    avisos, orfaos = [], []
    for t, codigo in criados:
        url, _incluir, excluir = PORTAS[t["natureza"]][:3]
        try:
            cli._call(url, excluir, {"codigo_lancamento_omie": int(codigo)})
            avisos.append(
                f"O título {codigo} ({t['papel_rotulo']}) tinha sido criado e "
                f"foi desfeito — não ficou meio aporte no OMIE.")
            _marcar_desfeito(codigo)
        except Exception as e:  # noqa: BLE001
            logger.exception("Aportes: falhou desfazer o título %s", codigo)
            orfaos.append({"codigo": codigo, "papel": t["papel_rotulo"],
                           "natureza": t["natureza"], "erro": str(e)})
            _registrar(plano, t, codigo, False, "orfao", str(e), quem)
    return {"avisos": avisos, "orfaos": orfaos}


# ---------------------------------------------------------------------------
# O REGISTRO
# ---------------------------------------------------------------------------
def _registrar(plano: dict, titulo: dict, codigo, baixado: bool,
               situacao: str, erro: str, quem: str) -> None:
    """Nunca derruba a gravação: registro que falha não pode desfazer trabalho
    que deu certo no OMIE. Mas a falha vai para o log, alta."""
    try:
        from .db import conexao
        with conexao() as con:
            con.execute(
                "INSERT INTO analisesps.aporte_lancamento "
                "  (grupo, operacao, papel, sentido, natureza, codigo_categoria,"
                "   id_conta_corrente, codigo_cliente_fornecedor, cod_departamento,"
                "   valor, data, numero_documento, observacao, codigo_integracao,"
                "   codigo_lancamento_omie, baixado, situacao, erro, criado_por) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT (codigo_integracao) WHERE codigo_integracao <> '' "
                "DO UPDATE SET codigo_lancamento_omie = EXCLUDED.codigo_lancamento_omie,"
                "  baixado = EXCLUDED.baixado, situacao = EXCLUDED.situacao,"
                "  erro = EXCLUDED.erro",
                (plano.get("grupo", ""), plano.get("operacao", ""),
                 titulo.get("papel", ""), titulo.get("sentido", ""),
                 titulo.get("natureza", ""), titulo.get("codigo_categoria", ""),
                 titulo.get("id_conta_corrente"),
                 titulo.get("codigo_cliente_fornecedor"),
                 titulo.get("cod_departamento", ""), float(titulo.get("valor") or 0),
                 titulo.get("data"), titulo.get("numero_documento", ""),
                 titulo.get("observacao", ""), titulo.get("codigo_integracao", ""),
                 codigo, bool(baixado), situacao, erro or "", quem or ""))
            con.commit()
    except Exception:  # noqa: BLE001
        logger.exception(
            "Aportes: NÃO CONSEGUI REGISTRAR o lançamento %s (título %s no "
            "OMIE). O título pode existir lá sem registro aqui.",
            titulo.get("codigo_integracao"), codigo)


def _marcar_desfeito(codigo) -> None:
    try:
        from .db import conexao
        with conexao() as con:
            con.execute(
                "UPDATE analisesps.aporte_lancamento "
                "   SET situacao = 'desfeito', desfeito_em = now() "
                " WHERE codigo_lancamento_omie = ?", (int(codigo),))
            con.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Aportes: falhou marcar %s como desfeito", codigo)


def historico(limite: int = 50) -> list:
    """Os últimos lançamentos, para a tela e para auditoria."""
    try:
        from .db import consultar
        linhas = consultar(
            "SELECT grupo, operacao, papel, sentido, natureza, valor, data, "
            "       numero_documento, codigo_lancamento_omie, baixado, "
            "       situacao, erro, criado_em, criado_por "
            "  FROM analisesps.aporte_lancamento "
            " ORDER BY criado_em DESC, id DESC LIMIT ?", (int(limite),))
    except Exception:  # noqa: BLE001 — migração 018 ainda não aplicada
        logger.exception("Aportes: não consegui ler o histórico")
        return []
    campos = ("grupo", "operacao", "papel", "sentido", "natureza", "valor",
              "data", "numero_documento", "codigo_lancamento_omie", "baixado",
              "situacao", "erro", "criado_em", "criado_por")
    return [dict(zip(campos, l)) for l in linhas]


def em_duvida() -> list:
    """Lançamentos que ficaram em "enviando" — nem confirmados, nem falhados.

    Uma linha só fica assim se o processo morreu no meio da chamada ao OMIE:
    o Render reiniciando, a rede caindo, a publicação de uma versão nova. O
    título PODE existir lá dentro. É a lista que responde "mandei de novo ou
    não?", e por isso ela aparece na tela em vez de dormir no banco.
    """
    return [h for h in historico(200) if h.get("situacao") == "enviando"]


def orfaos() -> list:
    """Os títulos que entraram no OMIE e não puderam ser desfeitos.

    Esta lista é trabalho manual pendente lá dentro. Ela fica visível na tela
    até alguém resolver — órfão esquecido é meio aporte que nenhum relatório
    fecha.
    """
    return [h for h in historico(200) if h.get("situacao") == "orfao"]
