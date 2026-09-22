# ============================================================================
# ERP — core/suprimentos/fornecedores.py
# O fornecedor visto por Suprimentos: o que ele vende, de onde atende, por
# onde recebe cotação e com quem se fala.
#
# O cadastro básico (documento, razão social, contas bancárias) continua sendo
# de `core/cadastros/fornecedores.py` — é o mesmo fornecedor que o financeiro
# paga, e ter duas bases seria o começo do fim. Aqui só entra o que Suprimentos
# acrescentou na migração 033, mais a visão de gestão da tela.
#
# Por que "quem não tem categoria não recebe cotação": disparar a cotação de
# cimento para quem vende cabo elétrico faz o fornecedor parar de responder —
# e aí o que falta não é preço, é resposta.
# ============================================================================
from __future__ import annotations

import logging
import unicodedata
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import fornecedores as base
from app.apps.erp.core.suprimentos import regioes as svc_regioes
from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)
from app.apps.erp.db.models.cadastros import (
    Fornecedor, FornecedorCategoria, FornecedorContato, FornecedorPorte,
    InsumoCategoria, Usuario,
)

logger = logging.getLogger(__name__)

PORTE_ROTULOS = {
    "FABRICA": "Fábrica",
    "REP_FABRICA": "Representante de fábrica",
    "DISTRIBUIDOR": "Distribuidor",
    "LOCAL": "Comércio local",
    "HOMECENTER": "Home center",
}
CANAIS = ("EMAIL", "WHATSAPP", "TELEFONE", "PORTAL", "PRESENCIAL")
CANAL_ROTULOS = {
    "EMAIL": "E-mail", "WHATSAPP": "WhatsApp", "TELEFONE": "Telefone",
    "PORTAL": "Portal do fornecedor", "PRESENCIAL": "Presencial",
}


def _texto(valor: Any) -> str:
    return " ".join(str(valor or "").split())


def _chave(texto: str) -> str:
    forma = unicodedata.normalize("NFKD", _texto(texto))
    return "".join(c for c in forma if not unicodedata.combining(c)).casefold()


def _obter(s: Session, fornecedor_id: int) -> Fornecedor:
    forn = s.get(Fornecedor, fornecedor_id, with_for_update=True,
                 populate_existing=True)
    if forn is None:
        raise ErroNaoEncontrado("Fornecedor não encontrado.")
    return forn


# ---------------------------------------------------------------------------
# Os campos que Suprimentos acrescentou
# ---------------------------------------------------------------------------
def _aplicar_campos_de_suprimentos(s: Session, forn: Fornecedor,
                                   dados: dict[str, Any]) -> dict[str, Any]:
    mudou: dict[str, Any] = {}

    if "porte" in dados:
        bruto = _texto(dados.get("porte")).upper().replace(" ", "_")
        if not bruto:
            forn.porte = None
        elif bruto in PORTE_ROTULOS:
            forn.porte = FornecedorPorte(bruto)
        else:
            raise ErroValidacao(f"Porte desconhecido: {dados.get('porte')!r}")
        mudou["porte"] = bruto or None

    if "regioes_atuacao" in dados:
        regioes = sorted({_texto(r).upper() for r in (dados.get("regioes_atuacao") or [])
                          if _texto(r)})
        forn.regioes_atuacao = regioes
        mudou["regioes_atuacao"] = regioes
        # SE AINDA NÃO TEM ALCANCE DEFINIDO, tenta traduzir o texto agora. É o
        # que faz o cadastro novo já nascer cruzável com a obra em vez de
        # esperar alguém rodar o "Padronizar as regiões".
        if (getattr(forn, "abrangencia", None) or "NAO_INFORMADA") == "NAO_INFORMADA":
            r = svc_regioes.traduzir(regioes)
            if r["abrangencia"] != svc_regioes.NAO_INFORMADA:
                forn.abrangencia = r["abrangencia"]
                forn.ufs_atendidas = r["ufs"]
                forn.municipios_atendidos = r["municipios"]

    # ALCANCE DITO DIRETAMENTE (tela de cadastro novo) vence a tradução: é
    # alguém escolhendo numa lista, não um de-para adivinhando texto livre.
    if dados.get("abrangencia"):
        alcance = _texto(dados.get("abrangencia")).upper()
        if alcance not in svc_regioes.ROTULOS:
            raise ErroValidacao(f"Alcance desconhecido: {alcance!r}.")
        ufs = [svc_regioes.normalizar(x) for x in (dados.get("ufs_atendidas") or [])]
        ufs = [u for u in ufs if u in svc_regioes.UFS]
        municipios = sorted({svc_regioes.normalizar(x)
                             for x in (dados.get("municipios_atendidos") or []) if x})
        if alcance == svc_regioes.ESTADUAL and not ufs:
            raise ErroValidacao("Diga quais estados este fornecedor atende.")
        if alcance in (svc_regioes.REGIONAL, svc_regioes.LOCAL) and not municipios:
            raise ErroValidacao("Diga quais municípios este fornecedor atende.")
        forn.abrangencia = alcance
        forn.ufs_atendidas = [] if alcance == svc_regioes.NACIONAL else ufs
        forn.municipios_atendidos = ([] if alcance == svc_regioes.NACIONAL
                                     else municipios)
        mudou["abrangencia"] = alcance

    if "canais_cotacao" in dados:
        canais = sorted({_texto(c).upper() for c in (dados.get("canais_cotacao") or [])
                         if _texto(c)})
        desconhecidos = [c for c in canais if c not in CANAIS]
        if desconhecidos:
            raise ErroValidacao(
                f"Canal de cotação desconhecido: {', '.join(desconhecidos)}.")
        forn.canais_cotacao = canais
        mudou["canais_cotacao"] = canais

    if "cotacao_automatica" in dados:
        # Pedido do dono: *"de repente eu tenho um fornecedor pequeno que eu
        # não costumo mandar para cotar, ou foi uma compra única"*. Desligar
        # tira da SUGESTÃO do disparo automático — não do cadastro, e não da
        # cotação montada à mão.
        forn.cotacao_automatica = dados.get("cotacao_automatica") is not False
        mudou["cotacao_automatica"] = forn.cotacao_automatica

    if "categorias" in dados:
        mudou["categorias"] = definir_categorias(s, forn, dados.get("categorias") or [])
    return mudou


def definir_categorias(s: Session, forn: Fornecedor,
                       categoria_ids: list[Any]) -> list[int]:
    """O que este fornecedor vende. Substitui a lista inteira — é assim que a
    tela funciona: marcar e desmarcar caixas."""
    desejadas = set()
    for bruto in categoria_ids:
        try:
            desejadas.add(int(bruto))
        except (TypeError, ValueError):
            continue
    conhecidas = {c.id for c in s.scalars(select(InsumoCategoria)).all()}
    invalidas = desejadas - conhecidas
    if invalidas:
        raise ErroValidacao(
            f"Categoria de insumo inexistente: {sorted(invalidas)}.")

    atuais = {v.categoria_insumo_id: v for v in s.scalars(
        select(FornecedorCategoria)).all() if v.fornecedor_id == forn.id}
    for categoria_id in desejadas - set(atuais):
        s.add(FornecedorCategoria(fornecedor_id=forn.id,
                                  categoria_insumo_id=categoria_id))
    for categoria_id in set(atuais) - desejadas:
        s.delete(atuais[categoria_id])
    s.flush()
    return sorted(desejadas)


# ---------------------------------------------------------------------------
# Criar e editar
# ---------------------------------------------------------------------------
def criar(s: Session, dados: dict[str, Any], usuario: Usuario) -> Fornecedor:
    """Cadastro novo pela tela de Suprimentos, já com o que a cotação precisa."""
    forn = base.criar(s, dados, usuario)
    _aplicar_campos_de_suprimentos(s, forn, dados)
    # O contato só nasce junto se houver e-mail ou telefone: sem isso o banco
    # recusaria a linha e derrubaria o cadastro inteiro do fornecedor.
    if _texto(dados.get("contato_nome")) and (_texto(dados.get("email"))
                                              or _texto(dados.get("telefone"))):
        acrescentar_contato(s, forn.id, {
            "nome": dados.get("contato_nome"), "funcao": dados.get("contato_funcao"),
            "email": dados.get("email"), "telefone": dados.get("telefone")}, usuario)
    s.flush()
    return forn


_CAMPOS_BASE = {"razao_social", "nome_fantasia", "email", "telefone",
                "municipio", "uf", "observacoes", "ativo"}


def editar(s: Session, fornecedor_id: int, dados: dict[str, Any],
           usuario: Usuario) -> Fornecedor:
    """Correção em linha. Documento e tipo de pessoa não mudam nunca — isso é
    regra do cadastro base, não escolha desta tela."""
    forn = _obter(s, fornecedor_id)
    do_base = {k: v for k, v in dados.items() if k in _CAMPOS_BASE}
    if do_base:
        base.atualizar(s, fornecedor_id, do_base, usuario)
    mudou = _aplicar_campos_de_suprimentos(s, forn, dados)
    if mudou:
        registrar_evento(s, "fornecedor", forn.id, "EDITADO_SUPRIMENTOS", mudou,
                         usuario.id if usuario else None)
    return forn


def acrescentar_contato(s: Session, fornecedor_id: int, dados: dict[str, Any],
                        usuario: Usuario) -> FornecedorContato:
    """O cotador: a pessoa que responde. Sem nome de gente, cotação vira
    e-mail para caixa geral — e caixa geral não responde."""
    forn = _obter(s, fornecedor_id)
    nome = _texto(dados.get("nome"))
    if len(nome) < 3:
        raise ErroValidacao("Diga o nome de quem responde por este fornecedor.")
    # O banco exige um dos dois (ck_contato_tem_canal), e com razão: contato
    # sem e-mail nem telefone não serve para disparar cotação, que é a única
    # razão de ele existir. Recusar aqui dá o recado em português — deixar
    # chegar no banco daria "violates check constraint" na cara de quem digita.
    if not _texto(dados.get("email")) and not _texto(dados.get("telefone")):
        raise ErroValidacao(
            f"Informe o e-mail ou o telefone de {nome} — sem um dos dois não "
            f"há como mandar cotação para essa pessoa.")
    contato = FornecedorContato(
        fornecedor_id=forn.id, nome=nome,
        funcao=_texto(dados.get("funcao")) or None,
        email=_texto(dados.get("email")) or None,
        telefone=_texto(dados.get("telefone")) or None,
        observacao=_texto(dados.get("observacao")) or None,
        recebe_cotacao=dados.get("recebe_cotacao") is not False)
    s.add(contato)
    s.flush()
    registrar_evento(s, "fornecedor", forn.id, "CONTATO_ACRESCENTADO",
                     {"nome": nome}, usuario.id if usuario else None)
    return contato


def editar_contato(s: Session, contato_id: int, dados: dict[str, Any],
                   usuario: Usuario) -> FornecedorContato:
    """Corrigir o vendedor sem apagar e recriar.

    Existe porque o campo mais usado aqui é a OBSERVAÇÃO — "atende o interior",
    "só linha elétrica" —, e ela é descoberta com o tempo, não no cadastro.
    """
    contato = s.get(FornecedorContato, contato_id)
    if contato is None:
        raise ErroNaoEncontrado("Contato não encontrado.")
    if "nome" in dados:
        nome = _texto(dados.get("nome"))
        if len(nome) < 3:
            raise ErroValidacao("Diga o nome de quem responde por este fornecedor.")
        contato.nome = nome
    for campo in ("funcao", "email", "telefone", "observacao"):
        if campo in dados:
            setattr(contato, campo, _texto(dados.get(campo)) or None)
    if not _texto(contato.email or "") and not _texto(contato.telefone or ""):
        raise ErroValidacao(
            f"Informe o e-mail ou o telefone de {contato.nome} — sem um dos "
            f"dois não há como mandar cotação para essa pessoa.")
    if "recebe_cotacao" in dados:
        contato.recebe_cotacao = dados.get("recebe_cotacao") is not False
    if "ativo" in dados:
        contato.ativo = dados.get("ativo") is not False
    s.flush()
    registrar_evento(s, "fornecedor", contato.fornecedor_id, "CONTATO_EDITADO",
                     {"contato_id": contato_id, "nome": contato.nome},
                     usuario.id if usuario else None)
    return contato


def remover_contato(s: Session, contato_id: int, usuario: Usuario) -> None:
    contato = s.get(FornecedorContato, contato_id)
    if contato is None:
        raise ErroNaoEncontrado("Contato não encontrado.")
    fornecedor_id = contato.fornecedor_id
    s.delete(contato)
    s.flush()
    registrar_evento(s, "fornecedor", fornecedor_id, "CONTATO_REMOVIDO",
                     {"contato_id": contato_id}, usuario.id if usuario else None)


# ---------------------------------------------------------------------------
# A tela de gestão
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Padronizar a região que o fornecedor atende
# ---------------------------------------------------------------------------
def padronizar_regioes(s: Session, usuario: Usuario, *,
                       simular: bool = False) -> dict[str, Any]:
    """Traduz o texto livre de "Região de Atuação" para algo que cruza com a
    obra. Roda em cima de todo o cadastro, e pode rodar quantas vezes precisar.

    NÃO APAGA o texto original: `regioes_atuacao` continua com o que a pessoa
    escreveu. É o que permite conferir uma conversão suspeita e refazer o
    de-para depois de acrescentar um apelido novo em `regioes.py`.

    O QUE ELA NÃO FAZ: inventar. Termo que não é UF, nem macrorregião, nem
    região conhecida, nem nome de lugar volta na lista de `nao_reconhecidos` —
    e o fornecedor fica NAO_INFORMADA, aparecendo no contador da tela até
    alguém resolver. Um cadastro que diz atender o lugar errado é pior que um
    que admite não saber: o primeiro manda cotação de Fortaleza para uma obra
    em São Paulo e ninguém desconfia.
    """
    convertidos = 0
    ja_estavam = 0
    sem_traducao: list[dict[str, Any]] = []
    termos_nao_reconhecidos: dict[str, int] = {}

    for f in s.scalars(select(Fornecedor)).all():
        if getattr(f, "ativo", True) is False:
            continue
        texto = list(getattr(f, "regioes_atuacao", None) or [])
        r = svc_regioes.traduzir(texto)
        for termo in r["desconhecidos"]:
            termos_nao_reconhecidos[termo] = termos_nao_reconhecidos.get(termo, 0) + 1

        if r["abrangencia"] == svc_regioes.NAO_INFORMADA:
            if (getattr(f, "abrangencia", None) or "NAO_INFORMADA") == "NAO_INFORMADA":
                sem_traducao.append({
                    "id": f.id, "fornecedor": f.razao_social,
                    "escrito": ", ".join(texto) or "— em branco —"})
            continue
        if (getattr(f, "abrangencia", None) or "NAO_INFORMADA") != "NAO_INFORMADA":
            # Já padronizado antes (ou preenchido à mão na tela): não se mexe.
            # Refazer por cima apagaria a correção de quem sabe mais que o
            # de-para.
            ja_estavam += 1
            continue
        if not simular:
            f.abrangencia = r["abrangencia"]
            f.ufs_atendidas = r["ufs"]
            f.municipios_atendidos = r["municipios"]
        convertidos += 1

    if not simular:
        registrar_evento(s, "fornecedor", 0, "REGIOES_PADRONIZADAS",
                         {"convertidos": convertidos,
                          "sem_traducao": len(sem_traducao)},
                         usuario.id if usuario else None)
        logger.info("ERP/suprimentos: regiões padronizadas — %d convertidos, "
                    "%d sem tradução", convertidos, len(sem_traducao))

    return {
        "convertidos": convertidos,
        "ja_estavam": ja_estavam,
        "sem_traducao": sorted(sem_traducao, key=lambda x: x["fornecedor"])[:200],
        "quantos_sem_traducao": len(sem_traducao),
        "termos_nao_reconhecidos": sorted(
            ({"termo": t, "vezes": n} for t, n in termos_nao_reconhecidos.items()),
            key=lambda x: -x["vezes"])[:50],
        "simulacao": simular,
        "recado": (
            f"{convertidos} fornecedor(es) "
            + ("teriam a região padronizada" if simular else "tiveram a região padronizada")
            + (f", {ja_estavam} já estavam" if ja_estavam else "")
            + (f". {len(sem_traducao)} ficaram SEM região — eles não entram no "
               f"disparo automático até alguém preencher" if sem_traducao else ".")),
    }


def definir_regiao(s: Session, fornecedor_id: int, dados: dict[str, Any],
                   usuario: Usuario) -> dict[str, Any]:
    """Grava a região à mão, pela ficha do fornecedor."""
    forn = s.get(Fornecedor, fornecedor_id)
    if forn is None:
        raise ErroNaoEncontrado("Fornecedor não encontrado.")

    abrangencia = (dados.get("abrangencia") or "").strip().upper()
    if abrangencia not in svc_regioes.ROTULOS:
        raise ErroValidacao("Escolha até onde este fornecedor vende.")

    ufs = [svc_regioes.normalizar(x) for x in (dados.get("ufs") or []) if x]
    ufs = [u for u in ufs if u in svc_regioes.UFS]
    municipios = sorted({svc_regioes.normalizar(x)
                         for x in (dados.get("municipios") or []) if x})

    if abrangencia == svc_regioes.ESTADUAL and not ufs:
        raise ErroValidacao(
            "Diga QUAIS estados. Um fornecedor estadual sem estado nenhum "
            "nunca seria escolhido para cotação, e ninguém perceberia.")
    if abrangencia in (svc_regioes.REGIONAL, svc_regioes.LOCAL) and not municipios:
        raise ErroValidacao("Diga QUAIS municípios ele atende.")
    if abrangencia == svc_regioes.NACIONAL:
        ufs, municipios = [], []

    antes = getattr(forn, "abrangencia", None)
    forn.abrangencia = abrangencia
    forn.ufs_atendidas = ufs
    forn.municipios_atendidos = municipios
    registrar_evento(s, "fornecedor", forn.id, "REGIAO_DEFINIDA",
                     {"antes": antes, "depois": abrangencia,
                      "ufs": ufs, "municipios": municipios},
                     usuario.id if usuario else None)
    return {"recado": f"Região de {forn.razao_social} definida: "
                      f"{svc_regioes.ROTULOS[abrangencia]}."}


# ---------------------------------------------------------------------------
# Consulta de CNPJ no meio do cadastro
# ---------------------------------------------------------------------------
def consultar_para_cadastro(s: Session, cnpj: str) -> dict[str, Any]:
    """O que a Receita sabe sobre este CNPJ — para preencher o formulário.

    PEDIDO DO DONO, 21/09/2026: *"que após digitação do CNPJ sejam pesquisados
    os dados para serem pré-preenchidos"*, e ele completou: *"quando sigo após
    a digitação, falo no cadastro"* — é dentro do formulário, não num botão ao
    lado. É a mesma regra do Cartão CNPJ da empresa e do contrato da obra:
    **uma porta só, e o documento é atalho DENTRO dela**.

    NÃO GRAVA NADA. Devolve os campos para a tela preencher, e a pessoa
    confere e corrige antes de mandar. Cadastro escrito por robô sem ninguém
    olhar é como entra endereço de filial no lugar da matriz.

    Também responde a pergunta que evita o estrago mais comum deste cadastro:
    **este CNPJ já está aqui?**. Cadastrar o mesmo fornecedor duas vezes é o
    que gerou boa parte dos 126 documentos repetidos da planilha antiga — e o
    banco recusaria no fim, depois de a pessoa ter digitado tudo.
    """
    from app.apps.erp.core.cadastros import receita
    from app.apps.erp.core.cadastros.validadores import somente_digitos

    digitos = somente_digitos(cnpj or "")
    if len(digitos) not in (11, 14):
        raise ErroValidacao("Digite o CNPJ com 14 dígitos (ou o CPF com 11).")

    ja = base.obter_por_documento(s, digitos)
    saida: dict[str, Any] = {
        "documento": digitos,
        "tipo_pessoa": "PJ" if len(digitos) == 14 else "PF",
        "ja_cadastrado": None if ja is None else {
            "id": ja.id, "razao_social": ja.razao_social,
            "ativo": getattr(ja, "ativo", True) is not False},
        "campos": {}, "situacao": "", "aviso": "",
    }
    if ja is not None:
        saida["aviso"] = (f"Este documento já está cadastrado como "
                          f"{ja.razao_social}. Abra o cadastro existente em vez "
                          f"de criar outro.")
        return saida
    if len(digitos) == 11:
        # CPF não tem cadastro público para consultar. Dizer isso é melhor do
        # que deixar a tela girando e não preencher nada.
        saida["aviso"] = "CPF não tem consulta pública — preencha à mão."
        return saida

    try:
        dados = receita.consultar(digitos)
    except receita.ReceitaIndisponivel as erro:
        saida["aviso"] = (f"Não consegui consultar agora ({erro}). "
                          f"Preencha à mão — dá para acertar depois pelo botão "
                          f"“Acertar o cadastro pela Receita”.")
        return saida
    if dados is None:
        saida["aviso"] = "A Receita não achou este CNPJ. Confira o número."
        return saida

    saida["campos"] = {c: v for c, v in dados.items()
                       if c in CAMPOS_DO_FORMULARIO and (v or "").strip()}
    saida["situacao"] = dados.get("situacao", "")
    if saida["situacao"] and saida["situacao"] != "ATIVA":
        saida["aviso"] = (f"Atenção: na Receita este CNPJ está "
                          f"{saida['situacao']}, não ATIVA.")
    return saida


# O que a consulta preenche no formulário. Deliberadamente sem e-mail e sem
# telefone da Receita: o que está lá é o do contador, quase nunca o do vendedor
# — e um e-mail errado no cadastro faz a cotação sair para o lugar errado.
CAMPOS_DO_FORMULARIO = (
    "razao_social", "nome_fantasia", "municipio", "uf", "cep",
    "logradouro", "numero", "complemento", "bairro", "cnae_principal",
)


def adotar_nome_oficial(s: Session, fornecedor_id: int,
                        usuario: Usuario) -> dict[str, Any]:
    """Troca a razão social cadastrada pela que a Receita tem.

    O nome antigo vai para `nome_fantasia` quando este estiver vazio: é por ele
    que o comprador reconhece o fornecedor, e perdê-lo faria a lista de cotação
    ficar cheia de razões sociais que ninguém liga a ninguém.
    """
    forn = s.get(Fornecedor, fornecedor_id)
    if forn is None:
        raise ErroNaoEncontrado("Fornecedor não encontrado.")
    oficial = (getattr(forn, "razao_social_rfb", None) or "").strip()
    if not oficial:
        raise ErroValidacao(
            "Este fornecedor não tem nome divergente guardado. Rode antes o "
            "“Acertar o cadastro pela Receita”.")

    antigo = forn.razao_social
    forn.razao_social = oficial
    if not (forn.nome_fantasia or "").strip():
        forn.nome_fantasia = antigo
    forn.razao_social_rfb = None
    registrar_evento(s, "fornecedor", forn.id, "NOME_OFICIAL_ADOTADO",
                     {"antes": antigo, "depois": oficial},
                     usuario.id if usuario else None)
    return {"recado": f"{antigo} passou a se chamar {oficial}.",
            "antes": antigo, "depois": oficial}


# ---------------------------------------------------------------------------
# Apagar — o que dá e o que não dá
# ---------------------------------------------------------------------------
# PEDIDO DO DONO, 20/09/2026: *"olhei aqui na tela de fornecedores: como é que
# eu excluo? Eu não estou vendo o botão para excluir."*
#
# A regra geral deste sistema é NADA SE APAGA, DESATIVA-SE — e ela continua
# valendo, porque fornecedor apagado levaria junto o título pago, a cotação que
# ele respondeu e o preço dele no histórico. Mas ela estava valendo DEMAIS: o
# cadastro criado por engano, que nunca foi usado para nada, não tem passado
# nenhum para preservar, e mantê-lo para sempre só suja a lista de cotação.
#
# Então: apaga quando NÃO HÁ NADA PENDURADO; desativa quando há. A tela mostra
# os dois caminhos e diz qual vale para aquele fornecedor, em vez de esconder o
# botão e deixar a pessoa procurando.
ONDE_APARECE = (
    ("titulos", "título"),
    ("pedidos", "pedido de pagamento"),
    ("pedidos_compra", "pedido de compra"),
    ("cotacao_fornecedores", "cotação"),
    ("precos_historico", "preço no histórico"),
    ("contratos_servico", "contrato de obra"),
    ("contratos_locacao", "contrato de locação"),
    ("contratos", "contrato"),
    ("documentos", "documento arquivado"),
    ("fornecedor_contas", "conta bancária"),
)


def uso_do_fornecedor(s: Session, fornecedor_id: int) -> list[dict[str, Any]]:
    """Onde este fornecedor aparece — em português, com a contagem.

    Uma consulta por tabela, feita com SQL cru de propósito: são dez tabelas de
    módulos diferentes, e importar dez modelos aqui amarraria Suprimentos ao
    financeiro inteiro só para contar linhas.
    """
    from sqlalchemy import text

    saida = []
    for tabela, rotulo in ONDE_APARECE:
        try:
            quantos = s.execute(
                text(f"SELECT count(*) FROM {tabela} WHERE fornecedor_id = :i"),
                {"i": int(fornecedor_id)}).scalar() or 0
        except Exception:       # pragma: no cover - tabela de outra migração
            continue
        if quantos:
            saida.append({"onde": rotulo, "quantos": int(quantos)})
    return saida


def apagar(s: Session, fornecedor_id: int, usuario: Usuario) -> dict[str, Any]:
    """Apaga o fornecedor — só quando ele nunca foi usado para nada.

    Contato e categoria vão junto: são do próprio cadastro, não são histórico.
    Qualquer outra coisa pendurada recusa o apagamento e a mensagem DIZ ONDE
    ele aparece, para a pessoa entender por que o botão não serve — em vez de
    receber "não é possível excluir" e ficar sem saída.
    """
    forn = s.get(Fornecedor, fornecedor_id)
    if forn is None:
        raise ErroNaoEncontrado("Fornecedor não encontrado.")

    usos = uso_do_fornecedor(s, fornecedor_id)
    if usos:
        onde = ", ".join(f"{u['quantos']} {u['onde']}" for u in usos)
        raise ErroValidacao(
            f"{forn.razao_social} não pode ser apagado: já aparece em {onde}. "
            f"Apagar levaria esse histórico junto. Desative-o — ele sai das "
            f"listas de cotação e o passado fica.")

    nome = forn.razao_social
    for contato in s.scalars(select(FornecedorContato)).all():
        if contato.fornecedor_id == fornecedor_id:
            s.delete(contato)
    for vinculo in s.scalars(select(FornecedorCategoria)).all():
        if vinculo.fornecedor_id == fornecedor_id:
            s.delete(vinculo)
    s.delete(forn)
    registrar_evento(s, "fornecedor", fornecedor_id, "APAGADO",
                     {"razao_social": nome, "cnpj_cpf": forn.cnpj_cpf},
                     usuario.id if usuario else None)
    logger.info("ERP/suprimentos: fornecedor %s apagado (nunca usado)", nome)
    return {"recado": f"{nome} foi apagado — ele nunca tinha sido usado."}


def gerenciar(s: Session) -> dict[str, Any]:
    """Todos os fornecedores com o que Suprimentos precisa ver, mais os
    números do topo e as listas para os filtros."""
    categorias = {c.id: c.nome for c in s.scalars(select(InsumoCategoria)).all()}
    # A MEMÓRIA (18/09/2026): respondeu quantas vezes, em quantos dias, entregou
    # no prazo. Vem calculada na hora, nunca guardada — nota gravada envelhece
    # e mente; nota calculada acompanha a realidade. Uma passada só para a tela
    # inteira: por fornecedor varreria as mesmas tabelas mil vezes.
    from app.apps.erp.core.suprimentos import desempenho as svc_desempenho
    memoria = svc_desempenho.por_fornecedor(s)
    por_fornecedor: dict[int, list[int]] = {}
    for v in s.scalars(select(FornecedorCategoria)).all():
        por_fornecedor.setdefault(v.fornecedor_id, []).append(v.categoria_insumo_id)

    contatos: dict[int, list[dict[str, Any]]] = {}
    for c in s.scalars(select(FornecedorContato)).all():
        contatos.setdefault(c.fornecedor_id, []).append(
            {"id": c.id, "nome": c.nome, "funcao": c.funcao or "",
             "email": c.email or "", "telefone": c.telefone or "",
             # DO QUE ELE TRATA (migração 075): é o que diferencia dois
             # vendedores do mesmo fornecedor.
             "observacao": getattr(c, "observacao", None) or "",
             "recebe_cotacao": getattr(c, "recebe_cotacao", True) is not False,
             "ativo": getattr(c, "ativo", True) is not False})

    linhas = []
    for f in s.scalars(select(Fornecedor)).all():
        if getattr(f, "e_fornecedor", True) is False:
            continue
        ids = sorted(set(por_fornecedor.get(f.id, [])))
        porte = f.porte.value if getattr(f, "porte", None) else ""
        linhas.append({
            "id": f.id, "razao_social": f.razao_social,
            "nome_fantasia": f.nome_fantasia or "",
            "cnpj_cpf": f.cnpj_cpf, "email": f.email or "",
            "telefone": f.telefone or "",
            "municipio": f.municipio or "", "uf": f.uf or "",
            "porte": porte, "porte_rotulo": PORTE_ROTULOS.get(porte, ""),
            "regioes": list(f.regioes_atuacao or []),
            "canais": list(f.canais_cotacao or []),
            "categorias_ids": ids,
            "categorias": [categorias.get(i, "") for i in ids],
            "contatos": contatos.get(f.id, []),
            "cotacao_automatica": getattr(f, "cotacao_automatica", True) is not False,
            "situacao_rfb": getattr(f, "situacao_rfb", None) or "",
            # O nome OFICIAL, quando diferente do cadastrado (migração 078).
            # Vazio quando bate, quando nunca foi consultado, ou depois de
            # alguém adotar o oficial.
            "razao_social_rfb": getattr(f, "razao_social_rfb", None) or "",
            # ATÉ ONDE ELE VENDE (migração 079)
            "abrangencia": getattr(f, "abrangencia", None) or "NAO_INFORMADA",
            "abrangencia_rotulo": svc_regioes.ROTULOS.get(
                getattr(f, "abrangencia", None) or "NAO_INFORMADA", ""),
            "ufs_atendidas": list(getattr(f, "ufs_atendidas", None) or []),
            "municipios_atendidos": list(
                getattr(f, "municipios_atendidos", None) or []),
            "ativo": getattr(f, "ativo", True) is not False,
            "historico": memoria.get(f.id),
        })
    linhas.sort(key=lambda x: _chave(x["razao_social"]))

    ativos = [l for l in linhas if l["ativo"]]
    contagem_porte: dict[str, int] = {}
    for l in ativos:
        chave = l["porte"] or "SEM_PORTE"
        contagem_porte[chave] = contagem_porte.get(chave, 0) + 1

    regioes = sorted({r for l in linhas for r in l["regioes"]})
    return {
        "fornecedores": linhas,
        "categorias": sorted(
            [{"id": i, "nome": n} for i, n in categorias.items()],
            key=lambda x: _chave(x["nome"])),
        "regioes": regioes,
        "portes": [{"chave": k, "rotulo": v} for k, v in PORTE_ROTULOS.items()],
        "canais": [{"chave": k, "rotulo": CANAL_ROTULOS[k]} for k in CANAIS],
        "indicadores": {
            "total": len(linhas),
            "ativos": len(ativos),
            "inativos": len(linhas) - len(ativos),
            # Estes três são a razão de a tela existir: cada um é um fornecedor
            # que NÃO vai receber a próxima cotação, e ninguém percebe até a
            # cotação voltar com três preços em vez de seis.
            "sem_categoria": sum(1 for l in ativos if not l["categorias_ids"]),
            "sem_contato": sum(1 for l in ativos if not l["contatos"]),
            "sem_email": sum(1 for l in ativos
                             if "EMAIL" in l["canais"] and not l["email"]),
            # Quem está FORA do disparo automático. O número existe para a
            # decisão ser visível: fornecedor desligado não some da tela, some
            # da sugestão — e sem este contador ninguém lembraria disso.
            "fora_do_automatico": sum(1 for l in ativos
                                      if not l["cotacao_automatica"]),
            "mais_de_um_contato": sum(1 for l in ativos if len(l["contatos"]) > 1),
            # NOME DIFERENTE DA RECEITA (migração 078). Antes esse número só
            # existia no relatório do trabalho em lote e sumia com ele; agora é
            # um estado do cadastro, que dá para filtrar e resolver.
            "nome_diferente": sum(1 for l in ativos if l["razao_social_rfb"]),
            # SEM REGIÃO DEFINIDA (migração 079). Este é o número que decide se
            # o disparo automático funciona: quem está aqui não é escolhido
            # para cotação nenhuma, porque não dá para saber se ele atende a
            # obra. Antes disso existir, ele entrava "porque sim".
            "sem_regiao": sum(1 for l in ativos
                              if l["abrangencia"] == "NAO_INFORMADA"),
            "por_porte": contagem_porte,
        },
    }


def para_cotar(s: Session, categoria_ids: list[int],
               regiao: Optional[str] = None) -> list[dict[str, Any]]:
    """Quem vende estas categorias — a lista que a tela de cotação oferece.

    Fornecedor sem categoria nenhuma NÃO aparece: é o efeito prático de
    manter o cadastro em dia, e o motivo de a tela de gestão avisar quantos
    estão assim.
    """
    alvo = {int(c) for c in categoria_ids if str(c).strip()}
    saida = []
    for f in gerenciar(s)["fornecedores"]:
        if not f["ativo"] or not f["categorias_ids"]:
            continue
        if alvo and not (alvo & set(f["categorias_ids"])):
            continue
        if regiao and f["regioes"] and regiao.upper() not in f["regioes"]:
            continue
        saida.append(f)
    return saida
