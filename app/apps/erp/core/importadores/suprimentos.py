# ============================================================================
# ERP — core/importadores/suprimentos.py
# Carga inicial de FORNECEDORES e INSUMOS a partir das planilhas em uso.
#
# Por que CSV e não leitura direta do Google: é o padrão que o ERP já usa para
# obras e plano de contas (`planilhas.py`) — o de-para coluna→campo fica
# explícito, o dono confere a prévia antes de gravar, e nenhum dado de
# fornecedor (CNPJ, e-mail, telefone de pessoa) precisa morar no repositório.
#
# A planilha é exportada aba a aba: Registro de Fornecedores › aba "Registro",
# Cadastro de Insumos › aba "Cadastrar". Os cabeçalhos são aceitos como estão
# lá, com acento e maiúscula — quem exporta não deveria ter de editar arquivo.
#
# Desde 10/09/2026 o arquivo pode vir em Excel (.xlsx) direto, sem passar pelo
# "salvar como CSV" — que era o passo que ninguém lembrava de fazer e o que
# estragava acento e ponto e vírgula. A primeira aba da pasta é a que vale.
#
# Rodar duas vezes NÃO duplica: fornecedor casa por CNPJ/CPF, insumo casa pela
# descrição. O que já existe é atualizado, e o relatório diz quantos foram.
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.apps.erp.core.cadastros import fornecedores as svc_forn
from app.apps.erp.core.cadastros.validadores import somente_digitos
from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.core.importadores.planilhas import ler_tabela
from app.apps.erp.db.models.cadastros import (
    Categoria, Fornecedor, FornecedorCategoria, FornecedorContato,
    FornecedorPorte, Insumo, InsumoCategoria, Usuario,
)

# Como a planilha escreve o porte → como o sistema guarda
PORTES = {
    "fabrica": FornecedorPorte.FABRICA,
    "rep. de fabrica": FornecedorPorte.REP_FABRICA,
    "rep de fabrica": FornecedorPorte.REP_FABRICA,
    "representante de fabrica": FornecedorPorte.REP_FABRICA,
    "distribuidor": FornecedorPorte.DISTRIBUIDOR,
    "fornecedor local": FornecedorPorte.LOCAL,
    "local": FornecedorPorte.LOCAL,
    "homecenter": FornecedorPorte.HOMECENTER,
    "home center": FornecedorPorte.HOMECENTER,
}

CANAIS = {"email": "EMAIL", "e-mail": "EMAIL",
          "whatsapp": "WHATSAPP", "whats app": "WHATSAPP", "zap": "WHATSAPP"}


def _chave(texto: Optional[str]) -> str:
    """Texto comparável: sem acento, sem caixa, sem espaço sobrando.

    É o que faz "Rep. de Fábrica" e "rep de fabrica" caírem no mesmo lugar, e
    o que evita cadastrar o mesmo insumo duas vezes por causa de um acento.
    """
    bruto = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    sem_acento = "".join(c for c in bruto if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", sem_acento)


def _campo(linha: dict[str, str], *nomes: str) -> str:
    """O primeiro cabeçalho que existir na linha. A planilha muda de nome com
    o tempo; o importador não deveria quebrar por causa disso."""
    for nome in nomes:
        for chave, valor in linha.items():
            if _chave(chave) == _chave(nome):
                return (valor or "").strip()
    return ""


def _lista(texto: str) -> list[str]:
    return [p.strip() for p in re.split(r"[;,/]", texto or "") if p.strip()]


# ---------------------------------------------------------------------------
# Fornecedores
# ---------------------------------------------------------------------------
def importar_fornecedores_csv(s: Session, conteudo: bytes, usuario: Optional[Usuario],
                              simular: bool = False) -> dict[str, Any]:
    """Fornecedores da planilha, com região, porte, canal, categorias e cotador.

    `simular=True` só relata o que aconteceria — é a prévia que o dono confere
    antes de deixar gravar.
    """
    linhas = ler_tabela(conteudo)
    categorias = {_chave(c.nome): c for c in s.scalars(select(InsumoCategoria)).all()}

    criados, atualizados, rejeitados, sem_categoria = 0, 0, [], set()
    for i, ln in enumerate(linhas, start=2):
        razao = _campo(ln, "razão social", "razao social", "fornecedor")
        doc = somente_digitos(_campo(ln, "cnpj/cpf", "cnpj", "cpf", "documento"))
        if not razao and not doc:
            continue                     # linha em branco no fim da planilha
        try:
            if not doc:
                raise ErroValidacao("CNPJ/CPF em branco.")
            tipo = "PJ" if len(doc) == 14 else "PF"
            dados = {
                "tipo_pessoa": tipo, "cnpj_cpf": doc, "razao_social": razao,
                "nome_fantasia": _campo(ln, "nome do fornecedor", "nome fantasia"),
                "email": _campo(ln, "email", "e-mail"),
                "telefone": _campo(ln, "telefone"),
                "municipio": _campo(ln, "cidade", "município", "municipio"),
            }
            forn = svc_forn.obter_por_documento(s, doc)
            if forn is None:
                if simular:
                    criados += 1
                    continue
                forn = svc_forn.criar(s, dados, usuario)
                criados += 1
            else:
                if not simular:
                    for campo in ("nome_fantasia", "email", "telefone", "municipio"):
                        if dados[campo]:
                            setattr(forn, campo, dados[campo])
                atualizados += 1
            if simular:
                continue

            porte = PORTES.get(_chave(_campo(ln, "porte do fornecedor", "porte")))
            if porte:
                forn.porte = porte
            regioes = [r.upper() for r in _lista(_campo(ln, "região de atuação",
                                                        "regiao de atuacao", "região"))]
            if regioes:
                forn.regioes_atuacao = regioes
            canais = [CANAIS[_chave(c)] for c in _lista(_campo(ln, "envio de cotações",
                                                               "envio de cotacoes", "canal"))
                      if _chave(c) in CANAIS]
            if canais:
                forn.canais_cotacao = sorted(set(canais))
            s.flush()

            _ligar_categorias(s, forn, _lista(_campo(ln, "categoria de insumo",
                                                     "categoria")), categorias,
                              sem_categoria)
            _garantir_contato(s, forn,
                              nome=_campo(ln, "contato", "nome do contato"),
                              email=dados["email"], telefone=dados["telefone"])
        except ErroValidacao as e:
            rejeitados.append({"linha": i, "fornecedor": razao or doc, "motivo": str(e)})

    return {"no_arquivo": len(linhas), "criados": criados, "atualizados": atualizados,
            "rejeitados": rejeitados,
            "categorias_nao_encontradas": sorted(sem_categoria),
            "simulacao": simular}


def _ligar_categorias(s: Session, forn: Fornecedor, nomes: list[str],
                      categorias: dict[str, InsumoCategoria], nao_achadas: set) -> None:
    """O que o fornecedor vende. Categoria que não existe é RELATADA, não
    criada: inventar categoria na importação é como a base começa a apodrecer."""
    atuais = {c.categoria_insumo_id for c in s.scalars(
        select(FornecedorCategoria).where(
            FornecedorCategoria.fornecedor_id == forn.id)).all()
        if c.fornecedor_id == forn.id}
    for nome in nomes:
        cat = categorias.get(_chave(nome))
        if cat is None:
            nao_achadas.add(nome)
            continue
        if cat.id not in atuais:
            s.add(FornecedorCategoria(fornecedor_id=forn.id, categoria_insumo_id=cat.id))
            atuais.add(cat.id)


def _garantir_contato(s: Session, forn: Fornecedor, nome: str,
                      email: str, telefone: str) -> None:
    """O cotador da planilha. Sem e-mail e sem telefone não entra: contato que
    não recebe cotação não serve para nada, e o banco recusa."""
    nome = (nome or "").strip()
    if not nome or not (email or telefone):
        return
    ja_tem = [c for c in s.scalars(select(FornecedorContato).where(
        FornecedorContato.fornecedor_id == forn.id)).all()
        if c.fornecedor_id == forn.id and _chave(c.nome) == _chave(nome)]
    if ja_tem:
        return
    s.add(FornecedorContato(fornecedor_id=forn.id, nome=nome,
                            email=email or None, telefone=telefone or None))


# ---------------------------------------------------------------------------
# Insumos
# ---------------------------------------------------------------------------
def importar_insumos_csv(s: Session, conteudo: bytes, usuario: Optional[Usuario],
                         simular: bool = False,
                         criar_categorias: bool = False) -> dict[str, Any]:
    """Insumos da planilha, com a categoria de suprimento e a conta do plano.

    Aceita CSV e Excel (.xlsx) — o formato é reconhecido pelo conteúdo, não
    pela extensão.

    A conta do plano é o que permite o pedido virar previsão de pagamento já
    apropriada — por isso insumo sem conta é aceito, mas contado e relatado.
    Quando o nome da conta na planilha não bate com o do plano por causa de uma
    renomeação, os APELIDOS do plano padrão resolvem.

    A coluna "Subcategoria" com o valor LOCAÇÃO marca o insumo como locável, e
    é isso que decide quais itens aparecem na tela de Locações — cimento não se
    aluga, andaime sim. A marca é só LIGADA pela planilha, nunca desligada.

    `criar_categorias` liga a criação das categorias de insumo que a planilha
    trouxer e o ERP ainda não tiver. Nasceu DESLIGADO de propósito: inventar
    categoria na carga é como a base começa a apodrecer, e "AREIA", "Areia" e
    "areia " viram três. O dono pediu a chave em 10/09/2026 — *"tanto insumos
    quanto a categoria de insumo eu quero importar do cadastro que a gente já
    tem"* —, e ela continua sendo uma DECISÃO dele, tomada na tela, marcação a
    marcação: o relatório diz exatamente quais categorias nasceram.

    Mesmo ligada, a comparação é sem acento e sem caixa (`_chave`), então a
    planilha com "Areia" e "AREIA" na mesma coluna cria UMA categoria, não
    duas.
    """
    linhas = ler_tabela(conteudo)
    categorias = {_chave(c.nome): c for c in s.scalars(select(InsumoCategoria)).all()}
    contas = _contas_por_nome(s)
    existentes = {_chave(i.descricao): i for i in s.scalars(select(Insumo)).all()}

    proximo = _proximo_codigo(s)
    proxima_cat = _proximo_codigo_categoria(s)
    criados, atualizados, rejeitados = 0, 0, []
    sem_categoria, sem_conta = set(), 0
    categorias_criadas: set[str] = set()
    contas_nao_encontradas: set[str] = set()
    locaveis = 0
    for i, ln in enumerate(linhas, start=2):
        descricao = _campo(ln, "insumos", "descrição do insumo", "descricao do insumo",
                           "insumo", "descrição", "descricao")
        if not descricao:
            continue
        nome_cat = _campo(ln, "categoria do insumo", "sub-categoria", "categoria")
        nome_conta = _campo(ln, "plano financeiro", "categoria (plano financeiro)",
                            "conta do plano")
        # A coluna "Subcategoria" da planilha da BWS diz LOCAÇÃO nos itens que
        # se alugam. É ela que faz o insumo aparecer na tela de Locações — por
        # isso cimento não aparece lá, e andaime aparece.
        locavel = _chave(_campo(ln, "subcategoria", "sub categoria",
                                "subcategoria de locação",
                                "subcategoria de locacao")) == "locacao"
        if locavel:
            # Contado aqui, e não na hora de gravar, para a PRÉVIA já dizer
            # quantos itens vão para a tela de Locações — é o número que o dono
            # confere antes de deixar gravar.
            locaveis += 1
        cat = categorias.get(_chave(nome_cat)) if nome_cat else None
        if nome_cat and cat is None:
            if criar_categorias:
                # O código é NOT NULL no banco. Sem gerar um aqui, a carga
                # morreria na primeira categoria nova — e o dublê dos testes,
                # que não checa restrição de banco, não acusaria.
                nova = InsumoCategoria(codigo=f"CAT-{proxima_cat:04d}",
                                       nome=nome_cat.strip(), ativo=True)
                proxima_cat += 1
                if not simular:
                    s.add(nova)
                    s.flush()
                    categorias[_chave(nome_cat)] = nova
                    cat = nova
                categorias_criadas.add(nome_cat.strip())
            else:
                sem_categoria.add(nome_cat)
        conta = contas.get(_chave(nome_conta)) if nome_conta else None
        if conta is None:
            sem_conta += 1
            if nome_conta:
                contas_nao_encontradas.add(nome_conta)

        insumo = existentes.get(_chave(descricao))
        if insumo is None:
            criados += 1
            if simular:
                continue
            insumo = Insumo(codigo=f"INS-{proximo:04d}", descricao=descricao.strip())
            proximo += 1
            s.add(insumo)
            existentes[_chave(descricao)] = insumo
        else:
            atualizados += 1
            if simular:
                continue
        if cat is not None:
            insumo.categoria_insumo_id = cat.id
        if conta is not None:
            insumo.categoria_id = conta.id
        # A marca de locável só é LIGADA pela planilha, nunca desligada: quem
        # marcou um item à mão na tela de Insumos não perde a marcação porque
        # a planilha veio sem ela.
        if locavel and not insumo.locavel:
            insumo.locavel = True
        unidade = _campo(ln, "und", "unidade")
        if unidade:
            insumo.unidade = unidade.upper()
        s.flush()

    if categorias_criadas and not simular:
        registrar_evento(s, "insumo_categoria", 0, "CRIADAS_POR_IMPORTACAO",
                         {"nomes": sorted(categorias_criadas)},
                         usuario.id if usuario else None)
    return {"no_arquivo": len(linhas), "criados": criados, "atualizados": atualizados,
            "rejeitados": rejeitados, "sem_conta_do_plano": sem_conta,
            "categorias_nao_encontradas": sorted(sem_categoria),
            "categorias_criadas": sorted(categorias_criadas),
            "contas_do_plano_nao_encontradas": sorted(contas_nao_encontradas),
            "marcados_locaveis": locaveis, "simulacao": simular}


def _contas_por_nome(s: Session) -> dict[str, Categoria]:
    """Conta do plano indexada pelo nome, aceitando os APELIDOS.

    A planilha escreve "Manutenção (Veículos e Máquinas)"; o plano já escreveu
    "Manutenção de veículos e máquinas". Sem os apelidos, o insumo entraria sem
    conta do plano por causa de um parêntese — e em silêncio, porque insumo sem
    conta é aceito de propósito.
    """
    from app.apps.erp.core.cadastros.plano_padrao import APELIDOS

    todas = s.scalars(select(Categoria)).all()
    por_nome = {_chave(c.descricao): c for c in todas}
    por_codigo = {c.codigo: c for c in todas}
    for apelido, codigo in APELIDOS.items():
        conta = por_codigo.get(codigo)
        if conta is not None:
            por_nome.setdefault(_chave(apelido), conta)
    return por_nome


def _proximo_codigo_categoria(s: Session) -> int:
    """Continua a numeração CAT-0001 das categorias de insumo."""
    numeros = []
    for c in s.scalars(select(InsumoCategoria)).all():
        codigo = (getattr(c, "codigo", "") or "")
        sufixo = codigo.split("-", 1)[1] if codigo.upper().startswith("CAT-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return (max(numeros) + 1) if numeros else 1


def _proximo_codigo(s: Session) -> int:
    """Continua a numeração INS-0001 de onde parou, para a carga poder ser
    feita em partes sem colidir."""
    numeros = []
    for insumo in s.scalars(select(Insumo)).all():
        codigo = (getattr(insumo, "codigo", "") or "")
        sufixo = codigo.split("-", 1)[1] if codigo.startswith("INS-") else ""
        if sufixo.isdigit():
            numeros.append(int(sufixo))
    return (max(numeros) + 1) if numeros else 1
