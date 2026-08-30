# ============================================================================
# ERP — core/cadastros/depara.py
# Tradução do plano ANTIGO (Omie, que chega nos cards do Pipefy) para o plano
# NOVO da BWS. Sem isto, toda SP importada cairia em pendência por "tipo de
# despesa sem categoria correspondente".
#
# Como funciona a resolução, em ordem:
#   1. tabela categoria_depara (inclui o que a BWS ajustou na tela — vence tudo)
#   2. mapa padrão abaixo (nome antigo → conta nova)
#   3. código Omie do card (2.01.89 → conta nova)
#   4. semelhança de nome com as contas do plano (só aceita ≥ 0,86)
#   5. nada resolveu → registra na fila de pendências para decisão humana,
#      contando quantas vezes apareceu (o que a BWS mais usa vem primeiro)
#
# Regras de tradução que mudam o significado (e por isso ficam explícitas):
#   - "Fundo Fixo" e "Despesa com Cartão" no Omie estavam dentro de Materiais
#     Aplicados. Fundo fixo vai para a conta própria de prestação de contas;
#     cartão NÃO tem conta nova (é forma de pagamento) e cai em pendência de
#     propósito, para o financeiro dizer o que foi comprado.
#   - "INSS", "ISS", "IR" das Despesas Tributárias e as "(-) Retenções" viram
#     A MESMA conta nova — é a unificação combinada.
#   - "Móveis e Utensílios" e "Equipamentos de Informática" saem de custo e vão
#     para investimento (grupo 8).
# ============================================================================
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import ErroValidacao, registrar_evento
from app.apps.erp.db.models.cadastros import Categoria, Usuario

LIMIAR_SEMELHANCA = 0.86

# nome no Omie/Pipefy  →  código da conta nova
MAPA_PADRAO: dict[str, str] = {
    # ---------------- materiais aplicados
    "Agregados (Areia, Brita, Arisco)": "3.1.02",
    "Argamassas": "3.1.01",
    "Cimento e Concreto Usinado": "3.1.01",
    "Armadura": "3.1.03",
    "Elementos de Vedação (Tijolo, Blocos e Paredes PVC)": "3.1.04",
    "Pré-Moldados de Concreto": "3.1.05",
    "Estrutura Metálica": "3.1.05",
    "Telhas e Material p/ Coberturas": "3.1.06",
    "Madeiramento": "3.1.07",
    "Material Elétrico": "3.1.08",
    "Material p/ Cabeamento Estruturado e CFTV": "3.1.08",
    "Material Hidráulico e Sanitário": "3.1.09",
    "Material p/ Gás": "3.1.09",
    "Material p/ Combate à Incêndio": "3.1.10",
    "Material p/ Climatização": "3.1.11",
    "Pisos, Cerâmicas e Revestimentos": "3.1.12",
    "Louças e Metais": "3.1.13",
    "Bancadas de Granito": "3.1.13",
    "Esquadrias de Alumínio, Metal e Madeira": "3.1.14",
    "Vidros e Espelhos": "3.1.14",
    "Materais p/ Serralheria (Tubos, Metalon, Perfis, etc)": "3.1.14",
    "Material para Pintura": "3.1.15",
    "Material p/ Fôrro": "3.1.16",
    "Impermeabilizantes, Aditivos e Colas": "3.1.17",
    "Parafusos, Ferragens e Acessórios": "3.1.18",
    "Ferramentas": "3.1.19",
    "Jardinagem": "3.1.20",
    "Outros Materiais": "3.1.99",
    "Material p/ Limpeza": "3.2.05",
    "ICMS (Diferencial de Alíquota)": "2.1.12",
    "Fundo Fixo": "3.4.08",
    "Móveis e Utensílios": "8.1.03",
    "Equipamentos de Informática": "8.1.02",

    # ---------------- terceiros e locações
    "Fretes e Postagens": "3.2.04",
    "Serviços de Terceiros (Pessoa Física ou Jurídica)": "3.2.02",
    "Locação de Equipamentos": "3.3.01",
    "Locação de Máquinas Pesadas": "3.3.02",
    "Locação de Veículos": "3.3.03",
    "Bonificações": "3.2.02",

    # ---------------- pessoal
    "Salários e Ordenados": "4.1.01",
    "Férias": "4.1.02",
    "13º Salário": "4.1.02",
    "Gratificações e Extras": "4.1.03",
    "Produção": "4.1.03",
    "Rescisões e Indenizações Trabalhistas": "4.1.04",
    "Participação nos Lucros e Resultados": "4.1.05",
    "Encargos Sociais INSS": "4.2.01",
    "Encargos Sociais FGTS": "4.2.02",
    "Imposto de Renda": "4.2.03",
    "Contribuição Sindical": "4.2.04",
    "Despesas com Transporte": "4.3.01",
    "Despesas com Alimentação": "4.3.02",
    "Cesta Básica": "4.3.02",
    "Seguro de Vida": "4.3.03",
    "Outros Benefícios": "4.3.04",
    "Cursos e Treinamentos": "4.4.01",
    "Multas e Processos Trabalhistas": "4.4.02",
    "Exames Médicos": "3.4.04",
    "EPI (Equipamento de Proteção Individual)": "3.4.03",

    # ---------------- administrativas
    "Água e Energia": "5.1.02",
    "Aluguéis e Condomínios": "5.1.01",
    "Internet, Telefonia e Sistemas": "5.1.03",
    "Conservação e Manutenção Predial": "5.1.04",
    "Limpeza": "5.1.05",
    "Segurança": "5.1.06",
    "Consultoria (Contabilidade, Jurídica)": "5.2.01",
    "Advogados": "5.2.02",
    "Auditorias": "5.2.03",
    "Cartórios, Crea, Taxas": "5.2.04",
    "Material de Escritório": "5.3.01",
    "Cópias, Reproduções e Impressões": "5.3.01",
    "Combustíveis": "5.3.02",
    "Manutenção (Veículos e Máquinas)": "5.3.03",
    "Manutenção (Ferramentas e Equipamentos)": "5.3.04",
    "Despesas com Viagens e Estadia": "5.3.05",
    "Veículos (Taxas, Impostos, Multas)": "5.3.06",
    "Outras Despesas": "5.3.99",

    # ---------------- financeiras
    "Juros sobre Empréstimos": "6.1.01",
    "Juros e Multas": "6.1.02",
    "Taxas, Tarifas Bancárias e IOF": "6.2.01",
    "Seguros e Garantias": "6.2.02",

    # ---------------- tributos (unificação retenção × guia)
    "ISS": "2.1.01",
    "(-) Retenção de ISS": "2.1.01",
    "INSS": "2.1.02",
    "(-) Retenção de INSS": "2.1.02",
    "(-) Retenção de IR": "2.1.03",
    "PIS": "2.1.04",
    "COFINS": "2.1.05",
    "CPRB": "2.1.07",
    "RET (1%)": "2.1.08",
    "IRPJ": "2.2.01",
    "CSLL": "2.2.02",
    "Juros e Multas Tributárias": "2.3.01",

    # ---------------- ativos e fluxo
    "Imóveis Aquisição": "8.2.01",
    "Patrimônio Aquisição": "8.1.01",
    "Aquisição de Veículos, Máquinas e Equipamentos": "8.1.01",
    "Aplicações Financeiras": "9.2.01",
    "Resgate de Aplicações Financeiras": "9.2.02",
    "Rendimentos de Aplicações": "1.3.01",
    "Aportes Parceiros": "9.3.02",
    "Aportes BWS": "9.3.03",
    "Devolução de Aportes": "9.3.06",
    "Empréstimos para Capital de Giro": "9.4.01",
    "Empréstimos (Pgt. do Principal)": "9.4.02",
    "Pagamento de Dividendos": "9.5.01",
    "Entrada de Transferência": "9.1.01",
    "Saída de Transferência": "9.1.01",
    "Estorno de Pagamento": "1.2.02",
    "Pagamento Estornado": "1.2.02",

    # ---------------- receitas
    "Receita de Obras": "1.1.01",
    "Receita de Devolução de Material": "1.2.01",
    "Estorno de Despesas": "1.2.02",
    "Reembolso de Custas processuais": "1.2.03",
    "Imóveis Venda": "1.4.01",
    "Patrimônio Venda": "1.4.03",
    "Veículos, Máquinas e Equipamentos": "1.4.02",
    "Venda de Ativos": "1.4.03",
    "Dividendos Recebidos": "1.3.02",
}

# código Omie → conta nova (rede de segurança quando o card traz o código)
MAPA_CODIGO_OMIE: dict[str, str] = {
    "2.01.99": "3.1.01", "2.01.01": "3.1.02", "2.01.03": "3.1.03",
    "2.01.89": "3.1.08", "2.01.88": "3.1.09", "2.01.04": "3.1.12",
    "2.01.73": "3.4.08", "2.01.71": "8.1.03", "2.01.93": "3.2.05",
    "2.02.01": "3.2.04", "2.02.02": "3.3.01", "2.02.03": "3.2.02",
    "2.02.98": "3.3.02", "2.02.99": "3.3.03",
    "2.03.01": "4.1.01", "2.03.10": "4.2.01", "2.03.11": "4.2.02",
    "2.04.01": "5.1.02", "2.04.02": "5.1.01", "2.04.07": "5.1.03",
    "2.05.01": "6.1.01", "2.05.04": "6.2.01",
    "2.06.04": "2.1.02", "2.06.06": "2.1.01", "2.06.05": "2.2.01",
    "1.03.97": "2.1.01", "1.03.98": "2.1.03", "1.03.99": "2.1.02",
    "2.07.03": "8.1.01", "2.07.06": "8.1.02", "2.07.04": "5.1.04",
    "1.01.01": "1.1.01",
}

# termos que NÃO devem virar conta — são forma de pagamento, não natureza
SEM_CONTA_PROPOSITAL = {
    "despesa com cartao": "Cartão é forma de pagamento — informe o que foi comprado.",
    "pagamento beevale": "BeeVale é forma de pagamento — informe o que foi comprado.",
}


def normalizar(texto: str) -> str:
    t = unicodedata.normalize("NFKD", (texto or "").strip().lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def _semelhanca(a: str, b: str) -> float:
    return SequenceMatcher(None, normalizar(a), normalizar(b)).ratio()


def instalar_depara_padrao(s: Session, usuario: Optional[Usuario] = None) -> dict[str, Any]:
    """Grava o mapa padrão na tabela, ligando cada nome antigo à conta nova.
    Idempotente; nunca sobrescreve tradução marcada como do USUÁRIO."""
    codigos = {c.codigo: c.id for c in s.scalars(select(Categoria)).all()}
    criados, atualizados, sem_conta = 0, 0, []
    for nome_antigo, cod_novo in MAPA_PADRAO.items():
        cat_id = codigos.get(cod_novo)
        if cat_id is None:
            sem_conta.append((nome_antigo, cod_novo))
            continue
        chave = normalizar(nome_antigo)
        linha = s.execute(text("SELECT id, origem_registro FROM categoria_depara "
                               "WHERE origem_chave = :k"), {"k": chave}).first()
        if linha is None:
            s.execute(text(
                "INSERT INTO categoria_depara (origem_texto, origem_chave, categoria_id, "
                "confirmado, origem_registro) VALUES (:t, :k, :c, TRUE, 'PADRAO')"),
                {"t": nome_antigo, "k": chave, "c": cat_id})
            criados += 1
        elif linha[1] != "USUARIO":
            s.execute(text("UPDATE categoria_depara SET categoria_id = :c, confirmado = TRUE "
                           "WHERE id = :i"), {"c": cat_id, "i": linha[0]})
            atualizados += 1
    # códigos Omie entram como chave alternativa
    for cod_omie, cod_novo in MAPA_CODIGO_OMIE.items():
        cat_id = codigos.get(cod_novo)
        if cat_id is None:
            continue
        chave = normalizar(cod_omie)
        if s.execute(text("SELECT 1 FROM categoria_depara WHERE origem_chave = :k"),
                     {"k": chave}).first() is None:
            s.execute(text(
                "INSERT INTO categoria_depara (origem_texto, origem_chave, origem_codigo, "
                "categoria_id, confirmado, origem_registro) "
                "VALUES (:t, :k, :t, :c, TRUE, 'PADRAO')"),
                {"t": cod_omie, "k": chave, "c": cat_id})
            criados += 1
    s.flush()
    registrar_evento(s, "categoria_depara", 0, "DEPARA_INSTALADO",
                     {"criados": criados, "atualizados": atualizados,
                      "sem_conta_nova": sem_conta}, usuario.id if usuario else None)
    return {"criados": criados, "atualizados": atualizados, "sem_conta_nova": sem_conta}


def resolver(s: Session, texto_origem: str,
             registrar_pendencia: bool = True) -> tuple[Optional[Categoria], str]:
    """Traduz o 'tipo de despesa' do card para a conta nova.
    Devolve (categoria, como_resolveu). Sem correspondência → (None, motivo)."""
    if not (texto_origem or "").strip():
        return None, "card sem tipo de despesa"
    chave = normalizar(texto_origem)

    # 1) tabela (inclui ajustes do usuário)
    linha = s.execute(text(
        "SELECT categoria_id, origem_registro FROM categoria_depara WHERE origem_chave = :k"),
        {"k": chave}).first()
    if linha and linha[0]:
        s.execute(text("UPDATE categoria_depara SET ocorrencias = ocorrencias + 1, "
                       "ultima_vez = :agora WHERE origem_chave = :k"),
                  {"agora": datetime.now(timezone.utc), "k": chave})
        return s.get(Categoria, linha[0]), f"de-para ({linha[1].lower()})"

    # 2) semelhança com as contas do plano
    ativas = s.scalars(select(Categoria).where(Categoria.ativo.is_(True))).all()
    melhor, escore = None, 0.0
    for cat in ativas:
        e = max(_semelhanca(texto_origem, cat.descricao),
                _semelhanca(texto_origem, cat.codigo))
        if e > escore:
            melhor, escore = cat, e
    if melhor is not None and escore >= LIMIAR_SEMELHANCA:
        s.execute(text(
            "INSERT INTO categoria_depara (origem_texto, origem_chave, categoria_id, "
            "confirmado, origem_registro, ocorrencias, ultima_vez) "
            "VALUES (:t, :k, :c, FALSE, 'AUTO', 1, :agora) "
            "ON CONFLICT (origem_chave) DO UPDATE SET ocorrencias = categoria_depara.ocorrencias + 1"),
            {"t": texto_origem, "k": chave, "c": melhor.id,
             "agora": datetime.now(timezone.utc)})
        return melhor, f"semelhança {escore:.0%} (confirmar)"

    # 3) fila de pendências
    motivo = SEM_CONTA_PROPOSITAL.get(chave, "sem correspondência no plano novo")
    if registrar_pendencia:
        s.execute(text(
            "INSERT INTO categoria_depara (origem_texto, origem_chave, categoria_id, "
            "confirmado, origem_registro, ocorrencias, ultima_vez) "
            "VALUES (:t, :k, NULL, FALSE, 'AUTO', 1, :agora) "
            "ON CONFLICT (origem_chave) DO UPDATE SET "
            "ocorrencias = categoria_depara.ocorrencias + 1, ultima_vez = :agora"),
            {"t": texto_origem, "k": chave, "agora": datetime.now(timezone.utc)})
    return None, motivo


def listar(s: Session) -> dict[str, list[dict[str, Any]]]:
    linhas = s.execute(text(
        "SELECT d.id, d.origem_texto, d.categoria_id, d.confirmado, d.origem_registro, "
        "       d.ocorrencias, c.codigo, c.descricao "
        "FROM categoria_depara d LEFT JOIN categorias c ON c.id = d.categoria_id "
        "ORDER BY d.categoria_id IS NULL DESC, d.ocorrencias DESC, d.origem_texto")).all()
    def _dic(r):
        return {"id": r[0], "origem": r[1], "categoria_id": r[2], "confirmado": r[3],
                "tipo": r[4], "ocorrencias": r[5],
                "destino": f"{r[6]} · {r[7]}" if r[6] else None}
    todas = [_dic(r) for r in linhas]
    return {"pendentes": [d for d in todas if not d["categoria_id"]],
            "a_confirmar": [d for d in todas if d["categoria_id"] and not d["confirmado"]],
            "confirmadas": [d for d in todas if d["categoria_id"] and d["confirmado"]]}


def definir(s: Session, depara_id: int, categoria_id: int, usuario: Usuario) -> None:
    cat = s.get(Categoria, categoria_id)
    if cat is None:
        raise ErroValidacao("Categoria de destino inexistente.")
    linha = s.execute(text("SELECT origem_texto FROM categoria_depara WHERE id = :i"),
                      {"i": depara_id}).first()
    if linha is None:
        raise ErroValidacao("Tradução inexistente.")
    s.execute(text("UPDATE categoria_depara SET categoria_id = :c, confirmado = TRUE, "
                   "origem_registro = 'USUARIO' WHERE id = :i"),
              {"c": categoria_id, "i": depara_id})
    registrar_evento(s, "categoria_depara", depara_id, "DEPARA_DEFINIDO",
                     {"origem": linha[0], "destino": f"{cat.codigo} · {cat.descricao}"},
                     usuario.id)
