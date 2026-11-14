# -*- coding: utf-8 -*-
"""
O CADASTRO DO TELEGRAM dentro do ERP — e o CONVITE pelo WhatsApp a quem não tem.

Pedido do dono em 14/11/2026: *"tem muita gente fazendo solicitações, mas não
está recebendo mensagens porque não tem cadastro no Telegram (…) o mensageria
identifica que a mensagem não foi entregue, aí ele manda uma mensagem no
WhatsApp explicando que a pessoa não vai receber algumas comunicações da
empresa (…) e que ela precisa fazer o cadastro do Telegram."* E: *"quem são as
pessoas que a gente permitiria serem cadastradas? Os colaboradores (…) que têm
telefone."*

O que mora aqui:

  · QUEM PODE: colaborador do ERP que não está desligado e tem telefone, e
    operador do ERP ativo com telefone. Ninguém mais recebe convite.
  · REGRA RÍGIDA (dono, 14/11/2026): o número que o Telegram confirma tem de
    bater com o telefone do cadastro do ERP. Não bateu, não cadastra — a
    pessoa pede ao DP para atualizar o telefone, e a tentativa fica na fila
    do DP (`mensageria.telegram_tentativas`). CPF digitado não cadastra.
  · O CADASTRO (`mensageria.telegram_cadastros`): quem tem Telegram ligado, e
    a quem do ERP pertence. Substitui a aba "TelegramID" da planilha: depois
    de "Migrar da planilha", nenhuma planilha é lida nem escrita.
  · O CONVITE (`mensageria.convites`): mensagem de WhatsApp com um LINK
    PESSOAL (t.me/<robô>?start=c_<código>). Quem toca no link é reconhecido
    pelo robô e só precisa confirmar o número.
      - AUTOMÁTICO: na PRIMEIRA mensagem do Telegram que não chega porque a
        pessoa não tem cadastro. O próximo só depois de mais N mensagens que
        não chegaram (padrão 5) E de N dias (padrão 7), e no máximo 3
        automáticos por pessoa — o Z-API bloqueia pelo volume, e a empresa
        já foi bloqueada uma vez.
      - MANUAL: pela tela, um a um ou em lote; no máximo um por dia por pessoa.
    Os dois passam pela política do tipo "telegram.aviso_cadastro", pela chave
    geral do WhatsApp e pelo teto da tela Mensagens, e ficam no registro.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
import secrets
import threading
from typing import Callable, Optional

from . import core, db

logger = logging.getLogger("mensageria.telegram")

TIPO_CONVITE = "telegram.aviso_cadastro"
PREFIXO_LINK = "c_"

CONVITE_AUTOMATICO = "convite.automatico"
CONVITE_INTERVALO = "convite.intervalo_dias"
CONVITE_TEXTO = "convite.texto"
CONVITE_FALHAS_ENTRE = "convite.falhas_entre"
CONVITE_MAXIMO = "convite.maximo_automatico"
# A planilha morre (decisão do dono, 14/11/2026): depois do botão "Migrar da
# planilha", o robô e os envios só usam o ERP. Antes dele, tudo segue como era.
MIGRADO_EM = "telegram.migrado_em"
ULTIMA_IMPORTACAO = MIGRADO_EM
RELATORIO_MIGRACAO = "telegram.relatorio_migracao"
INTERVALO_PADRAO_DIAS = 7
# Pedido do dono, 14/11/2026: "na primeira mensagem que não dá certo, a gente
# encaminha o WhatsApp. Mas não a cada mensagem que der errada (…) depois de
# algumas outras mensagens, aí manda mais uma (…) senão a gente pode correr o
# risco de ser banido de novo." Então o automático tem TRÊS travas juntas: o
# próximo convite só sai depois de N mensagens que não chegaram desde o último,
# E depois de N dias, E até um máximo de convites automáticos por pessoa.
FALHAS_ENTRE_PADRAO = 5
MAXIMO_AUTOMATICO_PADRAO = 3
INTERVALO_MANUAL_HORAS = 24
VALIDADE_DO_LINK_DIAS = 30
JANELA_DO_LINK_ABERTO_H = 48
LOTE_MAXIMO = 50

TEXTO_PADRAO = (
    "Olá, {nome}! Aqui é a *BWS Construções*.\n\n"
    "A BWS tentou te mandar uma mensagem pelo *Telegram*, mas ela não chegou: o seu Telegram "
    "ainda *não está cadastrado* no sistema da empresa.\n\n"
    "Hoje os avisos da BWS — pagamentos, comprovantes, respostas das suas solicitações e "
    "comunicados — saem pelo Telegram. Sem o cadastro, você deixa de receber essas mensagens.\n\n"
    "Para se cadastrar (leva 1 minuto):\n"
    "1️⃣ Se ainda não tem, instale o *Telegram* pela loja de aplicativos do celular.\n"
    "2️⃣ Toque neste link: {link}\n"
    "3️⃣ Toque em *INICIAR* e depois em *📱 Compartilhar meu número*.\n\n"
    "Pronto: a partir daí as mensagens da BWS chegam por lá.\n"
    "Dúvidas? Fale com o RH ou o Financeiro."
)
TEXTO_PADRAO_MANUAL = TEXTO_PADRAO.replace(
    "A BWS tentou te mandar uma mensagem pelo *Telegram*, mas ela não chegou: o seu Telegram "
    "ainda *não está cadastrado* no sistema da empresa.",
    "O seu Telegram ainda *não está cadastrado* no sistema da BWS.")


# ---------------------------------------------------------------------------
# Telefone e CPF — as mesmas regras do robô (variantes com e sem o nono dígito)
# ---------------------------------------------------------------------------
def digitos(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def normalizar_cpf(valor) -> str:
    d = digitos(valor)
    return d.zfill(11) if len(d) in (10, 11) else d


def variantes_telefone(bruto) -> set[str]:
    """DDD + número, sem o 55, com e sem o nono dígito."""
    d = digitos(bruto)
    if not d:
        return set()
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    if len(d) < 10:
        return {d}
    var = {d}
    if len(d) == 11 and d[2] == "9":
        var.add(d[:2] + d[3:])
    elif len(d) == 10:
        var.add(d[:2] + "9" + d[2:])
    return var


def telefone_canonico(bruto) -> str:
    d = digitos(bruto)
    if not d:
        return ""
    if d.startswith("55") and len(d) >= 12:
        return d
    return "55" + d


def link_do_robo() -> str:
    base = (os.environ.get("TELEGRAM_BOT_LINK") or "t.me/bwsconstrucoesbotbot").strip().rstrip("/")
    base = re.sub(r"^https?://", "", base)
    return "https://" + base


def link_pessoal(codigo: str) -> str:
    return f"{link_do_robo()}?start={PREFIXO_LINK}{codigo}"


# ---------------------------------------------------------------------------
# QUEM PODE: colaboradores não desligados e operadores ativos, com telefone
# ---------------------------------------------------------------------------
def pessoas_elegiveis(conn) -> list[dict]:
    colabs = db.todos(conn, """
        SELECT c.id, c.nome, c.cpf, c.telefone, c.situacao, c.usuario_id
          FROM public.colaboradores c
         WHERE coalesce(c.situacao, '') <> 'DESLIGADO' AND coalesce(c.telefone, '') <> ''""")
    usuarios = db.todos(conn, """
        SELECT u.id, u.nome, u.cpf, u.telefone
          FROM public.usuarios u
         WHERE u.ativo AND coalesce(u.telefone, '') <> ''""")
    pessoas, cpfs = [], set()
    for c in colabs:
        tel = telefone_canonico(c["telefone"])
        if len(digitos(tel)) < 12:
            continue
        cpf = normalizar_cpf(c["cpf"])
        cpfs.add(cpf)
        pessoas.append({"chave": f"C{c['id']}", "tipo": "COLABORADOR", "colaborador_id": c["id"],
                        "usuario_id": c.get("usuario_id"), "nome": (c["nome"] or "").strip(),
                        "cpf": cpf, "telefone": tel, "situacao": c.get("situacao") or ""})
    vinculados = {c.get("usuario_id") for c in colabs if c.get("usuario_id")}
    for u in usuarios:
        cpf = normalizar_cpf(u.get("cpf"))
        if u["id"] in vinculados or (cpf and cpf in cpfs):
            continue
        tel = telefone_canonico(u["telefone"])
        if len(digitos(tel)) < 12:
            continue
        pessoas.append({"chave": f"U{u['id']}", "tipo": "USUARIO", "colaborador_id": None,
                        "usuario_id": u["id"], "nome": (u["nome"] or "").strip(), "cpf": cpf,
                        "telefone": tel, "situacao": "OPERADOR"})
    pessoas.sort(key=lambda p: p["nome"].lower())
    return pessoas


def achar_pessoa(conn, *, telefone=None, cpf=None, pessoas: Optional[list] = None) -> Optional[dict]:
    """A pessoa elegível com este CPF ou este telefone (CPF tem preferência)."""
    pessoas = pessoas if pessoas is not None else pessoas_elegiveis(conn)
    alvo_cpf = normalizar_cpf(cpf) if cpf else ""
    if alvo_cpf:
        for p in pessoas:
            if p["cpf"] == alvo_cpf:
                return p
    alvo_tel = variantes_telefone(telefone) if telefone else set()
    if alvo_tel:
        for p in pessoas:
            if variantes_telefone(p["telefone"]) & alvo_tel:
                return p
    return None


# ---------------------------------------------------------------------------
# O CADASTRO
# ---------------------------------------------------------------------------
def cadastros_ativos(conn) -> list[dict]:
    return db.todos(conn, """SELECT * FROM mensageria.telegram_cadastros WHERE ativo
                              ORDER BY atualizado_em DESC""")


def cadastro_por_chat(conn, chat_id) -> Optional[dict]:
    return db.um(conn, "SELECT * FROM mensageria.telegram_cadastros WHERE chat_id = :c AND ativo",
                 c=str(chat_id))


def chat_id_de(conn, *, telefone=None, cpf=None) -> Optional[str]:
    """O id do chat de quem tem este CPF ou telefone. Se o telefone mudou no
    cadastro do RH, acha pela pessoa (telefone → CPF → cadastro)."""
    ativos = cadastros_ativos(conn)
    if not ativos:
        return None
    alvo_cpf = normalizar_cpf(cpf) if cpf else ""
    if alvo_cpf:
        for c in ativos:
            if c["cpf"] and normalizar_cpf(c["cpf"]) == alvo_cpf:
                return c["chat_id"]
    alvo_tel = variantes_telefone(telefone) if telefone else set()
    if alvo_tel:
        for c in ativos:
            if variantes_telefone(c["telefone"]) & alvo_tel:
                return c["chat_id"]
        pessoa = achar_pessoa(conn, telefone=telefone)
        if pessoa and pessoa["cpf"]:
            for c in ativos:
                if normalizar_cpf(c["cpf"]) == pessoa["cpf"]:
                    return c["chat_id"]
    return None


def gravar_cadastro(conn, *, chat_id, cpf="", telefone="", nome="", origem="", observacao="",
                    pessoa: Optional[dict] = None) -> dict:
    """Uma linha ativa por pessoa e uma por chat. Trocar de conta do Telegram
    desliga a linha antiga do mesmo CPF (mesma regra da aba TelegramID)."""
    chat_id = str(chat_id)
    cpf = normalizar_cpf(cpf)
    tel = telefone_canonico(telefone) if digitos(telefone) else ""
    if pessoa is None:
        pessoa = achar_pessoa(conn, cpf=cpf or None, telefone=tel or None)
    if pessoa:
        cpf = cpf or pessoa["cpf"]
        nome = nome or pessoa["nome"]
    if cpf:
        db.executar(conn, """UPDATE mensageria.telegram_cadastros SET ativo = FALSE, atualizado_em = now(),
                                    observacao = 'substituído por outra conta do Telegram'
                              WHERE cpf = :cpf AND chat_id <> :chat AND ativo""", cpf=cpf, chat=chat_id)
    db.executar(conn, """
        INSERT INTO mensageria.telegram_cadastros (chat_id, cpf, telefone, nome, colaborador_id, usuario_id,
                                                   origem, observacao, ativo)
        VALUES (:chat, :cpf, :tel, :nome, :col, :usu, :origem, :obs, TRUE)
        ON CONFLICT (chat_id) DO UPDATE SET
            cpf = CASE WHEN EXCLUDED.cpf <> '' THEN EXCLUDED.cpf ELSE mensageria.telegram_cadastros.cpf END,
            telefone = CASE WHEN EXCLUDED.telefone <> '' THEN EXCLUDED.telefone
                            ELSE mensageria.telegram_cadastros.telefone END,
            nome = CASE WHEN EXCLUDED.nome <> '' THEN EXCLUDED.nome ELSE mensageria.telegram_cadastros.nome END,
            colaborador_id = coalesce(EXCLUDED.colaborador_id, mensageria.telegram_cadastros.colaborador_id),
            usuario_id = coalesce(EXCLUDED.usuario_id, mensageria.telegram_cadastros.usuario_id),
            origem = CASE WHEN mensageria.telegram_cadastros.ativo THEN mensageria.telegram_cadastros.origem
                          ELSE EXCLUDED.origem END,
            observacao = EXCLUDED.observacao, ativo = TRUE, atualizado_em = now()
    """, chat=chat_id, cpf=cpf, tel=tel, nome=(nome or "")[:200],
         col=(pessoa or {}).get("colaborador_id"), usu=(pessoa or {}).get("usuario_id"),
         origem=(origem or "")[:20], obs=(observacao or "")[:300])
    return cadastro_por_chat(conn, chat_id) or {}


def desvincular(conn, cadastro_id: int, por: str = "") -> None:
    n = db.executar(conn, """UPDATE mensageria.telegram_cadastros SET ativo = FALSE, atualizado_em = now(),
                                    observacao = :obs WHERE id = :id AND ativo""",
                    id=int(cadastro_id), obs=f"desligado na tela por {por}"[:300])
    if not n:
        raise ValueError("cadastro não encontrado")


def migrado(conn) -> bool:
    return bool(core.ler_parametro(conn, MIGRADO_EM, ""))


def migrar_planilha(conn, linhas: list[list[str]], por: str = "") -> dict:
    """Traz a aba TelegramID (data, CPF, telefone, id do chat, nome, origem,
    observação) e CRUZA cada linha com o ERP:

      ok          a pessoa está no ERP e o telefone bate — entra ativa;
      divergente  a pessoa está no ERP pelo CPF, mas o telefone do Telegram é
                  outro — entra ATIVA (cortar quem já recebe não é o objetivo)
                  e vai para a lista do DP atualizar o telefone;
      fora        não é colaborador ativo nem operador do ERP (desligado, ou
                  nunca foi) — NÃO entra;
      sem_chat    linha sem id do Telegram — ignorada.

    Pode rodar de novo: quem já está no banco só ganha o que faltar. Depois
    dela, a planilha não é mais usada."""
    import json
    pessoas = pessoas_elegiveis(conn)
    rel = {"ok": [], "divergente": [], "fora": [], "sem_chat": 0, "novos": 0, "atualizados": 0}
    for linha in (linhas or [])[1:]:
        linha = list(linha) + [""] * 7
        chat = (linha[3] or "").strip()
        if not re.fullmatch(r"-?\d{3,20}", chat):
            rel["sem_chat"] += 1
            continue
        cpf_planilha, tel_planilha, nome_planilha = linha[1], linha[2], (linha[4] or "").strip()
        por_cpf = achar_pessoa(conn, cpf=cpf_planilha, pessoas=pessoas) if digitos(cpf_planilha) else None
        por_tel = achar_pessoa(conn, telefone=tel_planilha, pessoas=pessoas) if digitos(tel_planilha) else None
        pessoa = por_cpf or por_tel
        resumo = {"nome": (pessoa or {}).get("nome") or nome_planilha, "cpf": normalizar_cpf(cpf_planilha),
                  "telefone_telegram": telefone_canonico(tel_planilha),
                  "telefone_erp": (pessoa or {}).get("telefone", "")}
        if not pessoa:
            rel["fora"].append(resumo)
            continue
        bate = bool(variantes_telefone(tel_planilha) & variantes_telefone(pessoa["telefone"]))
        existia = cadastro_por_chat(conn, chat)
        gravar_cadastro(conn, chat_id=chat, cpf=pessoa["cpf"], telefone=tel_planilha or pessoa["telefone"],
                        nome=pessoa["nome"], origem=(existia or {}).get("origem") or "PLANILHA",
                        observacao=("migrado da aba TelegramID" if bate or not digitos(tel_planilha) else
                                    "migrado da aba TelegramID; telefone do Telegram diferente do cadastro"),
                        pessoa=pessoa)
        rel["atualizados" if existia else "novos"] += 1
        rel["ok" if bate or not digitos(tel_planilha) else "divergente"].append(resumo)
    momento = core._agora().isoformat()
    core.gravar_parametro(conn, MIGRADO_EM, momento, por)
    guardado = {**rel, "em": momento, "por": por,
                "contagem": {"ok": len(rel["ok"]), "divergente": len(rel["divergente"]),
                             "fora": len(rel["fora"]), "sem_chat": rel["sem_chat"]}}
    core.gravar_parametro(conn, RELATORIO_MIGRACAO, json.dumps(guardado, ensure_ascii=False)[:200000], por)
    return guardado


def relatorio_migracao(conn) -> Optional[dict]:
    import json
    bruto = core.ler_parametro(conn, RELATORIO_MIGRACAO, "")
    try:
        return json.loads(bruto) if bruto else None
    except ValueError:
        return None


def importar_planilha(conn, linhas, por: str = "") -> dict:
    """Nome antigo (mesma coisa que migrar_planilha), mantido para quem chamava."""
    r = migrar_planilha(conn, linhas, por)
    return {"novos": r["novos"], "atualizados": r["atualizados"], "ignorados": r["sem_chat"] + len(r["fora"])}


# ---------------------------------------------------------------------------
# A fila do DP: quem tentou e o número não bateu, quem não tem telefone
# ---------------------------------------------------------------------------
def registrar_tentativa(conn, *, chat_id, telefone="", cpf="", nome_telegram="", nome_erp="",
                        motivo="") -> None:
    db.executar(conn, """INSERT INTO mensageria.telegram_tentativas
                           (chat_id, telefone, cpf, nome_telegram, nome_erp, motivo)
                         VALUES (:chat, :tel, :cpf, :nt, :ne, :m)""",
                chat=str(chat_id), tel=telefone_canonico(telefone) if digitos(telefone) else "",
                cpf=normalizar_cpf(cpf), nt=(nome_telegram or "")[:120], ne=(nome_erp or "")[:200],
                m=(motivo or "")[:40])


def tentativas_pendentes(conn, dias: int = 60) -> list[dict]:
    """Tentativas recentes de quem AINDA não tem cadastro (a mais nova por chat)."""
    linhas = db.todos(conn, """
        SELECT DISTINCT ON (t.chat_id) t.*
          FROM mensageria.telegram_tentativas t
         WHERE t.criado_em > now() - make_interval(days => :d)
           AND NOT EXISTS (SELECT 1 FROM mensageria.telegram_cadastros c
                            WHERE c.chat_id = t.chat_id AND c.ativo)
         ORDER BY t.chat_id, t.criado_em DESC""", d=dias)
    return sorted(linhas, key=lambda l: l["criado_em"], reverse=True)


def sem_telefone(conn) -> list[dict]:
    return db.todos(conn, """SELECT id, nome, cpf FROM public.colaboradores
                              WHERE coalesce(situacao, '') <> 'DESLIGADO'
                                AND length(regexp_replace(coalesce(telefone, ''), '\\D', '', 'g')) < 10
                              ORDER BY nome""")


def colaborador_por_cpf(conn, cpf) -> Optional[dict]:
    """Para o contracheque do robô: nome, CPF, telefone e se está desligado —
    do ERP, no formato que o assistente já usava ({cpf, nome, tel, status})."""
    c = db.um(conn, """SELECT nome, cpf, telefone, situacao FROM public.colaboradores
                        WHERE regexp_replace(cpf, '\\D', '', 'g') = :cpf LIMIT 1""", cpf=normalizar_cpf(cpf))
    if not c:
        return None
    return {"cpf": normalizar_cpf(c["cpf"]), "nome": c["nome"] or "", "tel": c["telefone"] or "",
            "status": "Colaboradores Desligados" if c["situacao"] == "DESLIGADO" else "",
            "desligado": c["situacao"] == "DESLIGADO"}


def resumo_da_pessoa(conn, colaborador_id: int) -> dict:
    """O Telegram e as mensagens de UM colaborador — para a ficha dele no ERP."""
    c = db.um(conn, "SELECT id, nome, cpf, telefone, situacao FROM public.colaboradores WHERE id = :i",
              i=int(colaborador_id))
    if not c:
        raise ValueError("colaborador não encontrado")
    cpf = normalizar_cpf(c["cpf"])
    pessoa = {"cpf": cpf, "telefone": telefone_canonico(c["telefone"]) if digitos(c["telefone"]) else ""}
    cad = db.um(conn, """SELECT * FROM mensageria.telegram_cadastros
                          WHERE ativo AND (colaborador_id = :i OR (cpf <> '' AND cpf = :cpf))
                          ORDER BY atualizado_em DESC LIMIT 1""", i=c["id"], cpf=cpf or "~")
    destinos = _destinos_da_pessoa(pessoa) + ([cad["chat_id"]] if cad else [])
    mensagens = db.todos(conn, """SELECT criado_em, tipo, canal, status, detalhe, texto FROM mensageria.envios
                                   WHERE (cpf <> '' AND cpf = :cpf) OR destinatario = ANY(:d)
                                   ORDER BY id DESC LIMIT 15""", cpf=cpf or "~", d=destinos)
    contagem = db.todos(conn, """SELECT canal, status, count(*) AS n FROM mensageria.envios
                                  WHERE criado_em > now() - interval '30 days'
                                    AND ((cpf <> '' AND cpf = :cpf) OR destinatario = ANY(:d))
                                  GROUP BY canal, status""", cpf=cpf or "~", d=destinos)
    convite = db.um(conn, """SELECT criado_em, status, motivo, aberto_em, usado_em FROM mensageria.convites
                              WHERE (cpf <> '' AND cpf = :cpf) OR telefone = :tel
                              ORDER BY criado_em DESC LIMIT 1""", cpf=cpf or "~", tel=pessoa["telefone"] or "~")
    for m in mensagens:
        m["tipo_nome"] = core.nome_do_tipo(m["tipo"])
    return {"colaborador": {"id": c["id"], "nome": c["nome"], "situacao": c["situacao"],
                            "telefone": pessoa["telefone"]},
            "pode_cadastrar": bool(pessoa["telefone"]) and c["situacao"] != "DESLIGADO",
            "telegram": ({"chat_id": cad["chat_id"], "desde": cad["criado_em"], "origem": cad["origem"],
                          "telefone": cad["telefone"],
                          "telefone_diverge": bool(cad["telefone"] and pessoa["telefone"]
                                                   and not (variantes_telefone(cad["telefone"])
                                                            & variantes_telefone(pessoa["telefone"])))}
                         if cad else None),
            "convite": convite, "contagem_30d": contagem, "mensagens": mensagens,
            "chave": f"C{c['id']}"}


# ---------------------------------------------------------------------------
# O CONVITE
# ---------------------------------------------------------------------------
def convite_automatico_ligado(conn) -> bool:
    return core.ler_parametro(conn, CONVITE_AUTOMATICO, "1") == "1"


def intervalo_dias(conn) -> int:
    return max(1, core._inteiro(core.ler_parametro(conn, CONVITE_INTERVALO, ""), INTERVALO_PADRAO_DIAS))


def falhas_entre(conn) -> int:
    return max(1, core._inteiro(core.ler_parametro(conn, CONVITE_FALHAS_ENTRE, ""), FALHAS_ENTRE_PADRAO))


def maximo_automatico(conn) -> int:
    return max(1, core._inteiro(core.ler_parametro(conn, CONVITE_MAXIMO, ""), MAXIMO_AUTOMATICO_PADRAO))


def _destinos_da_pessoa(pessoa: dict) -> list[str]:
    """Como o telefone pode estar no registro de envios: com e sem 55, com e
    sem o nono dígito."""
    var = variantes_telefone(pessoa.get("telefone"))
    return sorted(var | {"55" + v for v in var}) or ["~"]


def falhas_desde(conn, pessoa: dict, desde: Optional[dt.datetime]) -> int:
    """Mensagens do Telegram que não chegaram a esta pessoa por falta de
    cadastro, desde `desde` (ou nos últimos 30 dias)."""
    linha = db.um(conn, """SELECT count(*) AS n FROM mensageria.envios
                            WHERE canal = 'telegram' AND status = 'SEM_DESTINO'
                              AND criado_em > coalesce(CAST(:desde AS timestamptz), now() - interval '30 days')
                              AND ((cpf <> '' AND cpf = :cpf) OR destinatario = ANY(:destinos))""",
                  desde=desde, cpf=pessoa.get("cpf") or "~", destinos=_destinos_da_pessoa(pessoa))
    return int(linha["n"]) if linha else 0


def texto_do_convite(conn) -> str:
    return core.ler_parametro(conn, CONVITE_TEXTO, "") or TEXTO_PADRAO


def montar_texto(modelo: str, nome: str, link: str, manual: bool = False) -> str:
    if manual and modelo == TEXTO_PADRAO:
        modelo = TEXTO_PADRAO_MANUAL
    primeiro = (nome or "").strip().split(" ")[0].title() or "tudo bem"
    texto = modelo.replace("{nome}", primeiro).replace("{link}", link)
    if link not in texto:              # quem editou o texto esqueceu o link
        texto += f"\n\n{link}"
    return texto


def validar_modelo(texto: str) -> str:
    texto = (texto or "").strip()
    if not texto:
        return ""
    if "{link}" not in texto:
        raise ValueError("o texto do convite precisa ter {link} — é por ele que a pessoa se cadastra")
    if len(texto) > 2000:
        raise ValueError("o texto do convite passou de 2.000 letras")
    return texto


def _pode_convidar(conn, pessoa: dict, *, manual: bool, agora: dt.datetime) -> tuple[bool, str]:
    if chat_id_de(conn, cpf=pessoa.get("cpf") or None, telefone=pessoa.get("telefone")):
        return False, "já tem Telegram cadastrado"
    ultimo = db.um(conn, """SELECT criado_em FROM mensageria.convites
                             WHERE ((cpf <> '' AND cpf = :cpf) OR telefone = :tel)
                               AND status IN ('ENVIADO', 'PENDENTE')
                             ORDER BY criado_em DESC LIMIT 1""",
                   cpf=pessoa.get("cpf") or "~", tel=pessoa.get("telefone") or "~")
    if ultimo:
        espera = (dt.timedelta(hours=INTERVALO_MANUAL_HORAS) if manual
                  else dt.timedelta(days=intervalo_dias(conn)))
        quando = ultimo["criado_em"]
        if quando.tzinfo is None:
            quando = quando.replace(tzinfo=dt.timezone.utc)
        if agora - quando < espera:
            local = quando.astimezone(core.FUSO).strftime("%d/%m %H:%M")
            return False, f"já recebeu convite em {local}"
    if manual:
        return True, ""
    # As travas do AUTOMÁTICO (a tela pode convidar quando quiser, uma vez por dia)
    automaticos = db.um(conn, """SELECT count(*) AS n FROM mensageria.convites
                                  WHERE ((cpf <> '' AND cpf = :cpf) OR telefone = :tel)
                                    AND motivo = 'AUTOMATICO' AND status = 'ENVIADO'""",
                        cpf=pessoa.get("cpf") or "~", tel=pessoa.get("telefone") or "~")["n"]
    if automaticos >= maximo_automatico(conn):
        return False, f"já recebeu {automaticos} convites automáticos; daqui em diante, só pela tela"
    if ultimo:
        faltam = falhas_entre(conn) - falhas_desde(conn, pessoa, ultimo["criado_em"])
        if faltam > 0:
            return False, (f"novo convite só depois de mais {faltam} "
                           f"{'mensagem que não chegue' if faltam == 1 else 'mensagens que não cheguem'}")
    return True, ""


def _status_do_resultado(r: dict) -> tuple[str, str]:
    wa = (r or {}).get("whatsapp") or {}
    if wa.get("ok"):
        return "ENVIADO", ""
    if "mensageria" in (r or {}):
        return "DESLIGADO", str(r["mensageria"].get("detalhe") or "")
    if wa.get("erro") == "limite":
        return "LIMITE", str(wa.get("detalhe") or "")
    if wa.get("ok") is None:
        return "DESLIGADO", str(wa.get("detalhe") or "WhatsApp desligado")
    return "FALHOU", str(wa.get("detalhe") or wa.get("erro") or r)[:300]


def _enviar_padrao(telefone: str, texto: str) -> dict:
    from app.apps.notificador import notificar
    return notificar(telefone=telefone, mensagem=texto, canais=("whatsapp",), finalidade=TIPO_CONVITE)


def convidar(pessoa: dict, *, motivo: str, tipo_que_falhou: str = "", por: str = "",
             enviar: Optional[Callable] = None, agora: Optional[dt.datetime] = None) -> dict:
    """Prepara (no banco), manda (fora da transação) e anota. Devolve
    {"status": ..., "detalhe": ...}. Recusa sem mandar quando não pode."""
    agora = agora or core._agora()
    manual = motivo == "MANUAL"
    with db.conexao() as conn:
        pode, porque = _pode_convidar(conn, pessoa, manual=manual, agora=agora)
        if not pode:
            return {"status": "RECUSADO", "detalhe": porque}
        codigo = secrets.token_urlsafe(12).replace("-", "x").replace("_", "y")
        texto = montar_texto(texto_do_convite(conn), pessoa.get("nome", ""), link_pessoal(codigo), manual)
        linha = db.um(conn, """
            INSERT INTO mensageria.convites (codigo, telefone, cpf, nome, colaborador_id, usuario_id, motivo,
                                             tipo_que_falhou, status, enviado_por)
            VALUES (:cod, :tel, :cpf, :nome, :col, :usu, :mot, :tipo, 'PENDENTE', :por) RETURNING id""",
            cod=codigo, tel=pessoa.get("telefone") or "", cpf=pessoa.get("cpf") or "",
            nome=(pessoa.get("nome") or "")[:200], col=pessoa.get("colaborador_id"),
            usu=pessoa.get("usuario_id"), mot=motivo, tipo=(tipo_que_falhou or "")[:80], por=(por or "")[:120])
    try:
        r = (enviar or _enviar_padrao)(pessoa["telefone"], texto)
    except Exception as e:  # noqa: BLE001 — o convite nunca derruba quem o disparou
        r = {"whatsapp": {"ok": False, "detalhe": str(e)[:200]}}
    status, detalhe = _status_do_resultado(r)
    with db.conexao() as conn:
        db.executar(conn, "UPDATE mensageria.convites SET status = :s, detalhe = :d WHERE id = :id",
                    s=status, d=detalhe[:300], id=linha["id"])
    logger.info("Mensageria: convite do Telegram %s para %s (%s)", status, pessoa.get("nome"), motivo)
    return {"status": status, "detalhe": detalhe}


def _em_segundo_plano(funcao: Callable) -> None:
    threading.Thread(target=funcao, name="convite-telegram", daemon=True).start()


def convidar_se_preciso(*, telefone=None, cpf=None, tipo_que_falhou: str = "") -> dict:
    """O convite AUTOMÁTICO, chamado quando uma mensagem do Telegram não chegou
    por falta de cadastro. Decide rápido e manda em segundo plano — quem chamou
    (o aviso de título pago, o Make) não espera o WhatsApp."""
    tipo = (tipo_que_falhou or "").strip()
    if tipo.startswith(TIPO_CONVITE) or tipo.startswith("mensageria."):
        return {"status": "IGNORADO", "detalhe": "convite não convida"}
    if not db.disponivel():
        return {"status": "IGNORADO", "detalhe": "mensageria fora do banco"}
    try:
        with db.conexao() as conn:
            if not convite_automatico_ligado(conn):
                return {"status": "IGNORADO", "detalhe": "convite automático desligado na tela Mensagens"}
            pessoa = achar_pessoa(conn, telefone=telefone, cpf=cpf)
            if not pessoa:
                return {"status": "IGNORADO", "detalhe": "não é colaborador nem operador com telefone no ERP"}
            pode, porque = _pode_convidar(conn, pessoa, manual=False, agora=core._agora())
            if not pode:
                return {"status": "IGNORADO", "detalhe": porque}
    except Exception as e:  # noqa: BLE001
        logger.warning("Mensageria: convite automático não decidiu (%s)", str(e)[:150])
        return {"status": "IGNORADO", "detalhe": "erro ao decidir"}
    _em_segundo_plano(lambda: convidar(pessoa, motivo="AUTOMATICO", tipo_que_falhou=tipo))
    return {"status": "AGENDADO", "detalhe": f"convite para {pessoa['nome']} saindo pelo WhatsApp"}


# ---------------------------------------------------------------------------
# O link pessoal, do lado do robô
# ---------------------------------------------------------------------------
def abrir_link(conn, codigo: str, chat_id) -> Optional[dict]:
    """A pessoa tocou no link: guarda o chat no convite e devolve o convite
    (ou None se o código não existe, venceu ou já foi usado)."""
    c = db.um(conn, """SELECT * FROM mensageria.convites WHERE codigo = :c AND usado_em IS NULL
                         AND criado_em > now() - make_interval(days => :d)""",
              c=(codigo or "")[:60], d=VALIDADE_DO_LINK_DIAS)
    if not c:
        return None
    db.executar(conn, "UPDATE mensageria.convites SET aberto_em = now(), chat_id = :chat WHERE id = :id",
                chat=str(chat_id), id=c["id"])
    return c


def convite_aberto_do_chat(conn, chat_id) -> Optional[dict]:
    return db.um(conn, """SELECT * FROM mensageria.convites WHERE chat_id = :chat AND usado_em IS NULL
                            AND aberto_em > now() - make_interval(hours => :h)
                          ORDER BY aberto_em DESC LIMIT 1""", chat=str(chat_id), h=JANELA_DO_LINK_ABERTO_H)


def marcar_usado(conn, chat_id, cpf: str = "") -> None:
    db.executar(conn, """UPDATE mensageria.convites SET usado_em = now()
                          WHERE usado_em IS NULL AND (chat_id = :chat OR (cpf <> '' AND cpf = :cpf))""",
                chat=str(chat_id), cpf=normalizar_cpf(cpf) or "~")


# ---------------------------------------------------------------------------
# Para a tela
# ---------------------------------------------------------------------------
def painel(conn, *, filtro: str = "", busca: str = "") -> dict:
    pessoas = pessoas_elegiveis(conn)
    ativos = cadastros_ativos(conn)
    por_cpf = {normalizar_cpf(c["cpf"]): c for c in ativos if c["cpf"]}
    falhas = db.todos(conn, """SELECT cpf, destinatario, max(criado_em) AS ultima, count(*) AS n
                                 FROM mensageria.envios
                                WHERE canal = 'telegram' AND status = 'SEM_DESTINO'
                                  AND criado_em > now() - interval '30 days'
                                GROUP BY cpf, destinatario""")
    convites = db.todos(conn, """SELECT DISTINCT ON (coalesce(nullif(cpf, ''), telefone))
                                        coalesce(nullif(cpf, ''), telefone) AS chave, criado_em, status, detalhe,
                                        motivo, aberto_em, usado_em
                                   FROM mensageria.convites
                                  ORDER BY coalesce(nullif(cpf, ''), telefone), criado_em DESC""")
    convite_por = {c["chave"]: c for c in convites}
    usados_ids = set()
    linhas = []
    for p in pessoas:
        cad = por_cpf.get(p["cpf"]) if p["cpf"] else None
        if not cad:
            var = variantes_telefone(p["telefone"])
            cad = next((c for c in ativos if variantes_telefone(c["telefone"]) & var), None)
        if cad:
            usados_ids.add(cad["id"])
        var = variantes_telefone(p["telefone"])
        n_falhas, ultima_falha = 0, None
        for f in falhas:
            if (p["cpf"] and normalizar_cpf(f["cpf"]) == p["cpf"]) or (variantes_telefone(f["destinatario"]) & var):
                n_falhas += int(f["n"])
                ultima_falha = max(filter(None, [ultima_falha, f["ultima"]]))
        conv = convite_por.get(p["cpf"]) or convite_por.get(p["telefone"])
        linhas.append({**p, "telegram": bool(cad), "cadastro_id": (cad or {}).get("id"),
                       "telegram_desde": (cad or {}).get("criado_em"), "telegram_origem": (cad or {}).get("origem"),
                       "falhas_30d": n_falhas, "ultima_falha": ultima_falha,
                       "convite": ({k: conv.get(k) for k in ("criado_em", "status", "detalhe", "motivo",
                                                             "aberto_em", "usado_em")} if conv else None)})
    soltos = [c for c in ativos if c["id"] not in usados_ids]
    for l in linhas:
        cad = next((c for c in ativos if c["id"] == l["cadastro_id"]), None)
        l["telefone_diverge"] = bool(cad and cad["telefone"]
                                     and not (variantes_telefone(cad["telefone"]) & variantes_telefone(l["telefone"])))
        l["telegram_telefone"] = (cad or {}).get("telefone", "")
    totais = {"pessoas": len(linhas), "com_telegram": sum(1 for l in linhas if l["telegram"]),
              "sem_telegram": sum(1 for l in linhas if not l["telegram"]),
              "com_falha": sum(1 for l in linhas if not l["telegram"] and l["falhas_30d"]),
              "a_conferir": len(soltos),
              "telefone_diverge": sum(1 for l in linhas if l["telefone_diverge"])}
    faltando_telefone = sem_telefone(conn)
    totais["sem_telefone"] = len(faltando_telefone)
    tentativas = tentativas_pendentes(conn)
    totais["tentativas"] = len(tentativas)
    b = (busca or "").strip().lower()
    bd = digitos(busca)
    if b:
        linhas = [l for l in linhas if b in l["nome"].lower() or (bd and (bd in l["cpf"] or bd in l["telefone"]))]
    if filtro == "sem":
        linhas = [l for l in linhas if not l["telegram"]]
    elif filtro == "falha":
        linhas = [l for l in linhas if not l["telegram"] and l["falhas_30d"]]
    elif filtro == "com":
        linhas = [l for l in linhas if l["telegram"]]
    elif filtro == "convidados":
        linhas = [l for l in linhas if not l["telegram"] and l["convite"]]
    elif filtro == "diverge":
        linhas = [l for l in linhas if l["telefone_diverge"]]
    return {"pessoas": linhas, "a_conferir": soltos, "totais": totais,
            "convite_automatico": convite_automatico_ligado(conn), "intervalo_dias": intervalo_dias(conn),
            "falhas_entre": falhas_entre(conn), "maximo_automatico": maximo_automatico(conn),
            "texto": texto_do_convite(conn), "texto_padrao": TEXTO_PADRAO,
            "ultima_importacao": core.ler_parametro(conn, ULTIMA_IMPORTACAO, ""), "link_do_robo": link_do_robo(),
            "migrado": migrado(conn), "relatorio_migracao": relatorio_migracao(conn),
            "sem_telefone": faltando_telefone, "tentativas": tentativas,
            "lote_maximo": LOTE_MAXIMO}
