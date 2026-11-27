# ============================================================================
# ERP — core/titulos/contrato_assinatura.py
# O CONTRATO DE EMPREITA GERADO PELO SISTEMA, E ASSINADO À DISTÂNCIA.
#
# ⚑ O PEDIDO (27/11/2026), com as palavras dele:
#
#   *"a gente precisaria coletar uma assinatura (…) do empreiteiro. Ou anexar um
#   documento (…) seria um terceiro, não é um colaborador (…) Como é que a gente
#   poderia encaminhar para a pessoa o contrato, via WhatsApp ou via e-mail, para
#   que fosse assinado? (…) duas pessoas assinariam. Seria o de fora e um de
#   dentro."* — e, decidindo: *"Quem assina é uma pessoa do cadastro também, de
#   colaboradores (…) aproveitar já o telefone que tem lá (…) Vai ser o WhatsApp
#   mesmo. WhatsApp e e-mail (…) que tenha valor jurídico (…) A parte jurídica
#   do contrato você faz aí. Botar alguma coisa selecionável (…) e dê espaço
#   também pra gente fazer a digitação (…) a IA (…) deveria dar uma ajustada."*
#
# O CAMINHO, e a ordem tem motivo:
#
#   1. CADASTRO — quem cadastra o contrato escolhe as cláusulas opcionais,
#      digita o que quiser acrescentar (a IA revisa, a pessoa aceita ou não) e
#      escolhe quem assina pela empresa (um colaborador). Nasce um RASCUNHO.
#   2. AVAL — quem tem alçada aprova o contrato. SÓ ENTÃO os links saem: não se
#      manda assinar um contrato que ainda pode ser recusado por dentro.
#   3. ASSINATURA — cada um recebe um link por WhatsApp (e e-mail, se houver).
#      Abre no celular, sem login, lê, pede um CÓDIGO de 6 dígitos (que chega no
#      mesmo telefone ou e-mail), confirma o CPF/CNPJ e assina com o dedo.
#   4. VIGENTE — com as duas assinaturas, o PDF final (com o registro de cada
#      assinatura) vai para o arquivo do contrato e ele pode ser medido.
#
# ⚑ O VALOR JURÍDICO. É assinatura eletrônica (Lei 14.063/2020; MP 2.200-2/2001,
# art. 10 §2º — as partes aceitam o meio na cláusula 10): prova QUEM assinou
# (posse do telefone/e-mail que recebeu o código + o documento digitado) e que
# o texto NÃO MUDOU (o lacre SHA-256 do texto, conferido no momento de cada
# assinatura). Ficam registrados hora do servidor, IP, navegador e canal do
# código. A mesma base da assinatura do ponto e dos termos do DP. **A confirmar
# com o advogado** — está no histórico.
#
# ⚑ CUSTO: abrir e listar não saem do banco. Cada código é UMA mensagem de
# WhatsApp ou um e-mail; o envio dos links no aval, uma por pessoa.
# ============================================================================
from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import secrets
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.apps.erp.core.comum.auditoria import (
    ErroNaoEncontrado, ErroValidacao, registrar_evento,
)

logger = logging.getLogger(__name__)

FINALIDADE = "erp.contrato_assinatura"       # o tipo na mensageria
CODIGO_VALE_MIN = 15
CODIGOS_POR_HORA = 5
TENTATIVAS_POR_CODIGO = 5
MAX_ASSINATURA = 400 * 1024

# ---------------------------------------------------------------------------
# 1. AS CLÁUSULAS
#
# A base vale para todo contrato (é o rascunho que o dono leu). As OPCIONAIS
# são marcadas no cadastro — algumas com um número a preencher (multa, por
# exemplo). E sempre há o espaço livre, que a IA revisa.
# ---------------------------------------------------------------------------
PARAMETROS = {
    "prazo_pagamento_dias": {"rotulo": "Pagar a medição em até (dias)", "padrao": 10},
    "aviso_rescisao_dias": {"rotulo": "Aviso para rescindir (dias)", "padrao": 15},
}

OPCIONAIS: dict[str, dict[str, Any]] = {
    "MATERIAL_DO_CONTRATADO": {
        "rotulo": "O empreiteiro fornece o material",
        "explicacao": "por padrão o material é da empresa; marque se for dele",
        "texto": "Por acordo entre as partes, o CONTRATADO fornece os materiais "
                 "necessários ao serviço, com a qualidade exigida pelas normas "
                 "técnicas, e o seu custo já está incluído no preço deste contrato.",
        "padrao": False,
    },
    "MULTA_ATRASO": {
        "rotulo": "Multa por atraso",
        "explicacao": "por dia de atraso que não for culpa da empresa",
        "texto": "O atraso injustificado na conclusão sujeita o CONTRATADO a multa "
                 "de {multa_pct}% do valor do contrato por dia de atraso, limitada a "
                 "10% do valor total, descontada das medições.",
        "campos": {"multa_pct": {"rotulo": "% ao dia", "padrao": "0,5"}},
        "padrao": False,
    },
    "LIMPEZA": {
        "rotulo": "Entregar limpo",
        "explicacao": "retirar entulho e sobras ao terminar cada etapa",
        "texto": "O CONTRATADO mantém o seu local de trabalho organizado e, ao fim "
                 "de cada etapa, retira o entulho e as sobras para o local indicado "
                 "pela obra.",
        "padrao": True,
    },
    "SEM_SUBCONTRATAR": {
        "rotulo": "Não pode repassar o serviço",
        "explicacao": "subcontratar só com autorização escrita",
        "texto": "É vedado ao CONTRATADO transferir ou subcontratar o serviço, no "
                 "todo ou em parte, sem autorização escrita da CONTRATANTE.",
        "padrao": True,
    },
    "AJUDANTES": {
        "rotulo": "Ajudantes do empreiteiro identificados",
        "explicacao": "quem ele trouxer, ele informa à obra antes de entrar",
        "texto": "Os ajudantes que o CONTRATADO trouxer são de sua exclusiva "
                 "responsabilidade e serão identificados à obra antes de ingressar "
                 "no canteiro, com nome e documento.",
        "padrao": True,
    },
    "EQUIPAMENTO_EMPRESTADO": {
        "rotulo": "Equipamento emprestado pela obra",
        "explicacao": "betoneira, andaime… devolvidos em bom estado",
        "texto": "A CONTRATANTE empresta ao CONTRATADO, para uso exclusivo neste "
                 "serviço, os seguintes equipamentos: {equipamentos}. O CONTRATADO "
                 "os devolve em bom estado ao fim do serviço e responde pelo dano "
                 "causado por mau uso.",
        "campos": {"equipamentos": {"rotulo": "Quais equipamentos", "padrao": ""}},
        "padrao": False,
    },
    "NOTA_FISCAL": {
        "rotulo": "Nota fiscal a cada medição",
        "explicacao": "para empreiteiro com CNPJ (inclusive MEI)",
        "texto": "Sendo pessoa jurídica, o CONTRATADO emite nota fiscal de serviço "
                 "correspondente a cada medição autorizada; o pagamento depende da "
                 "nota.",
        "padrao": False,
    },
    "FOTOS": {
        "rotulo": "Autoriza fotos do serviço",
        "explicacao": "as fotos comprovam a medição",
        "texto": "O CONTRATADO autoriza o registro fotográfico do serviço, que "
                 "integra as medições e serve para comprovar o que foi executado.",
        "padrao": True,
    },
}


def catalogo() -> dict[str, Any]:
    """O que a tela precisa para desenhar a parte do contrato."""
    return {"parametros": PARAMETROS,
            "opcionais": [{"chave": k, **v} for k, v in OPCIONAIS.items()]}


# ---------------------------------------------------------------------------
# 2. MONTAR O TEXTO
# ---------------------------------------------------------------------------
_UNIDADES = ("zero um dois três quatro cinco seis sete oito nove dez onze doze treze "
             "catorze quinze dezesseis dezessete dezoito dezenove").split()
_DEZENAS = "_ _ vinte trinta quarenta cinquenta sessenta setenta oitenta noventa".split()
_CENTENAS = ("_ cento duzentos trezentos quatrocentos quinhentos seiscentos "
             "setecentos oitocentos novecentos").split()


def _ate_mil(n: int) -> str:
    if n == 0:
        return ""
    if n == 100:
        return "cem"
    partes = []
    c, r = divmod(n, 100)
    if c:
        partes.append(_CENTENAS[c])
    if r:
        if r < 20:
            partes.append(_UNIDADES[r])
        else:
            d, u = divmod(r, 10)
            partes.append(_DEZENAS[d] + (f" e {_UNIDADES[u]}" if u else ""))
    return " e ".join(partes)


def por_extenso(valor: Any) -> str:
    """Valor em reais por extenso — o que o contrato escreve entre parênteses."""
    v = Decimal(str(valor or 0)).quantize(Decimal("0.01"))
    reais, centavos = int(v), int((v - int(v)) * 100)
    grupos = []
    milhoes, resto = divmod(reais, 1_000_000)
    milhares, unidades = divmod(resto, 1000)
    if milhoes:
        grupos.append(_ate_mil(milhoes) + (" milhão" if milhoes == 1 else " milhões"))
    if milhares:
        grupos.append("mil" if milhares == 1 else _ate_mil(milhares) + " mil")
    if unidades:
        grupos.append(_ate_mil(unidades))
    txt = " e ".join(grupos) if grupos else "zero"
    txt += " real" if reais == 1 else (" de reais" if reais and not unidades and not milhares
                                       else " reais")
    if centavos:
        txt += " e " + _ate_mil(centavos) + (" centavo" if centavos == 1 else " centavos")
    return txt


def _moeda(v: Any) -> str:
    n = Decimal(str(v or 0)).quantize(Decimal("0.01"))
    s = f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _data(d: Any) -> str:
    if not d:
        return "a combinar"
    if isinstance(d, str):
        d = date.fromisoformat(d[:10])
    return d.strftime("%d/%m/%Y")


def _doc(d: str) -> str:
    d = re.sub(r"\D", "", d or "")
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    if len(d) == 14:
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"
    return d


def _endereco(o: Any) -> str:
    if o is None:
        return ""
    partes = [getattr(o, "logradouro", None), getattr(o, "numero", None),
              getattr(o, "complemento", None), getattr(o, "bairro", None)]
    cidade = " / ".join(x for x in (getattr(o, "municipio", None), getattr(o, "uf", None)) if x)
    txt = ", ".join(str(p) for p in partes if p)
    if cidade:
        txt = f"{txt} — {cidade}" if txt else cidade
    return txt or "endereço não informado no cadastro"


def dados_do_contrato(s: Session, contrato_id: int,
                      assinante_colaborador_id: Optional[int]) -> dict[str, Any]:
    """Tudo que o texto precisa, do cadastro — nada digitado de novo."""
    from app.apps.erp.db.models.cadastros import Colaborador, Empresa, Fornecedor, Obra
    from app.apps.erp.db.models.financeiro import ContratoServico
    c = s.get(ContratoServico, contrato_id)
    if c is None:
        raise ErroNaoEncontrado("Contrato não encontrado.")
    obra = s.get(Obra, c.obra_id)
    empresa = s.get(Empresa, obra.empresa_id) if obra and obra.empresa_id else None
    if empresa is None:
        raise ErroValidacao(
            f"A obra {obra.codigo if obra else ''} não tem a empresa do grupo "
            "definida no cadastro — é ela que contrata. Defina a empresa da obra "
            "(Obras › Painel de obras) antes de gerar o contrato.")
    forn = s.get(Fornecedor, c.fornecedor_id)
    rep = s.get(Colaborador, int(assinante_colaborador_id)) if assinante_colaborador_id else None
    cargo = ""
    if rep is not None and rep.cargo_id:
        linha = s.execute(text("SELECT nome FROM cargos WHERE id = :i"),
                          {"i": rep.cargo_id}).first()
        cargo = linha[0] if linha else ""
    itens = s.execute(
        text("SELECT descricao, unidade, quantidade, preco_unitario FROM "
             "contrato_servico_itens WHERE contrato_id = :c ORDER BY ordem"),
        {"c": c.id}).mappings().all() if c.modo == "MEDICAO" else []
    obra_end = ", ".join(x for x in (obra.endereco, obra.numero_endereco, obra.bairro,
                                     " / ".join(y for y in (obra.municipio, obra.uf) if y))
                         if x) if obra else ""
    return {
        "contrato": c, "obra": obra, "empresa": empresa, "fornecedor": forn,
        "representante": rep, "representante_cargo": cargo or "representante",
        "itens": [dict(i) for i in itens], "obra_endereco": obra_end or "endereço da obra",
    }


def montar(s: Session, contrato_id: int, escolhas: dict[str, Any]) -> dict[str, str]:
    """O texto do contrato, pronto para mostrar, assinar e lacrar."""
    d = dados_do_contrato(s, contrato_id, escolhas.get("assinante_colaborador_id"))
    c, obra, emp, f, rep = (d["contrato"], d["obra"], d["empresa"], d["fornecedor"],
                            d["representante"])
    par = {k: str(escolhas.get(k) or v["padrao"]) for k, v in PARAMETROS.items()}
    pf = len(re.sub(r"\D", "", f.cnpj_cpf or "")) == 11
    L: list[str] = []
    L.append(f"CONTRATANTE: {emp.razao_social}, inscrita no CNPJ sob o nº "
             f"{_doc(emp.cnpj)}, com sede em {_endereco(emp)}, neste ato representada "
             f"por {rep.nome if rep else '(a definir)'}, {d['representante_cargo']}, "
             f"CPF {_doc(rep.cpf) if rep else '(a definir)'}.")
    # O RG vem da leitura do documento no cadastro do contrato (o credor não
    # tem coluna para ele — ver `empreiteiro.py`).
    rg = (escolhas.get("contratado_rg") or "").strip()
    rg_txt = ""
    if pf and rg:
        orgao = "/".join(x for x in ((escolhas.get("contratado_rg_orgao") or "").strip(),
                                     (escolhas.get("contratado_rg_uf") or "").strip()) if x)
        rg_txt = f", RG nº {rg}{' ' + orgao if orgao else ''}"
    L.append(f"CONTRATADO: {f.razao_social}, {'CPF' if pf else 'CNPJ'} nº "
             f"{_doc(f.cnpj_cpf)}{rg_txt}, {'residente' if pf else 'com sede'} em {_endereco(f)}"
             f"{', telefone ' + f.telefone if f.telefone else ''}"
             f"{', e-mail ' + f.email if f.email else ''}.")
    L.append("As partes contratam, por este instrumento, a execução de serviço por "
             "empreitada, nos termos dos artigos 610 a 626 do Código Civil e das "
             "cláusulas abaixo.")
    n = 0

    def clausula(titulo: str, *paragrafos: str) -> None:
        nonlocal n
        n += 1
        L.append(f"\nCLÁUSULA {n} — {titulo.upper()}")
        for i, p in enumerate(paragrafos, start=1):
            L.append(f"{n}.{i}. {p}")

    planilha = ""
    if d["itens"]:
        linhas = []
        for it in d["itens"]:
            total = Decimal(str(it["quantidade"])) * Decimal(str(it["preco_unitario"]))
            linhas.append(f"   • {it['descricao']}: {Decimal(str(it['quantidade'])).normalize():f} "
                          f"{it['unidade'] or 'un'} × {_moeda(it['preco_unitario'])} = {_moeda(total)}")
        planilha = "O serviço compreende os itens abaixo, com as quantidades e os preços combinados:\n" + "\n".join(linhas)
    else:
        planilha = "O serviço é contratado por preço fechado, conforme descrito no item anterior."
    clausula("Objeto",
             f"O CONTRATADO executará, com mão de obra própria, o serviço de "
             f"{c.objeto} na obra {obra.codigo} — {obra.nome}, situada em {d['obra_endereco']}.",
             planilha)
    clausula("Prazo",
             f"Início em {_data(c.data_inicio)} e término previsto para {_data(c.data_fim)}.",
             "O atraso que não for causado pela CONTRATANTE deve ser comunicado por "
             "escrito, com o motivo, assim que for conhecido.")
    valor = Decimal(str(c.valor_total))
    preco = [f"O valor total estimado deste contrato é de {_moeda(valor)} ({por_extenso(valor)})."]
    if c.modo == "MEDICAO":
        preco.append("O valor devido é apurado por medição: a quantidade efetivamente "
                     "executada de cada serviço, multiplicada pelo seu preço unitário. "
                     "Serviço não executado não é pago. Quantidade além da contratada "
                     "só é paga com aditivo escrito.")
    else:
        preco.append(f"O valor é pago em {c.parcelas_previstas or 1} parcela(s), cada "
                     f"uma liberada pela conferência da etapa correspondente.")
    material = escolhas.get("opcionais", {}).get("MATERIAL_DO_CONTRATADO")
    preco.append("O preço inclui mão de obra, ferramentas e equipamentos de uso pessoal "
                  "do CONTRATADO, transporte próprio e todos os encargos da sua "
                  "atividade." + ("" if material else
                                  " Os materiais são fornecidos pela CONTRATANTE."))
    clausula("Preço", *preco)
    garantia = Decimal(str(c.retencao_garantia_pct or 0))
    pagto = [
        "A medição é feita por período, com a quantidade executada de cada serviço, "
        "fotos do serviço e observações, registradas no sistema da CONTRATANTE.",
        f"A medição é conferida e autorizada por pessoa da CONTRATANTE com alçada. "
        f"Autorizada, o pagamento é feito em até {par['prazo_pagamento_dias']} dias, por "
        f"Pix ou transferência para conta de titularidade do CONTRATADO. Pagamento "
        f"a terceiro só com autorização escrita.",
        "Do valor de cada medição pode ser descontado: a) o adiantamento "
        "eventualmente concedido;"
        + (f" b) a retenção de garantia de {garantia.normalize():f}%, devolvida após a "
           "conclusão e a aceitação do serviço;" if garantia > 0 else "")
        + f" {'c' if garantia > 0 else 'b'}) os tributos que a lei mandar reter."]
    clausula("Medição e pagamento", *pagto)
    obrig = [
        "Executar o serviço com boa técnica, seguindo as normas aplicáveis, o projeto "
        "e as orientações técnicas da obra quanto ao resultado esperado.",
        "Usar os equipamentos de proteção individual exigidos e cumprir as normas de "
        "segurança do canteiro (NR-18).",
        "Responder pelos seus ajudantes, se os tiver, inclusive por pagamento, "
        "encargos e segurança, sem nenhum vínculo deles com a CONTRATANTE.",
        "Refazer, sem custo, o serviço executado com defeito.",
        "Manter regular a sua situação perante os órgãos públicos e apresentar, "
        "quando pedido, o comprovante de recolhimento dos seus tributos."]
    clausula("Obrigações do contratado", *obrig)
    clausula("Obrigações da contratante",
             "Dar acesso ao local, fornecer os materiais combinados e as informações "
             "técnicas necessárias.",
             "Medir e pagar nos prazos deste contrato.")
    clausula("Garantia do serviço",
             "O CONTRATADO responde pela solidez e pela segurança do serviço nos termos "
             "do artigo 618 do Código Civil, e pelos defeitos aparentes apontados na "
             "aceitação.")
    clausula("Rescisão",
             f"Qualquer das partes pode rescindir este contrato com aviso escrito de "
             f"{par['aviso_rescisao_dias']} dias. Paga-se o serviço medido até a data.",
             "O abandono do serviço, ou o descumprimento das obrigações do contratado, "
             "permite a rescisão imediata, descontado o custo de corrigir o que ficou "
             "mal feito.")
    clausula("Natureza do contrato",
             "Este é um contrato civil de empreitada. O CONTRATADO executa o serviço "
             "com autonomia quanto à forma, ao horário e aos meios, responde pelo "
             "resultado e pode prestar serviço a outros. Não há subordinação, horário "
             "fixo nem exclusividade, e por isso não há vínculo de emprego entre as "
             "partes.")
    # as opcionais marcadas, com os números preenchidos
    marcadas = []
    for k, op in OPCIONAIS.items():
        if not (escolhas.get("opcionais") or {}).get(k) or k == "MATERIAL_DO_CONTRATADO":
            continue
        valores = {ck: str((escolhas.get("campos") or {}).get(ck) or cv["padrao"])
                   for ck, cv in (op.get("campos") or {}).items()}
        if any(not v.strip() for v in valores.values()):
            raise ErroValidacao(f"Preencha o que falta na cláusula “{op['rotulo']}”.")
        marcadas.append(op["texto"].format(**valores))
    if material:
        marcadas.insert(0, OPCIONAIS["MATERIAL_DO_CONTRATADO"]["texto"])
    adicional = (escolhas.get("texto_adicional") or "").strip()
    if adicional:
        marcadas.extend(p.strip() for p in re.split(r"\n\s*\n", adicional) if p.strip())
    if marcadas:
        clausula("Condições particulares", *marcadas)
    clausula("Assinatura eletrônica e foro",
             "As partes aceitam a assinatura eletrônica deste contrato, com "
             "identificação por código enviado ao telefone ou e-mail informado, "
             "confirmação do CPF/CNPJ e registro de data, hora, endereço de rede e "
             "lacre do documento, nos termos da Lei 14.063/2020 e do artigo 10, § 2º, "
             "da MP 2.200-2/2001.",
             f"Fica eleito o foro de {emp.municipio or 'Recife'}/{emp.uf or 'PE'}.")
    titulo = f"CONTRATO DE PRESTAÇÃO DE SERVIÇOS POR EMPREITADA Nº {c.numero}"
    return {"titulo": titulo, "texto": "\n".join(L).strip()}


def lacre(titulo: str, texto_: str) -> str:
    return hashlib.sha256(f"{titulo}\n{texto_}".encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 3. A IA AJUSTA O QUE FOI DIGITADO
# ---------------------------------------------------------------------------
_INSTRUCAO_IA = """Você revisa cláusulas que uma construtora quer acrescentar a um
contrato civil de empreitada para pequenos serviços de obra (pessoa física ou
microempresa). O texto vem digitado às pressas, às vezes falado.

Reescreva em português jurídico claro e curto, um parágrafo por condição, sem
inventar obrigação que não foi dita. Use CONTRATANTE (a construtora) e
CONTRATADO (o empreiteiro). Aponte em "observacoes" o que for arriscado: o que
sugere vínculo de emprego (horário fixo, subordinação, exclusividade, pagamento
por dia), o que contradiz as cláusulas-base (pagamento por medição, material da
contratante, aviso de rescisão), ou o que for ilegal.

Responda SOMENTE com JSON: {"texto": "...", "observacoes": ["...", "..."]}"""


def ajustar_com_ia(texto_digitado: str, contexto: str = "") -> dict[str, Any]:
    """A IA sugere; a pessoa decide. Nada é trocado sem ela aceitar na tela."""
    from app.apps.erp.core.documentos.leitor import ErroLeitura, _chamar_ia
    bruto = (texto_digitado or "").strip()
    if len(bruto) < 10:
        raise ErroValidacao("Escreva um pouco mais para a IA ter o que revisar.")
    try:
        r = _chamar_ia(texto=f"Contrato: {contexto}\n\nCláusulas digitadas:\n{bruto[:4000]}",
                       instrucao=_INSTRUCAO_IA)
    except ErroLeitura as e:
        raise ErroValidacao(str(e))
    return {"texto": str(r.get("texto") or "").strip(),
            "observacoes": [str(x) for x in (r.get("observacoes") or []) if str(x).strip()]}


# ---------------------------------------------------------------------------
# 4. PREPARAR (no cadastro) e INICIAR (no aval)
# ---------------------------------------------------------------------------
_tem: dict[str, bool] = {}


def disponivel(s: Session) -> bool:
    """A migração 132 está aplicada? Sem ela, nada cai: a tela diz que falta."""
    if not _tem.get("ok"):
        _tem["ok"] = s.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' "
            "AND table_name = 'contrato_assinaturas'")).first() is not None
    return _tem["ok"]


def _resumo(v: str) -> str:
    return hashlib.sha256((v or "").encode()).hexdigest()


def _digitos(v: Any) -> str:
    return re.sub(r"\D", "", str(v or ""))


def preparar(s: Session, contrato_id: int, escolhas: dict[str, Any], usuario) -> dict[str, Any]:
    """O rascunho do documento e as duas assinaturas, ainda sem enviar nada.

    Quem assina pela empresa é um COLABORADOR (decisão dele): o telefone e o
    e-mail vêm do cadastro dele, e se mudarem lá, o próximo envio já sai certo.
    O contato do empreiteiro vem do cadastro do credor — o que vier do
    formulário atualiza o cadastro, para a próxima vez.
    """
    from app.apps.erp.db.models.cadastros import Colaborador, Fornecedor
    if not disponivel(s):
        raise ErroValidacao("A assinatura de contrato precisa da atualização do banco "
                            "(132). Peça ao administrador para aplicar.")
    assinante = escolhas.get("assinante_colaborador_id")
    rep = s.get(Colaborador, int(assinante)) if assinante else None
    if rep is None:
        raise ErroValidacao("Escolha quem assina pela empresa.")
    if (rep.situacao or "").upper() == "DESLIGADO":
        raise ErroValidacao(f"{rep.nome} está desligado(a) — escolha outra pessoa.")
    if not (_digitos(rep.telefone) or (rep.email or "").strip()):
        raise ErroValidacao(f"{rep.nome} não tem telefone nem e-mail no cadastro de "
                            "colaboradores. Atualize o cadastro dele antes.")
    texto_ = montar(s, contrato_id, escolhas)
    c_row = s.execute(text("SELECT fornecedor_id FROM contratos_servico WHERE id = :i"),
                      {"i": contrato_id}).first()
    f = s.get(Fornecedor, c_row[0])
    tel = _digitos(escolhas.get("contratado_telefone")) or _digitos(f.telefone)
    email = (escolhas.get("contratado_email") or f.email or "").strip()
    if not (tel or email):
        raise ErroValidacao(f"Informe o WhatsApp ou o e-mail de {f.razao_social} — é "
                            "por lá que o link de assinatura chega.")
    if tel and tel != _digitos(f.telefone):
        f.telefone = tel
    if email and email != (f.email or ""):
        f.email = email
    # Gerar de novo antes do aval troca o rascunho anterior (um vivo por contrato).
    s.execute(text("UPDATE contrato_documentos SET situacao = 'CANCELADO' "
                   "WHERE contrato_id = :c AND situacao = 'RASCUNHO'"), {"c": contrato_id})
    guardar = {k: v for k, v in escolhas.items() if not k.startswith("_")}
    doc_id = s.execute(text("""
        INSERT INTO contrato_documentos (contrato_id, titulo, texto, lacre, escolhas, criado_por)
        VALUES (:c, :t, :x, :l, CAST(:e AS jsonb), :u) RETURNING id"""),
        {"c": contrato_id, "t": texto_["titulo"], "x": texto_["texto"],
         "l": lacre(texto_["titulo"], texto_["texto"]),
         "e": json.dumps(guardar, ensure_ascii=False, default=str),
         "u": getattr(usuario, "id", None)}).scalar()
    for papel, ordem, nome, documento, fone, mail, col, forn in (
            ("CONTRATADO", 1, f.razao_social, f.cnpj_cpf, tel, email, None, f.id),
            ("CONTRATANTE", 2, rep.nome, rep.cpf, _digitos(rep.telefone),
             (rep.email or "").strip(), rep.id, None)):
        s.execute(text("""
            INSERT INTO contrato_assinaturas (documento_id, papel, ordem, nome, documento,
                   telefone, email, colaborador_id, fornecedor_id, token_resumo)
            VALUES (:d, :p, :o, :n, :doc, :t, :e, :col, :f, :tok)"""),
            {"d": doc_id, "p": papel, "o": ordem, "n": nome, "doc": _digitos(documento),
             "t": fone or None, "e": mail or None, "col": col, "f": forn,
             "tok": _resumo(secrets.token_urlsafe(24))})
    registrar_evento(s, "contrato_servico", contrato_id, "CONTRATO_PREPARADO",
                     {"documento_id": doc_id, "assinante_empresa": rep.nome,
                      "opcionais": [k for k, v in (escolhas.get("opcionais") or {}).items() if v]},
                     getattr(usuario, "id", None))
    return {"documento_id": doc_id, **texto_}


def documento_vivo(s: Session, contrato_id: int) -> Optional[dict[str, Any]]:
    if not disponivel(s):
        return None
    l = s.execute(text("""SELECT * FROM contrato_documentos
                          WHERE contrato_id = :c AND situacao <> 'CANCELADO'
                          ORDER BY id DESC LIMIT 1"""), {"c": contrato_id}).mappings().first()
    return dict(l) if l else None


def ao_aprovar(s: Session, contrato, base_url: str, usuario) -> dict[str, Any]:
    """Chamado DEPOIS do aval. Com documento a assinar, o contrato espera as
    assinaturas em vez de ficar vigente, e os links saem agora."""
    doc = documento_vivo(s, contrato.id)
    if doc is None or doc["situacao"] != "RASCUNHO":
        return {"assinatura": False}
    s.execute(text("UPDATE contrato_documentos SET situacao = 'EM_ASSINATURA' WHERE id = :d"),
              {"d": doc["id"]})
    contrato.status = "AGUARDANDO_ASSINATURA"
    envios = [enviar_link(s, a["id"], base_url, usuario)
              for a in _assinaturas(s, doc["id"])]
    return {"assinatura": True, "envios": envios}


def _assinaturas(s: Session, documento_id: int) -> list[dict[str, Any]]:
    return [dict(l) for l in s.execute(text(
        "SELECT * FROM contrato_assinaturas WHERE documento_id = :d ORDER BY ordem"),
        {"d": documento_id}).mappings().all()]


# ---------------------------------------------------------------------------
# 5. ENVIAR — por WhatsApp e por e-mail
# ---------------------------------------------------------------------------
def _empresa_do_documento(s: Session, documento_id: int):
    from app.apps.erp.db.models.cadastros import Empresa
    l = s.execute(text("""SELECT o.empresa_id FROM contrato_documentos d
                          JOIN contratos_servico c ON c.id = d.contrato_id
                          JOIN obras o ON o.id = c.obra_id WHERE d.id = :d"""),
                  {"d": documento_id}).first()
    return s.get(Empresa, l[0]) if l and l[0] else None


def _mandar(s: Session, a: dict[str, Any], assunto: str, mensagem: str,
            canal: str, usuario=None) -> dict[str, Any]:
    """Um envio, por um canal. Nunca levanta: devolve o que aconteceu."""
    if canal == "whatsapp":
        if not a.get("telefone"):
            return {"canal": canal, "ok": False, "detalhe": "sem telefone"}
        try:
            from app.apps.notificador import enviar_whatsapp
            r = enviar_whatsapp(a["telefone"], mensagem, finalidade=FINALIDADE)
            return {"canal": canal, "ok": bool(r.get("ok")),
                    "detalhe": str(r.get("detalhe") or r.get("erro") or "")}
        except Exception as e:  # noqa: BLE001 — o envio não derruba a assinatura
            logger.exception("Contrato/assinatura: WhatsApp falhou")
            return {"canal": canal, "ok": False, "detalhe": str(e)}
    if not a.get("email"):
        return {"canal": canal, "ok": False, "detalhe": "sem e-mail"}
    try:
        from app.apps.erp.core.comum import email as svc_email
        empresa = _empresa_do_documento(s, a["documento_id"])
        if empresa is None:
            return {"canal": canal, "ok": False, "detalhe": "obra sem empresa"}
        with s.begin_nested():
            reg = svc_email.enviar(s, empresa=empresa, para=[a["email"]], assunto=assunto,
                                   corpo=mensagem, entidade_tipo="contrato_assinatura",
                                   entidade_id=a["id"], usuario=usuario)
        return {"canal": canal, "ok": reg.situacao == "ENVIADO",
                "detalhe": reg.erro or ""}
    except Exception as e:  # noqa: BLE001
        logger.warning("Contrato/assinatura: e-mail não saiu — %s", e)
        return {"canal": canal, "ok": False, "detalhe": str(e)}


def enviar_link(s: Session, assinatura_id: int, base_url: str, usuario=None) -> dict[str, Any]:
    """Gera um link NOVO (o anterior deixa de valer) e manda pelos dois canais."""
    a = s.execute(text("SELECT * FROM contrato_assinaturas WHERE id = :i"),
                  {"i": assinatura_id}).mappings().first()
    if a is None:
        raise ErroNaoEncontrado("Assinatura não encontrada.")
    a = dict(a)
    if a["situacao"] != "AGUARDANDO":
        return {"papel": a["papel"], "nome": a["nome"], "ja_assinou": True}
    token = secrets.token_urlsafe(24)
    link = f"{base_url.rstrip('/')}/erp/assinar/{token}"
    doc = s.execute(text("SELECT titulo FROM contrato_documentos WHERE id = :d"),
                    {"d": a["documento_id"]}).first()
    primeiro = a["nome"].split()[0].title() if a["nome"] else ""
    msg = (f"Olá, {primeiro}. Há um contrato para você assinar: {doc[0]}.\n\n"
           f"Abra o link, leia com calma e assine pelo celular. Para confirmar que é "
           f"você, o sistema manda um código de 6 dígitos.\n\n{link}")
    resultados = [_mandar(s, a, "Contrato para assinar", msg, canal, usuario)
                  for canal in ("whatsapp", "email")]
    s.execute(text("""UPDATE contrato_assinaturas
                         SET token_resumo = :t, enviado_em = now(),
                             envios = envios || CAST(:e AS jsonb)
                       WHERE id = :i"""),
              {"t": _resumo(token), "i": a["id"],
               "e": json.dumps([{"quando": datetime.now(timezone.utc).isoformat(),
                                               "o_que": "link", **r} for r in resultados])})
    return {"papel": a["papel"], "nome": a["nome"],
            "saiu": [r["canal"] for r in resultados if r["ok"]],
            "falhou": [f"{r['canal']}: {r['detalhe']}" for r in resultados
                       if not r["ok"] and r["detalhe"] not in ("sem telefone", "sem e-mail")],
            "link": link}


def reenviar(s: Session, contrato_id: int, papel: str, base_url: str, usuario=None) -> dict[str, Any]:
    doc = documento_vivo(s, contrato_id)
    if doc is None or doc["situacao"] != "EM_ASSINATURA":
        raise ErroValidacao("Este contrato não está esperando assinatura.")
    alvo = [a for a in _assinaturas(s, doc["id"]) if a["papel"] == papel.upper()]
    if not alvo:
        raise ErroNaoEncontrado("Assinatura não encontrada.")
    return enviar_link(s, alvo[0]["id"], base_url, usuario)


# ---------------------------------------------------------------------------
# 6. A PÁGINA DE QUEM ASSINA — pelo token do link, sem login
# ---------------------------------------------------------------------------
def _pelo_token(s: Session, token: str) -> dict[str, Any]:
    if not disponivel(s) or not token or len(token) < 20:
        raise ErroNaoEncontrado("Link inválido ou vencido.")
    a = s.execute(text("""SELECT a.*, d.titulo, d.texto, d.lacre, d.situacao AS doc_situacao,
                                 d.contrato_id, d.assinado_anexo_id
                            FROM contrato_assinaturas a
                            JOIN contrato_documentos d ON d.id = a.documento_id
                           WHERE a.token_resumo = :t"""),
                  {"t": _resumo(token)}).mappings().first()
    if a is None or a["doc_situacao"] == "CANCELADO":
        raise ErroNaoEncontrado("Link inválido ou vencido. Peça um novo à obra.")
    return dict(a)


def _mascara(v: str, canal: str) -> str:
    if canal == "email":
        nome, _, dom = (v or "").partition("@")
        return (nome[:2] + "•••@" + dom) if dom else "•••"
    d = _digitos(v)
    return f"(••) •••••-{d[-4:]}" if len(d) >= 4 else "•••"


def abrir(s: Session, token: str) -> dict[str, Any]:
    a = _pelo_token(s, token)
    if a["aberto_em"] is None:
        s.execute(text("UPDATE contrato_assinaturas SET aberto_em = now() WHERE id = :i"),
                  {"i": a["id"]})
    outros = [{"papel": o["papel"], "nome": o["nome"], "assinou": o["situacao"] == "ASSINADO"}
              for o in _assinaturas(s, a["documento_id"]) if o["id"] != a["id"]]
    return {"titulo": a["titulo"], "texto": a["texto"], "lacre": a["lacre"],
            "papel": a["papel"], "nome": a["nome"],
            "documento_tipo": "CPF" if len(a["documento"]) == 11 else "CNPJ",
            "ja_assinou": a["situacao"] == "ASSINADO",
            "concluido": a["doc_situacao"] == "ASSINADO",
            "tem_pdf": bool(a["assinado_anexo_id"]),
            "canais": [{"canal": c, "destino": _mascara(a[campo], c)}
                       for c, campo in (("whatsapp", "telefone"), ("email", "email")) if a[campo]],
            "outros": outros}


def pedir_codigo(s: Session, token: str, canal: str) -> dict[str, Any]:
    a = _pelo_token(s, token)
    if a["situacao"] != "AGUARDANDO":
        raise ErroValidacao("Esta assinatura já foi feita.")
    canal = "email" if canal == "email" else "whatsapp"
    recentes = [e for e in (a["envios"] or [])
                if e.get("o_que") == "codigo" and
                e.get("quando", "") > (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()]
    if len(recentes) >= CODIGOS_POR_HORA:
        raise ErroValidacao("Muitos códigos pedidos na última hora. Espere um pouco e "
                            "tente de novo.")
    codigo = f"{secrets.randbelow(1_000_000):06d}"
    msg = (f"Seu código para assinar o contrato é {codigo}. Ele vale por "
           f"{CODIGO_VALE_MIN} minutos. Não passe este código a ninguém.")
    r = _mandar(s, a, "Código para assinar o contrato", msg, canal)
    if not r["ok"]:
        raise ErroValidacao("Não consegui enviar o código por "
                            f"{'e-mail' if canal == 'email' else 'WhatsApp'}. "
                            "Tente o outro canal, ou peça ajuda à obra.")
    s.execute(text("""UPDATE contrato_assinaturas
                         SET codigo_resumo = :c, codigo_expira_em = now() + make_interval(mins => :m),
                             codigo_canal = :k, tentativas = 0,
                             envios = envios || CAST(:e AS jsonb)
                       WHERE id = :i"""),
              {"c": _resumo(f"{a['id']}:{codigo}"), "m": CODIGO_VALE_MIN, "k": canal,
               "i": a["id"],
               "e": json.dumps([{"quando": datetime.now(timezone.utc).isoformat(),
                                               "o_que": "codigo", "canal": canal}])})
    return {"enviado_para": _mascara(a["telefone"] if canal == "whatsapp" else a["email"], canal)}


def _png(assinatura: Any) -> bytes:
    import base64
    bruto = str(assinatura or "")
    if "," in bruto:
        bruto = bruto.split(",", 1)[1]
    try:
        dados = base64.b64decode(bruto, validate=True)
    except Exception:
        raise ErroValidacao("A assinatura não chegou. Desenhe de novo.")
    if not dados.startswith(b"\x89PNG") or len(dados) > MAX_ASSINATURA:
        raise ErroValidacao("A assinatura não chegou. Desenhe de novo.")
    return dados


def assinar(s: Session, token: str, *, codigo: str, documento: str, assinatura: Any,
            lacre_visto: str, ip: str, navegador: str) -> dict[str, Any]:
    a = _pelo_token(s, token)
    if a["situacao"] != "AGUARDANDO":
        raise ErroValidacao("Esta assinatura já foi feita.")
    if lacre_visto != a["lacre"]:
        raise ErroValidacao("O texto do contrato mudou depois que você abriu. Recarregue "
                            "a página e leia de novo antes de assinar.")
    if _digitos(documento) != a["documento"]:
        raise ErroValidacao("O CPF/CNPJ digitado não confere com o do contrato.")
    if not a["codigo_resumo"] or a["codigo_expira_em"] is None \
            or a["codigo_expira_em"] < datetime.now(timezone.utc):
        raise ErroValidacao("Peça o código de confirmação (ele vale por 15 minutos).")
    if a["tentativas"] >= TENTATIVAS_POR_CODIGO:
        raise ErroValidacao("Código errado vezes demais. Peça um código novo.")
    if _resumo(f"{a['id']}:{_digitos(codigo)}") != a["codigo_resumo"]:
        s.execute(text("UPDATE contrato_assinaturas SET tentativas = tentativas + 1 WHERE id = :i"),
                  {"i": a["id"]})
        raise ErroValidacao("Código não confere.")
    png = _png(assinatura)
    s.execute(text("""UPDATE contrato_assinaturas
                         SET situacao = 'ASSINADO', assinado_em = now(), ip = :ip,
                             navegador = :nav, assinatura_png = :png,
                             lacre_conferido = :l, codigo_resumo = NULL
                       WHERE id = :i"""),
              {"ip": (ip or "")[:60], "nav": (navegador or "")[:300], "png": png,
               "l": a["lacre"], "i": a["id"]})
    registrar_evento(s, "contrato_servico", a["contrato_id"], "CONTRATO_ASSINADO_POR",
                     {"papel": a["papel"], "nome": a["nome"], "canal": a["codigo_canal"],
                      "ip": ip}, None)
    concluiu = False
    if all(x["situacao"] == "ASSINADO" for x in _assinaturas(s, a["documento_id"])):
        _concluir(s, a["documento_id"])
        concluiu = True
    return {"assinado": True, "concluido": concluiu}


# ---------------------------------------------------------------------------
# 7. CONCLUIR — o PDF final, com o registro das assinaturas
# ---------------------------------------------------------------------------
def documento_pdf(doc: dict[str, Any], assinaturas: list[dict[str, Any]]) -> bytes:
    """PURA (sem banco). O contrato e, no fim, o registro de cada assinatura."""
    from zoneinfo import ZoneInfo
    from fpdf import FPDF
    from app.apps.erp.core.termos import _latin
    pdf = FPDF(orientation="P", format="A4")
    pdf.set_auto_page_break(True, margin=14)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 12)
    pdf.multi_cell(0, 6.5, _latin(doc["titulo"]), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    for linha in doc["texto"].split("\n"):
        negrito = linha.startswith("CLÁUSULA")
        pdf.set_font("Helvetica", "B" if negrito else "", 9.5)
        pdf.multi_cell(0, 4.8, _latin(linha) or " ", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, _latin("Registro das assinaturas eletrônicas"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8.5)
    pdf.multi_cell(0, 4.2, _latin(
        f"Lacre do texto (SHA-256): {doc['lacre']}. Cada parte abaixo assinou o texto "
        "com este lacre, confirmou o próprio CPF/CNPJ e um código de uso único enviado "
        "ao telefone ou e-mail informado."), new_x="LMARGIN", new_y="NEXT")
    for a in assinaturas:
        pdf.ln(2)
        quando = a["assinado_em"].astimezone(ZoneInfo("America/Fortaleza")) \
            if a.get("assinado_em") else None
        pdf.set_font("Helvetica", "B", 9.5)
        pdf.cell(0, 5.5, _latin(f"{a['papel']}: {a['nome']} — {_doc(a['documento'])}"),
                 new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 8.5)
        pdf.multi_cell(0, 4.2, _latin(
            f"Assinado em {quando:%d/%m/%Y %H:%M:%S} (horário de Fortaleza)"
            f" · código por {'e-mail' if a.get('codigo_canal') == 'email' else 'WhatsApp'}"
            f" · IP {a.get('ip') or '—'}" if quando else "não assinado"),
            new_x="LMARGIN", new_y="NEXT")
        if a.get("assinatura_png"):
            y = pdf.get_y() + 1
            try:
                pdf.image(io.BytesIO(bytes(a["assinatura_png"])), x=pdf.l_margin, y=y, w=55)
                pdf.set_y(y + 22)
            except Exception:  # noqa: BLE001
                pass
    return bytes(pdf.output())


def _concluir(s: Session, documento_id: int) -> None:
    from app.apps.erp.core.documentos.armazenamento import salvar
    doc = dict(s.execute(text("SELECT * FROM contrato_documentos WHERE id = :d"),
                         {"d": documento_id}).mappings().first())
    pdf = documento_pdf(doc, _assinaturas(s, documento_id))
    numero = doc["titulo"].rsplit(" ", 1)[-1]
    anexo = salvar(s, pdf, f"contrato_{numero}_assinado.pdf", entidade_tipo="contrato_servico",
                   entidade_id=doc["contrato_id"], categoria="CONTRATO",
                   descricao="Contrato assinado eletronicamente pelas duas partes")
    s.execute(text("""UPDATE contrato_documentos SET situacao = 'ASSINADO', concluido_em = now(),
                             assinado_anexo_id = :a WHERE id = :d"""),
              {"a": anexo.id, "d": documento_id})
    s.execute(text("""UPDATE contratos_servico SET status = 'VIGENTE'
                       WHERE id = :c AND status = 'AGUARDANDO_ASSINATURA'"""),
              {"c": doc["contrato_id"]})
    registrar_evento(s, "contrato_servico", doc["contrato_id"], "CONTRATO_VIGENTE_ASSINADO",
                     {"documento_id": documento_id, "anexo_id": anexo.id}, None)


def pdf_assinado(s: Session, token: str) -> tuple[bytes, str]:
    from app.apps.erp.core.documentos.armazenamento import conteudo_de, obter
    a = _pelo_token(s, token)
    if not a["assinado_anexo_id"]:
        raise ErroNaoEncontrado("O contrato ainda não foi assinado pelas duas partes.")
    an = obter(s, a["assinado_anexo_id"])
    return conteudo_de(s, an), an.nome_arquivo


def situacao(s: Session, contrato_id: int) -> Optional[dict[str, Any]]:
    """Para as telas: em que pé está a assinatura deste contrato."""
    doc = documento_vivo(s, contrato_id)
    if doc is None:
        return None
    rot = {"RASCUNHO": "Contrato gerado — os links saem quando o contrato receber o aval",
           "EM_ASSINATURA": "Esperando as assinaturas",
           "ASSINADO": "Assinado pelas duas partes"}
    return {"documento_id": doc["id"], "situacao": doc["situacao"],
            "rotulo": rot.get(doc["situacao"], doc["situacao"]),
            "anexo_id": doc["assinado_anexo_id"],
            "assinaturas": [{"papel": a["papel"], "nome": a["nome"],
                             "assinou": a["situacao"] == "ASSINADO",
                             "assinado_em": a["assinado_em"].isoformat() if a["assinado_em"] else None,
                             "abriu": bool(a["aberto_em"]),
                             "enviado_em": a["enviado_em"].isoformat() if a["enviado_em"] else None}
                            for a in _assinaturas(s, doc["id"])]}
