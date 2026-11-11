# -*- coding: utf-8 -*-
"""Gera o DE-PARA do DP a partir da exportação dos dois pipes.

⚑ SÓ A ESTRUTURA. Nenhum nome de pessoa, e-mail, webhook ou token entra no
documento: o que o ERP precisa é o RÓTULO, o tipo, se é obrigatório e as
opções de cada caixinha.
"""
import json
from collections import OrderedDict

INTERNAL_TIPO = "322436706"          # "Tipo de Solicitação", no pipe de DP


def pipe(nome):
    return json.load(open(nome, encoding="utf-8"))["pipe"]


def linha(c, recuo=""):
    req = " ⭐" if c.get("required") else ""
    t = f"{recuo}- **{c['label']}**{req} — `{c['id']}` · {c.get('type')}"
    if c.get("options"):
        t += "\n" + recuo + "  - opções: " + " · ".join(
            f"`{o}`" for o in c["options"])
    for chave, rotulo in (("description", "ajuda"), ("help", "dica")):
        if c.get(chave):
            t += f"\n{recuo}  - {rotulo}: {c[chave]}"
    return t


def por_tipo(p):
    saida = OrderedDict()
    for cond in p.get("fieldConditions") or []:
        valores = [e["value"] for e in cond["condition"]["expressions"]
                   if e["field_address"] == INTERNAL_TIPO
                   and e["operation"] == "equals" and e["value"]]
        if not valores:
            continue
        mostra = [a["phaseField"] for a in cond["actions"]
                  if a["actionId"] == "show" and a.get("phaseField")]
        for v in valores:
            saida.setdefault(v, [])
            vistos = {x["label"] for x in saida[v]}
            for c in mostra:
                if c["label"] not in vistos:
                    saida[v].append(c)
                    vistos.add(c["label"])
    return saida


sol, cad = pipe("solicitacoes.json"), pipe("cadastro.json")
pt = por_tipo(sol)
condicionais = {c["label"] for l in pt.values() for c in l}
sempre = [c for c in sol["start_form_fields"]
          if c["label"] not in condicionais]

L = []
w = L.append
w("# DE-PARA DO DP — os campos como o Pipefy os tem")
w("")
w("> **Fonte:** exportação dos dois pipes pelo GraphQL do Pipefy, em")
w("> 04/11/2026 — `SOLICITAÇÕES DP` (id 301514316, 106 campos no formulário")
w("> inicial, 10 fases) e `CADASTRO COLABORADORES` (id 301487297, 51 campos,")
w("> 7 fases). Só a ESTRUTURA entra aqui: nenhum nome de pessoa, e-mail,")
w("> webhook ou token.")
w("")
w("## Por que este arquivo existe")
w("")
w("Porque eu inventei nome de campo, e ele cobrou — com razão — em")
w("10/11/2026:")
w("")
w("> *\"Prestador que passa a ter carteira. Qual foi o termo que eu usei? Por")
w("> que não está usando o termo que está no Pipe? Por que está querendo")
w("> inventar a roda de novo? Uma coisa que já está certa, já está")
w("> funcionando, já está consolidada. Para que inventar?\"*")
w("")
w("E, no mesmo dia:")
w("")
w("> *\"Pagar alimentação, aí segunda, quinta, esse termo está errado. Como é")
w("> que estava no Pipe? Para usar igual. É para usar a linguagem do Pipe, o")
w("> pessoal já está acostumado com o que está lá.\"*")
w("")
w("**A regra que nasce daqui:** rótulo de campo do DP não se traduz, não se")
w("melhora e não se inventa. Se está no Pipe, está aqui — e a tela usa este")
w("texto, letra por letra. Campo que o Pipe não tem, o ERP não inventa sem")
w("ele mandar.")
w("")
w("⭐ = obrigatório no Pipe. No ERP, campo obrigatório leva asterisco na tela,")
w("como lá — pedido dele no mesmo dia: *\"Pipe, todo campo obrigatório, ele")
w("tem um asterisco, facilita. Aqui não, ninguém sabe o que é obrigatório.\"*")
w("")
w("## ⚑ OS DOIS PIPES VIRAM UM SÓ")
w("")
w("Decisão dele em 10/11/2026: *\"nós temos dois pipes, um de cadastro para")
w("colaboradores e o outro de solicitações DP. Só que a gente está meio que")
w("unificando, né? Vai ser tudo DP, porque tudo é solicitação DP. O cadastro")
w("e as outras solicitações.\"*")
w("")
w("Ou seja: **o cadastro é mais um tipo de solicitação**, na mesma fila, com")
w("a mesma caixa de entrada e a mesma trilha. Não é uma tela à parte.")
w("")
w("## ⚑ QUEM PREENCHE O QUÊ — a leitura mais importante deste arquivo")
w("")
w("No Pipe, o que está no **formulário inicial** é da OBRA (quem pede); o que")
w("está numa **fase** é do **DP**. Essa separação é o que eu tinha quebrado:")
w("")
w("| Campo | Onde está no Pipe | Quem preenche |")
w("|---|---|---|")
w("| Matrícula | fase *Cadastro Inicial* | **DP** |")
w("| Convenção | fase *Cadastro Inicial* | **DP** |")
w("| Categoria e Valor do Auxílio Alimentação | fase *Cadastro Concluído* | **DP** |")
w("| Categoria e Valor do Auxílio Transporte | fase *Cadastro Concluído* | **DP** |")
w("| Forma de Desligamento | formulário inicial | obra |")
w("| Data do Aviso Prévio | formulário inicial | obra |")
w("")
w("E o que **não existe em pipe nenhum**, e portanto o ERP não pede a quem")
w("solicita: *último dia trabalhado*, *faltas a descontar*, *data de saída*")
w("(exceto no desligamento de PRESTADOR, onde ela existe), *conta no imposto")
w("de renda*, *tipo de contrato* (o nome certo é **Tipo de Readmissão** na")
w("readmissão, e **Tipo de Documentação para Cadastro** no cadastro).")
w("")
w("---")
w("")
w("# Pipe SOLICITAÇÕES DP")
w("")
w(f"## Os {len(pt)} tipos, e o que cada um mostra")
w("")
for v in sorted(pt):
    w(f"### {v}")
    w("")
    for c in pt[v]:
        w(linha(c))
    w("")
w("## Campos do formulário inicial que não dependem do tipo")
w("")
w("(muitos só aparecem por condição de OUTRO campo — por exemplo as chaves")
w("Pix, que aparecem conforme o tipo de chave escolhido.)")
w("")
for c in sempre:
    w(linha(c))
w("")
w("## As fases do pipe de solicitações")
w("")
for f in sol.get("phases") or []:
    cs = f.get("fields") or []
    w(f"### {f.get('name')} — {len(cs)} campo(s)")
    w("")
    for c in cs:
        w(linha(c))
    w("")
w("---")
w("")
w("# Pipe CADASTRO COLABORADORES")
w("")
w("## Formulário inicial — o que a OBRA preenche")
w("")
for c in cad.get("start_form_fields") or []:
    w(linha(c))
w("")
w("## As fases — o que o DP preenche")
w("")
for f in cad.get("phases") or []:
    cs = f.get("fields") or []
    w(f"### {f.get('name')} — {len(cs)} campo(s)")
    w("")
    for c in cs:
        w(linha(c))
    w("")
open("/home/user/aplicacoes/app/apps/erp/DE_PARA_PIPEFY_DP.md", "w",
     encoding="utf-8").write("\n".join(L) + "\n")
print("linhas:", len(L))
