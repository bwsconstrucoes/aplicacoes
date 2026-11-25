# -*- coding: utf-8 -*-
"""SEMEIA UM PORTAL DA OBRA CHEIO, num banco de teste, para fotografar as
telas do manual (`app/apps/erp/manual_portal/`).

⚑ POR QUE EXISTE (25/11/2026): o manual do portal tem 17 telas. Sem este
arquivo, atualizar uma delas custa meia hora remontando dado de exemplo à
mão — e é esse custo que faz manual envelhecer.

⚠️ RECUSA QUALQUER BANCO QUE NÃO TENHA "teste" NO NOME. Ele cria pedidos,
pendências e um desligamento; rodar isso na produção seria estrago de
verdade. Mesma trava do `tests/conftest.py`.

TODO NOME E TODO CPF AQUI SÃO INVENTADOS. Nada de pessoa de verdade entra
no repositório — os CPFs são válidos no dígito só para as máscaras da tela
não reclamarem.

COMO RODAR:

    DATABASE_URL=postgresql://postgres@localhost:5433/erp_teste \
    PYTHONPATH=. python3 app/apps/erp/scripts/semear_manual_portal.py

Depois: suba o ERP apontando para o mesmo banco, entre como
`marta@exemplo.com` e fotografe. A senha é a que este arquivo grava.
"""
import os
import sys

_url = os.environ.get("DATABASE_URL", "")
if "teste" not in _url.lower():
    sys.exit("RECUSADO: a DATABASE_URL precisa ser de um banco com 'teste' no "
             "nome. Este script escreve dado de exemplo e não pode encostar "
             "na produção.")

from datetime import date, timedelta
from sqlalchemy import text
from app.apps.erp.db.database import get_session
from app.apps.erp.core.auth.service import gerar_hash

HOJE = date.today()
with get_session() as s:
    x = lambda q, **p: s.execute(text(q), p)

    # --- a obra
    x("""INSERT INTO obras (codigo, nome, status, endereco)
         VALUES ('112', 'Residencial Vale Verde', 'ATIVA', 'Av. das Palmeiras, 1200 — Natal/RN')
         ON CONFLICT (codigo) DO UPDATE SET nome = EXCLUDED.nome, status = 'ATIVA'""")
    obra = x("SELECT id FROM obras WHERE codigo = '112'").scalar()

    # --- cargos
    for nome in ("Servente", "Carpinteiro", "Encarregado de obra", "Auxiliar administrativo"):
        x("INSERT INTO cargos (nome) SELECT :n WHERE NOT EXISTS (SELECT 1 FROM cargos WHERE nome = :n)", n=nome)
    cargo = {r[0]: r[1] for r in x("SELECT nome, id FROM cargos").all()}

    # --- a administrativa da obra: usuária do ERP e colaboradora
    x("""INSERT INTO colaboradores (nome, cpf, obra_id, situacao, vinculo, admissao, cargo_id, telefone)
         VALUES ('Marta Ribeiro', '52998224725', :o, 'ATIVO', 'CTPS', '2025-03-03', :g, '84988880001')
         ON CONFLICT (cpf) DO UPDATE SET obra_id = :o, nome = 'Marta Ribeiro'""",
      o=obra, g=cargo["Auxiliar administrativo"])
    x("""INSERT INTO usuarios (nome, email, senha_hash, perfil, ativo, cpf, escopo_visao)
         VALUES ('Marta Ribeiro', 'marta@exemplo.com', :h, 'ADMINISTRATIVO_OBRA', true,
                 '52998224725', 'OBRAS_DESIGNADAS')
         ON CONFLICT (email) DO UPDATE SET senha_hash = :h, ativo = true,
             perfil = 'ADMINISTRATIVO_OBRA', cpf = '52998224725',
             escopo_visao = 'OBRAS_DESIGNADAS'""", h=gerar_hash("OlhadaNaTela1!"))
    marta_u = x("SELECT id FROM usuarios WHERE email = 'marta@exemplo.com'").scalar()
    marta_c = x("SELECT id FROM colaboradores WHERE cpf = '52998224725'").scalar()
    x("""INSERT INTO usuario_obras (usuario_id, obra_id) VALUES (:u, :o)
         ON CONFLICT (usuario_id, obra_id) DO NOTHING""", u=marta_u, o=obra)

    # --- a equipe da obra
    equipe = [
        # 84 dias de casa com experiência de 45+45: a prorrogação acaba em 6
        # dias, dentro da janela de aviso (10 dias). É esta linha que faz
        # aparecer "Contratos de experiência terminando" na tela.
        ("José Nascimento",  "15350946056", "CTPS",      "Carpinteiro",        HOJE - timedelta(days=84),  HOJE + timedelta(days=6)),
        ("Rita Gomes",       "28625802023", "CTPS",      "Servente",           HOJE - timedelta(days=400), None),
        ("Davi Monteiro",    "70784221060", "CTPS",      "Encarregado de obra",HOJE - timedelta(days=900), None),
        ("Sueli Albuquerque","96087096020", "CTPS",      "Servente",           HOJE - timedelta(days=500), None),
        ("Nelson Teixeira",  "41526340088", "PRESTADOR", "Carpinteiro",        HOJE - timedelta(days=200), None),
    ]
    ids = {}
    for nome, cpf, vinc, cg, adm, fim_exp in equipe:
        x("""INSERT INTO colaboradores (nome, cpf, obra_id, situacao, vinculo, admissao,
                                        cargo_id, contrato_experiencia)
             VALUES (:n, :c, :o, 'ATIVO', :v, :a, :g, :e)
             ON CONFLICT (cpf) DO UPDATE SET obra_id = :o, nome = :n, vinculo = :v,
                 admissao = :a, cargo_id = :g, contrato_experiencia = :e, situacao = 'ATIVO'""",
          n=nome, c=cpf, o=obra, v=vinc, a=adm, g=cargo[cg],
          e=("45+45" if fim_exp else ""))
        ids[nome] = x("SELECT id FROM colaboradores WHERE cpf = :c", c=cpf).scalar()

    # --- férias: uma chegando, uma em curso
    x("DELETE FROM ferias_programadas WHERE colaborador_id = ANY(:ids)", ids=list(ids.values()))
    x("""INSERT INTO ferias_programadas (colaborador_id, obra_id, inicio, dias, fim, situacao)
         VALUES (:c, :o, :i, 30, :f, 'CONFIRMADA')""",
      c=ids["Rita Gomes"], o=obra, i=HOJE + timedelta(days=9), f=HOJE + timedelta(days=38))
    x("""INSERT INTO ferias_programadas (colaborador_id, obra_id, inicio, dias, fim, situacao)
         VALUES (:c, :o, :i, 20, :f, 'CONFIRMADA')""",
      c=ids["Sueli Albuquerque"], o=obra, i=HOJE - timedelta(days=5), f=HOJE + timedelta(days=14))

    # --- pedidos já abertos pela Marta, em situações diferentes
    x("DELETE FROM dp_solicitacoes WHERE solicitante_id = :u", u=marta_u)
    pedidos = [
        ("SP-2026-0418", "CADASTRO",        "ABERTA",     None,
         "Lucas Figueiredo", "33614355080", 'Admissão para servente — documentos anexados.'),
        ("SP-2026-0411", "FERIAS",          "EM_ANALISE", ids["Rita Gomes"], None, None, None),
        ("SP-2026-0402", "AJUDA_DE_CUSTO",  "ATENDIDA",   ids["Davi Monteiro"], None, None,
         "Liberado. Entra na folha deste mês."),
        ("SP-2026-0396", "DESLIGAMENTO",    "ATENDIDA",   ids["Nelson Teixeira"], None, None,
         "Rescisão feita. O aviso prévio foi para a obra assinar."),
    ]
    for p in pedidos:
        numero, tipo, situ, colab, nome_inf, cpf_inf, resp = (p + (None,) * 7)[:7]
        x("""INSERT INTO dp_solicitacoes (numero, tipo, situacao, colaborador_id,
                 nome_informado, cpf_informado, obra_id, solicitante_id,
                 solicitante_colaborador_id, dados, resposta, criado_em)
             VALUES (:num, :t, :s, :c, :ni, :ci, :o, :u, :uc, '{}'::jsonb, :r,
                     now() - (random()*interval '12 days'))""",
          num=numero, t=tipo, s=situ, c=colab, ni=nome_inf, ci=cpf_inf, o=obra,
          u=marta_u, uc=marta_c, r=resp)

    # --- documentos a devolver
    x("DELETE FROM dp_devolucoes WHERE obra_id = :o", o=obra)
    x("""INSERT INTO dp_devolucoes (numero, colaborador_id, obra_id, o_que,
             documento_esperado, prazo, situacao)
         VALUES ('DV-2026-0074', :c, :o,
                 'Rescisão do Nelson — colher a assinatura e devolver', 'RESCISAO-ASSINADA', :p, 'ABERTA')""",
      c=ids["Nelson Teixeira"], o=obra, p=HOJE + timedelta(days=2))
    x("""INSERT INTO dp_devolucoes (numero, colaborador_id, obra_id, o_que,
             documento_esperado, prazo, situacao, cobrancas)
         VALUES ('DV-2026-0068', :c, :o,
                 'ASO de retorno da Sueli', 'ASO', :p, 'COBRADA', 1)""",
      c=ids["Sueli Albuquerque"], o=obra, p=HOJE - timedelta(days=3))

    # --- um documento para o colaborador assinar
    from app.apps.erp.core import termos
    x("DELETE FROM dp_termos WHERE obra_id = :o", o=obra)
    texto = ("AVISO PRÉVIO DE DESLIGAMENTO\n\n"
             "Comunicamos a rescisão do seu contrato de trabalho, sem justa causa, "
             "com aviso prévio indenizado de 30 dias, a contar desta data.\n\n"
             "A empresa pagará as verbas rescisórias no prazo legal.")
    x("""INSERT INTO dp_termos (numero, colaborador_id, obra_id, tipo, titulo,
             texto, lacre, situacao, prazo)
         VALUES ('TM-2026-0031', :c, :o, 'AVISO_PREVIO', 'Aviso prévio de desligamento',
                 :tx, :lac, 'ABERTO', :p)""",
      c=ids["Nelson Teixeira"], o=obra, tx=texto, p=HOJE + timedelta(days=2),
      lac=termos.lacre("Aviso prévio de desligamento", texto, "41526340088"))
    s.commit()
    print("obra", obra, "| usuária marta@exemplo.com (senha OlhadaNaTela1!)",
          "| equipe", len(ids))
