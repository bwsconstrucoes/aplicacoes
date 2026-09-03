# -*- coding: utf-8 -*-
"""
gerar_semente.py — Cria seed_spsbd.csv a partir dos dados reais colados de SPsBD.

Use somente para popular o app na fase offline. Quando o conector do Google Sheets
estiver ligado, o seed é substituído pela carga real (ver gsheets.bootstrap).
"""
import csv
from datetime import datetime
from schema import ALL_KEYS

HOJE = "18/06/2026"

# Linhas reais (recorte de SPsBD enviado). conta/Conta Pagamento preenchida para
# demonstrar os KPIs por conta; nos dados reais virá da própria planilha.
ROWS = [
    dict(id="1384831053", solicitacao="18/06/2026", vencimento="24/06/2026",
         credor="MARCILIO LUIZ BARBOSA", documento="20.553.825/0001-40",
         descricao="Pagamento referente a serviço de instalação dos aparelhos de ar condicionados para creche olinda. Nf: 11",
         valor="6.750,00", centro_custo="CREPEOLINDA",
         tipo_despesa="Serviços de Terceiros (Pessoa Física ou Jurídica)",
         forma_pagamento="Pix", responsavel="PEDRO JOSE BENTO FERREIRA",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384831053",
         anexo_link="https://www.dropbox.com/scl/fi/aezvwlehvryqf3c8y6cmn/AnexoSP1384831053.pdf?dl=1",
         card_link="https://app.pipefy.com/open-cards/1384831053",
         info_pgt="Chave Pix: 064.523.894-59", nf="11", agendado="",
         pedido="55163", anuente="NILTON GOMES DA SILVA", status_anuencia="SIM",
         validacao="Sim", conta="50302-9",
         analise_ia="Não foram identificados lançamentos anteriores com o mesmo CNPJ, nº da Nota Fiscal 11 ou valores similares para a data informada. SEM RISCO"),

    dict(id="1384837877", solicitacao="18/06/2026", vencimento="24/06/2026",
         credor="MARCILIO LUIZ BARBOSA", documento="20.553.825/0001-40",
         descricao="Pagamento referente a serviço de instalação dos aparelhos de ar condicionados para ESC BOA VISTA. Nf: 10",
         valor="6.850,00", centro_custo="ESCBOAVISTA",
         tipo_despesa="Serviços de Terceiros (Pessoa Física ou Jurídica)",
         forma_pagamento="Pix", responsavel="PEDRO JOSE BENTO FERREIRA",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384837877",
         anexo_link="https://www.dropbox.com/scl/fi/h0fs0c7iwi2pwa85ft1qu/AnexoSP1384837877.pdf?dl=1",
         card_link="https://app.pipefy.com/open-cards/1384837877",
         info_pgt="Chave Pix: 064.523.894-59", nf="10", agendado="",
         pedido="55164", anuente="NILTON GOMES DA SILVA", status_anuencia="SIM",
         validacao="Sim", conta="50302-9",
         analise_ia="Fortes indícios de duplicidade (mesmo CPF/CNPJ e serviço) com ID 1384831053. COM RISCO"),

    dict(id="1384844943", solicitacao="18/06/2026", vencimento="10/06/2026",
         credor="T S ROCHA JUAZEIRO DO NORTE ALUGUEL DE EQUIPAMENTOS LTDA",
         documento="34.708.010/0006-02",
         descricao="Referente a renovação de locação mensal de andaimes para a obra SerraTalhada2 (contrato 506).",
         valor="1.008,00", centro_custo="CREPESERRATALHADA2",
         tipo_despesa="Locação de Equipamentos", forma_pagamento="Boleto",
         responsavel="ITALO GLEYDSON DE ARAUJO NOGUEIRA",
         dt_autorizacao="18/06/2026 11:59", resp_autorizacao="MARCELO LEITÃO",
         status_aut="Autorizado", status_pgt="Pago",
         codigo_integracao="Int1384844943",
         anexo_link="https://www.dropbox.com/scl/fi/jnn38eqfnmpaq0cm1xftm/AnexoSP1384844943.pdf?dl=1",
         card_link="https://app.pipefy.com/open-cards/1384844943",
         data_pagamento="18/06/2026", info_pgt="Boleto", nf="", agendado="Agendado",
         pedido="55165", validacao="Sim",
         codigo_barras="00190000090379018200400000164178914730000100800",
         id_contrato="1285178499", conta="50302-9",
         analise_ia="Mesmo valor, credor e descrição semelhante ao ID 1357812183. COM RISCO"),

    dict(id="1384852359", solicitacao="18/06/2026", vencimento="25/06/2026",
         credor="LOCADORA MACEDO", documento="04.891.279/0001-00",
         descricao="REFERENTE A LOCAÇÃO DE TRANSPORTE UTILIZADO POR FRANCENILDO EM OBRAS. COM FATURA DE NUMERO 2569",
         valor="3.000,00",
         centro_custo="CREPESERRATALHADA1, CREPESERRATALHADA2, CREPETRIUNFO, CREPEMIRANDIBA, CREPETUPARETAMA",
         tipo_despesa="Despesas com Transporte", forma_pagamento="Pix",
         responsavel="ADRIANA PATRICIA MENEZES NOVAES",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384852359",
         anexo_link="https://www.dropbox.com/scl/fi/eipl2i3pb8hyiftxowopc/AnexoSP1384852359.pdf?dl=1",
         card_link="https://app.pipefy.com/open-cards/1384852359",
         info_pgt="Chave Pix: 04.891.279/0001-00", nf="2569", agendado="",
         pedido="55166", validacao="Sim", conta="50302-9",
         analise_ia="Mesmo CNPJ e valor 3.000,00 do ID 1357164469. COM RISCO"),

    dict(id="1384859630", solicitacao="18/06/2026", vencimento="19/06/2026",
         credor="ANTHONNY LAURENTINO TEIXEIRA", documento="090.121.014-50",
         descricao="", valor="1.782,45", centro_custo="CRECHESUAPE",
         tipo_despesa="Fundo Fixo", forma_pagamento="Pix",
         responsavel="ANTHONNY LAURENTINO TEIXEIRA",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384859630",
         card_link="https://app.pipefy.com/open-cards/1384859630",
         info_pgt="Chave Pix: ba4b6f8f-1af6-4e39-a514-3fd71da3b8fe",
         pedido="55167", validacao="Sim", conta="50024-0",
         analise_ia="Indícios com o lançamento 1372594442 (mesmo CPF, CC semelhante, data próxima, valor <5%). COM RISCO"),

    dict(id="1384864652", solicitacao="18/06/2026", vencimento="22/06/2026",
         credor="LEANDRO DYOGO AMARAL VERAS", documento="703.528.054-55",
         descricao="", valor="1.118,10", centro_custo="CREPEOLINDA",
         tipo_despesa="Fundo Fixo", forma_pagamento="Pix",
         responsavel="LEANDRO DYOGO AMAFAL",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384864652",
         card_link="https://app.pipefy.com/open-cards/1384864652",
         info_pgt="Chave Pix: DYOGOOLEANDRO@GMAIL.COM",
         pedido="55168", validacao="Sim", conta="50024-0",
         analise_ia="Lançamentos 1354248791 e 1372660284 próximos em data/valor, mesmo credor e CC. COM RISCO"),

    dict(id="1384867350", solicitacao="18/06/2026", vencimento="18/06/2026",
         credor="JOAO PAULO CHAGAS DE FARIAS", documento="10217106447",
         descricao=("Conta Origem: 50024-0\n\nDespesa com Colaborador - Rescisões e Indenizações Trabalhistas\n"
                    "Valor Total: R$ 4.926,04\n\nDescrição original:\nTRCT - JOAO PAULO CHAGAS DE FARIAS.\n\n"
                    "Resumo do colaborador:\nCPF: 102.171.064-47\nCargo: ELETRICISTA\n"
                    "Código da Obra: CREPESERRATALHADA1"),
         valor="4.926,04", centro_custo="CONS",
         tipo_despesa="Rescisões e Indenizações Trabalhistas", forma_pagamento="Pix",
         responsavel="LUELIA MADIDA GOMES TOMAS",
         dt_autorizacao="18/06/2026", resp_autorizacao="PRÉ-AUTORIZADO",
         status_aut="Pré-Autorizado", status_pgt="Pagar",
         codigo_integracao="Int1384867350",
         card_link="https://app.pipefy.com/pipes/301426645#cards/1384867350",
         info_pgt="Chave Pix: Atualizar Chave Pix", parcela="001/001",
         pedido="55169", validacao="Sim", conta="50024-0",
         analise_ia="Não foram encontrados lançamentos semelhantes por CPF, data, valor ou descrição. SEM RISCO"),
]


def main():
    for r in ROWS:
        for k in ALL_KEYS:
            r.setdefault(k, "")
        r["carimbo"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("seed_spsbd.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ALL_KEYS)
        w.writeheader()
        for r in ROWS:
            w.writerow({k: r.get(k, "") for k in ALL_KEYS})
    print(f"seed_spsbd.csv gerado com {len(ROWS)} linhas e {len(ALL_KEYS)} colunas.")


if __name__ == "__main__":
    main()
