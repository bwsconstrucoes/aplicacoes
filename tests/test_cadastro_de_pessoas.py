# -*- coding: utf-8 -*-
"""O CADASTRO COMPLETO DA PESSOA (migrações 085 a 087, 05/11/2026).

O cadastro saiu do Pipefy e veio para o ERP. Estes testes defendem as três
decisões do dono e as duas peças que o Pipefy não conseguia guardar:

  · *"colaborador e prestador tudo é no mesmo local"* — um cadastro, com
    `vinculo` como campo;
  · *"o RP também tem que alertar (…) se a pessoa está começando a digitar o
    cadastro, esse cadastro aqui já existe"* — e, se for desligado, oferecer
    readmissão;
  · *"a gente tem a base dos desligados, para a gente não refazer um cadastro
    que já existe"* — readmitir sem apagar a passagem anterior;
  · dependentes sem o teto de 5 filhos do pipe;
  · *"as alterações salariais pra serem gravadas"*.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from app.apps.erp.core.cadastros import pessoas as svc
from app.apps.erp.core.comum.auditoria import ErroValidacao


# ---------------------------------------------------------------------------
# SEM BANCO: o PIS e o "o que falta"
# ---------------------------------------------------------------------------
def _pis(base="1205641254"):
    pesos = (3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    r = sum(int(base[i]) * pesos[i] for i in range(10)) % 11
    return base + str(0 if r < 2 else 11 - r)


def test_o_pis_e_conferido_pelo_digito():
    """PIS errado só aparece na hora de transmitir a folha, semanas depois."""
    bom = _pis()
    assert svc.pis_valido(bom)
    assert svc.pis_valido(f"{bom[:3]}.{bom[3:8]}.{bom[8:10]}-{bom[10]}")   # com máscara
    assert not svc.pis_valido(bom[:10] + str((int(bom[10]) + 1) % 10))
    assert not svc.pis_valido("11111111111")
    assert not svc.pis_valido("123")
    assert not svc.pis_valido("")


def test_o_que_falta_nao_cobra_PIS_de_prestador():
    """Exigir PIS e CTPS de prestador foi o que, no pipe, levou a gente a
    preencher o campo com "0" para o formulário deixar salvar."""
    from app.apps.erp.db.models.cadastros import Colaborador
    ctps = Colaborador(nome="Alguém", cpf="1", vinculo="CTPS")
    prest = Colaborador(nome="Alguém", cpf="2", vinculo="PRESTADOR")

    assert "nº do PIS" in svc.o_que_falta(ctps)
    assert "nº do PIS" not in svc.o_que_falta(prest)
    assert "nº da CTPS" not in svc.o_que_falta(prest)
    # o que vale para os dois continua cobrado
    assert "cargo" in svc.o_que_falta(prest) and "obra" in svc.o_que_falta(prest)


def test_as_listas_de_opcoes_saem_prontas_para_a_tela():
    o = svc.opcoes()
    assert {x["valor"] for x in o["vinculos"]} == {"CTPS", "PRESTADOR"}
    assert len(o["tipos_contrato"]) == 9           # as 9 do pipe
    assert len(o["categorias_transporte"]) == 9    # as 9 do pipe, com "Cartão"
    assert any(x["valor"] == "CARTAO" and "não paga" in x["rotulo"]
               for x in o["categorias_transporte"])
    assert len(o["ufs"]) == 27


# ===========================================================================
# COM BANCO
# ===========================================================================
pytestmark_banco = pytest.mark.banco
banco_ = pytest.mark.banco

CPF_A = "52998224725"          # CPFs válidos de teste
CPF_B = "11144477735"


@pytest.fixture
def dono(sessao_real):
    from app.apps.erp.core.auth.service import gerar_hash
    from app.apps.erp.db.models.cadastros import PerfilUsuario, Usuario
    u = Usuario(nome="DP de teste", email="dp@cadastro.teste", ativo=True,
                senha_hash=gerar_hash("senha-de-teste-123"),
                perfil=PerfilUsuario.DEPARTAMENTO_PESSOAL, telefone="5585900000000")
    sessao_real.add(u)
    sessao_real.flush()
    return u


@banco_
def test_o_cadastro_grava_os_campos_que_a_folha_le(sessao_real, dono):
    """As colunas vêm do mapa das 74 que o Make escrevia na aba do Pipefy."""
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.db.models.cadastros import Obra
    cargo = svc_cargos.achar_ou_criar_cargo(sessao_real, "PEDREIRO", cbo="7152-10")
    obra = Obra(codigo="CAD-01", nome="Obra do cadastro", status="ATIVA")
    sessao_real.add(obra)
    sessao_real.flush()
    c = svc.salvar(sessao_real, {
        "cargo_id": cargo.id, "obra_id": obra.id,
        "cpf": CPF_A, "nome": "José da Silva Teste",
        "data_nascimento": "1990-05-17", "sexo": "M", "nome_mae": "Maria da Silva",
        "nome_pai": "João da Silva", "estado_civil": "CASADO",
        "escolaridade": "MEDIO", "naturalidade": "Fortaleza", "naturalidade_uf": "CE",
        "rg": "2001123456", "rg_orgao": "SSP", "rg_uf": "CE", "rg_emissao": "2010-03-01",
        "pis": _pis(), "ctps_numero": "1234567", "ctps_serie": "0001-CE",
        "ctps_uf": "CE", "ctps_digital": "sim", "primeiro_emprego": "não",
        "cep": "60000-000", "logradouro": "Rua A", "numero_endereco": "100",
        "bairro": "Centro", "municipio": "Fortaleza", "uf": "CE",
        "email": "jose@exemplo.com", "telefone": "85999990000",
        "vinculo": "CTPS", "tipo_contrato": "CLT_INDETERMINADO",
        "contrato_experiencia": "45+45 dias", "admissao": "2026-01-05",
        "salario": "2.230,00", "mensalista": "sim",
        "categoria_alimentacao": "SEG_SEX", "aux_alimentacao": "25,00",
        "categoria_transporte": "CARTAO", "meio_transporte": "COLETIVO",
        "solicitar_salario_familia": "sim",
    }, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert f["nome"] == "José da Silva Teste" and f["cpf"] == CPF_A
    assert f["data_nascimento"] == "1990-05-17" and f["sexo"] == "M"
    assert f["escolaridade"] == "MEDIO" and f["estado_civil"] == "CASADO"
    assert f["salario"] == 2230.0                      # "2.230,00" entendido
    assert f["categoria_transporte"] == "CARTAO"       # a categoria, não só o valor
    assert f["ctps_digital"] is True and f["primeiro_emprego"] is False
    assert f["falta"] == []                            # o cadastro está completo


@banco_
def test_pis_invalido_e_recusado_com_recado(sessao_real, dono):
    with pytest.raises(ErroValidacao) as e:
        svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Alguém de Teste",
                                 "pis": "12345678901"}, dono)
    assert "PIS" in str(e.value)


@banco_
def test_valor_de_lista_inventado_e_recusado(sessao_real, dono):
    """Lista fechada existe para a folha não receber "Ensino Medio" e
    "ENSINO MÉDIO" como duas escolaridades."""
    with pytest.raises(ErroValidacao):
        svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Alguém de Teste",
                                 "escolaridade": "FACULDADE"}, dono)


@banco_
def test_o_primeiro_vinculo_nasce_com_o_cadastro(sessao_real, dono):
    """Sem ele, a primeira readmissão não teria o que fechar."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05", "vinculo": "CTPS"}, dono)
    sessao_real.flush()
    f = svc.ficha(sessao_real, c.id)
    assert len(f["vinculos"]) == 1
    assert f["vinculos"][0]["aberto"] is True and f["vinculos"][0]["admissao"] == "2026-01-05"


# ---------------------------------------------------------------------------
# O ALERTA DE DUPLICADO — *"o RP também tem que alertar"*
# ---------------------------------------------------------------------------
@banco_
def test_cpf_novo_esta_livre(sessao_real):
    assert svc.conferir_cpf(sessao_real, CPF_A)["situacao"] == "livre"


@banco_
def test_cpf_incompleto_nao_responde_nada_ainda(sessao_real):
    """A tela chama a cada tecla: responder "livre" com 5 dígitos seria mentir."""
    assert svc.conferir_cpf(sessao_real, "529")["situacao"] == "incompleto"


@banco_
def test_cpf_que_nao_passa_na_conta_avisa_antes_de_tudo(sessao_real):
    r = svc.conferir_cpf(sessao_real, "11111111111")
    assert r["situacao"] == "invalido" and "dígitos" in r["aviso"]


@banco_
def test_pessoa_ativa_manda_abrir_a_ficha_em_vez_de_cadastrar(sessao_real, dono):
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Já Cadastrado",
                             "admissao": "2026-01-05"}, dono)
    sessao_real.flush()

    r = svc.conferir_cpf(sessao_real, CPF_A)
    assert r["situacao"] == "ativo" and r["nome"] == "José Já Cadastrado"
    assert "ATIVO" in r["aviso"] and "duas vezes" in r["aviso"]


@banco_
def test_pessoa_desligada_oferece_READMISSAO(sessao_real, dono):
    """É o caso que o dono descreveu: *"a gente só procura a pessoa e seleciona,
    essa pessoa sai, mas ele está sendo readmitido"*."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Que Saiu",
                                 "admissao": "2024-01-05"}, dono)
    sessao_real.flush()
    svc.desligar(sessao_real, c.id, {"data_saida": "2025-06-30",
                                     "forma_desligamento": "PEDIDO_COLABORADOR"}, dono)
    sessao_real.flush()

    r = svc.conferir_cpf(sessao_real, CPF_A)
    assert r["situacao"] == "desligado"
    assert "readmissão" in r["aviso"] and "30/06/2025" in r["aviso"]


# ---------------------------------------------------------------------------
# READMISSÃO — sem apagar o passado
# ---------------------------------------------------------------------------
@banco_
def test_readmitir_abre_vinculo_novo_e_guarda_o_anterior(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Readmitido",
                                 "admissao": "2024-01-05", "salario": "1800"}, dono)
    sessao_real.flush()
    svc.desligar(sessao_real, c.id, {"data_saida": "2025-06-30",
                                     "forma_desligamento": "AVISO_INDENIZADO"}, dono)
    sessao_real.flush()
    svc.readmitir(sessao_real, c.id, {"admissao": "2026-02-01", "salario": "2230"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert f["situacao"] == "ATIVO" and f["demissao"] is None
    assert len(f["vinculos"]) == 2
    novo, antigo = f["vinculos"]
    assert novo["admissao"] == "2026-02-01" and novo["aberto"] and novo["readmissao"]
    assert antigo["demissao"] == "2025-06-30" and antigo["forma_rotulo"].startswith("Demissão")
    assert antigo["salario"] == 1800.0        # o salário de então não foi reescrito
    assert f["salario"] == 2230.0


@banco_
def test_readmitir_sem_desligar_antes_e_recusado(sessao_real, dono):
    """Dois contratos abertos fariam a folha pagar os dois."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Ativo",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao) as e:
        svc.readmitir(sessao_real, c.id, {"admissao": "2026-03-01"}, dono)
    assert "vínculo aberto" in str(e.value)


@banco_
def test_readmissao_antes_da_saida_anterior_e_recusada(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2024-01-05"}, dono)
    sessao_real.flush()
    svc.desligar(sessao_real, c.id, {"data_saida": "2025-06-30"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.readmitir(sessao_real, c.id, {"admissao": "2025-01-01"}, dono)


# ---------------------------------------------------------------------------
# PRESTADOR → CTPS: *"quer admitir na carteira (…) e tornar ele CTPS"*
# ---------------------------------------------------------------------------
@banco_
def test_prestador_que_vira_CTPS_mantem_o_periodo_de_prestador(sessao_real, dono):
    """O período de prestador tem de continuar legível: é o que o INSS e a
    Justiça do Trabalho perguntam."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Prestador",
                                 "vinculo": "PRESTADOR", "admissao": "2025-03-01",
                                 "salario": "1500"}, dono)
    sessao_real.flush()
    svc.trocar_vinculo(sessao_real, c.id, {
        "vinculo": "CTPS", "tipo_contrato": "CLT_INDETERMINADO",
        "admissao": "2026-02-01", "salario": "2230"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert f["vinculo"] == "CTPS" and f["tipo_contrato"] == "CLT_INDETERMINADO"
    assert len(f["vinculos"]) == 2
    novo, antigo = f["vinculos"]
    assert novo["vinculo"] == "CTPS" and novo["aberto"]
    # o vínculo antigo fecha no dia ANTERIOR: sem buraco e sem sobreposição
    assert antigo["vinculo"] == "PRESTADOR" and antigo["demissao"] == "2026-01-31"
    assert "carteira" in antigo["motivo"].lower()


@banco_
def test_trocar_para_o_mesmo_vinculo_e_recusado(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José CLT",
                                 "vinculo": "CTPS", "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao) as e:
        svc.trocar_vinculo(sessao_real, c.id, {"vinculo": "CTPS",
                                               "admissao": "2026-03-01"}, dono)
    assert "já está" in str(e.value)


# ---------------------------------------------------------------------------
# DEPENDENTES — sem o teto de 5 do pipe
# ---------------------------------------------------------------------------
@banco_
def test_dependentes_passam_de_cinco(sessao_real, dono):
    """O pipe tinha ONZE campos fixos e parava em "Filho 5"."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Com Filhos",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    for i in range(1, 8):
        svc.salvar_dependente(sessao_real, c.id, {
            "nome": f"Filho {i} de Teste", "parentesco": "FILHO",
            "data_nascimento": f"201{i}-01-01", "salario_familia": "sim"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert len(f["dependentes"]) == 7
    assert all(d["salario_familia"] for d in f["dependentes"])


@banco_
def test_dependente_que_nao_e_filho_nem_conjuge_cabe(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    svc.salvar_dependente(sessao_real, c.id, {"nome": "Maria Mãe de Teste",
                                              "parentesco": "MAE"}, dono)
    sessao_real.flush()
    f = svc.ficha(sessao_real, c.id)
    assert f["dependentes"][0]["parentesco_rotulo"] == "Mãe"


@banco_
def test_salario_familia_e_dependente_de_IR_sao_marcas_diferentes(sessao_real, dono):
    """Guardar uma só obrigaria a folha a adivinhar a outra."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    svc.salvar_dependente(sessao_real, c.id, {
        "nome": "Filho Grande de Teste", "parentesco": "FILHO",
        "salario_familia": "não", "dependente_ir": "sim"}, dono)
    sessao_real.flush()
    d = svc.ficha(sessao_real, c.id)["dependentes"][0]
    assert d["salario_familia"] is False and d["dependente_ir"] is True


@banco_
def test_dependente_com_cpf_repetido_na_mesma_pessoa_e_recusado(sessao_real, dono):
    from sqlalchemy.exc import IntegrityError
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    svc.salvar_dependente(sessao_real, c.id, {"nome": "Filho Um de Teste",
                                              "parentesco": "FILHO", "cpf": CPF_B}, dono)
    sessao_real.flush()
    # O erro vem do banco, no flush que a própria função faz: é o índice único
    # que impede o mesmo dependente entrar duas vezes e a folha contar dobrado.
    with pytest.raises(IntegrityError):
        svc.salvar_dependente(sessao_real, c.id, {"nome": "Filho Dois de Teste",
                                                  "parentesco": "FILHO", "cpf": CPF_B}, dono)
    sessao_real.rollback()


# ---------------------------------------------------------------------------
# ALTERAÇÃO SALARIAL — *"pra serem gravadas"*
# ---------------------------------------------------------------------------
@banco_
def test_a_alteracao_salarial_fica_gravada_com_motivo(sessao_real, dono):
    """No Pipefy o pedido existia e o resultado não ficava em lugar nenhum: o
    card fechava e o campo do cadastro era sobrescrito."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José Promovido",
                                 "admissao": "2026-01-05", "salario": "1800"}, dono)
    sessao_real.flush()
    svc.registrar_alteracao(sessao_real, c.id, {
        "salario": "2230", "vigencia": "2026-06-01", "motivo": "MERITO",
        "justificativa": "assumiu a frente de serviço"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert f["salario"] == 2230.0
    a = f["alteracoes"][0]
    assert (a["tipo"], a["salario_de"], a["salario_para"]) == ("SALARIO", 1800.0, 2230.0)
    assert a["motivo_rotulo"] == "Mérito" and "frente de serviço" in a["justificativa"]


@banco_
def test_alteracao_sem_dizer_o_que_muda_e_recusada(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.registrar_alteracao(sessao_real, c.id, {"motivo": "MERITO"}, dono)


@banco_
def test_a_alteracao_de_cargo_e_salario_junta_e_um_tipo_proprio(sessao_real, dono):
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05", "salario": "1800"}, dono)
    cargo = svc_cargos.achar_ou_criar_cargo(sessao_real, "ENCARREGADO DE OBRAS")
    sessao_real.flush()
    svc.registrar_alteracao(sessao_real, c.id, {
        "cargo_id": cargo.id, "salario": "2620", "motivo": "PROMOCAO"}, dono)
    sessao_real.flush()

    a = svc.ficha(sessao_real, c.id)["alteracoes"][0]
    assert a["tipo"] == "CARGO_E_SALARIO"
    assert a["cargo_para"] == "ENCARREGADO DE OBRAS" and a["salario_para"] == 2620.0


# ---------------------------------------------------------------------------
# DESLIGAMENTO
# ---------------------------------------------------------------------------
@banco_
def test_desligar_fecha_o_vinculo_e_guarda_a_forma(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2024-01-05"}, dono)
    sessao_real.flush()
    svc.desligar(sessao_real, c.id, {
        "data_saida": "2026-03-31", "aviso_previo": "2026-03-01",
        "ultimo_dia": "2026-03-30", "forma_desligamento": "AVISO_TRABALHADO",
        "motivo": "fim da obra"}, dono)
    sessao_real.flush()

    f = svc.ficha(sessao_real, c.id)
    assert f["situacao"] == "DESLIGADO" and f["demissao"] == "2026-03-31"
    assert f["aviso_previo"] == "2026-03-01" and f["ultimo_dia"] == "2026-03-30"
    v = f["vinculos"][0]
    assert v["aberto"] is False and v["forma_rotulo"] == "Demissão com aviso trabalhado"


@banco_
def test_desligar_sem_data_e_recusado(sessao_real, dono):
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2026-01-05"}, dono)
    sessao_real.flush()
    with pytest.raises(ErroValidacao):
        svc.desligar(sessao_real, c.id, {}, dono)


@banco_
def test_demissao_lancada_desliga_mesmo_sem_trocar_a_caixinha(sessao_real, dono):
    """A situação segue a data, e não o contrário."""
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "José de Teste",
                                 "admissao": "2024-01-05", "demissao": "2025-01-05",
                                 "situacao": "ATIVO"}, dono)
    sessao_real.flush()
    assert c.situacao == "DESLIGADO"


# ---------------------------------------------------------------------------
# OS PAPÉIS DA ADMISSÃO (05/11/2026)
# ---------------------------------------------------------------------------
@banco_
def test_a_ficha_diz_quais_papeis_faltam_no_arquivo(sessao_real, dono):
    """O pipe exigia seis anexos para admitir, e o anexo era campo obrigatório
    do formulário. No ERP o cadastro salva sem os papéis — senão ninguém
    consegue começar — e a ficha diz, em letras, o que falta. Pedido do dono em
    05/11/2026: *"todo documento, anexo, pdf, imagem (…) salvar no Google
    Drive"* — é o Arquivo quem guarda, e é dele que esta lista lê."""
    from app.apps.erp.core.cadastros import pessoas as svc
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Quem Falta Papel"}, dono)
    falta = svc.documentos_que_faltam(sessao_real, c.id, "CTPS")
    assert "comprovante de endereço" in falta
    assert "foto do colaborador" in falta
    assert "carteira de trabalho" in falta


@banco_
def test_prestador_nao_e_cobrado_de_ctps_nem_no_papel(sessao_real, dono):
    """A mesma regra dos campos: exigir CTPS de prestador foi o que levou a
    gente a preencher campo com "0" no pipe."""
    from app.apps.erp.core.cadastros import pessoas as svc
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Prestador De Servico",
                                 "vinculo": "PRESTADOR"}, dono)
    falta = svc.documentos_que_faltam(sessao_real, c.id, "PRESTADOR")
    assert "carteira de trabalho" not in falta
    assert "comprovante de endereço" in falta


@banco_
def test_papel_arquivado_sai_da_lista_e_papel_excluido_volta_para_ela(
        sessao_real, dono):
    """O Arquivo APAGA o registro ao excluir (não desativa), então o papel
    excluído volta a ser cobrado — que é o certo: se foi excluído por estar
    errado, tem de ser pedido de novo."""
    from app.apps.erp.core.arquivo import catalogo
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.financeiro import Anexo, Documento
    catalogo.aplicar(sessao_real)
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Com Papel No Drive"}, dono)
    a = Anexo(entidade_tipo="colaborador", entidade_id=c.id,
              nome_arquivo="foto.jpg", guardado_em="DRIVE", drive_file_id="x",
              mime_type="image/jpeg", hash_sha256="a" * 64, tamanho_bytes=10)
    sessao_real.add(a)
    sessao_real.flush()
    d = Documento(tipo_codigo="FOTO-COLABORADOR", anexo_id=a.id,
                  nome_padronizado="foto do colaborador", colaborador_id=c.id)
    sessao_real.add(d)
    sessao_real.flush()
    assert "foto do colaborador" not in svc.documentos_que_faltam(sessao_real, c.id)
    sessao_real.delete(d)
    sessao_real.flush()
    assert "foto do colaborador" in svc.documentos_que_faltam(sessao_real, c.id)


# ---------------------------------------------------------------------------
# A LEITURA PARA A FOLHA (05/11/2026) — o ERP como fonte
#
# Dele: *"o analisesps na parte de Folha de PGT, ele fará a leitura das
# informações e ponto a partir do ERP pra poder montar os dados de pgt."*
# Os nomes dos campos são de propósito os mesmos da tabela
# `analisesps.colaborador`, para trocar a ORIGEM sem reescrever a folha.
# ---------------------------------------------------------------------------
CAMPOS_QUE_A_FOLHA_LE = (
    "cpf", "nome", "matricula", "celular", "cargo", "tipo_contrato", "fase",
    "convencao", "obra_codigo", "obra_cadastro", "valor_alimentacao",
    "modo_alimentacao", "valor_transporte", "modo_transporte",
    "valor_gratificacao", "parcela_unica", "aviso_previo", "ultimo_dia",
    "data_saida", "observacao_auxilio", "data_inicio", "data_admissao",
    "data_nascimento", "card_pipefy",
)


@banco_
def test_a_leitura_para_a_folha_traz_todos_os_campos_que_ela_le(sessao_real, dono):
    """TRAVA: campo que a folha lê e esta função não manda apareceria como
    vazio na folha — e folha com auxílio vazio é alguém sem receber."""
    from app.apps.erp.core.cadastros import cargos as svc_cargos
    from app.apps.erp.core.cadastros import pessoas as svc
    from app.apps.erp.db.models.cadastros import Obra
    obra = Obra(codigo="FOLHA-01", nome="Obra da folha", status="ATIVA")
    sessao_real.add(obra)
    cargo = svc_cargos.achar_ou_criar_cargo(sessao_real, "PEDREIRO")
    conv = svc_cargos.achar_ou_criar_convencao(sessao_real, "CE-Fortaleza")
    sessao_real.flush()
    svc.salvar(sessao_real, {
        "cpf": CPF_A, "nome": "Quem Entra Na Folha", "matricula": "4455",
        "telefone": "85999990000", "cargo_id": cargo.id, "convencao_id": conv.id,
        "obra_id": obra.id, "tipo_contrato": "CLT_INDETERMINADO",
        "salario": "2.100,00", "aux_alimentacao": "300,00",
        "categoria_alimentacao": "SEG_SEX", "aux_transporte": "180,00",
        "categoria_transporte": "CARTAO", "mensalista": True,
        "admissao": "2026-03-01"}, dono)
    linhas = svc.para_folha(sessao_real)
    eu = next(x for x in linhas if x["cpf"] == CPF_A)
    for campo in CAMPOS_QUE_A_FOLHA_LE:
        assert campo in eu, f"a folha lê “{campo}” e a leitura não manda"
    assert eu["cargo"] == "PEDREIRO"
    assert eu["obra_codigo"] == "FOLHA-01"
    assert eu["tipo_contrato"] == "CLT (tempo indeterminado)"
    assert eu["modo_alimentacao"] == "Segunda à sexta"
    assert eu["valor_transporte"] == 180.0
    assert eu["pagamento"] == "MENSALISTA"
    assert eu["fase"] == "ATIVO" and eu["ativo"] is True


@banco_
def test_a_palavra_cartao_chega_inteira_na_folha(sessao_real, dono):
    """"Cartão" no transporte quer dizer que NÃO se paga em dinheiro. Foi a
    origem de pagamento em dobro — a folha precisa da palavra, não do código."""
    from app.apps.erp.core.cadastros import pessoas as svc
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Recebe No Cartao",
                             "categoria_transporte": "CARTAO"}, dono)
    eu = next(x for x in svc.para_folha(sessao_real) if x["cpf"] == CPF_A)
    assert "não paga em dinheiro" in eu["modo_transporte"]


@banco_
def test_desligado_fica_fora_da_folha_por_padrao(sessao_real, dono):
    """Quem saiu não entra em pagamento novo."""
    from app.apps.erp.core.cadastros import pessoas as svc
    svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Esta Trabalhando"}, dono)
    svc.salvar(sessao_real, {"cpf": CPF_B, "nome": "Saiu Em Outubro",
                             "demissao": "2026-10-31"}, dono)
    ativos = {x["cpf"] for x in svc.para_folha(sessao_real)}
    assert CPF_A in ativos and CPF_B not in ativos
    todos = {x["cpf"] for x in svc.para_folha(sessao_real, so_ativos=False)}
    assert CPF_B in todos


@banco_
def test_a_folha_conta_os_dependentes_de_salario_familia(sessao_real, dono):
    """Salário-família e dependente de IR são marcas DIFERENTES: a folha pede a
    primeira, e contar as duas juntas pagaria a mais."""
    from app.apps.erp.core.cadastros import pessoas as svc
    c = svc.salvar(sessao_real, {"cpf": CPF_A, "nome": "Tem Dois Filhos"}, dono)
    svc.salvar_dependente(sessao_real, c.id, {
        "nome": "Filho Um", "parentesco": "FILHO", "salario_familia": True}, dono)
    svc.salvar_dependente(sessao_real, c.id, {
        "nome": "Filho Dois", "parentesco": "FILHO", "dependente_ir": True}, dono)
    eu = next(x for x in svc.para_folha(sessao_real) if x["cpf"] == CPF_A)
    assert eu["salario_familia"] == 1
